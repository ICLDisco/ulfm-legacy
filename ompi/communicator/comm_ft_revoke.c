/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2011-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 *
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"

#include "ompi/runtime/params.h"
#include "ompi/group/group.h"
#include "ompi/communicator/communicator.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/bml/bml.h"
#include "ompi/mca/bml/base/base.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/mca/coll/base/coll_tags.h"


typedef struct ompi_revoke_message_t {
    uint32_t   cid;
    uint32_t   epoch;
    int32_t    leader;
    int8_t     round;
} ompi_revoke_message_t;

static int ompi_comm_revoke_bml_send_msg(
        ompi_proc_t* proc,
        ompi_revoke_message_t* msg);

static void ompi_revoke_bml_send_complete_cb(
        struct mca_btl_base_module_t* module,
        struct mca_btl_base_endpoint_t* endpoint,
        struct mca_btl_base_descriptor_t* descriptor,
        int status);

static void ompi_comm_revoke_bml_recv_cb(
        struct mca_btl_base_module_t* btl,
        mca_btl_base_tag_t tag,
        mca_btl_base_descriptor_t* descriptor,
        void* cbdata);

static int ompi_comm_revoke_local(ompi_communicator_t* comm);

static int ompi_comm_revoke_rbcast_null(ompi_communicator_t* comm, ompi_revoke_message_t* msg) { return OMPI_SUCCESS; }
static int ompi_comm_revoke_rbcast_bmg(ompi_communicator_t* comm, ompi_revoke_message_t* msg);
static int ompi_comm_revoke_rbcast_n2(ompi_communicator_t* comm, ompi_revoke_message_t* msg);
static int ompi_comm_revoke_rbcast_ringleader(ompi_communicator_t* comm, ompi_revoke_message_t* msg);
static int ompi_comm_revoke_fw_ringleader(ompi_communicator_t* comm, ompi_revoke_message_t* msg);

static int (*ompi_comm_revoke_rbcast)(ompi_communicator_t* comm, ompi_revoke_message_t* msg) = ompi_comm_revoke_rbcast_null;
static int (*ompi_comm_revoke_fw)    (ompi_communicator_t* comm, ompi_revoke_message_t* msg) = ompi_comm_revoke_rbcast_null;

static bool comm_revoke_listener_started = false;

int ompi_comm_init_revoke(void) {
    int ret, id, rbcast;

    if( comm_revoke_listener_started ) {
        return OMPI_SUCCESS;
    }

    id = mca_base_param_register_int("ompi", "ft", "revoke_bcast", NULL, 1);
    mca_base_param_lookup_int(id, &rbcast);
    switch( rbcast ) {
        case 0:
            return OMPI_SUCCESS;
        case 1:
            ompi_comm_revoke_rbcast = ompi_comm_revoke_rbcast_bmg;
            ompi_comm_revoke_fw     = ompi_comm_revoke_rbcast_bmg;
            break;
        case 2:
            ompi_comm_revoke_rbcast = ompi_comm_revoke_rbcast_n2;
            ompi_comm_revoke_fw     = ompi_comm_revoke_rbcast_n2;
            break;
        case 3:
            ompi_comm_revoke_rbcast = ompi_comm_revoke_rbcast_ringleader;
            ompi_comm_revoke_fw     = ompi_comm_revoke_fw_ringleader;
            break;
        default:
            return OMPI_ERR_BAD_PARAM;
    }
    ret = mca_bml.bml_register(MCA_BTL_TAG_FT_REVOKE, ompi_comm_revoke_bml_recv_cb, NULL);
    if( OMPI_SUCCESS == ret ) {
        comm_revoke_listener_started = true;
    }
    return ret;
}

int ompi_comm_finalize_revoke(void) {
    if( !comm_revoke_listener_started ) {
        return OMPI_SUCCESS;
    }
    comm_revoke_listener_started = false;

    return OMPI_SUCCESS;
}

/** MPI_Comm_revoke(comm)
 * uplevel call from the API to initiate a revoke
 */
int ompi_comm_revoke_internal(ompi_communicator_t* comm)
{
    ompi_revoke_message_t msg;
    int ret;

    if( comm->comm_revoked ) return OMPI_SUCCESS;

    /* Broadcast the 'revoke' signal to all other processes. */
    msg.cid = comm->c_contextid;
    msg.epoch = comm->c_epoch;
    OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                         "%s %s: Initiate a revoke on communicator %3d:%d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __func__, comm->c_contextid, comm->c_epoch ));
    ret = ompi_comm_revoke_rbcast(comm, &msg);
    /* Mark locally revoked */
    ompi_comm_revoke_local(comm);
    return ret;
}


/* internal code to revoke the communicator structure. Can be called from the
 * API or from receiving a revoke message */
static int ompi_comm_revoke_local(ompi_communicator_t* comm) {
    if( comm->comm_revoked ) {
        OPAL_OUTPUT_VERBOSE((9, ompi_ftmpi_output_handle,
                "%s %s: comm %3d:%d is already revoked, nothing to do",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __func__, comm->c_contextid, comm->c_epoch));
        return true;
    }
    /*
     * Locally revoke the communicator
     *
     * Just to be pedantic, as the 'revoke' condition is checked first
     * by all other communication,
     * - Turn off collectives
     * - Turn off ANY_SOURCE receives
     */
    comm->comm_revoked              = true;
    comm->collectives_force_error   = true;
    comm->any_source_enabled        = false;
    /* purge the communicator unexpected fragments and matching logic */
    MCA_PML_CALL(revoke_comm(comm));
    /* Signal the point-to-point stack to recheck requests */
    OPAL_THREAD_LOCK(&ompi_request_lock);
    opal_condition_signal(&ompi_request_cond);
    OPAL_THREAD_UNLOCK(&ompi_request_lock);
    return true;
}


/* Broadcast a Revoke token to the appropriate neighbors in a BMG */
static int ompi_comm_revoke_rbcast_bmg(ompi_communicator_t* comm, ompi_revoke_message_t* msg) {
    int me, np, ret = OMPI_SUCCESS;
    int i,d;
    ompi_group_t* lgrp, * hgrp;

    if( comm->comm_revoked ) return OMPI_SUCCESS;

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s %s: Ask others to revoke communicator %3d:%d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __func__, msg->cid, msg->epoch ));

    if( OMPI_COMM_IS_INTER(comm) ) {
        int first = ompi_comm_determine_first_auto(comm);
        np = ompi_comm_size(comm) + ompi_comm_remote_size(comm);
        lgrp = first? comm->c_local_group: comm->c_remote_group;
        hgrp = first? comm->c_remote_group: comm->c_local_group;
        me = first? ompi_comm_rank(comm): ompi_comm_rank(comm)+ompi_comm_remote_size(comm);
    }
    else {
        np = ompi_comm_size(comm);
        lgrp = comm->c_local_group;
        me = ompi_comm_rank(comm);
    }

    /* d is the direction, forward (1*2^i), then backward (-1*2^i) */
    for(d=1; d >= -1; d-=2) for(i=1; i < np; i*=2) {
        ompi_proc_t* proc;
        int idx = (np+me+d*i)%np;
        if(OPAL_LIKELY( idx < ompi_group_size(lgrp) )) {
            proc = ompi_group_peer_lookup(lgrp, idx);
        }
        else {
            proc = ompi_group_peer_lookup(hgrp, idx-ompi_group_size(lgrp));
        }
        ret = ompi_comm_revoke_bml_send_msg(proc, msg);
        if(OPAL_UNLIKELY( OMPI_SUCCESS != ret )) {
            if(OPAL_UNLIKELY( OMPI_ERR_UNREACH != ret )) {
                return ret;
            }
        }
    }
    ompi_comm_revoke_local(comm);
    return OMPI_SUCCESS;
}

/* N^2 */
static int ompi_comm_revoke_rbcast_n2(ompi_communicator_t* comm, ompi_revoke_message_t* msg) {
    int i, ret;
    ompi_group_t* grp;

    /* send a message to all procs in local_group, then all procs in
     * remote_group (if distinct) */
    for(grp = comm->c_local_group;
        grp == comm->c_local_group && grp != comm->c_remote_group;
        grp = comm->c_remote_group)
      for(i = 0; i < ompi_group_size(grp); i++) {
        ompi_proc_t* proc;
        mca_bml_base_btl_t *bml_btl;
        mca_btl_base_descriptor_t *des;
        ompi_revoke_message_t* msgdes;

        proc = ompi_group_peer_lookup(grp, i);
        if( ompi_proc_local_proc == proc ) continue;
        ret = ompi_comm_revoke_bml_send_msg(proc, msg);
        if(OPAL_UNLIKELY( OMPI_SUCCESS != ret )) {
            if(OPAL_UNLIKELY( OMPI_ERR_UNREACH != ret )) {
                return ret;
            }
        }
    }
    ompi_comm_revoke_local(comm);
    return OMPI_SUCCESS;
}


/** RINGLEADER */

static int ompi_comm_revoke_rbcast_ringleader(ompi_communicator_t* comm, ompi_revoke_message_t* msg) {
    ompi_group_t* grp;
    int leader;
    int pred;
    int ret, exit_status = OMPI_SUCCESS;
    ompi_proc_t* proc;

    if( OMPI_COMM_IS_INTER(comm) ) {
        ompi_group_union(comm->c_local_group, comm->c_remote_group, &grp);
    }
    else {
        grp = comm->c_local_group;
        OBJ_RETAIN(grp);
    }

    leader = ompi_group_rank(grp);
    pred = (leader-1) % ompi_group_size(grp);

    msg->leader = leader;
    msg->round = 1;
retry_send:
    proc = ompi_group_peer_lookup(grp, pred);
    ret = ompi_comm_revoke_bml_send_msg(proc, msg);
    if(OPAL_UNLIKELY( OMPI_SUCCESS != ret )) {
        if(OPAL_UNLIKELY( OMPI_ERR_UNREACH != ret )) {
            exit_status = ret;
            goto release_and_return;
        }
        pred = (pred-1) % ompi_group_size(grp);
        if( leader != pred ) goto retry_send;
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                "%s %s: all other ranks are dead, the revoke of %3d is complete",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __func__, msg->cid ));
    }
 release_and_return:
    OBJ_RELEASE(grp);
    return exit_status;
}

static int ompi_comm_revoke_fw_ringleader(ompi_communicator_t* comm, ompi_revoke_message_t* msg) {
    int me, exit_status = OMPI_SUCCESS;
    ompi_group_t* grp;

    if( OMPI_COMM_IS_INTER(comm) ) {
        ompi_group_union(comm->c_local_group, comm->c_remote_group, &grp);
    }
    else {
        grp = comm->c_local_group;
        OBJ_RETAIN(grp);
    }

    me = ompi_group_rank(grp);
    if( msg->round == 2 ) {
        /* a token has made a full round, great, I don't need to act as a leader */
        comm->comm_revoked = 2;
        if( msg->leader == me ) return OMPI_SUCCESS; /* no further propagation */
    }
    else if( msg->leader == me ) {
        /* This token has made its first round successfully,
        initiate second round to release would-be leaders */
        assert( msg->round == 1 );
        msg->round++;
    }

    /* Ok, if I reach here, I need to try a forward */
    if( msg->leader <= me ) {
        int pred = (me-1) % ompi_group_size(grp);
        int ret;
        ompi_proc_t* proc;

retry_send:
        proc = ompi_group_peer_lookup(grp, pred);
        ret = ompi_comm_revoke_bml_send_msg(proc, msg);
        if(OPAL_UNLIKELY( OMPI_SUCCESS != ret )) {
            if(OPAL_UNLIKELY( OMPI_ERR_UNREACH != ret )) {
                exit_status = ret;
                goto release_and_return;
            }
            if( (msg->leader == pred) && (msg->round == 1) ) {
                /* Leader is dead, but the round 1 is complete,
                 initiate round 2 instead of the dead leader */
                msg->round++;
            }
            else if( (msg->leader == pred) && (msg->round == 2) ) {
                /* Leader is dead, but the round 2 is complete,
                  the revoke is complete */
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                    "%s %s: the leader %d is dead, but the second round is complete, so revoke %3d is done",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __func__, msg->leader, msg->cid ));
                /* mark completed*/
                comm->comm_revoked = 2;
                goto release_and_return;
            }
            pred = (pred-1) % ompi_group_size(grp);
            goto retry_send;
        }
    }
 release_and_return:
    OBJ_RELEASE(grp);
    return OMPI_SUCCESS;
}

/* BML HELPERS */

static void ompi_comm_revoke_bml_recv_cb(
        struct mca_btl_base_module_t* btl,
        mca_btl_base_tag_t tag,
        mca_btl_base_descriptor_t* descriptor,
        void* cbdata)
{
    ompi_revoke_message_t* msg;
    ompi_communicator_t* comm;

    /* Parse the revoke fragment */
    assert( MCA_BTL_TAG_FT_REVOKE == tag );
    assert( 1 == descriptor->des_dst_cnt );
    assert( sizeof(ompi_revoke_message_t) == descriptor->des_dst->seg_len );
    msg = (ompi_revoke_message_t*) descriptor->des_dst->seg_addr.pval;

    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s %s: Info: Recieved revoke tag for communicator with CID %3d:%d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __func__, msg->cid, msg->epoch));

    /* Find the target comm */
    comm = (ompi_communicator_t *)ompi_comm_lookup(msg->cid);
    if(OPAL_UNLIKELY( NULL == comm )) {
        OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle,
                             "%s %s: Info: Could not find the communicator with CID %3d:%d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __func__, msg->cid, msg->epoch));
        return;
    }
    if(OPAL_UNLIKELY( msg->cid != comm->c_contextid )) {
        OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle,
                             "%s %s: Info: received a late revoke message with CID %3d:%d during an MPI_COMM_DUP that is trying to reuse that CID (thus increasing the epoch) - ignoring, nothing to do",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __func__, msg->cid, msg->epoch));
        return;
    }
    /* Check if this is a delayed revoke for an old communicator whose CID has been reused */
    if(OPAL_UNLIKELY( comm->c_epoch != msg->epoch )) {
        OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle,
                             "%s %s: Info: Received a late revoke order for the communicator with CID %3d:%d when is is now at epoch %d - ignoring, nothing to do",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __func__, msg->cid, msg->epoch, comm->c_epoch));
        return;
    }

    /* forward the revoke */
    ompi_comm_revoke_fw(comm, msg);
    /* revoke the local comm */
    ompi_comm_revoke_local(comm);
}

static int ompi_comm_revoke_bml_send_msg(ompi_proc_t* proc, ompi_revoke_message_t* msg) {
    mca_bml_base_btl_t *bml_btl;
    mca_btl_base_descriptor_t *des;
    ompi_revoke_message_t* msgdes;
    int ret;

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
        "%s %s: preparing a fragment to %s to revoke %3d:%d",
        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __func__, ORTE_NAME_PRINT(&proc->proc_name), msg->cid, msg->epoch));
    assert( proc->proc_bml );
    bml_btl = mca_bml_base_btl_array_get_next(&proc->proc_bml->btl_send);
    mca_bml_base_alloc(bml_btl, &des, MCA_BTL_NO_ORDER,
                       sizeof(ompi_revoke_message_t),
                       MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
    if(OPAL_UNLIKELY(NULL == des)) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }
    assert( des->des_src->seg_len == sizeof(ompi_revoke_message_t) ) ;
    des->des_cbfunc = ompi_revoke_bml_send_complete_cb;
    msgdes = des->des_src->seg_addr.pval;
    *msgdes = *msg;
    ret = mca_bml_base_send(bml_btl, des, MCA_BTL_TAG_FT_REVOKE);
    if(OPAL_LIKELY( ret >= 0 )) {
        if(OPAL_LIKELY( 1 == ret )) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                "%s %s: fragment to %s to revoke %3d:%d is on the wire",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __func__, ORTE_NAME_PRINT(&proc->proc_name), msg->cid, msg->epoch));
        }
    }
    else {
        mca_bml_base_free(bml_btl, des);
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
            "%s %s: could not send a fragment to %s to revoke %3d (status %d)",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __func__, ORTE_NAME_PRINT(&proc->proc_name), msg->cid, ret));
            return ret;
    }
    return OMPI_SUCCESS;
}

static void ompi_revoke_bml_send_complete_cb(
        struct mca_btl_base_module_t* module,
        struct mca_btl_base_endpoint_t* endpoint,
        struct mca_btl_base_descriptor_t* descriptor,
        int status)
{
    OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle,
        "%s %s: status %d", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), __func__, status));
    return;
}

