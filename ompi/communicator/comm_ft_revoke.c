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



static bool comm_revoke_listener_started = false;
static int comm_revoke_rbcast = 1;

static void ompi_comm_revoke_bml_cb_fn(
        struct mca_btl_base_module_t* btl,
        mca_btl_base_tag_t tag,
        mca_btl_base_descriptor_t* descriptor,
        void* cbdata);

typedef struct ompi_revoke_message_t {
    uint32_t   cid;
    uint32_t   epoch;
    int32_t    leader;
    int8_t     round;
} ompi_revoke_message_t;


static int ompi_comm_revoke_internal_local(ompi_revoke_message_t* msg);

static int ompi_comm_revoke_internal_rbcast_bmg(ompi_revoke_message_t* msg);
static int ompi_comm_revoke_internal_rbcast_n2(ompi_revoke_message_t* msg);
static int ompi_comm_revoke_internal_rbcast_ringleader(ompi_revoke_message_t* msg);
#define ompi_comm_revoke_internal_rbcast(msg) ( \
    comm_revoke_rbcast == 0 ? OMPI_SUCCESS : \
    comm_revoke_rbcast == 1 ? ompi_comm_revoke_internal_rbcast_bmg(msg) : \
    comm_revoke_rbcast == 2 ? ompi_comm_revoke_internal_rbcast_n2(msg) : \
                              ompi_comm_revoke_internal_rbcast_ringleader(msg) )

static int ompi_comm_revoke_internal_fw_ringleader(ompi_revoke_message_t* msg);
static int ompi_comm_revoke_internal_fw_n2(ompi_revoke_message_t* msg);
#define ompi_comm_revoke_internal_fw(msg) ( \
    comm_revoke_rbcast == 0 ? OMPI_SUCCESS : \
    comm_revoke_rbcast == 1 ? ompi_comm_revoke_internal_fw_n2(msg) : /*this is not a mistake, BMG uses the n2 fw */ \
    comm_revoke_rbcast == 2 ? ompi_comm_revoke_internal_fw_n2(msg) : \
                              ompi_comm_revoke_internal_fw_ringleader(msg) )





int ompi_comm_init_revoke(void)
{
    int ret;

    if( comm_revoke_listener_started ) {
        return OMPI_SUCCESS;
    }

    {
    int rbcast = 1;
    int id = mca_base_param_register_int("ompi", "ft", "revoke_bcast", NULL, 1);
    mca_base_param_lookup_int(id, &rbcast);
    comm_revoke_rbcast = rbcast; }

    ret = mca_bml.bml_register(MCA_BTL_TAG_FT_REVOKE, ompi_comm_revoke_bml_cb_fn, NULL);
    if( OMPI_SUCCESS != ret ) {
        return ret;
    }

    comm_revoke_listener_started = true;

    return OMPI_SUCCESS;
}

int ompi_comm_finalize_revoke(void)
{
    if( !comm_revoke_listener_started ) {
        return OMPI_SUCCESS;
    }
    comm_revoke_listener_started = false;

    return OMPI_SUCCESS;
}



static void ompi_comm_revoke_bml_cb_fn(
        struct mca_btl_base_module_t* btl,
        mca_btl_base_tag_t tag,
        mca_btl_base_descriptor_t* descriptor,
        void* cbdata)
{
    ompi_revoke_message_t* msg;
    
    /* Parse the revoke fragment */
    assert(MCA_BTL_TAG_FT_REVOKE == tag);
    assert(1 == descriptor->des_dst_cnt);
    assert(sizeof(ompi_revoke_message_t) == descriptor->des_dst->seg_len);
    msg = (ompi_revoke_message_t*) descriptor->des_dst->seg_addr.pval;
    
    /* forward the revoke */
    ompi_comm_revoke_internal_fw(msg);
    /* revoke the local comm */
    ompi_comm_revoke_internal_local(msg);
    
    return;    
}

static void ompi_revoke_bml_complete_fn(
        struct mca_btl_base_module_t* module,
        struct mca_btl_base_endpoint_t* endpoint,
        struct mca_btl_base_descriptor_t* descriptor,
        int status)
{
    OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle, 
        "%s ompi: comm_revoke: Send complete CB: status %d", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), status));
    return;
}


static int ompi_comm_revoke_internal_local(ompi_revoke_message_t* msg) {
    /* Find the communicator */
    ompi_communicator_t* comm = (ompi_communicator_t *)ompi_comm_lookup(msg->cid);
    if( NULL == comm ) {
        OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle, 
                             "%s ompi: comm_revoke: Info: Could not find the communicator with CID %3d:%d - has it been freed?",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), msg->cid, msg->epoch ));
        return false;
    }
    if( msg->cid != comm->c_contextid ) {
        OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle,
                             "%s ompi: comm_revoke: Info: received a stall revoke message with CID %3d:%d while a COMM_DUP is ongoing and trying to reuse that CID",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), msg->cid, msg->epoch ));
        return false;
    }
    /* Check if this is a delayed revoke for an old communicator whose CID has been reused */
    if( comm->c_epoch != msg->epoch ) {
        OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle, 
                             "%s ompi: comm_revoke: Info: Received a late revoke order for the communicator with CID %3d:%d when is is now at epoch %d - ignoring, nothing to do",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), msg->cid, msg->epoch, comm->c_epoch ));
        return false;
    }
    /* Ignore duplicates */
    if( comm->comm_revoked ) {
        OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle,
                             "%s ompi: comm_revoke: Info: Received a duplicate revoke order for communicator with CID %3d:%d - Already revoked, nothing to do",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), msg->cid, msg->epoch ));
         return false;
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
    /* Signal the point-to-point stack to recheck requests */
    OPAL_THREAD_LOCK(&ompi_request_lock);
    opal_condition_signal(&ompi_request_cond);
    OPAL_THREAD_UNLOCK(&ompi_request_lock);
    return true;
}

static ompi_group_t* ompi_comm_revoke_internal_fw_grp(ompi_revoke_message_t* msg) {
    ompi_group_t* grp;
    ompi_communicator_t* comm = (ompi_communicator_t *)ompi_comm_lookup(msg->cid);

    if(NULL == comm || comm->c_contextid != msg->cid ||
       comm->c_epoch != msg->epoch || comm->comm_revoked) {
        return NULL;
    }
    if(OMPI_COMM_IS_INTER(comm)) {
        ompi_group_union(comm->c_local_group, comm->c_remote_group, &grp);
    }
    else {
        grp = comm->c_local_group;
        OBJ_RETAIN(grp);
    }
    return grp;   
}


int ompi_comm_revoke_internal(ompi_communicator_t* comm)
{
    ompi_revoke_message_t msg;
    int ret;

    if( comm->comm_revoked ) return OMPI_SUCCESS;

    msg.cid = comm->c_contextid;
    msg.epoch = comm->c_epoch;
    /*
     * Broadcast the 'revoke' signal to all other processes.
     */
    OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                         "%s ompi: comm_revoke: API: Ask others to revoke communicator %3d:%d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), comm->c_contextid, comm->c_epoch ));
    ret = ompi_comm_revoke_internal_rbcast(&msg);
    ompi_comm_revoke_internal_local(&msg);
    return ret;
}

static int ompi_comm_revoke_internal_rbcast_n2(ompi_revoke_message_t* msg) {
    int i, exit_status = OMPI_SUCCESS, ret;
    ompi_group_t* grp = ompi_comm_revoke_internal_fw_grp(msg);
    if(NULL == grp) { 
        return OMPI_ERR_NOT_FOUND;
    }
    for(i = 0; i < ompi_group_size(grp); i++) {
        ompi_proc_t* proc;
        mca_bml_base_btl_t *bml_btl;
        mca_btl_base_descriptor_t *des;
        ompi_revoke_message_t* msgdes;

        if(ompi_group_rank(grp) == i) continue;
        proc = ompi_group_peer_lookup(grp, i);
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                "%s ompi: comm_revoke: N2 Send: preparing a fragment to %s to revoke %3d",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), msg->cid));

        assert(proc->proc_bml);
        bml_btl = mca_bml_base_btl_array_get_next(&proc->proc_bml->btl_send);
        mca_bml_base_alloc(bml_btl, &des, MCA_BTL_NO_ORDER, 
                           sizeof(ompi_revoke_message_t),
                           MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
        if(OPAL_UNLIKELY(NULL == des)) {
            exit_status = OMPI_ERR_OUT_OF_RESOURCE;
            break;
        }
        assert(des->des_src->seg_len == sizeof(ompi_revoke_message_t));
        des->des_cbfunc = ompi_revoke_bml_complete_fn;
     
        msgdes = des->des_src->seg_addr.pval;
        *msgdes = *msg;
        ret = mca_bml_base_send(bml_btl, des, MCA_BTL_TAG_FT_REVOKE);
        if(OPAL_LIKELY(ret >= 0)) {
            if(OPAL_LIKELY(1 == ret)) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                        "%s ompi: comm_revoke: N2 Send: fragment to %s to revoke %3d is gone",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), msg->cid));
            }
        }
        else {
            mca_bml_base_free(bml_btl, des);
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                    "%s ompi: comm_revoke: N2 Send: could not send a fragment to %s to revoke %3d (code %d)",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), msg->cid, ret ));
            /* Don't stop yet, keep sending the revoke message */
        }
    }
    OBJ_RELEASE(grp);
    return exit_status;
}

static int ompi_comm_revoke_internal_fw_n2(ompi_revoke_message_t* msg) {
    /*
     * Broadcast the 'revoke' signal to all other processes.
     */
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ompi: comm_revoke: FW : Ask others to revoke communicator %3d:%d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), msg->cid, msg->epoch ));
    return ompi_comm_revoke_internal_rbcast(msg);
}



static int ompi_comm_revoke_internal_rbcast_bmg(ompi_revoke_message_t* msg) {
    int i, ret, exit_status = OMPI_SUCCESS, me;
    ompi_group_t* grp = ompi_comm_revoke_internal_fw_grp(msg);
    if(NULL == grp) {
        return OMPI_ERR_NOT_FOUND;
    }
    me = ompi_group_rank(grp);

    for(i = 1; i < ompi_group_size(grp); i*=2) {
        ompi_proc_t* proc;
        mca_bml_base_btl_t *bml_btl;
        mca_btl_base_descriptor_t *des;
        ompi_revoke_message_t* msgdes;

        proc = ompi_group_peer_lookup(grp, (me+i)%ompi_group_size(grp));
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                "%s ompi: comm_revoke: BMG Send: preparing a fragment to %s to revoke %3d",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), msg->cid));

        assert(proc->proc_bml);
        bml_btl = mca_bml_base_btl_array_get_next(&proc->proc_bml->btl_send);
        mca_bml_base_alloc(bml_btl, &des, MCA_BTL_NO_ORDER, 
                           sizeof(ompi_revoke_message_t),
                           MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
        if(OPAL_UNLIKELY(NULL == des)) {
            exit_status = OMPI_ERR_OUT_OF_RESOURCE;
            break;
        }
        assert(des->des_src->seg_len == sizeof(ompi_revoke_message_t));
        des->des_cbfunc = ompi_revoke_bml_complete_fn;
        msgdes = des->des_src->seg_addr.pval;
        *msgdes = *msg;
        ret = mca_bml_base_send(bml_btl, des, MCA_BTL_TAG_FT_REVOKE);
        if(OPAL_LIKELY(ret >= 0)) {
            if(OPAL_LIKELY(1 == ret)) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                        "%s ompi: comm_revoke: BMG Send: fragment to %s to revoke %3d is on the wire",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), msg->cid));
            }
        }
        else {
            mca_bml_base_free(bml_btl, des);
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                    "%s ompi: comm_revoke: BMG Send: could not send a fragment to %s to revoke %3d (code %d)",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), msg->cid, ret ));
        }
    }
    OBJ_RELEASE(grp);
    return exit_status;
}



static int ompi_comm_revoke_internal_rbcast_ringleader(ompi_revoke_message_t* msg) {
    ompi_group_t* grp = ompi_comm_revoke_internal_fw_grp(msg);
    int leader;
    int pred;
    int ret, exit_status = OMPI_SUCCESS;
    ompi_proc_t* proc;
    mca_bml_base_btl_t *bml_btl;
    mca_btl_base_descriptor_t *des;
    ompi_revoke_message_t* msgdes;

    if(NULL == grp) { 
        return OMPI_ERR_NOT_FOUND;
    }

    leader = ompi_group_rank(grp);
    pred = (leader-1) % ompi_group_size(grp);
    
retry_send:
    proc = ompi_group_peer_lookup(grp, pred);
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
            "%s ompi: comm_revoke: RL Send: preparing a fragment to %s to revoke %3d",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), msg->cid));

    assert(proc->proc_bml);
    bml_btl = mca_bml_base_btl_array_get_next(&proc->proc_bml->btl_send);
    mca_bml_base_alloc(bml_btl, &des, MCA_BTL_NO_ORDER, 
                       sizeof(ompi_revoke_message_t),
                       MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
    if(OPAL_UNLIKELY(NULL == des)) {
        exit_status = OMPI_ERR_OUT_OF_RESOURCE;
        goto release_and_return;
    }
    assert(des->des_src->seg_len == sizeof(ompi_revoke_message_t));
    des->des_cbfunc = ompi_revoke_bml_complete_fn;
    msgdes = des->des_src->seg_addr.pval;

    msgdes->cid = msg->cid;
    msgdes->epoch = msg->epoch;
    msgdes->leader = leader;
    msgdes->round = 1;
    ret = mca_bml_base_send(bml_btl, des, MCA_BTL_TAG_FT_REVOKE);
    if(OPAL_LIKELY(ret >= 0)) {
        if(OPAL_LIKELY(1 == ret)) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                    "%s ompi: comm_revoke: RL Send: fragment to %s to revoke %3d is gone",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), msg->cid));
        }
    }
    else {
        mca_bml_base_free(bml_btl, des);
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                "%s ompi: comm_revoke: RL Send: could not send a fragment to %s to revoke %3d (code %d)",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), msg->cid, ret ));
        pred = (pred-1) % ompi_group_size(grp);
        if( leader != pred ) goto retry_send; 
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                "%s ompi: comm_revoke: RL Send: all other ranks are dead, the revoke of %3d is complete",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), msg->cid ));
    }
 release_and_return:
    OBJ_RELEASE(grp);
    return exit_status;
}

static int ompi_comm_revoke_internal_fw_ringleader(ompi_revoke_message_t* msg) {
    int me, exit_status = OMPI_SUCCESS; 
    ompi_communicator_t* comm;
    ompi_group_t* grp;

    comm = ompi_comm_lookup( msg->cid );
    if(OMPI_COMM_IS_INTER(comm)) {
        ompi_group_union(comm->c_local_group, comm->c_remote_group, &grp);
    }
    else {
        grp = comm->c_local_group;
        OBJ_RETAIN(grp);
    }

    me = ompi_group_rank(grp);
    if(msg->round == 2) {
        /* a token has made a full round, great, I don't need to act as a leader */
        comm->comm_revoked = 2;
        if(msg->leader == me) return OMPI_SUCCESS; /* no further propagation */
    }
    else if(msg->leader == me) {
        /* This token has made its first round successfully, 
        initiate second round to release would-be leaders */
        assert(msg->round == 1);
        msg->round++;
    }

    /* Ok, if I reach here, I need to try a forward */
    if(msg->leader <= me) {
        int pred = (me-1) % ompi_group_size(grp);
        int ret;
        ompi_proc_t* proc;
        mca_btl_base_descriptor_t *des;
        mca_bml_base_btl_t *bml_btl;

retry_send:
        proc = ompi_group_peer_lookup(grp, pred);
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ompi: comm_revoke: RL Send: preparing a fragment to %s to revoke %3d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), msg->cid));
        assert(proc->proc_bml);
        bml_btl = mca_bml_base_btl_array_get_next(&proc->proc_bml->btl_send);
        mca_bml_base_alloc(bml_btl, &des, MCA_BTL_NO_ORDER, 
                           sizeof(ompi_revoke_message_t),
                           MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
        if(OPAL_UNLIKELY(NULL == des)) {
            exit_status = OMPI_ERR_OUT_OF_RESOURCE;
            goto release_and_return;
        }
        assert(des->des_src->seg_len == sizeof(ompi_revoke_message_t));
        des->des_cbfunc = ompi_revoke_bml_complete_fn;
        (*(ompi_revoke_message_t*)des->des_src->seg_addr.pval) = *msg;
        ret = mca_bml_base_send(bml_btl, des, MCA_BTL_TAG_FT_REVOKE);
        if(OPAL_LIKELY(ret >= 0)) {
            if(OPAL_LIKELY(1 == ret)) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                    "%s ompi: comm_revoke: RL Send: fragment to %s to revoke %3d is gone",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), msg->cid));
            }
        }
        else {
            mca_bml_base_free(bml_btl, des);
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                "%s ompi: comm_revoke: RL Send: could not send a fragment to %s to revoke %3d (code %d)",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), msg->cid, ret ));
            if( (msg->leader == pred) && (msg->round == 1) ) {
                /* Leader is dead, but the round 1 is complete, 
                 initiate round 2 instead of the dead leader */
                msg->round++;
            }
            else if( (msg->leader == pred) && (msg->round == 2) ) {
                /* Leader is dead, but the round 2 is complete,
                  the revoke is complete */
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle, 
                    "%s ompi: comm_revoke: RL the leader %d is dead, but the second round is complete, so revoke %3d is done",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), msg->leader, msg->cid ));
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






