/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 *
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "opal/dss/dss.h"

#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/grpcomm/grpcomm.h"

#include "ompi/runtime/params.h"
#include "ompi/group/group.h"
#include "ompi/communicator/communicator.h"
#include "ompi/op/op.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/bml/bml.h"
#include "ompi/mca/bml/base/base.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/mca/coll/base/coll_tags.h"


static bool comm_revoke_listener_started = false;
static int comm_revoke_rbcast = 1;

static void comm_revoke_notice_recv(int status,
                                    orte_process_name_t* sender,
                                    opal_buffer_t* buffer,
                                    orte_rml_tag_t tag,
                                    void* cbdata);


int ompi_comm_failure_ack_internal(ompi_communicator_t* comm)
{
    /* Re-enable ANY_SOURCE */
    comm->any_source_enabled = true;

    /* Fix offset in the global failed list */
    comm->any_source_offset = ompi_group_size(ompi_group_all_failed_procs);

    return OMPI_SUCCESS;
}

int ompi_comm_failure_get_acked_internal(ompi_communicator_t* comm, ompi_group_t **group )
{
    int ret, exit_status = OMPI_SUCCESS;
    int range[3];
    ompi_group_t *tmp_sub_group = NULL;

    /*
     * If no failure present, then return the empty group
     */
    if( 0 == comm->any_source_offset ) {
        *group = MPI_GROUP_EMPTY;
        OBJ_RETAIN(MPI_GROUP_EMPTY);
        exit_status = OMPI_SUCCESS;
        goto cleanup;
    }

    tmp_sub_group = OBJ_NEW(ompi_group_t);

    /*
     * Access just the offset number of failures
     */
    range[0] = 0;
    range[1] = comm->any_source_offset - 1;
    range[2] = 1;

    ret = ompi_group_range_incl(ompi_group_all_failed_procs, 1, &range, &tmp_sub_group);
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto cleanup;
    }

    /*
     * Access the intersection between the failed subgroup and our group
     */
    if( OMPI_COMM_IS_INTER(comm) ) {
        ret = ompi_group_intersection(tmp_sub_group,
                                      comm->c_local_group,
                                      group);
    } else {
        ret = ompi_group_intersection(tmp_sub_group,
                                      comm->c_remote_group,
                                      group);
    }

    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto cleanup;
    }

 cleanup:
    if( NULL != tmp_sub_group ) {
        OBJ_RELEASE(tmp_sub_group);
        tmp_sub_group = NULL;
    }

    return exit_status;
}

static void ompi_comm_revoke_bml_cb_fn(
        struct mca_btl_base_module_t* btl,
        mca_btl_base_tag_t tag,
        mca_btl_base_descriptor_t* descriptor,
        void* cbdata);


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

    ret = mca_bml.bml_register(MCA_BTL_TAG_FT, ompi_comm_revoke_bml_cb_fn, NULL);
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

typedef struct ompi_revoke_message_t {
    int32_t    cid;
    int32_t    leader;
    int8_t     round;
} ompi_revoke_message_t;

static int ompi_comm_revoke_internal_fw_ringleader(ompi_revoke_message_t* msg);
static int ompi_comm_revoke_internal_fw_n2(ompi_revoke_message_t* msg);
#define ompi_comm_revoke_internal_fw(msg) ( \
    comm_revoke_rbcast == 0 ? OMPI_SUCCESS : \
    comm_revoke_rbcast == 1 ? ompi_comm_revoke_internal_fw_n2(msg) : \
    comm_revoke_rbcast == 2 ? ompi_comm_revoke_internal_fw_n2(msg) /*this is not a mistake*/ : \
                              ompi_comm_revoke_internal_fw_ringleader(msg) )


static void ompi_comm_revoke_bml_cb_fn(
        struct mca_btl_base_module_t* btl,
        mca_btl_base_tag_t tag,
        mca_btl_base_descriptor_t* descriptor,
        void* cbdata)
{
    ompi_revoke_message_t* msg;
    int cid_to_revoke;
    ompi_communicator_t *comm = NULL;
    
    /* Parse the revoke fragment */
    assert(MCA_BTL_TAG_FT == tag);
    assert(1 == descriptor->des_dst_cnt);
    assert(sizeof(ompi_revoke_message_t) == descriptor->des_dst->seg_len);
    msg = (ompi_revoke_message_t*) descriptor->des_dst->seg_addr.pval;
    
    /* forward the revoke */
    ompi_comm_revoke_internal_fw(msg);

    /* Find the communicator */
    cid_to_revoke = msg->cid;
    comm = (ompi_communicator_t *)ompi_comm_lookup(cid_to_revoke);
    if( NULL == comm ){
        opal_output(0, "%s ompi: comm_revoke: Error: Could not find the communicator with CID %3d",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), cid_to_revoke );
        return;
    }
    assert( cid_to_revoke == (int)(comm->c_contextid) );
    
    /* Revoke the communicator locally */
    if( comm->comm_revoked ) {
        OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle,
                             "%s ompi: comm_revoke: Recv: Asked to revoke communicator %3d - Already revoked",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), cid_to_revoke ));
    }
    else {
        comm->comm_revoked        = true;
        comm->collectives_force_error = true;
        comm->any_source_enabled  = false;
        /* Signal the point-to-point stack to recheck requests */
        OPAL_THREAD_LOCK(&ompi_request_lock);
        opal_condition_signal(&ompi_request_cond);
        OPAL_THREAD_UNLOCK(&ompi_request_lock);
    }

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

static int ompi_comm_revoke_internal_rbcast_n2(ompi_group_t* grp, int cid);
static int ompi_comm_revoke_internal_rbcast_ringleader(ompi_group_t* grp, int cid);
static int ompi_comm_revoke_internal_rbcast_bmg(ompi_group_t* grp, int cid);
#define ompi_comm_revoke_internal_rbcast(grp, cid) ( \
    comm_revoke_rbcast == 0 ? OMPI_SUCCESS : \
    comm_revoke_rbcast == 1 ? ompi_comm_revoke_internal_rbcast_n2(grp, cid) : \
    comm_revoke_rbcast == 2 ? ompi_comm_revoke_internal_rbcast_bmg(grp, cid) : \
                              ompi_comm_revoke_internal_rbcast_ringleader(grp, cid) )


int ompi_comm_revoke_internal(ompi_communicator_t* comm)
{
    int i, exit_status;
    ompi_group_t* grp;

    /* if the communicator is already revoked, nothing to be done */
    if(comm->comm_revoked) 
        return OMPI_SUCCESS;
        
    /*
     * Locally revoke the communicator
     *
     * Just to be pedantic, as the 'revoke' condition is checked first
     * by all other communication,
     * - Turn off collectives
     * - Turn off ANY_SOURCE receives
     */
    comm->comm_revoked        = true;
    comm->collectives_force_error = true;
    comm->any_source_enabled  = false;

    OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                         "%s ompi: comm_revoke: Send: Ask others to revoke communicator %3d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), comm->c_contextid ));

    /*
     * Broadcast the 'revoke' signal to all other processes.
     */
    if(OMPI_COMM_IS_INTER(comm)) {
        ompi_group_union(comm->c_local_group, comm->c_remote_group, &grp);
    }
    else {
        grp = comm->c_local_group;
        OBJ_RETAIN(grp);
    }
    exit_status = ompi_comm_revoke_internal_rbcast(grp, comm->c_contextid);
    OBJ_RELEASE(grp);
    
    return exit_status;
}

static int ompi_comm_revoke_internal_rbcast_n2(ompi_group_t* grp, int cid) {
    int i, exit_status = OMPI_SUCCESS, ret;

    for(i = 0; i < ompi_group_size(grp); i++) {
        ompi_proc_t* proc;
        mca_btl_base_descriptor_t *des;
        mca_bml_base_btl_t *bml_btl;
        ompi_revoke_message_t* msg;

        if(ompi_group_rank(grp) == i) continue;
        proc = ompi_group_peer_lookup(grp, i);
        OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                "%s ompi: comm_revoke: N2 Send: preparing a fragment to %s to revoke %3d",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), cid));

        assert(proc->proc_bml);
        bml_btl = mca_bml_base_btl_array_get_next(&proc->proc_bml->btl_send);
        mca_bml_base_alloc(bml_btl, &des, MCA_BTL_NO_ORDER, 
                           sizeof(ompi_revoke_message_t),
                           MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
        if(OPAL_UNLIKELY(NULL == des)) return OMPI_ERR_OUT_OF_RESOURCE;
        assert(des->des_src->seg_len == sizeof(ompi_revoke_message_t));
        des->des_cbfunc = ompi_revoke_bml_complete_fn;
     
        msg = des->des_src->seg_addr.pval;
        msg->cid = cid;
        ret = mca_bml_base_send(bml_btl, des, MCA_BTL_TAG_FT);
        if(OPAL_LIKELY(ret >= 0)) {
            if(OPAL_LIKELY(1 == ret)) {
                OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle,
                        "%s ompi: comm_revoke: N2 Send: fragment to %s to revoke %3d is gone",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), cid));
            }
        }
        else {
            mca_bml_base_free(bml_btl, des);
            OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                    "%s ompi: comm_revoke: N2 Send: could not send a fragment to %s to revoke %3d (code %d)",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), cid, ret ));
            exit_status = ret;
        }
    }
    return exit_status;
}

static int ompi_comm_revoke_internal_fw_n2(ompi_revoke_message_t* msg) {
    ompi_communicator_t* comm; 
    
    comm = (ompi_communicator_t *)ompi_comm_lookup(msg->cid);
    if( comm->comm_revoked ) return OMPI_SUCCESS;
    return ompi_comm_revoke_internal(comm);
}



static int ompi_comm_revoke_internal_rbcast_bmg(ompi_group_t* grp, int cid) {
    int i, ret;

    for(i = 1; i < ompi_group_size(grp); i*=2) {
        ompi_proc_t* proc;
        mca_btl_base_descriptor_t *des;
        mca_bml_base_btl_t *bml_btl;
        ompi_revoke_message_t* msg;

        int me = ompi_group_rank(grp);
        proc = ompi_group_peer_lookup(grp, (me+i)%ompi_group_size(grp));
        OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                "%s ompi: comm_revoke: BMG Send: preparing a fragment to %s to revoke %3d",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), cid));

        assert(proc->proc_bml);
        bml_btl = mca_bml_base_btl_array_get_next(&proc->proc_bml->btl_send);
        mca_bml_base_alloc(bml_btl, &des, MCA_BTL_NO_ORDER, 
                           sizeof(ompi_revoke_message_t),
                           MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
        if(OPAL_UNLIKELY(NULL == des)) return OMPI_ERR_OUT_OF_RESOURCE;
        assert(des->des_src->seg_len == sizeof(ompi_revoke_message_t));
        des->des_cbfunc = ompi_revoke_bml_complete_fn;
     
        msg = des->des_src->seg_addr.pval;
        msg->cid = cid;
        ret = mca_bml_base_send(bml_btl, des, MCA_BTL_TAG_FT);
        if(OPAL_LIKELY(ret >= 0)) {
            if(OPAL_LIKELY(1 == ret)) {
                OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle,
                        "%s ompi: comm_revoke: BMG Send: fragment to %s to revoke %3d is gone",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), cid));
            }
        }
        else {
            mca_bml_base_free(bml_btl, des);
            OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                    "%s ompi: comm_revoke: BMG Send: could not send a fragment to %s to revoke %3d (code %d)",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), cid, ret ));
        }
    }
    return OMPI_SUCCESS;
}



static int ompi_comm_revoke_internal_rbcast_ringleader(ompi_group_t* grp, int cid) {
    int leader = ompi_group_rank(grp);
    int pred = (leader-1) % ompi_group_size(grp);
    int ret;
    ompi_proc_t* proc;
    mca_btl_base_descriptor_t *des;
    mca_bml_base_btl_t *bml_btl;
    ompi_revoke_message_t* msg;
    
retry_send:
    proc = ompi_group_peer_lookup(grp, pred);
    OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
            "%s ompi: comm_revoke: RL Send: preparing a fragment to %s to revoke %3d",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), cid));

    assert(proc->proc_bml);
    bml_btl = mca_bml_base_btl_array_get_next(&proc->proc_bml->btl_send);
    mca_bml_base_alloc(bml_btl, &des, MCA_BTL_NO_ORDER, 
                       sizeof(ompi_revoke_message_t),
                       MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
    if(OPAL_UNLIKELY(NULL == des)) return OMPI_ERR_OUT_OF_RESOURCE;
    assert(des->des_src->seg_len == sizeof(ompi_revoke_message_t));
    des->des_cbfunc = ompi_revoke_bml_complete_fn;
    msg = des->des_src->seg_addr.pval;

    msg->cid = cid;
    msg->leader = leader;
    msg->round = 1;
    ret = mca_bml_base_send(bml_btl, des, MCA_BTL_TAG_FT);
    if(OPAL_LIKELY(ret >= 0)) {
        if(OPAL_LIKELY(1 == ret)) {
            OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle,
                    "%s ompi: comm_revoke: RL Send: fragment to %s to revoke %3d is gone",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), cid));
        }
    }
    else {
        mca_bml_base_free(bml_btl, des);
        OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                "%s ompi: comm_revoke: RL Send: could not send a fragment to %s to revoke %3d (code %d)",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), cid, ret ));
        pred = (pred-1) % ompi_group_size(grp);
        if( leader != pred ) goto retry_send; 
        OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                "%s ompi: comm_revoke: RL Send: all other ranks are dead, the revoke of %3d is complete",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), cid ));
    }
    

    return OMPI_SUCCESS;
}

static int ompi_comm_revoke_internal_fw_ringleader(ompi_revoke_message_t* msg) {
    int me, leader; 
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
        OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
            "%s ompi: comm_revoke: RL Send: preparing a fragment to %s to revoke %3d",
            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), msg->cid));
        assert(proc->proc_bml);
        bml_btl = mca_bml_base_btl_array_get_next(&proc->proc_bml->btl_send);
        mca_bml_base_alloc(bml_btl, &des, MCA_BTL_NO_ORDER, 
                       sizeof(ompi_revoke_message_t),
                       MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
        if(OPAL_UNLIKELY(NULL == des)) return OMPI_ERR_OUT_OF_RESOURCE;
        assert(des->des_src->seg_len == sizeof(ompi_revoke_message_t));
        des->des_cbfunc = ompi_revoke_bml_complete_fn;
        (*(ompi_revoke_message_t*)des->des_src->seg_addr.pval) = *msg;
        ret = mca_bml_base_send(bml_btl, des, MCA_BTL_TAG_FT);
        if(OPAL_LIKELY(ret >= 0)) {
            if(OPAL_LIKELY(1 == ret)) {
                OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle,
                    "%s ompi: comm_revoke: RL Send: fragment to %s to revoke %3d is gone",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&proc->proc_name), msg->cid));
            }
        }
        else {
            mca_bml_base_free(bml_btl, des);
            OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
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
                OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle, 
                    "%s ompi: comm_revoke: RL the leader %d is dead, but the second round is complete, so revoke %3d is done",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), msg->leader, msg->cid ));
                /* mark completed*/
                comm->comm_revoked = 2;
                return OMPI_SUCCESS;
            }
            pred = (pred-1) % ompi_group_size(grp);
            goto retry_send; 
        }
    }
    
    OBJ_RELEASE(grp);
    return OMPI_SUCCESS;
}




int ompi_comm_shrink_internal(ompi_communicator_t* comm, ompi_communicator_t** newcomm)
{
    int ret, exit_status = OMPI_SUCCESS;
    int flag;
    ompi_group_t *failed_group = NULL, *comm_group = NULL, *alive_group = NULL;
    ompi_communicator_t *comp = NULL;
    ompi_communicator_t *newcomp = NULL;
    int size, lsize, rsize, inrank;
    int *rranks = NULL;
    int mode;

    /*
     * JJH: Do not support intercommunicators (for now)
     */
    if ( OMPI_COMM_IS_INTER(comm) ) {
        exit_status = MPI_ERR_UNSUPPORTED_OPERATION;
        goto cleanup;
    }

    /*
     * The communicator must have been 'revoked' before calling this.
     * Otherwise return an error.
     */
    if( !ompi_comm_is_revoked(comm) ) {
        exit_status = MPI_ERR_ARG;
        goto cleanup;
    }

    /*
     * Step 1: Agreement on failed group in comm
     */
    /* --------------------------------------------------------- */
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ompi: comm_shrink: Agreement on failed processes",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
    failed_group = OBJ_NEW(ompi_group_t);
    flag = (OMPI_SUCCESS == OMPI_SUCCESS);
double start = MPI_Wtime();
    ret = comm->c_coll.coll_agreement( (ompi_communicator_t*)comm,
                                       &failed_group,
                                       &flag,
                                       comm->c_coll.coll_agreement_module);
double stop = MPI_Wtime();
printf("SHRINK AGREE: %g seconds\n", stop-start);
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto cleanup;
    }

 retry_shrink:
    /*
     * Step 2: Determine ranks for new communicator
     */
    /* --------------------------------------------------------- */
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ompi: comm_shrink: Determine ranking for new communicator",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
start = MPI_Wtime();
    comp = (ompi_communicator_t *) comm;

    if ( OMPI_COMM_IS_INTER(comp) ) {
        exit_status = MPI_ERR_UNSUPPORTED_OPERATION;
        goto cleanup;
    } else {
        rsize  = 0;
        rranks = NULL;
        mode   = OMPI_COMM_CID_INTRA_FT;
    }

    /* Create 'alive' group */
    size        = ompi_comm_size(comm);
    lsize       = size - ompi_group_size(failed_group);
    alive_group = OBJ_NEW(ompi_group_t);
    ompi_comm_group(comm, &comm_group);
    ret = ompi_group_difference(comm_group, failed_group, &alive_group);
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto decide_commit;
    }

    /* Determine the collective leader - Lowest rank in the alive set */
    inrank = 0;
    ompi_group_translate_ranks(alive_group, 1, &inrank,
                               comm_group, &comm->lleader);

    *newcomm = MPI_COMM_NULL;

    ret = ompi_comm_set( &newcomp,                 /* new comm */
                         comp,                     /* old comm */
                         lsize,                    /* local_size */
                         NULL,                     /* local_ranks */
                         rsize,                    /* remote_size */
                         rranks,                   /* remote_ranks */
                         comp->c_keyhash,          /* attrs */
                         comp->error_handler,      /* error handler */
                         NULL,                     /* topo component */
                         alive_group,              /* local group */
                         NULL                      /* remote group */
                         );
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto decide_commit;
    }
    if( NULL == newcomp ) {
        exit_status = MPI_ERR_INTERN;
        goto decide_commit;
    }
stop = MPI_Wtime();
printf("SHRINK GRP COMPUTATION: %g seconds\n", stop-start);
    /*
     * Step 3: Determine context id
     */
    /* --------------------------------------------------------- */
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ompi: comm_shrink: Determine context id",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
start = MPI_Wtime();
    ret = ompi_comm_nextcid( newcomp,  /* new communicator */ 
                             comp,     /* old comm */
                             NULL,     /* bridge comm */
                             NULL,     /* local leader */
                             NULL,     /* remote_leader */
                             mode,     /* mode */
                             -1 );     /* send_first */
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto decide_commit;
    }
stop = MPI_Wtime();
printf("SHRINK NEXT CID: %g seconds\n", stop-start);
    /*
     * Step 4: Activate the communicator
     */
    /* --------------------------------------------------------- */
    /* Set name for debugging purposes */
    snprintf(newcomp->c_name, MPI_MAX_OBJECT_NAME, "MPI COMMUNICATOR %d SHRUNK FROM %d", 
             newcomp->c_contextid, comm->c_contextid );
start = MPI_Wtime();
    /* activate communicator and init coll-module */
    ret = ompi_comm_activate( &newcomp, /* new communicator */ 
                              comp,
                              NULL, 
                              NULL, 
                              NULL, 
                              mode, 
                              -1 );  
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto decide_commit;
    }
stop = MPI_Wtime();
printf("SHRINK COLL SELECT: %g seconds\n", stop-start);
 decide_commit:    
    /*
     * Step 5: Agreement on whether the operation was successful or not
     */
    /* --------------------------------------------------------- */
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ompi: comm_shrink: Agreement on failed processes",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
    /* Refresh the group here, so that if we need to iterate again we have the
     * most current list to work with */
    if( NULL != failed_group ) {
        OBJ_RELEASE(failed_group);
        failed_group = NULL;
    }
start = MPI_Wtime();
    failed_group = OBJ_NEW(ompi_group_t);
    flag = (OMPI_SUCCESS == exit_status);
    ret = comm->c_coll.coll_agreement( (ompi_communicator_t*)comm,
                                       &failed_group,
                                       &flag,
                                       comm->c_coll.coll_agreement_module);
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto cleanup;
    }
stop = MPI_Wtime();
printf("SHRINK COMMIT: %g seconds\n", stop-start);
    if( flag ) {
        *newcomm = newcomp;
    } else {
        exit_status = OMPI_SUCCESS;
        goto retry_shrink;
    }


 cleanup:
    if( NULL != failed_group ) {
        OBJ_RELEASE(failed_group);
        failed_group = NULL;
    }
    if( NULL != alive_group ) {
        OBJ_RELEASE(alive_group);
        alive_group = NULL;
    }
    if( NULL != comm_group ) {
        OBJ_RELEASE(comm_group);
        comm_group = NULL;
    }

    return exit_status;
}


bool ompi_comm_is_proc_active(ompi_communicator_t *comm, int peer_id, bool remote)
{
    bool active = false;

#if OPAL_ENABLE_DEBUG
    /* Sanity check
     * Do not call with MPI_ANY_SOURCE, use the _any variation for the approprate list
     */
    if( (peer_id < 0 && peer_id != MPI_ANY_SOURCE && peer_id != MPI_PROC_NULL ) ||
        (!OMPI_COMM_IS_INTRA(comm) && peer_id >= ompi_comm_remote_size(comm)) ||
        ( OMPI_COMM_IS_INTRA(comm) && peer_id >= ompi_comm_size(comm) ) ) {
        return false;
    }
#endif

    /*
     * PROC_NULL is always 'ok'
     */
    if( OPAL_UNLIKELY(peer_id == MPI_PROC_NULL) ) {
        return true;
    }
    /*
     * Check MPI_ANY_SOURCE differently
     */
    else if( OPAL_UNLIKELY(peer_id == MPI_ANY_SOURCE) ) {
        return ompi_comm_is_any_source_enabled(comm);
    }
    else if( !remote ) {
        ompi_group_get_rank_state(comm->c_local_group, peer_id, &active);
        return active;
    }
    else {
        ompi_group_get_rank_state(comm->c_remote_group, peer_id, &active);
        return active;
    }
}

int ompi_comm_set_rank_failed(ompi_communicator_t *comm, int peer_id, bool remote)
{
    int ret, exit_status = OMPI_SUCCESS;
    ompi_group_t *tmp_group = NULL;

    /* Disable ANY_SOURCE */
    comm->any_source_enabled = false;
    /* Disable collectives */
    comm->collectives_force_error = true;

    tmp_group = OBJ_NEW(ompi_group_t);

    if( !remote ) {
        comm->num_active_local -= 1;

        /* Extend the failed group */
        ret = ompi_group_incl(comm->c_local_group,
                              1,
                              &peer_id,
                              &tmp_group);
        if( OMPI_SUCCESS != ret ) {
            exit_status = ret;
            goto cleanup;
        }

        ret = ompi_group_union(ompi_group_all_failed_procs,
                               tmp_group,
                               &ompi_group_all_failed_procs);
        if( OMPI_SUCCESS != ret ) {
            exit_status = ret;
            goto cleanup;
        }
    } else {
        comm->num_active_remote -= 1;

        /* Extend the failed group */
        ret = ompi_group_incl(comm->c_remote_group,
                              1,
                              &peer_id,
                              &tmp_group);
        if( OMPI_SUCCESS != ret ) {
            exit_status = ret;
            goto cleanup;
        }

        ret = ompi_group_union(ompi_group_all_failed_procs,
                               tmp_group,
                               &ompi_group_all_failed_procs);
        if( OMPI_SUCCESS != ret ) {
            exit_status = ret;
            goto cleanup;
        }

    }

 cleanup:
    if( NULL != tmp_group ) {
        OBJ_RELEASE(tmp_group);
        tmp_group = NULL;
    }

    return exit_status;
}

int ompi_comm_allreduce_intra_ft( int *inbuf, int *outbuf, 
                                  int count, struct ompi_op_t *op, 
                                  ompi_communicator_t *comm,
                                  ompi_communicator_t *bridgecomm, 
                                  void* local_leader, 
                                  void* remote_leader, 
                                  int send_first )
{
    int ret, exit_status = OMPI_SUCCESS;
    int root, rank, size, i;
    int *tmp_buffer = NULL;

    /* JJH: This is a linear algorithm just for prototyping.
     * JJH: Additionaly it uses 'agreement' which is costly.
     * JJH: This can be improved, so return and implement something
     * JJH: better later.
     */

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);

    /*
     * Elect 'root'
     */
    root = comm->lleader;
    if( root < 0 ) {
        exit_status = OMPI_ERROR;
        goto cleanup;
    }

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ompi: comm_shrink: Allreduce: Linear Algorithm (Root = %3d)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), root ));

    /*
     * Reduce to 'root'
     */
    if( rank != root ) {
        ret = MCA_PML_CALL(send(inbuf, count, MPI_INT, root,
                                MCA_COLL_BASE_TAG_SHRINK,
                                MCA_PML_BASE_SEND_STANDARD, comm));
        if( OMPI_SUCCESS != ret ) {
            exit_status = ret;
            goto cleanup;
        }
    } else {
        tmp_buffer = (int*) malloc(sizeof(int) * count);
        if(NULL == tmp_buffer ) {
            ret = OMPI_ERR_OUT_OF_RESOURCE;
            goto cleanup;
        }

        /*
         * Initialize the receive buffer
         */
        tmp_buffer[0] = -1;
        ompi_datatype_copy_content_same_ddt(MPI_INT, count, (char*)outbuf, (char*)inbuf);

        for( i = 0; i < size; ++i ) {
            if( rank == i ) {
                ompi_op_reduce(op, inbuf, outbuf, count, MPI_INT);
            } else {
                if( !ompi_comm_is_proc_active(comm, i, false) ) {
                    continue; /* Ignore failed processes */
                }

                ret = MCA_PML_CALL(recv(tmp_buffer, count, MPI_INT, i,
                                        MCA_COLL_BASE_TAG_SHRINK, comm,
                                        MPI_STATUS_IGNORE));
                if( OMPI_SUCCESS != ret ) {
                    continue; /* Ignore failed processes */
                }

                ompi_op_reduce(op, tmp_buffer, outbuf, count, MPI_INT);
            }
        }
    }

    /*
     * Broadcast solution
     */
    if( rank != root ) {
        ret = MCA_PML_CALL(recv(outbuf, count, MPI_INT, root,
                                MCA_COLL_BASE_TAG_SHRINK, comm,
                                MPI_STATUS_IGNORE));
        if( OMPI_SUCCESS != ret ) {
            exit_status = ret;
            goto cleanup;
        }
    } else {
        for( i = 0; i < size; ++i ) {
            /* Root already has the output inthe outbuf, so skip */
            if( rank != i ) {
                if( !ompi_comm_is_proc_active(comm, i, false) ) {
                    continue; /* Ignore failed processes */
                }

                ret = MCA_PML_CALL(send(outbuf, count, MPI_INT, i,
                                        MCA_COLL_BASE_TAG_SHRINK,
                                        MCA_PML_BASE_SEND_STANDARD, comm));
                if( OMPI_SUCCESS != ret ) {
                    continue; /* Ignore failed processes */
                }
            }
        }
    }

    /*
     * It is possible that the above algorithm failed due to root failure
     * (all other failures are skipped over). In this case the calling
     * operation will fail. This is checked at a higher level, and the
     * routine will be tried again, with a new root.
     */

 cleanup:
    if( NULL != tmp_buffer ) {
        free(tmp_buffer);
        tmp_buffer = NULL;
    }

    return exit_status;
}

int ompi_comm_allreduce_inter_ft( int *inbuf, int *outbuf, 
                                  int count, struct ompi_op_t *op, 
                                  ompi_communicator_t *intercomm,
                                  ompi_communicator_t *bridgecomm, 
                                  void* local_leader, 
                                  void* remote_leader, 
                                  int send_first )
{
    return MPI_ERR_UNSUPPORTED_OPERATION;
}

int ompi_comm_allreduce_intra_bridge_ft(int *inbuf, int *outbuf, 
                                        int count, struct ompi_op_t *op, 
                                        ompi_communicator_t *comm,
                                        ompi_communicator_t *bcomm, 
                                        void* lleader, void* rleader,
                                        int send_first )
{
    return MPI_ERR_UNSUPPORTED_OPERATION;
}

int ompi_comm_allreduce_intra_oob_ft(int *inbuf, int *outbuf, 
                                     int count, struct ompi_op_t *op, 
                                     ompi_communicator_t *comm,
                                     ompi_communicator_t *bridgecomm, 
                                     void* lleader, void* rleader,
                                     int send_first )
{
    return MPI_ERR_UNSUPPORTED_OPERATION;
}

/*********************************************************
 * Internal support functions
 *********************************************************/
