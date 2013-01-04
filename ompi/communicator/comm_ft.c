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
#include "ompi/mca/coll/base/base.h"
#include "ompi/mca/coll/base/coll_tags.h"


static bool comm_revoke_listener_started = false;

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

int ompi_comm_init_revoke(void)
{
    int ret;

    if( comm_revoke_listener_started ) {
        return OMPI_SUCCESS;
    }

    /*
     * Register an RML receive for 'revoke' messages
     */
    ret = orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                                  ORTE_RML_TAG_REVOKE_NOTICE,
                                  ORTE_RML_PERSISTENT,
                                  comm_revoke_notice_recv,
                                  NULL);
    if( OMPI_SUCCESS != ret ) {
        return ret;
    }

    comm_revoke_listener_started = true;

    return OMPI_SUCCESS;
}

int ompi_comm_finalize_revoke(void)
{
    int ret;

    if( !comm_revoke_listener_started ) {
        return OMPI_SUCCESS;
    }

    ret = orte_rml.recv_cancel(ORTE_NAME_WILDCARD,
                               ORTE_RML_TAG_REVOKE_NOTICE);
    if( OMPI_SUCCESS != ret ) {
        return ret;
    }

    comm_revoke_listener_started = false;

    return OMPI_SUCCESS;
}

static void comm_revoke_notice_recv(int status,
                                    orte_process_name_t* sender,
                                    opal_buffer_t* buffer,
                                    orte_rml_tag_t tag,
                                    void* cbdata)
{
    int ret;
    orte_std_cntr_t count;
    int cid_to_revoke;
    ompi_communicator_t *comm = NULL;
    ompi_proc_t *ompi_proc_peer = NULL;
    int proc_rank;
    orte_process_name_t true_sender;

    /*
     * Get the true sender from the message
     */
    count = 1;
    ret = opal_dss.unpack(buffer, &(true_sender), &count, ORTE_NAME);
    if( OMPI_SUCCESS != ret ){
        return;
    }

    /*
     * Get the 'cid' from the message
     */
    count = 1;
    ret = opal_dss.unpack(buffer, &(cid_to_revoke), &count, OPAL_INT);
    if( OMPI_SUCCESS != ret ){
        return;
    }

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ompi: comm_revoke: Recv: %s Asked to revoke communicator %3d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&true_sender), cid_to_revoke ));

    /*
     * Find the communicator
     */
    comm = (ompi_communicator_t *)opal_pointer_array_get_item(&ompi_mpi_communicators, cid_to_revoke);
    if( NULL == comm || cid_to_revoke != (int)(comm->c_contextid) ) {
        opal_output(0, "%s ompi: comm_revoke: Error: Could not find the communicator with CID %3d",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), cid_to_revoke );
        return;
    }

    /*
     * Check to make sure the sender is in this communicator
     */
    ompi_proc_peer = ompi_proc_find(&true_sender);
    if( NULL == ompi_proc_peer ) {
        opal_output(0, "%s ompi: comm_revoke: Error: Could not find the sender's ompi_proc_t (%s)",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&true_sender) );
        return;
    }
    /* Look in the local, then the remote group for the process */
    proc_rank = ompi_group_peer_lookup_id(comm->c_local_group, ompi_proc_peer);
    if( proc_rank < 0 ) {
        proc_rank = ompi_group_peer_lookup_id(comm->c_remote_group, ompi_proc_peer);
        if( proc_rank < 0 ) {
            /* Not in this communicator, ignore */
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ompi: comm_revoke: Recv: %s Revoked a different communicator %3d - Ignore ",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&true_sender), cid_to_revoke ));
            return;
        }
    }

    /*
     * Revoke the communicator
     */
    if( comm->comm_revoked ) {
        OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                             "%s ompi: comm_revoke: Recv: Asked to revoke communicator %3d - Already revoked",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), cid_to_revoke ));
        return;
    }
    /* Revoke the communicator */
    comm->comm_revoked        = true;
    comm->collectives_force_error = true;
    comm->any_source_enabled  = false;

    /*
     * Signal the point-to-point stack to recheck requests
     */
    OPAL_THREAD_LOCK(&ompi_request_lock);
    opal_condition_signal(&ompi_request_cond);
    OPAL_THREAD_UNLOCK(&ompi_request_lock);

    return;
}

int ompi_comm_revoke_internal(ompi_communicator_t* comm)
{
    int ret, exit_status = OMPI_SUCCESS;
    opal_buffer_t buffer;
    orte_jobid_t jobid = 0;

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

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ompi: comm_revoke: Send: Ask others to revoke communicator %3d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), comm->c_contextid ));

    /*
     * Broadcast the 'revoke' signal to all other processes.
     */
    OBJ_CONSTRUCT(&buffer, opal_buffer_t);

    if (OMPI_SUCCESS != (ret = opal_dss.pack(&buffer, ORTE_PROC_MY_NAME, 1, ORTE_NAME))) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    if (OMPI_SUCCESS != (ret = opal_dss.pack(&buffer, &(comm->c_contextid), 1, OPAL_INT))) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    jobid = orte_process_info.my_name.jobid;
    if( OMPI_SUCCESS != (ret = orte_grpcomm.xcast(jobid, &buffer, ORTE_RML_TAG_REVOKE_NOTICE)) ) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

 cleanup:
    OBJ_DESTRUCT(&buffer);

    return exit_status;
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
    double start, stop;

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
    start = MPI_Wtime();
    ret = comm->c_coll.coll_agreement( (ompi_communicator_t*)comm,
                                       &failed_group,
                                       &flag,
                                       comm->c_coll.coll_agreement_module);
    stop = MPI_Wtime();
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ompi: comm_shrink: AGREE: %g seconds", 
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), stop-start));
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
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle, 
                         "%s ompi: comm_shrink: GRP COMPUTATION: %g seconds\n", 
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), stop-start));
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
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle, 
                         "%s ompi: comm_shrink: NEXT CID: %g seconds\n", 
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), stop-start));
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
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ompi: comm_shrink: COLL SELECT: %g seconds\n", 
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), stop-start));
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
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ompi: comm_shrink: COMMIT: %g seconds\n", 
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), stop-start));
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
