/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2011-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
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

ompi_comm_rank_failure_callback_t *ompi_rank_failure_cbfunc = NULL;

/**
 * The handling of known failed processes is based on a two level process. On one
 * side the MPI library itself must know the failed processes (in order to be able
 * to correctly handle complex operations such as shrink). On the other side, the
 * failed processes acknowledged by the users shuould not be altered during any of
 * the internal calls, as they must only be updated upon user request.
 * Thus, the global list (ompi_group_all_failed_procs) is the list of all known
 * failed processes (by the MPI library internals), and it is allegedly updated
 * by the MPI library whenever new failure are noticed. However, the user interact
 * with this list via the MPI functions, and all failure notifications are reported
 * in the context of a communicator. Thus, using a single index to know the user-level
 * acknowledged failure is the simplest solution.
 */
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

int ompi_comm_shrink_internal(ompi_communicator_t* comm, ompi_communicator_t** newcomm)
{
    int ret, exit_status = OMPI_SUCCESS;
    int flag = 1;
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
     * Step 1: Agreement on failed group in comm
     */
    /* --------------------------------------------------------- */
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ompi: comm_shrink: Agreement on failed processes",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
    failed_group = OBJ_NEW(ompi_group_t);
    start = MPI_Wtime();
    do {
        /* We need to create the list of alive processes. Thus, we don't care about
         * the value of flag, instead we are only using the globally consistent
         * return value.
         */
        ret = comm->c_coll.coll_agreement( (ompi_communicator_t*)comm,
                                           &failed_group,
                                           &ompi_mpi_op_band.op,
                                           &ompi_mpi_int.dt,
                                           1,
                                           &flag,
                                           comm->c_coll.coll_agreement_module);
    } while( MPI_ERR_PROC_FAILED == ret );
    stop = MPI_Wtime();
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ompi: comm_shrink: AGREE: %g seconds",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), stop-start));
    if( (OMPI_SUCCESS != ret) && (MPI_ERR_PROC_FAILED != ret) ) {
        opal_output(0, "%s:%d Agreement failure: %d\n", __FILE__, __LINE__, ret);
        exit_status = ret;
        goto cleanup;
    }

    /*
     * Step 2: Determine ranks for new communicator
     */
    /* --------------------------------------------------------- */
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ompi: comm_shrink: Determine ranking for new communicator",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
    start = MPI_Wtime();
    comp = comm;

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
        goto cleanup;
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
        goto cleanup;
    }
    if( NULL == newcomp ) {
        exit_status = MPI_ERR_INTERN;
        goto cleanup;
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
        goto cleanup;
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
        goto cleanup;
    }
    stop = MPI_Wtime();
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ompi: comm_shrink: COLL SELECT: %g seconds\n", 
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), stop-start));
    
    /** Step 5: assign the output communicator */
    *newcomm = newcomp;

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
    ompi_proc_t* ompi_proc;

    /* Check MPI_ANY_SOURCE differently */
    if( OPAL_UNLIKELY(peer_id == MPI_ANY_SOURCE) ) {
        return ompi_comm_is_any_source_enabled(comm);
    }
    /* PROC_NULL is always 'ok' */
    if( OPAL_UNLIKELY(peer_id == MPI_PROC_NULL) ) {
        return true;
    }
#if OPAL_ENABLE_DEBUG
    /* Sanity check. Only valid ranks are accepted.  */
    if( (peer_id < 0) ||
        (!OMPI_COMM_IS_INTRA(comm) && peer_id >= ompi_comm_remote_size(comm)) ||
        ( OMPI_COMM_IS_INTRA(comm) && peer_id >= ompi_comm_size(comm) ) ) {
        return false;
    }
#endif
    ompi_proc = ompi_group_get_proc_ptr((remote ? comm->c_remote_group : comm->c_local_group),
                                        peer_id);
    return (NULL == ompi_proc) ? false : ompi_proc_is_active(ompi_proc);
}

int ompi_comm_set_rank_failed(ompi_communicator_t *comm, int peer_id, bool remote)
{
    /* Disable ANY_SOURCE */
    comm->any_source_enabled = false;
    /* Disable collectives */
    MCA_PML_CALL(revoke_comm(comm, true));

    if( !remote ) {
        comm->num_active_local -= 1;
    } else {
        comm->num_active_remote -= 1;
    }

    if( NULL != ompi_rank_failure_cbfunc ) {
        (*ompi_rank_failure_cbfunc)(comm, peer_id, remote);
    }

    return OMPI_SUCCESS;
}

/**
 * Reduction operation using an agreement, to ensure that all processes
 *  agree on the same list.
 */
int ompi_comm_allreduce_intra_ft( int *inbuf, int *outbuf, 
                                  int count, struct ompi_op_t *op, 
                                  ompi_communicator_t *comm,
                                  ompi_communicator_t *bridgecomm, 
                                  void* local_leader, 
                                  void* remote_leader, 
                                  int send_first )
{
    int ret;
    ompi_group_t *failed_group = NULL;

    /** Because the agreement operates "in place",
     *  one needs first to copy the inbuf into the outbuf
     */
    if( inbuf != outbuf ) {
        memcpy(outbuf, inbuf, count * sizeof(int));
    }

    failed_group = OBJ_NEW(ompi_group_t);
    do {
        ret = comm->c_coll.coll_agreement( (ompi_communicator_t*)comm,
                                           &failed_group,
                                           op,
                                           &ompi_mpi_int.dt,
                                           count,
                                           outbuf,
                                           comm->c_coll.coll_agreement_module);
        if( ret != MPI_SUCCESS && ret != MPI_ERR_PROC_FAILED )
            goto cleanup;
    } while( ret == MPI_ERR_PROC_FAILED );

 cleanup:
    OBJ_RELEASE(failed_group);
    return ret;
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
