/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2015       The University of Tennessee and The University
 *                          of Tennessee Research Foundation.  All rights
 *                          reserved.
 *
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 */

#include "ompi_config.h"

#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include "opal/class/opal_pointer_array.h"

#include "orte/util/name_fns.h"
#include "orte/util/error_strings.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/errmgr/base/base.h"
#include "orte/mca/plm/plm_types.h"

#include "ompi/communicator/communicator.h"
#include "ompi/request/request.h"
#include "ompi/runtime/params.h"

#include "ompi/errhandler/errhandler.h"
#include "ompi/errhandler/errhandler_predefined.h"

#if OPAL_ENABLE_FT_MPI

/*
 * Local variables and functions
 */
static int ompi_errmgr_rte_callback(orte_process_name_t proc, orte_proc_state_t state);

/*
 * Interface Functions
 */
int ompi_errhandler_internal_rte_init(void)
{
    int ret, exit_status = OMPI_SUCCESS;

    /*
     * Register to get a callback when a process fails
     */
    if( OMPI_SUCCESS != (ret = orte_errmgr_base_app_reg_notify_callback(ompi_errmgr_rte_callback, NULL)) ) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    if( OMPI_SUCCESS != (ret = ompi_comm_init_failure_propagate()) ) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

 cleanup:
    return exit_status;
}

int ompi_errhandler_internal_rte_finalize(void)
{
    int ret, exit_status = OMPI_SUCCESS;

    /*
     * Deregister the process fail callback.
     */
    if( OMPI_SUCCESS != (ret = orte_errmgr_base_app_reg_notify_callback(NULL, NULL)) ) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    if( OMPI_SUCCESS != (ret = ompi_comm_finalize_failure_propagate()) ) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

 cleanup:
    return exit_status;
}

int ompi_errmgr_mark_failed_peer_fw(ompi_proc_t *ompi_proc, orte_proc_state_t state, int forward)
{
    int exit_status = OMPI_SUCCESS, max_num_comm = 0, i, proc_rank;
    ompi_communicator_t *comm = NULL;
    ompi_group_t* group = NULL;
    bool remote = false;

    /*
     * If we have already detected this error, ignore
     */
    if( !ompi_proc_is_active(ompi_proc) ) {
        goto cleanup;
    }

    OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                         "%s ompi: Process %s failed (state = %s).",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(&ompi_proc->proc_name),
                         orte_proc_state_to_str(state) ));

    /*
     * Process State:
     * Update process state to failed
     */
    ompi_proc_mark_as_failed( ompi_proc );

    /*
     * Communicator State:
     * Let them know about the failure.
     */
    max_num_comm = opal_pointer_array_get_size(&ompi_mpi_communicators);
    for( i = 0; i < max_num_comm; ++i ) {
        comm = (ompi_communicator_t *)opal_pointer_array_get_item(&ompi_mpi_communicators, i);
        if( NULL == comm ) {
            continue;
        }

        /*
         * Look in both the local and remote group for this process
         */
        proc_rank = ompi_group_peer_lookup_id(comm->c_local_group, ompi_proc);
        remote = false;
        if( (proc_rank < 0) && (comm->c_local_group != comm->c_remote_group) ) {
            proc_rank = ompi_group_peer_lookup_id(comm->c_remote_group, ompi_proc);
            remote = true;
        }
        if( proc_rank < 0 )
            continue;  /* Not in this communicator, continue */

        /* Notify the communicator to update as necessary */
        ompi_comm_set_rank_failed(comm, proc_rank, remote);

        if( NULL == group ) {  /* Build the group with the failed process */
            (void)ompi_group_incl((remote ? comm->c_remote_group : comm->c_local_group),
                                  1,
                                  &proc_rank,
                                  &group);
        }
        OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                             "%s ompi: Process %s is in comm (%d) with rank %d. (%2d of %2d / %2d of %2d) [%s]",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&ompi_proc->proc_name),
                             comm->c_contextid,
                             proc_rank,
                             ompi_comm_num_active_local(comm),
                             ompi_group_size(comm->c_local_group),
                             ompi_comm_num_active_remote(comm),
                             ompi_group_size(comm->c_remote_group),
                             (OMPI_ERRHANDLER_TYPE_PREDEFINED == comm->errhandler_type ? "P" :
                              (OMPI_ERRHANDLER_TYPE_COMM == comm->errhandler_type ? "C" :
                               (OMPI_ERRHANDLER_TYPE_WIN == comm->errhandler_type ? "W" :
                                (OMPI_ERRHANDLER_TYPE_FILE == comm->errhandler_type ? "F" : "U") ) ) )
                             ));
    }

    /*
     * Group State: Add the failed process to the global group of failed processes.
     */
    if( group != NULL ) {
        ompi_group_t* old_failed = ompi_group_all_failed_procs;
        (void)ompi_group_union(ompi_group_all_failed_procs,
                               group,
                               &ompi_group_all_failed_procs);
        OBJ_RELEASE(old_failed);
    }
    /*
     * Point-to-Point:
     * Let the active request know of the process state change.
     * The wait function has a check, so all we need to do here is
     * signal it so it will check again.
     */
    OPAL_THREAD_LOCK(&ompi_request_lock);
    opal_condition_signal(&ompi_request_cond);
    OPAL_THREAD_UNLOCK(&ompi_request_lock);

    if( forward ) {
        ompi_comm_failure_propagate(&ompi_mpi_comm_world.comm, ompi_proc, state);
    }

    /*
     * Flush modex information?
     */

    /*
     * Collectives:
     * Flush collective P2P channels?
     */

 cleanup:
    return exit_status;
}

/*
 * Local Functions
 */
static int ompi_errmgr_rte_callback(orte_process_name_t proc, orte_proc_state_t state)
{
    ompi_proc_t *ompi_proc = NULL;

    /* Find the ompi_proc_t */
    if( NULL == (ompi_proc = ompi_proc_find(&proc)) ) {
        /** TODO: RACE CONDITION -- This may be the notification for a process we don't know about
         *        yet -- We should keep this information for later notification / check when
         *        adding this proc
         */
        return OMPI_SUCCESS; /** Ignore if this is an unknown processor: it can happen and should
                              *   not interrupt the loop in caller
                              */
    }

    return ompi_errmgr_mark_failed_peer(ompi_proc, state);
}

#endif /* OPAL_ENABLE_FT_MPI */
