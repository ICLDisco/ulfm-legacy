/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2011-2013 The University of Tennessee and The University
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
    ompi_communicator_t *comm = NULL;
    ompi_proc_t *ompi_proc_peer = NULL;
    orte_process_name_t true_sender;
    int to_revoke[2], ret, proc_rank;
    orte_std_cntr_t count;

    /* Get the true sender from the message */
    count = 1;
    ret = opal_dss.unpack(buffer, &(true_sender), &count, ORTE_NAME);
    if( OMPI_SUCCESS != ret ) {
        return;
    }

    /* Get the 'cid' and the epoch of the communicator to be revoked from the message */
    count = 2;
    ret = opal_dss.unpack(buffer, to_revoke, &count, OPAL_INT);
    if( OMPI_SUCCESS != ret ){
        return;
    }

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ompi: comm_revoke: Recv: %s Asked to revoke communicator %d:%d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(&true_sender),
                         to_revoke[0], to_revoke[1] ));

    /*
     * Find the communicator
     */
    comm = (ompi_communicator_t *)opal_pointer_array_get_item(&ompi_mpi_communicators, to_revoke[0]);
    if( NULL == comm || to_revoke[0] != (int)(comm->c_contextid) ) {
        opal_output(0, "%s ompi: comm_revoke: Error: Could not find the communicator with CID %d:%d",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), to_revoke[0], to_revoke[1] );
        return;
    }
    if( to_revoke[1] != (int)comm->epoch ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ompi: comm_revoke: Info: Receive a late revoke order for the communicator with CID %3d:%d when we are now at %d:%d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), to_revoke[0], to_revoke[1],
                             (int)comm->c_contextid, comm->epoch));
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
                                 "%s ompi: comm_revoke: Recv: %s Revoked a different communicator %d:%d - Ignore ",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&true_sender),
                                 to_revoke[0], to_revoke[1] ));
            return;
        }
    }

    /*
     * Revoke the communicator
     */
    if( comm->comm_revoked ) {
        OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                             "%s ompi: comm_revoke: Recv: Asked to revoke communicator %d:%d - Already revoked",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), to_revoke[0], to_revoke[1] ));
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

    if (OMPI_SUCCESS != (ret = opal_dss.pack(&buffer, &(comm->epoch), 1, OPAL_INT))) {
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

