/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2008 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010      Oracle and/or its affiliates.  All rights reserved.
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "ompi_config.h"
#include "ompi/constants.h"
#include "ompi/request/request.h"
#include "ompi/errhandler/errcode.h"

#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"
#include "ompi/runtime/params.h"

#include "opal/runtime/opal_cr.h"
#include "ompi/mca/crcp/crcp.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/base/pml_base_request.h"


/**************************
 * Support Routines
 **************************/
bool ompi_request_state_ok(ompi_request_t *req)
{
#if OPAL_ENABLE_DEBUG
    /*
     * Sanity check
     */
    if( NULL == req->req_mpi_object.comm ) {
        opal_output(0,
                    "%s ompi_request_state_ok: Warning: Communicator is NULL - Should not happen!",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) );
        return true;
    }
#endif /* OPAL_ENABLE_DEBUG */

    if( !ompi_ftmpi_enabled ) {
        return true;
    }

    /*
     * Toggle 'off' the MPI_ANY_SOURCE MPI_ERR_PROC_FAILED_PENDING flag
     */
    req->req_any_source_pending = false;

    /*
     * If the request is complete, then just skip it
     */
    if( req->req_complete ) {
        return true;
    }

    /*
     * Has this communicator been 'revoked'?
     *
     * If so unless we are in the FT part (propagate revoke, agreement or
     * shrink) this should fail.
     */
    if( OPAL_UNLIKELY(ompi_comm_is_revoked(req->req_mpi_object.comm) && !ompi_request_tag_is_ft(req->req_tag)) ) {
        /* Do not set req->req_status.MPI_SOURCE */
        req->req_status.MPI_ERROR  = MPI_ERR_REVOKED;

        opal_output_verbose(10, ompi_ftmpi_output_handle,
                            "%s ompi_request_state_ok: %p Communicator %s(%d) has been revoked!",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), (void*)req,
                            req->req_mpi_object.comm->c_name, req->req_mpi_object.comm->c_contextid);
        goto return_with_error;
    }

    /* Corner-cases: two processes that can't fail (NULL and myself) */
    if((req->req_peer == MPI_PROC_NULL) ||
       (OMPI_COMM_IS_INTRA(req->req_mpi_object.comm) && req->req_peer == req->req_mpi_object.comm->c_local_group->grp_my_rank)) {
        return true;
    }

    /* If any_source but not FT related then the request is always marked for return */
    if( OPAL_UNLIKELY(MPI_ANY_SOURCE == req->req_peer && !ompi_comm_is_any_source_enabled(req->req_mpi_object.comm)) ) {
        if( !ompi_request_tag_is_ft(req->req_tag) ) {
            req->req_status.MPI_ERROR  = MPI_ERR_PROC_FAILED_PENDING;
            /* If it is a probe/mprobe, escalate the error */
            if( (MCA_PML_REQUEST_MPROBE == ((mca_pml_base_request_t*)req)->req_type) ||
                (MCA_PML_REQUEST_PROBE == ((mca_pml_base_request_t*)req)->req_type) ) {
                req->req_status.MPI_ERROR  = MPI_ERR_PROC_FAILED;
            }
            req->req_any_source_pending = true;
            opal_output_verbose(10, ompi_ftmpi_output_handle,
                                "%s ompi_request_state_ok: Request %p in comm %s(%d) peer ANY_SOURCE %s!",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), (void*)req,
                                req->req_mpi_object.comm->c_name, req->req_mpi_object.comm->c_contextid,
                                ompi_mpi_errnum_get_string(req->req_status.MPI_ERROR));
            goto return_with_error;
        }
    }

    /* Any type of request with a dead process must be terminated with error */
    if( OPAL_UNLIKELY(!ompi_comm_is_proc_active(req->req_mpi_object.comm, req->req_peer,
                                  OMPI_COMM_IS_INTER(req->req_mpi_object.comm))) ) {
        req->req_status.MPI_SOURCE = req->req_peer;
        req->req_status.MPI_ERROR  = MPI_ERR_PROC_FAILED;
        if( MPI_ANY_SOURCE == req->req_peer ) {
            req->req_any_source_pending = true;
        }

        opal_output_verbose(10, ompi_ftmpi_output_handle,
                            "%s ompi_request_state_ok: Request %p in comm %s(%d) peer %3d failed - Ret %s",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), (void*)req,
                            req->req_mpi_object.comm->c_name, req->req_mpi_object.comm->c_contextid,
                            req->req_status.MPI_SOURCE,
                            ompi_mpi_errnum_get_string(req->req_status.MPI_ERROR));
        goto return_with_error;
    }

    /*
     * Collectives Check:
     * If the request is part of a collective, then the whole communicator
     * must be ok to continue. If not then return first failed process
     */
    if( OPAL_UNLIKELY(ompi_comm_coll_revoked(req->req_mpi_object.comm) &&
                      ompi_request_tag_is_collective(req->req_tag)) ) {
        /* Return the last process known to have failed, may not have been the
         * first to cause the collectives to be disabled.
         */
        req->req_status.MPI_SOURCE = req->req_peer;
        req->req_status.MPI_ERROR  = MPI_ERR_PROC_FAILED;

        opal_output_verbose(10, ompi_ftmpi_output_handle,
                            "%s ompi_request_state_ok: Request is part of a collective, and some process died. (rank %3d)",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), req->req_status.MPI_SOURCE );
        goto return_with_error;
    }

    return true;

 return_with_error:
    if( MPI_ERR_PROC_FAILED_PENDING != req->req_status.MPI_ERROR ) {
        int cancelled = req->req_status._cancelled;
        opal_output_verbose(10, ompi_ftmpi_output_handle,
                            "%s ompi_request_state_ok: Request %p cancelled due to completion with error %d\n", 
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), (void*)req, req->req_status.MPI_ERROR);
#if 0
        { int btsize=32; void*bt[32]={NULL}; btsize=backtrace(bt,btsize);
          backtrace_symbols_fd(bt,btsize, ompi_ftmpi_output_handle);
        }
        mca_pml.pml_dump(req->req_mpi_object.comm, ompi_ftmpi_output_handle);
#endif
        /* Cancel and force completion immmediately
         * However, for Revoked and Collective error we can't complete 
         * with an error before the buffer is unpinned (i.e. the request gets
         * wire cancelled).
         */
        ompi_request_cancel(req);
        req->req_status._cancelled = cancelled; /* This request is not user cancelled here, it is completed in error */
        return !req->req_complete; /* If this request is not complete yet, it is stil ok and needs more spinning */
    }
    return (MPI_SUCCESS == req->req_status.MPI_ERROR);
}
