/*
 * Copyright (c) 2004-2010 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2010 The University of Tennessee and The University
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
#include "ompi/request/grequest.h"

#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"
#include "ompi/runtime/params.h"

#include "opal/runtime/opal_cr.h"
#include "ompi/mca/crcp/crcp.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/base/pml_base_request.h"

#define REQUEST_IS_COLLECTIVE(req) ( ((req)->req_tag <= MCA_COLL_BASE_TAG_MIN) && ((req)->req_tag >= MCA_COLL_BASE_TAG_MAX_PRE_AGREEMENT) )

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
     * Toggle 'off' the MPI_ANY_SOURCE MPI_ERR_PENDING flag
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
     * If so (unless we are in agreement) this should fail.
     */
    if( ompi_comm_is_revoked(req->req_mpi_object.comm) &&
        (req->req_tag >= MPI_ANY_TAG || REQUEST_IS_COLLECTIVE(req) ) ) {
        /* Do not set req->req_status.MPI_SOURCE */
        req->req_status.MPI_ERROR  = MPI_ERR_INVALIDATED;
        if( !req->req_complete ) {
            ompi_request_cancel(req);
            ompi_request_complete(req, false);
        }
        req->req_complete = true;

        opal_output_verbose(10, ompi_ftmpi_output_handle,
                            "%s ompi_request_state_ok: Communicator has been revoked!",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        return false;
    }
    /*
     * Point-to-Point Check:
     * If the request is -not- part of a collective then it is point-to-point.
     * Only need to check the specific peer target/source.
     */
    else if( req->req_tag >= 0 ||
             MPI_ANY_TAG == req->req_tag ||
             !REQUEST_IS_COLLECTIVE(req) ) {
        if( req->req_peer == MPI_PROC_NULL ) {
            return true;
        }
        /* The call below handles the MPI_ANY_SOURCE case as well as the
         * directed case.
         */
        if( !ompi_comm_is_proc_active( req->req_mpi_object.comm, req->req_peer,
                                       OMPI_COMM_IS_INTRA(req->req_mpi_object.comm) ) ) {
            /*
             * MPI_ANY_SOURCE issues are marked as MPI_ERR_PENDING and -not-
             * removed from the message queue. Keep the request active, so the
             * user can keep waiting on it.
             */
            if( MPI_ANY_SOURCE == req->req_peer ) {
                /* Do not set req->req_status.MPI_SOURCE */
                req->req_any_source_pending = true;
            }
            /*
             * Directed peer errors are always critical, and return in error.
             */
            else {
                req->req_status.MPI_SOURCE = req->req_peer;
                req->req_status.MPI_ERROR  = MPI_ERR_PROC_FAILED;
                if( !req->req_complete ) {
                    ompi_request_cancel(req);
                    ompi_request_complete(req, false);
                }
                req->req_complete = true;
            }

            opal_output_verbose(10, ompi_ftmpi_output_handle,
                                "%s ompi_request_state_ok: Rank %3d in communicator (%3d) failed - Ret %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                req->req_status.MPI_SOURCE,
                                req->req_mpi_object.comm->c_contextid,
                                (MPI_ERR_PROC_FAILED == req->req_status.MPI_ERROR ? "MPI_ERR_PROC_FAILED" : "MPI_ERR_PENDING") );
            return false;
        }
    }
    /*
     * Collectives Check:
     * If the request is part of a collective, then the whole communicator
     * must be ok to continue. If not then return first failed process
     */
    else if( !ompi_comm_are_collectives_enabled(req->req_mpi_object.comm) ) {
        /* Return the last process known to have failed, may not have been the
         * first to cause the collectives to be disabled.
         */
        req->req_status.MPI_SOURCE = req->req_peer;
        req->req_status.MPI_ERROR  = MPI_ERR_PROC_FAILED;
        if( !req->req_complete ) {
            ompi_request_cancel(req);
            ompi_request_complete(req, false);
        }
        req->req_complete = true;

        opal_output_verbose(10, ompi_ftmpi_output_handle,
                            "%s ompi_request_state_ok: Request is part of a collective, and some process died. (rank %3d)",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), req->req_status.MPI_SOURCE );

        return false;
    }

    return true;
}
