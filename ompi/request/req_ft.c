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

static int ompi_request_ft_wait(ompi_request_t ** req_ptr,
                                ompi_status_public_t * status);
static int ompi_request_ft_wait_any(size_t count,
                                    ompi_request_t ** requests,
                                    int *index,
                                    ompi_status_public_t * status);
static int ompi_request_ft_wait_all( size_t count,
                                     ompi_request_t ** requests,
                                     ompi_status_public_t * statuses );
static int ompi_request_ft_wait_some(size_t count,
                                     ompi_request_t ** requests,
                                     int * outcount,
                                     int * indices,
                                     ompi_status_public_t * statuses);
static int ompi_request_ft_test( ompi_request_t ** rptr,
                                 int *completed,
                                 ompi_status_public_t * status );
static int ompi_request_ft_test_any(size_t count,
                                    ompi_request_t ** requests,
                                    int *index,
                                    int *completed,
                                    ompi_status_public_t * status);
static int ompi_request_ft_test_all(size_t count,
                                    ompi_request_t ** requests,
                                    int *completed,
                                    ompi_status_public_t * statuses);
static int ompi_request_ft_test_some(size_t count,
                                     ompi_request_t ** requests,
                                     int * outcount,
                                     int * indices,
                                     ompi_status_public_t * statuses);

#define REQUEST_IS_COLLECTIVE(req) ( ((req)->req_tag <= MCA_COLL_BASE_TAG_MIN) && ((req)->req_tag >= MCA_COLL_BASE_TAG_MAX_PRE_AGREEMENT) )

/**************************
 * Support Routines
 **************************/
int ompi_request_ft_init(void)
{
    ompi_request_functions.req_wait      = ompi_request_ft_wait;
    ompi_request_functions.req_wait_all  = ompi_request_ft_wait_all;
    ompi_request_functions.req_wait_any  = ompi_request_ft_wait_any;
    ompi_request_functions.req_wait_some = ompi_request_ft_wait_some;

    ompi_request_functions.req_test      = ompi_request_ft_test;
    ompi_request_functions.req_test_all  = ompi_request_ft_test_all;
    ompi_request_functions.req_test_any  = ompi_request_ft_test_any;
    ompi_request_functions.req_test_some = ompi_request_ft_test_some;

    return OMPI_SUCCESS;
}

int ompi_request_ft_finalize(void)
{
    return OMPI_SUCCESS;
}

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
        req->req_status.MPI_SOURCE = req->req_mpi_object.comm->last_failed;
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
                req->req_status.MPI_SOURCE = req->req_mpi_object.comm->last_failed;
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
        req->req_status.MPI_SOURCE = req->req_mpi_object.comm->last_failed;
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

/**************************
 * Wait Routines
 **************************/
static int ompi_request_ft_wait(ompi_request_t ** req_ptr,
                                ompi_status_public_t * status)
{
    ompi_request_t *req = *req_ptr;

    ompi_request_wait_completion(req);

#if OPAL_ENABLE_FT_CR == 1
    OMPI_CRCP_REQUEST_COMPLETE(req);
#endif

    /* Special case for MPI_ANY_SOURCE */
    if( req->req_any_source_pending ) {
        return MPI_ERR_PENDING;
    }

    /* return status.  If it's a generalized request, we *have* to
       invoke the query_fn, even if the user procided STATUS_IGNORE.
       MPI-2:8.2. */
    if (OMPI_REQUEST_GEN == req->req_type) {
        ompi_grequest_invoke_query(req, &req->req_status);
    }
    if( MPI_STATUS_IGNORE != status ) {
        /* Do *NOT* set status->MPI_ERROR here!  See MPI-1.1 doc, sec
           3.2.5, p.22 */
        status->MPI_TAG    = req->req_status.MPI_TAG;
        status->MPI_SOURCE = req->req_status.MPI_SOURCE;
        OMPI_STATUS_SET_COUNT(&status->_ucount, &req->req_status._ucount);
        status->_cancelled = req->req_status._cancelled;
    }
    if( req->req_persistent ) {
        if( req->req_state == OMPI_REQUEST_INACTIVE ) {
            return OMPI_SUCCESS;
        }
        req->req_state = OMPI_REQUEST_INACTIVE;
        return req->req_status.MPI_ERROR;
    }

    /* If there was an error, don't free the request -- just return
       the single error. */
    if (MPI_SUCCESS != req->req_status.MPI_ERROR) {
        return req->req_status.MPI_ERROR;
    }

    /* If there's an error while freeing the request, assume that the
       request is still there.  Otherwise, Bad Things will happen
       later! */
    return ompi_request_free(req_ptr);
}


static int ompi_request_ft_wait_any(
    size_t count,
    ompi_request_t ** requests,
    int *index,
    ompi_status_public_t * status)
{
#if OMPI_ENABLE_PROGRESS_THREADS
    int c;
#endif
    size_t i=0, num_requests_null_inactive=0;
    int rc = OMPI_SUCCESS;
    int completed = -1;
    ompi_request_t **rptr=NULL;
    ompi_request_t *request=NULL;

#if OMPI_ENABLE_PROGRESS_THREADS
    /* poll for completion */
    OPAL_THREAD_ADD32(&opal_progress_thread_count,1);
    for (c = 0; completed < 0 && c < opal_progress_spin_count; c++) {
        rptr = requests;
        num_requests_null_inactive = 0;
        for (i = 0; i < count; i++, rptr++) {
            request = *rptr;
            /*
             * Check for null or completed persistent request.
             * For MPI_REQUEST_NULL, the req_state is always OMPI_REQUEST_INACTIVE
             */
            if( request->req_state == OMPI_REQUEST_INACTIVE ) {
                num_requests_null_inactive++;
                continue;
            }
            if (true == request->req_complete) {
                completed = i;
                OPAL_THREAD_ADD32(&opal_progress_thread_count,-1);
                goto finished;
            }
        }
        if( num_requests_null_inactive == count ) {
            OPAL_THREAD_ADD32(&opal_progress_thread_count,-1);
            goto finished;
        }
        opal_progress();
    }
    OPAL_THREAD_ADD32(&opal_progress_thread_count,-1);
#endif

    /* give up and sleep until completion */
    OPAL_THREAD_LOCK(&ompi_request_lock);
    ompi_request_waiting++;
    do {
        rptr = requests;
        num_requests_null_inactive = 0;
        for (i = 0; i < count; i++, rptr++) {
            request = *rptr;

            /* Sanity test */
            if( NULL == request) {
                continue;
            }

            /*
             * Check for null or completed persistent request.
             * For MPI_REQUEST_NULL, the req_state is always OMPI_REQUEST_INACTIVE.
             */
            if( request->req_state == OMPI_REQUEST_INACTIVE ) {
                num_requests_null_inactive++;
                continue;
            }
            if (request->req_complete == true) {
                completed = i;
                break;
            }
            if (request->req_complete != true &&
                request->req_state == OMPI_REQUEST_ACTIVE) {
                if( !ompi_request_state_ok(request) ) {
                    /* Special case for MPI_ANY_SOURCE */
                    if( request->req_any_source_pending ) {
                        *index = i;
                        return MPI_ERR_PENDING;
                    }
                    completed = i;
                    break;
                }
            }
        }
        if(num_requests_null_inactive == count) {
            break;
        }
        if (completed < 0) {
            opal_condition_wait(&ompi_request_cond, &ompi_request_lock);
        }
    } while (completed < 0);
    ompi_request_waiting--;
    OPAL_THREAD_UNLOCK(&ompi_request_lock);

#if OMPI_ENABLE_PROGRESS_THREADS
finished:
#endif  /* OMPI_ENABLE_PROGRESS_THREADS */

    if(num_requests_null_inactive == count) {
        *index = MPI_UNDEFINED;
        if (MPI_STATUS_IGNORE != status) {
            OMPI_STATUS_SET(status, &ompi_status_empty);
        }
    } else {
        assert( true == request->req_complete );
        /* Per note above, we have to call gen request query_fn even
           if STATUS_IGNORE was provided */
        if (OMPI_REQUEST_GEN == request->req_type) {
            rc = ompi_grequest_invoke_query(request, &request->req_status);
        }
        if (MPI_STATUS_IGNORE != status) {
            /* Do *NOT* set status->MPI_ERROR here!  See MPI-1.1 doc,
               sec 3.2.5, p.22 */
            int old_error = status->MPI_ERROR;
            OMPI_STATUS_SET(status, &request->req_status);
            status->MPI_ERROR = old_error;
        }
        rc = request->req_status.MPI_ERROR;
        if( request->req_persistent ) {
            request->req_state = OMPI_REQUEST_INACTIVE;
        } else if (MPI_SUCCESS == rc) {
            /* Only free the request if there is no error on it */
            /* If there's an error while freeing the request,
               assume that the request is still there.  Otherwise,
               Bad Things will happen later! */
            rc = ompi_request_free(rptr);
        }
        *index = completed;
    }

#if OPAL_ENABLE_FT_CR == 1
    if( opal_cr_is_enabled) {
        rptr = requests;
        for (i = 0; i < count; i++, rptr++) {
            request = *rptr;
            if( true == request->req_complete) {
                OMPI_CRCP_REQUEST_COMPLETE(request);
            }
        }
    }
#endif

    return rc;
}


static int ompi_request_ft_wait_all( size_t count,
                           ompi_request_t ** requests,
                           ompi_status_public_t * statuses )
{
    size_t completed = 0, i, failed = 0;
    ompi_request_t **rptr;
    ompi_request_t *request;
    int mpi_error = OMPI_SUCCESS;

    rptr = requests;
    for (i = 0; i < count; i++) {
        request = *rptr++;
        if (request->req_complete == true) {
            if( OPAL_UNLIKELY( MPI_SUCCESS != request->req_status.MPI_ERROR ) ) {
                failed++;
            }
            completed++;
        }
    }

    if( failed > 0 ) {
        goto finish;
    }

    /* if all requests have not completed -- defer acquiring lock
     * unless required
     */
    if (completed != count) {
        /*
         * acquire lock and test for completion - if all requests are
         * not completed pend on condition variable until a request
         * completes
         */
        OPAL_THREAD_LOCK(&ompi_request_lock);
        ompi_request_waiting++;
#if OPAL_ENABLE_MULTI_THREADS
        /*
         * confirm the status of the pending requests. We have to do it before
         * taking the condition or otherwise we can miss some requests completion (the
         * one that happpens between our initial test and the aquisition of the lock).
         */
        rptr = requests;
        for( completed = i = 0; i < count; i++ ) {
            request = *rptr++;
            if (request->req_complete == true) {
                if( MPI_SUCCESS != request->req_status.MPI_ERROR ) {
                    failed++;
                }
                completed++;
            }
        }
        if( failed > 0 ) {
            ompi_request_waiting--;
            OPAL_THREAD_UNLOCK(&ompi_request_lock);
            goto finish;
        }
#endif  /* OPAL_ENABLE_MULTI_THREADS */
        while( completed != count ) {
            /* check number of pending requests */
            size_t start = ompi_request_completed;
            size_t pending = count - completed;
            size_t start_failed = ompi_request_failed;
            /*
             * wait until at least pending requests complete
             */
            while (pending > ompi_request_completed - start) {
                /*
                 * Check for failed requests. If one request fails, then
                 * this operation completes in error marking the remaining
                 * requests as PENDING.
                 */
                if( OPAL_UNLIKELY( 0 < (ompi_request_failed - start_failed) ) ) {
                    failed += (ompi_request_failed - start_failed);
                    ompi_request_waiting--;
                    OPAL_THREAD_UNLOCK(&ompi_request_lock);
                    goto finish;
                }

                /* Check for dead requests
                 * JJH - Find a better way to process this
                 */
                rptr = requests;
                for(i = 0; i < count; ++i ) {
                    request = *rptr++;
                    if (request->req_complete != true &&
                        request->req_state == OMPI_REQUEST_ACTIVE) {
                        if( !ompi_request_state_ok(request) ) {
                            ++failed;
                        }
                    }
                }
                /* Check failed here so that we can mark as many failed
                 * requests as such when returning */
                if( OPAL_UNLIKELY( 0 < failed ) ) {
                    ompi_request_waiting--;
                    OPAL_THREAD_UNLOCK(&ompi_request_lock);
                    goto finish;
                }

                opal_condition_wait(&ompi_request_cond, &ompi_request_lock);
            }
            /*
             * confirm that all pending operations have completed.
             */
            rptr = requests;
            for( completed = i = 0; i < count; i++ ) {
                request = *rptr++;
                if (request->req_complete == true) {
                    if( MPI_SUCCESS != request->req_status.MPI_ERROR ) {
                        failed++;
                    }
                    completed++;
                }
            }
        }
        ompi_request_waiting--;
        OPAL_THREAD_UNLOCK(&ompi_request_lock);
    }

#if OPAL_ENABLE_FT_CR == 1
    if( opal_cr_is_enabled) {
        rptr = requests;
        for (i = 0; i < count; i++, rptr++) {
            request = *rptr;
            if( true == request->req_complete) {
                OMPI_CRCP_REQUEST_COMPLETE(request);
            }
        }
    }
#endif

 finish:
    rptr = requests;
    if (MPI_STATUSES_IGNORE != statuses) {
        /* fill out status and free request if required */
        for( i = 0; i < count; i++, rptr++ ) {
            request = *rptr;

            /*
             * Assert only if no requests were failed.
             * Since some may still be pending.
             */
            if( 0 >= failed ) {
                assert( true == request->req_complete );
            }

            if (OMPI_REQUEST_GEN == request->req_type) {
                ompi_grequest_invoke_query(request, &request->req_status);
            }

            OMPI_STATUS_SET(&statuses[i], &request->req_status);

            /*
             * Per MPI 2.2 p 60:
             * Allows requests to be marked as MPI_ERR_PENDING if they are
             * "neither failed nor completed." Which can only happen if
             * there was an error in one of the other requests.
             */
            if( OPAL_UNLIKELY(0 < failed) ) {
                if( !request->req_complete ) {
                    statuses[i].MPI_ERROR = MPI_ERR_PENDING;
                    mpi_error = MPI_ERR_IN_STATUS;
                    continue;
                }
            }

            if( request->req_persistent ) {
                request->req_state = OMPI_REQUEST_INACTIVE;
            } else {
                /* Only free the request if there is no error on it */
                if (MPI_SUCCESS == request->req_status.MPI_ERROR) {
                    /* If there's an error while freeing the request,
                       assume that the request is still there.
                       Otherwise, Bad Things will happen later! */
                    int tmp = ompi_request_free(rptr);
                    if (OMPI_SUCCESS == mpi_error && OMPI_SUCCESS != tmp) {
                        mpi_error = tmp;
                    }
                }
            }
            if( statuses[i].MPI_ERROR != OMPI_SUCCESS) {
                mpi_error = MPI_ERR_IN_STATUS;
            }
        }
    } else {
        /* free request if required */
        for( i = 0; i < count; i++, rptr++ ) {
            int rc;
            request = *rptr;

            /*
             * Assert only if no requests were failed.
             * Since some may still be pending.
             */
            if( 0 >= failed ) {
                assert( true == request->req_complete );
            } else {
                /* If the request is still pending due to a failed request
                 * then skip it in this loop.
                 */
                if( !request->req_complete ) {
                    mpi_error = MPI_ERR_IN_STATUS;
                    continue;
                }
            }

            /* Per note above, we have to call gen request query_fn
               even if STATUSES_IGNORE was provided */
            if (OMPI_REQUEST_GEN == request->req_type) {
                rc = ompi_grequest_invoke_query(request, &request->req_status);
            }
            if( request->req_state == OMPI_REQUEST_INACTIVE ) {
                rc = ompi_status_empty.MPI_ERROR;
            } else {
                rc = request->req_status.MPI_ERROR;
            }
            if( request->req_persistent ) {
                request->req_state = OMPI_REQUEST_INACTIVE;
            } else if (MPI_SUCCESS == rc) {
                /* Only free the request if there is no error on it */
                int tmp = ompi_request_free(rptr);
                if (OMPI_SUCCESS == mpi_error && OMPI_SUCCESS != tmp) {
                    mpi_error = tmp;
                }
            }
            /*
             * Per MPI 2.2 p34:
             * "It is possible for an MPI function to return MPI_ERR_IN_STATUS
             *  even when MPI_STATUS_IGNORE or MPI_STATUSES_IGNORE has been
             *  passed to that function."
             * So we should do so here as well.
             */
            if( OMPI_SUCCESS == mpi_error && rc != OMPI_SUCCESS) {
                mpi_error = MPI_ERR_IN_STATUS;
            }
        }
    }
    return mpi_error;
}


static int ompi_request_ft_wait_some(
    size_t count,
    ompi_request_t ** requests,
    int * outcount,
    int * indices,
    ompi_status_public_t * statuses)
{
#if OMPI_ENABLE_PROGRESS_THREADS
    int c;
#endif
    size_t i, num_requests_null_inactive=0, num_requests_done=0;
    int rc = MPI_SUCCESS;
    ompi_request_t **rptr=NULL;
    ompi_request_t *request=NULL;
    bool is_ok = true;

    *outcount = 0;
    for (i = 0; i < count; i++){
        indices[i] = 0;
    }

#if OMPI_ENABLE_PROGRESS_THREADS
    /* poll for completion */
    OPAL_THREAD_ADD32(&opal_progress_thread_count,1);
    for (c = 0; c < opal_progress_spin_count; c++) {
        rptr = requests;
        num_requests_null_inactive = 0;
        num_requests_done = 0;
        for (i = 0; i < count; i++, rptr++) {
            request = *rptr;
            /*
             * Check for null or completed persistent request.
             * For MPI_REQUEST_NULL, the req_state is always OMPI_REQUEST_INACTIVE
             */
            if (request->req_state == OMPI_REQUEST_INACTIVE ) {
                num_requests_null_inactive++;
                continue;
            }
            if (true == request->req_complete) {
                indices[i] = 1;
                num_requests_done++;
            }
        }
        if (num_requests_null_inactive == count ||
            num_requests_done > 0) {
            OPAL_THREAD_ADD32(&opal_progress_thread_count,-1);
            goto finished;
        }
        opal_progress();
    }
    OPAL_THREAD_ADD32(&opal_progress_thread_count,-1);
#endif

    /*
     * We only get here when outcount still is 0.
     * give up and sleep until completion
     */
    OPAL_THREAD_LOCK(&ompi_request_lock);
    ompi_request_waiting++;
    do {
        rptr = requests;
        num_requests_null_inactive = 0;
        num_requests_done = 0;
        for (i = 0; i < count; i++, rptr++) {
            request = *rptr;
            /*
             * Check for null or completed persistent request.
             * For MPI_REQUEST_NULL, the req_state is always OMPI_REQUEST_INACTIVE.
             */
            if( request->req_state == OMPI_REQUEST_INACTIVE ) {
                num_requests_null_inactive++;
                continue;
            }
            /* Check for dead requests due to process failure */
            if( !request->req_complete ) {
                is_ok = ompi_request_state_ok(request);
                /* Special case for MPI_ANY_SOURCE - Error managed below */
                if( !is_ok && request->req_any_source_pending ) {
                    indices[i] = 1;
                    num_requests_done++;
                }
            }
            if (request->req_complete == true) {
                indices[i] = 1;
                num_requests_done++;
            }
        }
        if (num_requests_null_inactive == count ||
            num_requests_done > 0) {
            break;
        }
        opal_condition_wait(&ompi_request_cond, &ompi_request_lock);
    } while (1);
    ompi_request_waiting--;
    OPAL_THREAD_UNLOCK(&ompi_request_lock);

#if OMPI_ENABLE_PROGRESS_THREADS
finished:
#endif  /* OMPI_ENABLE_PROGRESS_THREADS */

#if OPAL_ENABLE_FT_CR == 1
    if( opal_cr_is_enabled) {
        rptr = requests;
        for (i = 0; i < count; i++, rptr++) {
            request = *rptr;
            if( true == request->req_complete) {
                OMPI_CRCP_REQUEST_COMPLETE(request);
            }
        }
    }
#endif

    if(num_requests_null_inactive == count) {
        *outcount = MPI_UNDEFINED;
    } else {
        /*
         * Compress the index array.
         */
        for (i = 0, num_requests_done = 0; i < count; i++) {
            if (0 != indices[i]) {
                indices[num_requests_done++] = i;
            }
        }

        *outcount = num_requests_done;

        for (i = 0; i < num_requests_done; i++) {
            request = requests[indices[i]];

            /* Special case for MPI_ANY_SOURCE */
            if( request->req_any_source_pending ) {
                if (MPI_STATUSES_IGNORE != statuses) {
                    OMPI_STATUS_SET(&statuses[i], &request->req_status);
                    statuses[i].MPI_ERROR = MPI_ERR_PENDING;
                }
                rc = MPI_ERR_IN_STATUS;
                continue;
            }

            assert( true == request->req_complete );
            /* return status */
            /* Per note above, we have to call gen request query_fn even
               if STATUS_IGNORE was provided */
            if (OMPI_REQUEST_GEN == request->req_type) {
                ompi_grequest_invoke_query(request, &request->req_status);
            }
            if (MPI_STATUSES_IGNORE != statuses) {
                OMPI_STATUS_SET(&statuses[i], &request->req_status);
            }

            if (MPI_SUCCESS != request->req_status.MPI_ERROR) {
                rc = MPI_ERR_IN_STATUS;
            }

            if( request->req_persistent ) {
                request->req_state = OMPI_REQUEST_INACTIVE;
            } else {
                /* Only free the request if there was no error */
                if (MPI_SUCCESS == request->req_status.MPI_ERROR) {
                    int tmp;
                    tmp = ompi_request_free(&(requests[indices[i]]));
                    if (OMPI_SUCCESS != tmp) {
                        return tmp;
                    }
                }
            }
        }
    }
    return rc;
}


/**************************
 * Test Routines
 **************************/
static int ompi_request_ft_test( ompi_request_t ** rptr,
                                 int *completed,
                                 ompi_status_public_t * status )
{
    ompi_request_t *request = *rptr;
    bool is_ok = true;
#if OMPI_ENABLE_PROGRESS_THREADS == 0
    int do_it_once = 0;

 recheck_request_status:
#endif
    opal_atomic_mb();
    if( request->req_state == OMPI_REQUEST_INACTIVE ) {
        *completed = true;
        if (MPI_STATUS_IGNORE != status) {
            OMPI_STATUS_SET(status, &ompi_status_empty);
        }
        return OMPI_SUCCESS;
    }

    /* Check for dead requests due to process failure */
    if( !request->req_complete ) {
        is_ok = ompi_request_state_ok(request);

        /* Special case for MPI_ANY_SOURCE */
        if( !is_ok && request->req_any_source_pending ) {
            *completed = false;
            return MPI_ERR_PENDING;
        }
    }

    if (request->req_complete) {
        OMPI_CRCP_REQUEST_COMPLETE(request);

        *completed = true;
        /* For a generalized request, we *have* to call the query_fn
           if it completes, even if the user provided
           STATUS_IGNORE.  See MPI-2:8.2. */
        if (OMPI_REQUEST_GEN == request->req_type) {
            ompi_grequest_invoke_query(request, &request->req_status);
            if (MPI_STATUS_IGNORE != status) {
                int old_error = status->MPI_ERROR;
                OMPI_STATUS_SET(status, &request->req_status);
                status->MPI_ERROR = old_error;
            }
        } else if (MPI_STATUS_IGNORE != status) {
            /* Do *NOT* set a new value for status->MPI_ERROR here!
               See MPI-1.1 doc, sec 3.2.5, p.22 */
            int old_error = status->MPI_ERROR;
            OMPI_STATUS_SET(status, &request->req_status);
            status->MPI_ERROR = old_error;
        }
        if( request->req_persistent ) {
            request->req_state = OMPI_REQUEST_INACTIVE;
            return request->req_status.MPI_ERROR;
        }
        /* If there was an error, don't free the request -- just
           return the single error. */
        if (MPI_SUCCESS != request->req_status.MPI_ERROR) {
            return request->req_status.MPI_ERROR;
        }
        /* If there's an error on the request, assume that the request
           is still there.  Otherwise, Bad Things will happen
           later! */
        return ompi_request_free(rptr);
    }

#if OMPI_ENABLE_PROGRESS_THREADS == 0
    if( 0 == do_it_once ) {
        /**
         * If we run the opal_progress then check the status of the request before
         * leaving. We will call the opal_progress only once per call.
         */
        opal_progress();
        do_it_once++;
        goto recheck_request_status;
    }
#endif
    *completed = false;
    return OMPI_SUCCESS;
}

static int ompi_request_ft_test_any(
    size_t count,
    ompi_request_t ** requests,
    int *index,
    int *completed,
    ompi_status_public_t * status)
{
    size_t i;
    size_t num_requests_null_inactive = 0;
    ompi_request_t **rptr;
    ompi_request_t *request;
    bool is_ok = true;

    opal_atomic_mb();
    rptr = requests;
    for (i = 0; i < count; i++, rptr++) {
        request = *rptr;
        if( request->req_state == OMPI_REQUEST_INACTIVE ) {
            num_requests_null_inactive++;
            continue;
        }

        /* Check for dead requests due to process failure */
        if( !request->req_complete ) {
            is_ok = ompi_request_state_ok(request);
            /* Special case for MPI_ANY_SOURCE */
            if( !is_ok && request->req_any_source_pending ) {
                *index = i;
                *completed = false;
                return MPI_ERR_PENDING;
            }
        }

        if( request->req_complete ) {
            OMPI_CRCP_REQUEST_COMPLETE(request);

            *index = i;
            *completed = true;
            /* MPI 2:8.2 says that generalized requests always have
               the query function invoked in TEST* / WAIT*
               (#@$%@#$%!!! Would have been simpler to call it in
               GREQUEST_COMPLETE!), even if the user passed in
               STATUS_IGNORE */
            if (OMPI_REQUEST_GEN == request->req_type) {
                ompi_grequest_invoke_query(request, &request->req_status);
                if (MPI_STATUS_IGNORE != status) {
                    /* Do *NOT* set a new value for status->MPI_ERROR
                       here!  See MPI-1.1 doc, sec 3.2.5, p.22 */
                    int old_error = status->MPI_ERROR;
                    OMPI_STATUS_SET(status, &request->req_status);
                    status->MPI_ERROR = old_error;
                }
            } else if (MPI_STATUS_IGNORE != status) {
                /* Do *NOT* set a new value for status->MPI_ERROR
                   here!  See MPI-1.1 doc, sec 3.2.5, p.22 */
                int old_error = status->MPI_ERROR;
                OMPI_STATUS_SET(status, &request->req_status);
                status->MPI_ERROR = old_error;
            }

            if( request->req_persistent ) {
                request->req_state = OMPI_REQUEST_INACTIVE;
                return OMPI_SUCCESS;
            }
            /* If there is an error on the request, don't free it */
            if (MPI_SUCCESS != request->req_status.MPI_ERROR) {
                return request->req_status.MPI_ERROR;
            }
            /* If there's an error while freeing the request, assume
               that the request is still there.  Otherwise, Bad Things
               will happen later! */
            return ompi_request_free(rptr);
        }
    }

    /* Only fall through here if we found nothing */
    *index = MPI_UNDEFINED;
    if(num_requests_null_inactive != count) {
        *completed = false;
#if OMPI_ENABLE_PROGRESS_THREADS == 0
        opal_progress();
#endif
    } else {
        *completed = true;
        if (MPI_STATUS_IGNORE != status) {
            OMPI_STATUS_SET(status, &ompi_status_empty);
        }
    }
    return OMPI_SUCCESS;
}


static int ompi_request_ft_test_all(
    size_t count,
    ompi_request_t ** requests,
    int *completed,
    ompi_status_public_t * statuses)
{
    size_t i, rc = MPI_SUCCESS;
    ompi_request_t **rptr;
    size_t num_completed = 0;
    ompi_request_t *request;
    bool is_ok = true;

    opal_atomic_mb();
    rptr = requests;
    for (i = 0; i < count; i++, rptr++) {
        request = *rptr;

        /* Check for dead requests due to process failure */
        if( !request->req_complete ) {
            is_ok = ompi_request_state_ok(request);
            /* Special case for MPI_ANY_SOURCE */
            if( !is_ok && request->req_any_source_pending ) {
                if (MPI_STATUSES_IGNORE != statuses) {
                    OMPI_STATUS_SET(&statuses[i], &request->req_status);
                    statuses[i].MPI_ERROR = MPI_ERR_PENDING;
                }
                *completed = false;
                return MPI_ERR_IN_STATUS;
            }
        }

        if( request->req_state == OMPI_REQUEST_INACTIVE ||
            request->req_complete ) {
            OMPI_CRCP_REQUEST_COMPLETE(request);
            num_completed++;
        }
    }

    if (num_completed != count) {
        *completed = false;
#if OMPI_ENABLE_PROGRESS_THREADS == 0
        opal_progress();
#endif
        return OMPI_SUCCESS;
    }

    rptr = requests;
    *completed = true;

    if (MPI_STATUSES_IGNORE != statuses) {
        /* fill out completion status and free request if required */
        for( i = 0; i < count; i++, rptr++ ) {
            request  = *rptr;
            /*
             * If the request is OMPI_REQUEST_INACTIVE set the status
             * (either in the case of standard and persistent requests),
             * to the already initialized req_status.
             * Works also in the case of persistent request w/ MPI_PROC_NULL.
             */
            if( request->req_state == OMPI_REQUEST_INACTIVE ) {
                OMPI_STATUS_SET(&statuses[i], &request->req_status);
                continue;
            }
            if (OMPI_REQUEST_GEN == request->req_type) {
                ompi_grequest_invoke_query(request, &request->req_status);
            }
            OMPI_STATUS_SET(&statuses[i], &request->req_status);
            if( request->req_persistent ) {
                request->req_state = OMPI_REQUEST_INACTIVE;
                continue;
            }
            /* MPI-2:4.5.1 says that we can return MPI_ERR_IN_STATUS
               even if MPI_STATUSES_IGNORE was used.  Woot! */
            /* Only free the request if there was no error on it */
            if (MPI_SUCCESS == request->req_status.MPI_ERROR) {
                int tmp = ompi_request_free(rptr);
                if (tmp != OMPI_SUCCESS) {
                    return tmp;
                }
            } else {
                rc = MPI_ERR_IN_STATUS;
            }
        }
    } else {
        /* free request if required */
        for( i = 0; i < count; i++, rptr++ ) {
            request = *rptr;
            if( request->req_state == OMPI_REQUEST_INACTIVE) {
                continue;
            }
            /* See note above: if a generalized request completes, we
               *have* to call the query fn, even if STATUSES_IGNORE
               was supplied */
            if (OMPI_REQUEST_GEN == request->req_type) {
                ompi_grequest_invoke_query(request, &request->req_status);
            }
            if( request->req_persistent ) {
                request->req_state = OMPI_REQUEST_INACTIVE;
                continue;
            }
            /* Only free the request if there was no error */
            if (MPI_SUCCESS == request->req_status.MPI_ERROR) {
                int tmp = ompi_request_free(rptr);
                if (tmp != OMPI_SUCCESS) {
                    return tmp;
                }
            } else {
                rc = MPI_ERR_IN_STATUS;
            }
        }
    }

    return rc;
}


static int ompi_request_ft_test_some(
    size_t count,
    ompi_request_t ** requests,
    int * outcount,
    int * indices,
    ompi_status_public_t * statuses)
{
    size_t i, num_requests_null_inactive=0, num_requests_done = 0;
    int rc = OMPI_SUCCESS;
    ompi_request_t **rptr;
    ompi_request_t *request;
    bool is_ok = true;

    opal_atomic_mb();
    rptr = requests;
    for (i = 0; i < count; i++, rptr++) {
        request = *rptr;
        if (request->req_state == OMPI_REQUEST_INACTIVE) {
            num_requests_null_inactive++;
            continue;
        }

        /* Check for dead requests due to process failure */
        if( !request->req_complete ) {
            is_ok = ompi_request_state_ok(request);
            /* Special case for MPI_ANY_SOURCE - Error managed below */
            if( !is_ok && request->req_any_source_pending ) {
                indices[num_requests_done++] = i;
            }
        }

        if (true == request->req_complete) {
            OMPI_CRCP_REQUEST_COMPLETE(request);
            indices[num_requests_done++] = i;
        }
    }

    /*
     * If there are no active requests, no need to progress
     */
    if (num_requests_null_inactive == count) {
        *outcount = MPI_UNDEFINED;
        return OMPI_SUCCESS;
    }

    *outcount = num_requests_done;

    if (num_requests_done == 0) {
#if OMPI_ENABLE_PROGRESS_THREADS == 0
        opal_progress();
#endif
        return OMPI_SUCCESS;
    }

    /* fill out completion status and free request if required */
    for( i = 0; i < num_requests_done; i++) {
        request = requests[indices[i]];

        /* Special case for MPI_ANY_SOURCE */
        if( request->req_any_source_pending ) {
            if (MPI_STATUSES_IGNORE != statuses) {
                OMPI_STATUS_SET(&statuses[i], &request->req_status);
                statuses[i].MPI_ERROR = MPI_ERR_PENDING;
            }
            rc = MPI_ERR_IN_STATUS;
            continue;
        }

        if (MPI_STATUSES_IGNORE != statuses) {
            if (OMPI_REQUEST_GEN == request->req_type) {
                ompi_grequest_invoke_query(request, &request->req_status);
            }
            OMPI_STATUS_SET(&statuses[i], &request->req_status);
        }

        if (MPI_SUCCESS != request->req_status.MPI_ERROR) {
            rc = MPI_ERR_IN_STATUS;
        }

        if( request->req_persistent ) {
            request->req_state = OMPI_REQUEST_INACTIVE;
        } else {
            /* Only free the request if there was no error */
            if (MPI_SUCCESS == request->req_status.MPI_ERROR) {
                int tmp;
                tmp = ompi_request_free(&(requests[indices[i]]));
                if (OMPI_SUCCESS != tmp) {
                    return tmp;
                }
            }
        }
    }

    return rc;
}
