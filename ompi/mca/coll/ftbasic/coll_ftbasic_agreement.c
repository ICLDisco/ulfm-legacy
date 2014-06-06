/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2014      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */
#include "ompi_config.h"

#include "coll_ftbasic.h"
#include "coll_ftbasic_agreement.h"

#include <unistd.h>
#include "orte/util/name_fns.h"
#include "orte/mca/plm/plm_types.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/grpcomm/grpcomm.h"

#include "mpi.h"
#include "ompi/constants.h"
#include "opal/util/bit_ops.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/proc/proc.h"
#include "ompi/mpiext/ftmpi/mpiext_ftmpi_c.h"

#include MCA_timer_IMPLEMENTATION_HEADER

/*************************************
 * Local Functions
 *************************************/
static int coll_ftbasic_agreement_base_setup_common(ompi_communicator_t* comm,
                                                    opal_bitmap_t *local_bitmap,
                                                    int *flag,
                                                    mca_coll_ftbasic_module_t *ftbasic_module);
static int coll_ftbasic_agreement_base_finish_common(ompi_communicator_t* comm,
                                                     opal_bitmap_t *local_bitmap,
                                                     ompi_group_t **group,
                                                     int *flag,
                                                     mca_coll_ftbasic_module_t *ftbasic_module);

static int mca_coll_ftbasic_request_free(struct ompi_request_t** request);
static int mca_coll_ftbasic_request_cancel(struct ompi_request_t* request, int complete);

#if OPAL_ENABLE_DEBUG
static char * get_local_bitmap_str(ompi_communicator_t* comm);
#endif


static bool comm_help_listener_started = false;
static void comm_help_notice_recv(int status,
                                  orte_process_name_t* sender,
                                  opal_buffer_t* buffer,
                                  orte_rml_tag_t tag,
                                  void* cbdata);

/*************************************
 * Global Variables
 *************************************/
ompi_free_list_t mca_coll_ftbasic_remote_bitmap_free_list;
int mca_coll_ftbasic_remote_bitmap_num_modules = 0;
int mca_coll_ftbasic_agreement_num_active_nonblocking = 0;
int mca_coll_ftbasic_agreement_help_num_asking = 0;
int mca_coll_ftbasic_agreement_help_wait_cycles = 0;

/*
 * Timing stuff
 */
#if AGREEMENT_ENABLE_TIMING == 1
double timer_start[COLL_FTBASIC_AGREEMENT_TIMER_MAX];
double timer_end[COLL_FTBASIC_AGREEMENT_TIMER_MAX];
char * timer_label[COLL_FTBASIC_AGREEMENT_TIMER_MAX];
#endif /* AGREEMENT_ENABLE_TIMING */


/*************************************
 * Agreement Object Support
 *************************************/
static void mca_coll_ftbasic_agreement_construct(mca_coll_ftbasic_agreement_t *v_info)
{
    v_info->agreement_seq_num = 0;

    v_info->fail_bitmap = NULL;

    v_info->agreement_log = OBJ_NEW(opal_list_t);

    v_info->cur_request = NULL;

    OBJ_CONSTRUCT(&(v_info->remote_bitmaps),    opal_list_t);
}

static void mca_coll_ftbasic_agreement_destruct(mca_coll_ftbasic_agreement_t *v_info)
{
    opal_list_item_t* item = NULL;

    if( NULL != v_info->fail_bitmap ) {
        OBJ_RELEASE(v_info->fail_bitmap);
        v_info->fail_bitmap = NULL;
    }

    if( NULL != v_info->agreement_log ) {
        while (NULL != (item = opal_list_remove_first(v_info->agreement_log)) ) {
            OBJ_RELEASE(item);
        }
        OBJ_RELEASE(v_info->agreement_log);
        v_info->agreement_log = NULL;
    }

    if( NULL != v_info->cur_request ) {
        OBJ_RELEASE(v_info->cur_request);
        v_info->cur_request = NULL;
    }

    while( NULL != (item = opal_list_remove_first(&(v_info->remote_bitmaps)) ) ) {
        REMOTE_BITMAP_RETURN(item);
    }
    OBJ_DESTRUCT(&(v_info->remote_bitmaps));
}

OBJ_CLASS_INSTANCE(mca_coll_ftbasic_agreement_t,
                   opal_object_t,
                   mca_coll_ftbasic_agreement_construct,
                   mca_coll_ftbasic_agreement_destruct);


/*************************************
 * Log Entry Object Support
 *************************************/
static void mca_coll_ftbasic_agreement_log_entry_construct(mca_coll_ftbasic_agreement_log_entry_t *entry)
{
    entry->attempt_num = 0;

    entry->seq_num = -1;

    entry->state = AGREEMENT_STATE_NONE;

    entry->commit_bitmap = OBJ_NEW(opal_bitmap_t);
    opal_bitmap_init(entry->commit_bitmap, 8);
}

static void mca_coll_ftbasic_agreement_log_entry_destruct(mca_coll_ftbasic_agreement_log_entry_t *entry)
{
    entry->attempt_num = 0;

    entry->seq_num = -1;

    entry->state = AGREEMENT_STATE_NONE;

    if( NULL != entry->commit_bitmap ) {
        OBJ_RELEASE(entry->commit_bitmap);
        entry->commit_bitmap = NULL;
    }
}

OBJ_CLASS_INSTANCE(mca_coll_ftbasic_agreement_log_entry_t,
                   opal_list_item_t,
                   mca_coll_ftbasic_agreement_log_entry_construct,
                   mca_coll_ftbasic_agreement_log_entry_destruct);


/*************************************
 * Remote Bitmap Object Support
 *************************************/
static void mca_coll_ftbasic_remote_bitmap_construct(mca_coll_ftbasic_remote_bitmap_t *rbm)
{
    rbm->rank = -1;

    rbm->bitmap = OBJ_NEW(opal_bitmap_t);
    /* We will initialize in the bitmap_alloc */
}

static void mca_coll_ftbasic_remote_bitmap_destruct(mca_coll_ftbasic_remote_bitmap_t *rbm)
{
    rbm->rank = -1;

    if( NULL != rbm->bitmap ) {
        OBJ_RELEASE(rbm->bitmap);
        rbm->bitmap = NULL;
    }
}

OBJ_CLASS_INSTANCE(mca_coll_ftbasic_remote_bitmap_t,
                   ompi_free_list_item_t,
                   mca_coll_ftbasic_remote_bitmap_construct,
                   mca_coll_ftbasic_remote_bitmap_destruct);


/*************************************
 * Request Object Support
 *************************************/
static void mca_coll_ftbasic_request_construct(mca_coll_ftbasic_request_t *req)
{
    req->mpi_source = MPI_PROC_NULL;
    req->mpi_error  = MPI_SUCCESS;

    req->free_called = false;

    req->local_bitmap = OBJ_NEW(opal_bitmap_t);
    req->group        = NULL;
    req->flag         = NULL;
    req->coordinator = MPI_PROC_NULL;
    req->stage = -1;
    req->log_entry = NULL;
    req->num_reqs = 0;

    req->req_ompi.req_type = OMPI_REQUEST_COLL;

    req->req_ompi.req_cancel = mca_coll_ftbasic_request_cancel;
    req->req_ompi.req_free   = mca_coll_ftbasic_request_free;

    req->req_ompi.req_peer = req->mpi_source;

    req->req_ompi.req_status._cancelled   = 0;
}

static void mca_coll_ftbasic_request_destruct(mca_coll_ftbasic_request_t *req)
{
    if( NULL != req->local_bitmap ) {
        OBJ_RELEASE(req->local_bitmap);
        req->local_bitmap = NULL;
    }

    if( NULL != req->log_entry ) {
        OBJ_RELEASE(req->log_entry);
        req->log_entry = NULL;
    }

    if( NULL != req->group ) {
        OBJ_RELEASE(req->group);
        req->group = NULL;
    }
}

OBJ_CLASS_INSTANCE(mca_coll_ftbasic_request_t,
                   ompi_request_t,
                   mca_coll_ftbasic_request_construct,
                   mca_coll_ftbasic_request_destruct);


/*************************************
 * Initalize and Finalize Operations
 *************************************/
int mca_coll_ftbasic_agreement_init(mca_coll_ftbasic_module_t *module)
{
    int ret;

    /*
     * Make the remote bitmap free list only once (shared between modules)
     */
    if( 0 == mca_coll_ftbasic_remote_bitmap_num_modules ) {
        OBJ_CONSTRUCT(&mca_coll_ftbasic_remote_bitmap_free_list, ompi_free_list_t);
        ompi_free_list_init_new( &mca_coll_ftbasic_remote_bitmap_free_list,
                                 sizeof(mca_coll_ftbasic_remote_bitmap_t),
                                 opal_cache_line_size,
                                 OBJ_CLASS(mca_coll_ftbasic_remote_bitmap_t),
                                 0, opal_cache_line_size,
                                 8,  /* Initial number */
                                 -1, /* Max = Unlimited */
                                 8,  /* Increment by */
                                 NULL);
    }
    mca_coll_ftbasic_remote_bitmap_num_modules++;

    if( !comm_help_listener_started ) {
        ret = orte_rml.recv_buffer_nb(ORTE_NAME_WILDCARD,
                                      ORTE_RML_TAG_COLL_AGREE_TERM,
                                      ORTE_RML_PERSISTENT,
                                      comm_help_notice_recv,
                                      NULL);
        if( OMPI_SUCCESS != ret ) {
            return ret;
        }
        comm_help_listener_started = true;
    }

    switch( mca_coll_ftbasic_cur_agreement_method ) {
    case COLL_FTBASIC_ALLREDUCE:
        mca_coll_ftbasic_agreement_allreduce_init(module);
        break;
    case COLL_FTBASIC_TWO_PHASE:
        mca_coll_ftbasic_agreement_two_phase_init(module);
        break;
    case COLL_FTBASIC_LOG_TWO_PHASE:
        mca_coll_ftbasic_agreement_log_two_phase_init(module);
        break;
    case COLL_FTBASIC_EARLY_RETURNING:
        mca_coll_ftbasic_agreement_era_comm_init(module);
        break;
    default:
        break;
    }

    REMOTE_BITMAP_RESET_NEXT(module);

#if AGREEMENT_ENABLE_TIMING == 1
    AGREEMENT_CLEAR_ALL_TIMERS();

    /* Only need to do this once per component, not per communicator */
    if( mca_coll_ftbasic_use_agreement_timer && NULL == timer_label[0] ) {
        timer_label[COLL_FTBASIC_AGREEMENT_TIMER_PROTOCOL]     = strdup("Protocol ");
        timer_label[COLL_FTBASIC_AGREEMENT_TIMER_DECIDE]       = strdup("Decide   ");
        timer_label[COLL_FTBASIC_AGREEMENT_TIMER_REBALANCE]    = strdup("Rebalance");
        timer_label[COLL_FTBASIC_AGREEMENT_TIMER_TOTAL]        = strdup("Total    ");
        timer_label[COLL_FTBASIC_AGREEMENT_TIMER_OTHER]        = strdup("Other    ");

        /* Two Phase Timers */
        timer_label[COLL_FTBASIC_AGREEMENT_TIMER_2P_SETUP]     = strdup("2P: Setup     ");
        timer_label[COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ] = strdup("2P: Bcast Req.");
        timer_label[COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER]    = strdup("2P: Gather    ");
        timer_label[COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE]    = strdup("2P: Decide    ");
        timer_label[COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST]     = strdup("2P: Bcast     ");

        /* AllReduce Timers */
        timer_label[COLL_FTBASIC_AGREEMENT_TIMER_AR_SETUP]     = strdup("AR: Setup     ");
        timer_label[COLL_FTBASIC_AGREEMENT_TIMER_AR_GATHER]    = strdup("AR: Gather    ");
        timer_label[COLL_FTBASIC_AGREEMENT_TIMER_AR_DECIDE]    = strdup("AR: Decide    ");
        timer_label[COLL_FTBASIC_AGREEMENT_TIMER_AR_BCAST]     = strdup("AR: Bcast     ");
    }
#endif /* AGREEMENT_ENABLE_TIMING */

    return OMPI_SUCCESS;
}

int mca_coll_ftbasic_agreement_finalize(mca_coll_ftbasic_module_t *module)
{
    int ret;
#if AGREEMENT_ENABLE_TIMING == 1
    int i;
#endif /* AGREEMENT_ENABLE_TIMING */

    switch( mca_coll_ftbasic_cur_agreement_method ) {
    case COLL_FTBASIC_ALLREDUCE:
        mca_coll_ftbasic_agreement_allreduce_finalize(module);
        break;
    case COLL_FTBASIC_TWO_PHASE:
        mca_coll_ftbasic_agreement_two_phase_finalize(module);
        break;
    case COLL_FTBASIC_LOG_TWO_PHASE:
        mca_coll_ftbasic_agreement_log_two_phase_finalize(module);
        break;
    case COLL_FTBASIC_EARLY_RETURNING:
        mca_coll_ftbasic_agreement_era_comm_finalize(module);
        break;
    default:
        break;
    }

#if AGREEMENT_ENABLE_TIMING == 1
    /* Only need to do this once per component, not per communicator */
    if( mca_coll_ftbasic_use_agreement_timer && NULL != timer_label[0] ) {
        for(i = 0; i < COLL_FTBASIC_AGREEMENT_TIMER_MAX; ++i) {
            if( NULL != timer_label[i] ) {
                free(timer_label[i]);
                timer_label[i] = NULL;
            }
        }
    }
#endif /* AGREEMENT_ENABLE_TIMING */

    mca_coll_ftbasic_remote_bitmap_num_modules--;
    if( 0 >= mca_coll_ftbasic_remote_bitmap_num_modules ) {
        if( comm_help_listener_started ) {
            ret = orte_rml.recv_cancel(ORTE_NAME_WILDCARD,
                                       ORTE_RML_TAG_COLL_AGREE_TERM);
            if( OMPI_SUCCESS != ret ) {
                return ret;
            }

            comm_help_listener_started = false;
        }
        OBJ_DESTRUCT(&mca_coll_ftbasic_remote_bitmap_free_list);
    }

    return OMPI_SUCCESS;
}

/*************************************
 * Agreement Setup
 *************************************/
static int coll_ftbasic_agreement_base_setup_common(ompi_communicator_t* comm,
                                                    opal_bitmap_t *local_bitmap,
                                                    int *flag,
                                                    mca_coll_ftbasic_module_t *ftbasic_module)
{
    AGREEMENT_CLEAR_ALL_TIMERS();
    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_TOTAL);

    ftbasic_module->agreement_info->agreement_seq_num += 1;

    OPAL_OUTPUT_VERBOSE((7, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (common) Rank %2d entering agreement %2d (tag = %4d / %4d)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ompi_comm_rank(comm), ftbasic_module->agreement_info->agreement_seq_num,
                         MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module), MCA_COLL_FTBASIC_TAG_STEP));

    /************************************************
     * Copy my local fault information to the local_bitmap
     ************************************************/
    opal_bitmap_init(local_bitmap, ompi_comm_size(comm) + FTBASIC_AGREEMENT_EXTRA_BITS);
    agreement_init_local_bitmap(comm, local_bitmap);

    /* COMMIT / ABORT Bit */
    opal_bitmap_clear_bit(local_bitmap, ompi_comm_size(comm));
    /* Flag Bit */
    if( *flag ) {
        opal_bitmap_set_bit(local_bitmap,
                            FTBASIC_AGREEMENT_FLAG_BIT_LOC(local_bitmap) );
    } else {
        opal_bitmap_clear_bit(local_bitmap,
                              FTBASIC_AGREEMENT_FLAG_BIT_LOC(local_bitmap) );
    }

    if( NULL == ftbasic_module->agreement_info->fail_bitmap ) {
        ftbasic_module->agreement_info->fail_bitmap = OBJ_NEW(opal_bitmap_t);
        opal_bitmap_init(ftbasic_module->agreement_info->fail_bitmap,
                         ompi_comm_size(comm) + FTBASIC_AGREEMENT_EXTRA_BITS );

        switch( mca_coll_ftbasic_cur_agreement_method ) {
        case COLL_FTBASIC_ALLREDUCE:
        case COLL_FTBASIC_TWO_PHASE:
            opal_bitmap_copy(ftbasic_module->agreement_info->fail_bitmap, local_bitmap);
            break;
        case COLL_FTBASIC_LOG_TWO_PHASE:
            mca_coll_ftbasic_agreement_log_two_phase_refresh_tree(local_bitmap, comm, ftbasic_module);
            break;
        default:
            break;
        }
    }

    return OMPI_SUCCESS;
}

/*************************************
 * Agreement Setup (Blocking)
 *************************************/
int mca_coll_ftbasic_agreement_base_setup_blocking(ompi_communicator_t* comm,
                                                   ompi_group_t **group,
                                                   int *flag,
                                                   opal_bitmap_t *local_bitmap,
                                                   mca_coll_ftbasic_module_t *ftbasic_module)
{
    int ret, exit_status = OMPI_SUCCESS;

    ret = coll_ftbasic_agreement_base_setup_common(comm, local_bitmap, flag, ftbasic_module);
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto cleanup;
    }

    /*
     * If there are no other processes in this group, then we are done
     */
    if( 1 == ompi_comm_num_active_local(comm) ) {
        if( NULL != group ) {
            *group = MPI_GROUP_EMPTY;
            OBJ_RETAIN(MPI_GROUP_EMPTY);
        }

        mca_coll_ftbasic_agreement_base_finish_blocking(comm,
                                                        group,
                                                        flag,
                                                        local_bitmap,
                                                        ftbasic_module);
        exit_status = MPI_SUCCESS;
        goto cleanup;
    }

    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_PROTOCOL);

 cleanup:
    return exit_status;
}


/*************************************
 * Agreement Setup (Nonblocking)
 *************************************/
int mca_coll_ftbasic_agreement_base_setup_nonblocking(ompi_communicator_t* comm,
                                                      ompi_group_t **group,
                                                      int *flag,
                                                      opal_bitmap_t *local_bitmap,
                                                      mca_coll_ftbasic_module_t *ftbasic_module,
                                                      ompi_request_t **request)
{
    int ret, exit_status = OMPI_SUCCESS;
    mca_coll_ftbasic_request_t *collreq = NULL;

    ret = coll_ftbasic_agreement_base_setup_common(comm, local_bitmap, flag, ftbasic_module);
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto cleanup;
    }

    /************************************************
     * Setup the request
     ************************************************/
    MCA_COLL_FTBASIC_REQUEST_ALLOC(collreq, comm);
    *request = (ompi_request_t *) collreq;
    if( NULL != group ) {
        collreq->group = *group;
    } else {
        collreq->group = NULL;
    }
    collreq->flag  = flag;

    ftbasic_module->agreement_info->cur_request = collreq;
    OBJ_RETAIN(ftbasic_module->agreement_info->cur_request);

    opal_bitmap_init(ftbasic_module->agreement_info->cur_request->local_bitmap,
                     ompi_comm_size(comm) + FTBASIC_AGREEMENT_EXTRA_BITS);
    opal_bitmap_copy(ftbasic_module->agreement_info->cur_request->local_bitmap, local_bitmap);

    /*
     * If there are no other processes in this group, then we are done
     */
    if( 1 == ompi_comm_num_active_local(comm) ) {
        if( NULL != group ) {
            *group = MPI_GROUP_EMPTY;
            OBJ_RETAIN(MPI_GROUP_EMPTY);
        }

        ftbasic_module->agreement_info->cur_request->mpi_error = OMPI_SUCCESS;
        agreement_init_local_bitmap(comm, ftbasic_module->agreement_info->cur_request->local_bitmap);

        mca_coll_ftbasic_agreement_base_finish_nonblocking(comm,
                                                           group,
                                                           flag,
                                                           local_bitmap,
                                                           ftbasic_module);
        exit_status = MPI_SUCCESS;
        goto cleanup;
    }

    ++mca_coll_ftbasic_agreement_num_active_nonblocking;
    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_PROTOCOL);

 cleanup:
    return exit_status;
}

/*************************************
 * Agreement Finish (Blocking)
 *************************************/
int mca_coll_ftbasic_agreement_base_finish_blocking(ompi_communicator_t* comm,
                                                    ompi_group_t **group,
                                                    int *flag,
                                                    opal_bitmap_t *local_bitmap,
                                                    mca_coll_ftbasic_module_t *ftbasic_module)
{
    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_PROTOCOL);

    /************************************************
     * Commit the accumulated list of failures
     * Rebalance Collectives
     ************************************************/
    coll_ftbasic_agreement_base_finish_common(comm, local_bitmap,
                                              group, flag,
                                              ftbasic_module);

    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_TOTAL);

    AGREEMENT_DISPLAY_ALL_TIMERS();

    return OMPI_SUCCESS;
}

/*************************************
 * Agreement Finish (Nonblocking)
 *************************************/
int mca_coll_ftbasic_agreement_base_finish_nonblocking(ompi_communicator_t* comm,
                                                       ompi_group_t **group,
                                                       int *flag,
                                                       opal_bitmap_t *local_bitmap,
                                                       mca_coll_ftbasic_module_t *ftbasic_module)
{
    mca_coll_ftbasic_request_t *current_collreq = ftbasic_module->agreement_info->cur_request;

    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_PROTOCOL);

    /************************************************
     * Commit the accumulated list of failures
     * Rebalance Collectives
     ************************************************/
    coll_ftbasic_agreement_base_finish_common(comm, local_bitmap,
                                              group, flag,
                                              ftbasic_module);

    if( NULL != group ) {
        (current_collreq->group) = *group;
    } else {
        (current_collreq->group) = NULL;
    }
    (current_collreq->flag)  = flag;

    --mca_coll_ftbasic_agreement_num_active_nonblocking;

    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_TOTAL);
    /* AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_OTHER); */


    MCA_COLL_FTBASIC_REQUEST_COMPLETE(current_collreq);
    OBJ_RELEASE(ftbasic_module->agreement_info->cur_request);
    ftbasic_module->agreement_info->cur_request = NULL;


    /* AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_OTHER); */

    AGREEMENT_DISPLAY_ALL_TIMERS();

    return OMPI_SUCCESS;
}

/*************************************
 * Agreement Finish (Common)
 *************************************/
static int coll_ftbasic_agreement_base_finish_common(ompi_communicator_t* comm,
                                                     opal_bitmap_t *local_bitmap,
                                                     ompi_group_t **group,
                                                     int *flag,
                                                     mca_coll_ftbasic_module_t *ftbasic_module)
{
    int ret, i, exit_status = OMPI_SUCCESS;
    ompi_proc_t *failed_peer = NULL;
    int rank, size;
    int num_failed = 0;
    bool is_proc_active = false;
    int *fail_ranks = NULL;
    bool clear = true;
#if OPAL_ENABLE_DEBUG
    char *tmp_bitstr_local = NULL;
    char *tmp_bitstr_agree = NULL;
#endif

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);

    /************************************************
     * Commit the accumulated list of failures
     * Note: This operation is similar to a biwise OR, but we need to do it
     *       the long way so that we can change state according to which
     *       process failures are newly discovered, locally-but-not-globally
     *       known, and confirmed.
     ************************************************/
    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_DECIDE);

#if OPAL_ENABLE_DEBUG
    tmp_bitstr_local = get_local_bitmap_str(comm);
    tmp_bitstr_agree = opal_bitmap_get_string(local_bitmap);
    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (base  ) *** Compare Local [%s] to Agree [%s]",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         tmp_bitstr_local, tmp_bitstr_agree ));
    if( NULL != tmp_bitstr_local ) {
        free(tmp_bitstr_local);
        tmp_bitstr_local = NULL;
    }
    if( NULL != tmp_bitstr_agree ) {
        free(tmp_bitstr_agree);
        tmp_bitstr_agree = NULL;
    }
#endif

    fail_ranks = (int *) malloc(size * sizeof(int));
    if( NULL == fail_ranks ) {
        return OMPI_ERR_OUT_OF_RESOURCE;
    }

    num_failed = 0;
    for(i = 0; i < size; ++i ) {
        is_proc_active = ompi_comm_is_proc_active(comm, i, false);

        /* If remote marked this peer as failed, and I have not then process */
        if( opal_bitmap_is_set_bit(local_bitmap, i) ) {
            /* Mark new failures */
            if( is_proc_active ) {
                OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle,
                                     "%s ftbasic: agreement) (base  ) *** Found previously unknown failure of %2d",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     i));
                failed_peer = ompi_comm_peer_lookup(comm, i);

                /* ERROR: Should not be in the list */
                if( failed_peer->proc_name.vpid == ORTE_PROC_MY_NAME->vpid ) {
                    opal_output(0,
                                "%s ftbasic: agreement) (base  ) Error: Told to mark myself as failed!",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                    continue;
                }

                /*
                 * Mark new failure
                 */
                ompi_errmgr_mark_failed_peer(failed_peer, ORTE_PROC_STATE_TERMINATED);
            }
            /* Confirm already known failures */
            else {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                     "%s ftbasic: agreement) (base  ) *** Confirm peer failure of %2d",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     i));
            }

            /*
             * Add to group
             */
            fail_ranks[num_failed] = i;
            ++num_failed;
        }
        /* Check to make sure that the agreed upon list does not miss any failures
         * This can happen if a failure occurs near the end of the protocol, so 
         * it is a -new- failure at the bottom of the agreement.
         *
         * Do not increment the outcount, since we need to report uniformly across
         * all calling processes. The application can find out about the new failure
         * during the next agreement check.
         */
        else {
            if( !is_proc_active ) {
                OPAL_OUTPUT_VERBOSE((2, ompi_ftmpi_output_handle,
                                     "%s ftbasic: agreement) (base  ) *** Found locally, but not globally known failure of %2d",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     i));
                /* Must not re-enable collectives in this case */
                clear = false;
            }
        }
    }
    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_DECIDE);

    /*
     * Define the failed group
     */
    if( NULL != group ) {
        if( OMPI_SUCCESS != (ret = ompi_group_incl(comm->c_local_group,
                                                   num_failed,
                                                   fail_ranks,
                                                   group)) ) {
            return ret;
        }
    }

    /*
     * Define the flag
     */
    if( opal_bitmap_is_set_bit(local_bitmap, FTBASIC_AGREEMENT_FLAG_BIT_LOC(local_bitmap) ) ) {
        *flag = true;
    } else {
        *flag = false;
    }

    /************************************************
     * If we can clear the communicator, and re-enable collectives
     *
     * Step the agreement tag here so we do not accidentally do
     * so during the agreement operation.
     ************************************************/
    /*
     * If there are 'new' failures that were agreed upon since the last agreement
     *  - Advance the tag space
     *  - Rebalance the 'agreement' communication based on this set.
     * If there are no 'additional' locally known failures
     *  - Enable collectives
     *  - Rebalance collective operations
     * ow
     *  - Disable collectives (as if detected after the agreement)
     */
    if( opal_bitmap_are_different(local_bitmap, ftbasic_module->agreement_info->fail_bitmap) ) {
        OPAL_OUTPUT_VERBOSE((7, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (base  ) *** (Seq. %3d) Update Agreement Tags (%3d to %3d) - UPDATE",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ftbasic_module->agreement_info->agreement_seq_num,
                             ftbasic_module->mccb_coll_tag_agreement,
                             (ftbasic_module->mccb_coll_tag_agreement + MCA_COLL_FTBASIC_TAG_STEP) ));
        /*
         * Update the collective tag for agreement.
         * JJH TODO:
         * It would be nice if we did not need to do this, but it is
         * difficult to guarantee that cancel actually worked when trying
         * to revoke an errant message.
         * JJH: Is this strictly needed or just a safety precaution?
         */
        ftbasic_module->mccb_coll_tag_agreement += MCA_COLL_FTBASIC_TAG_STEP;
        /* Do not Increment the following, since we need them to remain valid across failure recoveries
         * - MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP
         * - MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ
         * - MCA_COLL_BASE_TAG_AGREEMENT_UR_ELECTED
         */


        /************************************************
         * Tell the agreement operation that we are done.
         * Then it might want to rebalance any internal communication
         * structures.
         ************************************************/
        switch( mca_coll_ftbasic_cur_agreement_method ) {
        case COLL_FTBASIC_ALLREDUCE:
        case COLL_FTBASIC_TWO_PHASE:
            opal_bitmap_copy(ftbasic_module->agreement_info->fail_bitmap, local_bitmap);
            break;
        case COLL_FTBASIC_LOG_TWO_PHASE:
            mca_coll_ftbasic_agreement_log_two_phase_refresh_tree(local_bitmap, comm, ftbasic_module);
            break;
        default:
            break;
        }
    } else {
        OPAL_OUTPUT_VERBOSE((7, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (base  ) *** (Seq. %3d) Update Agreement Tags (%3d to %3d) - NO UPDATE",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ftbasic_module->agreement_info->agreement_seq_num,
                             ftbasic_module->mccb_coll_tag_agreement,
                             (ftbasic_module->mccb_coll_tag_agreement + MCA_COLL_FTBASIC_TAG_STEP) ));
    }

    return exit_status;
}

/*************************************
 * Agreement Request for Assistance Deciding
 *************************************/
int mca_coll_ftbasic_agreement_base_term_request_help(ompi_communicator_t* comm,
                                                      mca_coll_ftbasic_module_t *ftbasic_module)
{
    int ret, exit_status = OMPI_SUCCESS;
    opal_buffer_t buffer;
    orte_jobid_t jobid = 0;

    /*
     * Instead of constantly polling for termination requests, instead have the
     * requestor xcast a 'wakeup' signal to all processes. This will cause them
     * to check for requests for a period of time. This reduces the overall
     * performance penalty.
     * JJH: We can make this more efficient later.
     */

    /*
     * Broadcast the 'help' signal to all other processes.
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
    if( OMPI_SUCCESS != (ret = orte_grpcomm.xcast(jobid, &buffer, ORTE_RML_TAG_COLL_AGREE_TERM)) ) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

 cleanup:
    OBJ_DESTRUCT(&buffer);

    return exit_status;
}

static void comm_help_notice_recv(int status,
                                  orte_process_name_t* sender,
                                  opal_buffer_t* buffer,
                                  orte_rml_tag_t tag,
                                  void* cbdata)
{
    int ret;
    orte_std_cntr_t count;
    int cid_to_help;
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
    ret = opal_dss.unpack(buffer, &(cid_to_help), &count, OPAL_INT);
    if( OMPI_SUCCESS != ret ){
        return;
    }

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (common): Recv: %s Asked for help during agreement %3d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&true_sender), cid_to_help ));

    /*
     * If this was my request, ignore
     */
    if( OPAL_EQUAL == orte_util_compare_name_fields(ORTE_NS_CMP_JOBID | ORTE_NS_CMP_VPID,
                                                    ORTE_PROC_MY_NAME, &true_sender) ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (common): Recv: %s is self, skip",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&true_sender)));
        return;
    }

    /*
     * Find the communicator
     */
    comm = (ompi_communicator_t *)opal_pointer_array_get_item(&ompi_mpi_communicators, cid_to_help);
    if( NULL == comm || cid_to_help != (int)(comm->c_contextid) ) {
        opal_output(0, "%s ftbasic: agreement) (common): Error: Could not find the communicator with CID %3d",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), cid_to_help );
        return;
    }

    /*
     * Check to make sure the sender is in this communicator
     */
    ompi_proc_peer = ompi_proc_find(&true_sender);
    if( NULL == ompi_proc_peer ) {
        opal_output(0, "%s ftbasic: agreement) (common): Error: Could not find the sender's ompi_proc_t (%s)",
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
                                 "%s ftbasic: agreement) (common): Recv: %s on a different communicator %3d - Ignore ",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&true_sender), cid_to_help ));
            return;
        }
    }

    /*
     * Trigger the progress function to fire
     */
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (common): Recv: %s Asked for help on communicator %3d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(&true_sender), cid_to_help ));
    mca_coll_ftbasic_agreement_help_num_asking += 1;

    return;
}

/*************************************
 * Bitmap operators
 *************************************/
int agreement_init_local_bitmap(ompi_communicator_t* comm, opal_bitmap_t *local_bitmap)
{
    int i, size;

    size = ompi_comm_size(comm);

    for( i = 0; i < size; ++i ) {
        if( ompi_comm_is_proc_active(comm, i, false) ) {
            opal_bitmap_clear_bit(local_bitmap, i);
        } else {
            opal_bitmap_set_bit(local_bitmap, i);
        }
    }

    return OMPI_SUCCESS;
}

#if OPAL_ENABLE_DEBUG
static char * get_local_bitmap_str(ompi_communicator_t* comm)
{
    int i, size;
    opal_bitmap_t *local_bitmap = NULL;
    char * loc_str = NULL;

    local_bitmap = OBJ_NEW(opal_bitmap_t);
    size = comm->c_local_group->grp_proc_count;
    opal_bitmap_init(local_bitmap, size + FTBASIC_AGREEMENT_EXTRA_BITS);

    for( i = 0; i < size; ++i ) {
        if( ompi_comm_is_proc_active(comm, i, false) ) {
            opal_bitmap_clear_bit(local_bitmap, i);
        } else {
            opal_bitmap_set_bit(local_bitmap, i);
        }
    }

    loc_str = opal_bitmap_get_string(local_bitmap);

    if( NULL != local_bitmap ) {
        OBJ_RELEASE(local_bitmap);
        local_bitmap = NULL;
    }

    return loc_str;
}
#endif

/*************************************
 * Request Object Support
 *************************************/
static int mca_coll_ftbasic_request_free(struct ompi_request_t** request)
{
    mca_coll_ftbasic_request_t* collreq = *(mca_coll_ftbasic_request_t**)request;

    collreq->free_called = true;

    return OMPI_SUCCESS;
}

static int mca_coll_ftbasic_request_cancel(struct ompi_request_t* request, int complete)
{
    return OMPI_SUCCESS;
}

/*************************************
 * Log Support
 *************************************/
#if 0
mca_coll_ftbasic_agreement_log_entry_t *
mca_coll_ftbasic_agreement_log_entry_find(int seq_num,
                                         bool create,
                                         mca_coll_ftbasic_module_t *ftbasic_module)
{
    mca_coll_ftbasic_agreement_log_entry_t *log_entry = NULL;
    opal_list_item_t *item = NULL;
    bool found = false;

    /*
     * Search for an existing seq num
     */
    for (item  = opal_list_get_first(ftbasic_module->agreement_info->agreement_log);
         item != opal_list_get_end(ftbasic_module->agreement_info->agreement_log);
         item  = opal_list_get_next(item)) {
        log_entry = (mca_coll_ftbasic_agreement_log_entry_t*)item;
        if( log_entry->seq_num == ftbasic_module->agreement_info->agreement_seq_num ) {
            found = true;
            log_entry->attempt_num += 1;
            break;
        }
    }

    /*
     * If not found then create if asked
     */
    if( !found ) {
        if( create ) {
            log_entry = OBJ_NEW(mca_coll_ftbasic_agreement_log_entry_t);
            log_entry->seq_num = ftbasic_module->agreement_info->agreement_seq_num;
            log_entry->state   = AGREEMENT_STATE_NONE;
            opal_list_append(ftbasic_module->agreement_info->agreement_log, &(log_entry->super));
        } else {
            return NULL;
        }
    }

    return log_entry;
}
#else
/*
 * Keep only as many logs as the user wants (no less than 2)
 */
mca_coll_ftbasic_agreement_log_entry_t *
mca_coll_ftbasic_agreement_log_entry_find(ompi_communicator_t* comm,
                                         int seq_num,
                                         bool create,
                                         mca_coll_ftbasic_module_t *ftbasic_module)
{
    mca_coll_ftbasic_agreement_log_entry_t *log_entry = NULL;
    opal_list_item_t *item = NULL;
    bool found = false;
    int i;

    /*
     * If the list is not created, then create 2 entries
     */
    if( opal_list_get_size(ftbasic_module->agreement_info->agreement_log) <= 0 ) {
        for( i = 0; i < mca_coll_ftbasic_agreement_log_max_len; ++i ) {
            log_entry = OBJ_NEW(mca_coll_ftbasic_agreement_log_entry_t);
            log_entry->seq_num     = -1;
            log_entry->attempt_num = -1;
            log_entry->state       = AGREEMENT_STATE_NONE;
            opal_bitmap_init(log_entry->commit_bitmap, ompi_comm_size(comm) + FTBASIC_AGREEMENT_EXTRA_BITS);
            opal_list_append(ftbasic_module->agreement_info->agreement_log, &(log_entry->super));
        }

        if( create ) {
            /* Use the last entry to add new entries */
            log_entry->seq_num     = seq_num;
            log_entry->attempt_num = 0;

            return (mca_coll_ftbasic_agreement_log_entry_t*)opal_list_get_last(ftbasic_module->agreement_info->agreement_log);
        } else {
            return NULL;
        }
    }

    /*
     * Search for an existing seq num
     */
    for (item  = opal_list_get_first(ftbasic_module->agreement_info->agreement_log);
         item != opal_list_get_end(ftbasic_module->agreement_info->agreement_log);
         item  = opal_list_get_next(item)) {
        log_entry = (mca_coll_ftbasic_agreement_log_entry_t*)item;
        if( log_entry->seq_num == seq_num ) {
            found = true;
            break;
        }
    }

    /*
     * If not found then reuse if asked to 'create'
     */
    if( !found ) {
        if( create ) {
            /* Remove first */
            log_entry = (mca_coll_ftbasic_agreement_log_entry_t*)opal_list_remove_first(ftbasic_module->agreement_info->agreement_log);

            /* Put first to last, and use it */
            opal_list_append(ftbasic_module->agreement_info->agreement_log, &(log_entry->super));

#if OPAL_ENABLE_DEBUG
            if( ORTE_PROC_MY_NAME->vpid == 0 ) {
                OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                                     "%s ftbasic: agreement) (2phase) Replace Log %3d with Log %3d",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     log_entry->seq_num, seq_num));
            }
#endif

            log_entry->seq_num     = seq_num;
            log_entry->attempt_num = 0;
            log_entry->state       = AGREEMENT_STATE_NONE;
        } else {
            return NULL;
        }
    } else if( create ) {
        log_entry->attempt_num += 1;
    }

    return log_entry;
}
#endif

/*************************************
 * Timer Support
 *************************************/
#if AGREEMENT_ENABLE_TIMING == 1
void agreement_start_time(int idx) {
    if(idx < COLL_FTBASIC_AGREEMENT_TIMER_MAX ) {
        timer_start[idx] = agreement_get_time();
    }
}

void agreement_end_time(int idx) {
    if(idx < COLL_FTBASIC_AGREEMENT_TIMER_MAX ) {
        timer_end[idx] = agreement_get_time();
    }
}

double agreement_get_time() {
    double wtime;

#if OPAL_TIMER_USEC_NATIVE
    wtime = (double)opal_timer_base_get_usec() / 1000000.0;
#else
    struct timeval tv;
    gettimeofday(&tv, NULL);
    wtime = tv.tv_sec;
    wtime += (double)tv.tv_usec / 1000000.0;
#endif

    return wtime;
}

void agreement_clear_timers(void) {
    int i;
    for(i = 0; i < COLL_FTBASIC_AGREEMENT_TIMER_MAX; ++i) {
        timer_start[i] = 0.0;
        timer_end[i]   = 0.0;
    }
}

void agreement_display_all_timers(void) {
    double diff;
    int i;

    if( 0 != ORTE_PROC_MY_NAME->vpid ) {
        return;
    }

    opal_output(0, "coll:ftbasic: timing(%20s): ******************** Begin:\n",
                "Summary");

    for( i = 0; i < COLL_FTBASIC_AGREEMENT_TIMER_MAX; ++i) {
        /* If not two phase, then skip these timers */
        if( COLL_FTBASIC_TWO_PHASE != mca_coll_ftbasic_cur_agreement_method &&
            COLL_FTBASIC_LOG_TWO_PHASE != mca_coll_ftbasic_cur_agreement_method ) {
            if( COLL_FTBASIC_AGREEMENT_TIMER_2P_SETUP     == i ||
                COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ == i ||
                COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER    == i ||
                COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE    == i ||
                COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST     == i ) {
                continue;
            }
        }
        /* If not allreduce, then skip these timers */
        if( COLL_FTBASIC_ALLREDUCE != mca_coll_ftbasic_cur_agreement_method ) {
            if( COLL_FTBASIC_AGREEMENT_TIMER_AR_SETUP     == i ||
                COLL_FTBASIC_AGREEMENT_TIMER_AR_GATHER    == i ||
                COLL_FTBASIC_AGREEMENT_TIMER_AR_DECIDE    == i ||
                COLL_FTBASIC_AGREEMENT_TIMER_AR_BCAST     == i ) {
                continue;
            }
        }

        diff = (timer_end[i] - timer_start[i])*1000000.0;
        opal_output(0,
                    "coll:ftbasic: timing(%20s): %20s = %10.2f us\n",
                    "",
                    timer_label[i], diff);
    }

    opal_output(0, "coll:ftbasic: timing(%20s): ******************** End:\n",
                "Summary");
}
#endif /* AGREEMENT_ENABLE_TIMING */
