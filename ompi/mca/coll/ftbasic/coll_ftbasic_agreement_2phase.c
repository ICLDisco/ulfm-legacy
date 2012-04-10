/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 *
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

/*
 * Todo:
 * - Make free list for log entries
 * - Rolling cleanup of the log to limit size
 * - Look into a single level heirarchy for scalability
 * - Should we look at 'nested' 2 phase commit?
 * - ...
 */
#include "ompi_config.h"
#include "coll_ftbasic.h"
#include "coll_ftbasic_agreement.h"

#include <unistd.h>
#include "orte/util/name_fns.h"
#include "orte/mca/plm/plm_types.h"
#include "orte/mca/errmgr/errmgr.h"

#include "mpi.h"
#include "ompi/constants.h"

#include "opal/util/bit_ops.h"
#include "opal/mca/event/event.h"

#include "ompi/mca/pml/pml.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/proc/proc.h"
#include "ompi/mpiext/ftmpi/mpiext_ftmpi_c.h"
#include "ompi/runtime/mpiruntime.h"

#include MCA_timer_IMPLEMENTATION_HEADER


/*************************************
 * Testing Globals
 *************************************/
#if OPAL_ENABLE_DEBUG
/* No testing enabled */
#define TWO_PHASE_TEST_NONE             0

/* Coordinator: Fail before 'Vote Req'      */
#define TWO_PHASE_TEST_COORD_PRE_REQ    1
/* Coordinator: Fail before Receiving Votes */
#define TWO_PHASE_TEST_COORD_PRE_GATHER 2
/* Coordinator: Fail before Bcast of Decision */
#define TWO_PHASE_TEST_COORD_PRE_DECIDE 3
/* Coordinator: Fail Mid Bcast of Decision */
#define TWO_PHASE_TEST_COORD_MID_DECIDE 4

/* Participant: 1 becomes uncertain due to late notification of coordinator failure = Abort */
#define TWO_PHASE_TEST_PART_UNCERTAIN   5
/* Participant: 1 slow, 2 fast to decide. 1 becomes uncertain, but 2 leads the decision = Commit */
#define TWO_PHASE_TEST_PART_SLOW_FAST   6
/* Participant: Rolling failure of coordinators 0 and 1 before bcast = 2 elected, decide commit*/
#define TWO_PHASE_TEST_PART_ROLL_PRE_DECIDE 7

#define TWO_PHASE_TEST TWO_PHASE_TEST_NONE

#if TWO_PHASE_TEST != TWO_PHASE_TEST_NONE
static int two_phase_test_counter = 0;
static bool test_phase_active = false;
#endif
#endif


/*************************************
 * Nonblocking states
 *************************************/
enum {
    /*
     * Collective not active state
     */
    TWO_PHASE_NB_STAGE_NULL = 0,
    /*
     * Coordinator: Bcast Decision Request
     */
    TWO_PHASE_NB_STAGE_COORD_WAIT_BCAST_REQ = 1,
    /*
     * Coordinator: Gather lists
     */
    TWO_PHASE_NB_STAGE_COORD_WAIT_GATHER = 2,
    /*
     * Coordinator: Bcast final list
     */
    TWO_PHASE_NB_STAGE_COORD_WAIT_BCAST = 3,
    /*
     * Participant: Receive Request
     */
    TWO_PHASE_NB_STAGE_PART_WAIT_RECV_REQ = 4,
    /*
     * Participant: Send List
     */
    TWO_PHASE_NB_STAGE_PART_WAIT_SEND = 5,
    /*
     * Participant: Receive final list
     */
    TWO_PHASE_NB_STAGE_PART_WAIT_RECV = 6,
    /*
     * (Term Protocol)
     * Initiator: Sending request to peer
     */
    TWO_PHASE_NB_STAGE_TERM_WAIT_SEND = 7,
    /*
     * (Term Protocol)
     * Initiator: Recv information from peer
     */
    TWO_PHASE_NB_STAGE_TERM_WAIT_RECV = 8,
    /*
     * All done
     */
    TWO_PHASE_NB_STAGE_DONE = 9
};


/*************************************
 * Algorithm specific agreement structure
 *************************************/
struct mca_coll_ftbasic_agreement_twophase_t {
    /* Base object */
    mca_coll_ftbasic_agreement_t super;
};
typedef struct mca_coll_ftbasic_agreement_twophase_t mca_coll_ftbasic_agreement_twophase_t;
OBJ_CLASS_DECLARATION(mca_coll_ftbasic_agreement_twophase_t);

static void mca_coll_ftbasic_agreement_twophase_construct(mca_coll_ftbasic_agreement_twophase_t *v_info)
{
    /* Nothing needed */
}

static void mca_coll_ftbasic_agreement_twophase_destruct(mca_coll_ftbasic_agreement_twophase_t *v_info)
{
    /* Nothing needed */
}
OBJ_CLASS_INSTANCE(mca_coll_ftbasic_agreement_twophase_t,
                   mca_coll_ftbasic_agreement_t,
                   mca_coll_ftbasic_agreement_twophase_construct,
                   mca_coll_ftbasic_agreement_twophase_destruct);


/*************************************
 * Supporting functions
 *************************************/
static int internal_agreement_two_phase(ompi_communicator_t* comm,
                                        ompi_group_t **group,
                                        int *flag,
                                        opal_bitmap_t *local_bitmap,
                                        mca_coll_ftbasic_module_t *ftbasic_module);
static int internal_iagreement_two_phase(ompi_communicator_t* comm,
                                         ompi_group_t **group,
                                         int *flag,
                                         opal_bitmap_t *local_bitmap,
                                         mca_coll_ftbasic_module_t *ftbasic_module,
                                         struct mca_coll_ftbasic_request_t *collreq);

static int two_phase_elect_coordinator(ompi_communicator_t* comm,
                                       mca_coll_ftbasic_module_t *ftbasic_module);

static int two_phase_coordinator(ompi_communicator_t* comm,
                                 opal_bitmap_t *local_bitmap,
                                 mca_coll_ftbasic_module_t *ftbasic_module,
                                 mca_coll_ftbasic_agreement_log_entry_t *log_entry);
static int two_phase_participant(ompi_communicator_t* comm,
                                 opal_bitmap_t *local_bitmap,
                                 mca_coll_ftbasic_module_t *ftbasic_module,
                                 mca_coll_ftbasic_agreement_log_entry_t *log_entry);
static int two_phase_term_initiator(ompi_communicator_t* comm,
                                    opal_bitmap_t *local_bitmap,
                                    mca_coll_ftbasic_module_t *ftbasic_module,
                                    mca_coll_ftbasic_agreement_log_entry_t *log_entry);
static int two_phase_term_responder(ompi_communicator_t* comm, int peer,
                                    mca_coll_ftbasic_module_t *ftbasic_module);

static int two_phase_test_all(ompi_communicator_t* comm,
                              mca_coll_ftbasic_module_t *ftbasic_module,
                              int num_reqs,
                              bool *is_finished,
                              bool return_error);

static int two_phase_coordinator_start_bcast_req(ompi_communicator_t* comm,
                                                 mca_coll_ftbasic_module_t *ftbasic_module,
                                                 int *num_reqs);
static int two_phase_coordinator_start_gather(ompi_communicator_t* comm,
                                              mca_coll_ftbasic_module_t *ftbasic_module,
                                              opal_bitmap_t *local_bitmap,
                                              int *num_reqs);
static int two_phase_coordinator_commit_final_list(ompi_communicator_t* comm,
                                                   mca_coll_ftbasic_module_t *ftbasic_module,
                                                   opal_bitmap_t *local_bitmap);
static int two_phase_coordinator_start_bcast(ompi_communicator_t* comm,
                                             mca_coll_ftbasic_module_t *ftbasic_module,
                                             opal_bitmap_t *local_bitmap,
                                             int *num_reqs);

static int two_phase_participant_start_recv_req(ompi_communicator_t* comm,
                                                mca_coll_ftbasic_module_t *ftbasic_module,
                                                int *num_reqs);
static int two_phase_participant_start_send(ompi_communicator_t* comm,
                                            mca_coll_ftbasic_module_t *ftbasic_module,
                                            opal_bitmap_t *local_bitmap,
                                            int *num_reqs);
static int two_phase_participant_start_recv(ompi_communicator_t* comm,
                                            mca_coll_ftbasic_module_t *ftbasic_module,
                                            opal_bitmap_t *local_bitmap,
                                            int *num_reqs);

/*************************************
 * Global Vars
 *************************************/
static int coordinator = 0;

static bool two_phase_inside_progress = false;
static bool two_phase_inside_term_progress = false;
static int  two_phase_progress_num_active = 0;

int mca_coll_ftbasic_agreement_two_phase_progress(void);
static int mca_coll_ftbasic_agreement_two_phase_progress_comm(ompi_communicator_t *comm,
                                                             mca_coll_ftbasic_module_t *ftbasic_module);
int mca_coll_ftbasic_agreement_two_phase_term_progress(void);
static int mca_coll_ftbasic_agreement_two_phase_term_progress_comm(ompi_communicator_t *comm);


/*************************************
 * Initialization and Finalization
 *************************************/
int mca_coll_ftbasic_agreement_two_phase_init(mca_coll_ftbasic_module_t *module)
{
    mca_coll_ftbasic_agreement_twophase_t *loc_info;

    loc_info = OBJ_NEW(mca_coll_ftbasic_agreement_twophase_t);
    module->agreement_info = (mca_coll_ftbasic_agreement_t*)loc_info;

    /*
     * Register the termination progress function
     * But only once for the module, not each communicator
     */
    if( mca_coll_ftbasic_agreement_use_progress &&
        two_phase_progress_num_active == 0 ) {
        two_phase_inside_term_progress = false;
        two_phase_inside_progress = false;
        opal_progress_register(mca_coll_ftbasic_agreement_two_phase_progress);
        opal_progress_register(mca_coll_ftbasic_agreement_two_phase_term_progress);
    }
    two_phase_progress_num_active++;

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Initialize (%d)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         two_phase_progress_num_active));

    return OMPI_SUCCESS;
}

int mca_coll_ftbasic_agreement_two_phase_finalize(mca_coll_ftbasic_module_t *module)
{
    mca_coll_ftbasic_agreement_twophase_t *loc_info = (mca_coll_ftbasic_agreement_twophase_t*)(module->agreement_info);

    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Finalize (%d)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         two_phase_progress_num_active));

    /*
     * Deregister the termination progress function
     * But only once per job.
     */
    two_phase_progress_num_active--;
    if( mca_coll_ftbasic_agreement_use_progress &&
        (two_phase_progress_num_active == 0 || ompi_mpi_finalized) ) {
        OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (2phase) Finalize - Cancel Progress handler (%s)",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             (ompi_mpi_finalized ? "Finalizing" : "") ));

        opal_progress_unregister(mca_coll_ftbasic_agreement_two_phase_progress);
        opal_progress_unregister(mca_coll_ftbasic_agreement_two_phase_term_progress);

        two_phase_inside_progress = false;
        two_phase_inside_term_progress = false;
    }

    OBJ_RELEASE(loc_info);
    module->agreement_info = NULL;

    return OMPI_SUCCESS;
}

/*************************************
 * High level methods
 *************************************/
int mca_coll_ftbasic_agreement_two_phase(ompi_communicator_t* comm,
                                         ompi_group_t **group,
                                         int *flag,
                                         mca_coll_base_module_t *module)
{
    int ret, exit_status = OMPI_SUCCESS;
    mca_coll_ftbasic_module_t *ftbasic_module = (mca_coll_ftbasic_module_t*) module;
    opal_bitmap_t *local_bitmap = NULL;

    local_bitmap = OBJ_NEW(opal_bitmap_t);

    /*
     * Common setup before the agreement operation
     */
    ret = mca_coll_ftbasic_agreement_base_setup_blocking(comm, group, flag,
                                                         local_bitmap, ftbasic_module);
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto cleanup;
    }
    if( 1 == ompi_comm_num_active_local(comm) ) {
        goto cleanup;
    }

    /*
     * Core agreement operation
     */
    do {
        ret = internal_agreement_two_phase(comm, group, flag,
                                           local_bitmap, ftbasic_module);
        /* Success, continue */
        if( OMPI_SUCCESS == ret ) { break; }
        /* FAIL_STOP if peer failure, try again */
        else if( MPI_ERR_PROC_FAILED == ret ) { continue; }
        /* Some other error, just fail */
        else { break; }
    } while(1);

    /*
     * The protocols should be reliable, so a failure at this point should
     * not happen. If it does report and bail.
     */
    if( OMPI_SUCCESS != ret ) {
        opal_output(0, "%s ftbasic: agreement) (base  ) Error: Reliable consensus protocol failed (should never happen) ret = %d",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret);
        exit_status = ret;
        goto cleanup;
    }

    /*
     * Common cleanup after the agreement operation
     */
    ret = mca_coll_ftbasic_agreement_base_finish_blocking(comm, group, flag,
                                                          local_bitmap, ftbasic_module);
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto cleanup;
    }

 cleanup:
    if( NULL != local_bitmap ) {
        OBJ_RELEASE(local_bitmap);
        local_bitmap = NULL;
    }

    return exit_status;
}

int mca_coll_ftbasic_iagreement_two_phase(ompi_communicator_t* comm,
                                          ompi_group_t **group,
                                          int *flag,
                                          mca_coll_base_module_t *module,
                                          ompi_request_t **request)
{
    int ret, exit_status = OMPI_SUCCESS;
    mca_coll_ftbasic_module_t *ftbasic_module = (mca_coll_ftbasic_module_t*) module;
    opal_bitmap_t *local_bitmap = NULL;

    local_bitmap = OBJ_NEW(opal_bitmap_t);

    /*
     * Common setup before the agreement operation
     */
    ret = mca_coll_ftbasic_agreement_base_setup_nonblocking(comm, group, flag,
                                                            local_bitmap, ftbasic_module, request);
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto cleanup;
    }
    if( 1 == ompi_comm_num_active_local(comm) ) {
        goto cleanup;
    }

    /*
     * Core agreement operation
     */
    do {
        ret = internal_iagreement_two_phase(comm, group, flag,
                                            local_bitmap, ftbasic_module,
                                            ftbasic_module->agreement_info->cur_request);
        /* Success, continue */
        if( OMPI_SUCCESS == ret ) { break; }
        /* FAIL_STOP if peer failure, try again */
        else if( MPI_ERR_PROC_FAILED == ret ) { continue; }
        /* Some other error, just fail */
        else { break; }
    } while(1);

    /*
     * The protocols should be reliable, so a failure at this point should
     * not happen. If it does report and bail.
     */
    if( OMPI_SUCCESS != ret ) {
        opal_output(0, "%s ftbasic: agreement) (base  ) Error: Reliable consensus protocol failed (should never happen) ret = %d",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret);
        exit_status = ret;
        goto cleanup;
    }

 cleanup:
    /* Ok to delete this here, since we cache the value on the request object */
    if( NULL != local_bitmap ) {
        OBJ_RELEASE(local_bitmap);
        local_bitmap = NULL;
    }

    return exit_status;
}

/*************************************
 * Internal interfaces
 *************************************/
static int internal_agreement_two_phase(ompi_communicator_t* comm,
                                        ompi_group_t **group,
                                        int *flag,
                                        opal_bitmap_t *local_bitmap,
                                        mca_coll_ftbasic_module_t *ftbasic_module)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank = ompi_comm_rank(comm);
    mca_coll_ftbasic_agreement_log_entry_t *log_entry = NULL;
#if OPAL_ENABLE_DEBUG
    int size = ompi_comm_size(comm);
#endif

    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_SETUP);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Rank %2d/%2d Starting agreement...",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank, size));

    /*
     * Sanity check: If it is just me, then return directly
     */
    if( 1 == ompi_comm_num_active_local(comm) ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (2phase) All alone, skip agreement.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

        agreement_init_local_bitmap(comm, local_bitmap);

        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
        AGREEMENT_END_TIMER(  COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER);
        AGREEMENT_END_TIMER(  COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER);
        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE);
        AGREEMENT_END_TIMER(  COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE);
        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST);
        AGREEMENT_END_TIMER(  COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST);

        exit_status = OMPI_SUCCESS;
        goto cleanup;
    }

    /************************************************
     * Find or make a new log entry
     ************************************************/
    log_entry = mca_coll_ftbasic_agreement_log_entry_find(comm,
                                                          ftbasic_module->agreement_info->agreement_seq_num,
                                                          true, ftbasic_module);


    /************************************************
     * Elect a coordinator
     ************************************************/
    coordinator = two_phase_elect_coordinator(comm, ftbasic_module);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Elected Coordinator is %2d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), coordinator));

    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_SETUP);


    /************************************************
     * Coordinator vs. Participant
     ************************************************/
    if( rank == coordinator ) {
#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_COORD_PRE_REQ
        /* TEST: Failure before request
         * Expected behavior:
         *   participants abort - Retry 2 phase commit
         */
        if( test_phase_active ) {
            if( two_phase_test_counter <= 0 ) {
                sleep(2);
                kill(getpid(), SIGKILL);
            }
        }
#endif
#endif

        if( OMPI_SUCCESS != (ret = two_phase_coordinator(comm, local_bitmap, ftbasic_module, log_entry)) ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (2phase) Coordinator Failed (%2d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret));
            exit_status = ret;
            goto cleanup;
        }
    }
    else {
        if( OMPI_SUCCESS != (ret = two_phase_participant(comm, local_bitmap, ftbasic_module, log_entry)) ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (2phase) Participant Failed (%2d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret));
            exit_status = ret;
            goto cleanup;
        }
    }

    /*
     * All done
     */
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Rank %2d/%2d Finished agreement.",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank, size));

 cleanup:
#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST != TWO_PHASE_TEST_NONE
    if( test_phase_active ) {
        two_phase_test_counter++;
    }
#endif
#endif

    return exit_status;
}

static int internal_iagreement_two_phase(ompi_communicator_t* comm,
                                         ompi_group_t **group,
                                         int *flag,
                                         opal_bitmap_t *local_bitmap,
                                         mca_coll_ftbasic_module_t *ftbasic_module,
                                         struct mca_coll_ftbasic_request_t *collreq)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank = ompi_comm_rank(comm);
    mca_coll_ftbasic_agreement_log_entry_t *log_entry = NULL;
#if OPAL_ENABLE_DEBUG
    int size = ompi_comm_size(comm);

#if TWO_PHASE_TEST != TWO_PHASE_TEST_NONE
    test_phase_active = true;
#endif
#endif

    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_SETUP);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic:iagreement) (2phase) Rank %2d/%2d Starting agreement...",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank, size));

    /*
     * Make sure our progress function does not get accidentally triggered while
     * setting up this new request.
     */
    if( two_phase_inside_progress ) {
        opal_output(0, "%s ftbasic:iagreement) (2phase) "
                    "Warning: Inside progress flag set. Re-entering iagreement - should not happen!",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
    }
    two_phase_inside_progress = true;

    ftbasic_module->agreement_info->cur_request->stage = TWO_PHASE_NB_STAGE_NULL;

    /*
     * Sanity check: If it is just me, then return directly
     */
    if( 1 == ompi_comm_num_active_local(comm) ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (2phase) All alone, skip agreement.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

        ftbasic_module->agreement_info->cur_request->mpi_error = OMPI_SUCCESS;
        agreement_init_local_bitmap(comm, ftbasic_module->agreement_info->cur_request->local_bitmap);

        mca_coll_ftbasic_agreement_base_finish_nonblocking(comm,
                                                           &ftbasic_module->agreement_info->cur_request->group,
                                                           ftbasic_module->agreement_info->cur_request->flag,
                                                           ftbasic_module->agreement_info->cur_request->local_bitmap,
                                                           ftbasic_module);

        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
        AGREEMENT_END_TIMER(  COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER);
        AGREEMENT_END_TIMER(  COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER);
        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE);
        AGREEMENT_END_TIMER(  COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE);
        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST);
        AGREEMENT_END_TIMER(  COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST);

        exit_status = OMPI_SUCCESS;
        goto cleanup;
    }

    /************************************************
     * Find or make a new log entry
     ************************************************/
    log_entry = mca_coll_ftbasic_agreement_log_entry_find(comm, ftbasic_module->agreement_info->agreement_seq_num, true, ftbasic_module);
    ftbasic_module->agreement_info->cur_request->log_entry = (opal_list_item_t*)log_entry;
    OBJ_RETAIN(ftbasic_module->agreement_info->cur_request->log_entry);


    /************************************************
     * Elect a coordinator
     ************************************************/
    coordinator = two_phase_elect_coordinator(comm, ftbasic_module);
    ftbasic_module->agreement_info->cur_request->coordinator = coordinator;

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic:iagreement) (2phase) Elected Coordinator is %2d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ftbasic_module->agreement_info->cur_request->coordinator));

    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_SETUP);


    /************************************************
     * Coordinator vs. Participant
     ************************************************/
    if( rank == ftbasic_module->agreement_info->cur_request->coordinator ) {
#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_COORD_PRE_REQ
        /* TEST: Failure before request
         * Expected behavior:
         *   participants abort - Retry 2 phase commit
         */
        if( test_phase_active ) {
            if( two_phase_test_counter <= 0 ) {
                sleep(2);
                kill(getpid(), SIGKILL);
            }
        }
#endif
#endif
        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
        if( OMPI_SUCCESS != (ret = two_phase_coordinator_start_bcast_req(comm, ftbasic_module,
                                                                         &(ftbasic_module->agreement_info->cur_request->num_reqs))) ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (2phase) Coordinator Failed (%2d) - Bcast Req.",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret));
            exit_status = ret;
            goto cleanup;
        }

        ftbasic_module->agreement_info->cur_request->stage = TWO_PHASE_NB_STAGE_COORD_WAIT_BCAST_REQ;
    }
    else {
        if( OMPI_SUCCESS != (ret = two_phase_participant_start_recv_req(comm, ftbasic_module,
                                                                        &(ftbasic_module->agreement_info->cur_request->num_reqs))) ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (2phase) Participant Failed (%2d) - Recv. Req.",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret));
            exit_status = ret;
            goto cleanup;
        }

        ftbasic_module->agreement_info->cur_request->stage = TWO_PHASE_NB_STAGE_PART_WAIT_RECV_REQ;
    }

 cleanup:
    /*
     * Progress function will take it from here
     */
    two_phase_inside_progress = false;

    return exit_status;
}


/*************************************
 * Progress Callbacks
 *************************************/
int mca_coll_ftbasic_agreement_two_phase_term_progress(void)
{
    static int last_asking = 0, last_wait_limit = 1;
    ompi_communicator_t *comm = NULL;
    int max_num_comm = 0, i;
    int num_processed = 0;

#if OPAL_ENABLE_DEBUG
    /* Sanity Check */
    if( mca_coll_ftbasic_agreement_help_num_asking < 0 ) {
        opal_output(ompi_ftmpi_output_handle,
                    "%s ftbasic:iagreement) (2phase) Progress: Warning: Num_Asking less than 0 (%d)! Should not happen!",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), mca_coll_ftbasic_agreement_help_num_asking);
        return 0;
    }
#endif

    /*
     * Only proceed when there are outstanding requests for help
     */
    if( mca_coll_ftbasic_agreement_help_num_asking <= 0 ) {
        return 0;
    }

    if( mca_coll_ftbasic_agreement_help_num_asking != last_asking ) {
        opal_output_verbose(5, ompi_ftmpi_output_handle,
                            "%s ftbasic: agreement) (2phase) Listener: Processing... (%d / %d / %d)",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            mca_coll_ftbasic_agreement_help_num_asking, last_asking,
                            mca_coll_ftbasic_agreement_help_wait_cycles );

        last_asking = mca_coll_ftbasic_agreement_help_num_asking;
        mca_coll_ftbasic_agreement_help_wait_cycles = mca_coll_ftbasic_agreement_help_wait_cycles_inc;
        last_wait_limit = 1;
    }
    else if( mca_coll_ftbasic_agreement_help_wait_cycles > 0 ) {
        /* Throttle how often we call this operation */
        --mca_coll_ftbasic_agreement_help_wait_cycles;
        return 0;
    }
    else {
        last_wait_limit = (last_wait_limit)%10 + 1;
        mca_coll_ftbasic_agreement_help_wait_cycles = (mca_coll_ftbasic_agreement_help_wait_cycles_inc*last_wait_limit);
    }


    if( ompi_mpi_finalized ) {
        return 0;
    }

    if( two_phase_inside_term_progress ) {
        return num_processed;
    }
    two_phase_inside_term_progress = true;

    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Listener: Polling...",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

    /*
     * For each active communicator
     */
    max_num_comm = opal_pointer_array_get_size(&ompi_mpi_communicators);
    for( i = 0; i < max_num_comm; ++i ) {
        comm = (ompi_communicator_t *)opal_pointer_array_get_item(&ompi_mpi_communicators, i);
        if( NULL == comm ) {
            continue;
        }
        /* Skip those without our agreement function */
        if( mca_coll_ftbasic_iagreement_two_phase != comm->c_coll.coll_iagreement ) {
            continue;
        }

        num_processed += mca_coll_ftbasic_agreement_two_phase_term_progress_comm(comm);
    }

    /************************************************
     * Setup the timer again
     ************************************************/
    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Listener: Polling... (%3d Processed)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), num_processed ));

    two_phase_inside_term_progress = false;

    return num_processed;
}

static int mca_coll_ftbasic_agreement_two_phase_term_progress_comm(ompi_communicator_t *comm)
{
    int ret, flag;
    MPI_Status status;
    int num_processed = 0;

    /*
     * Process pending messages until there are no more
     */
    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Listener: Polling Comm %3d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), comm->c_contextid ));

    /************************************************
     * Catchup Request from a peer
     ************************************************/
    do {
        ret = MCA_PML_CALL(iprobe(MPI_ANY_SOURCE,
                                  MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                                  comm, &flag, &status));
        if( OMPI_SUCCESS != ret ) {
            opal_output_verbose(1, ompi_ftmpi_output_handle,
                                "%s ftbasic: agreement) (2phase) Error: Listener: Probe failed (%3d)",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret );
            goto cleanup;
        }

        if( flag ) {
            num_processed++;

            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (2phase) Listener: "
                                 "Responding to Peer %3d on Comm %3d [Num %3d]",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), status.MPI_SOURCE, comm->c_contextid,
                                 num_processed ));

            opal_output( ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Listener: "
                         "Responding to Peer %3d on Comm %3d [Num %3d]",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), status.MPI_SOURCE, comm->c_contextid,
                         num_processed );

            two_phase_term_responder(comm, status.MPI_SOURCE,
                                     (mca_coll_ftbasic_module_t*)comm->c_coll.coll_agreement_module);
        }
    } while(flag);

 cleanup:
    return num_processed;
}

int mca_coll_ftbasic_agreement_two_phase_progress(void)
{
    ompi_communicator_t *comm = NULL;
    int max_num_comm = 0, i;
    int num_processed = 0;
    mca_coll_ftbasic_module_t *ftbasic_module = NULL;

#if OPAL_ENABLE_DEBUG
    /* Sanity Check */
    if( mca_coll_ftbasic_agreement_num_active_nonblocking < 0 ) {
        opal_output(ompi_ftmpi_output_handle,
                    "%s ftbasic:iagreement) (2phase) Progress: Warning: Num_Active less than 0 (%d)! Should not happen!",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), mca_coll_ftbasic_agreement_num_active_nonblocking );
        return 0;
    }
#endif

    /*
     * Only proceed when there are outstanding nonblocking collectives
     */
    if( OPAL_LIKELY(mca_coll_ftbasic_agreement_num_active_nonblocking <= 0) ) {
        return 0;
    }

    if( ompi_mpi_finalized ) {
        return 0;
    }

    /* Sanity check:
     * - Do not recursively enter this function from progress
     * - Do not enter this function if we are executing the termination protocol
     *   (could cause a deadlock)
     */
    if( two_phase_inside_progress ||
        two_phase_inside_term_progress ) {
        return num_processed;
    }

    two_phase_inside_progress = true;

    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic:iagreement) (2phase) Progress: Progressing... (%3d)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), mca_coll_ftbasic_agreement_num_active_nonblocking ));

    /*
     * For each active communicator
     */
    max_num_comm = opal_pointer_array_get_size(&ompi_mpi_communicators);
    for( i = 0; i < max_num_comm; ++i ) {
        comm = (ompi_communicator_t *)opal_pointer_array_get_item(&ompi_mpi_communicators, i);
        if( NULL == comm ) {
            continue;
        }
        /* Skip those without our agreement function */
        if( mca_coll_ftbasic_agreement_two_phase != comm->c_coll.coll_agreement ) {
            continue;
        }
        ftbasic_module = (mca_coll_ftbasic_module_t*) comm->c_coll.coll_agreement_module;
        /* Skip those without an active request */
        if( NULL == ftbasic_module->agreement_info->cur_request ) {
            continue;
        }

        num_processed += mca_coll_ftbasic_agreement_two_phase_progress_comm(comm,
                                                                           ftbasic_module);
    }

    /************************************************
     * Done
     ************************************************/
    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic:iagreement) (2phase) Listener: Progressing... (%3d Processed)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), num_processed ));

    two_phase_inside_progress = false;

    return num_processed;
}

static int mca_coll_ftbasic_agreement_two_phase_progress_comm(ompi_communicator_t *comm,
                                                             mca_coll_ftbasic_module_t *ftbasic_module)
{
    int ret, exit_status = OMPI_SUCCESS, test_exit_status = OMPI_SUCCESS;
    mca_coll_ftbasic_request_t *current_collreq = ftbasic_module->agreement_info->cur_request;
    int rank, size;
    bool is_finished;
    mca_coll_ftbasic_agreement_log_entry_t *log_entry = NULL;
    int num_processed = 0;
#if OPAL_ENABLE_DEBUG
    char *tmp_bitstr = NULL;
#if TWO_PHASE_TEST == TWO_PHASE_TEST_PART_UNCERTAIN || TWO_PHASE_TEST == TWO_PHASE_TEST_PART_SLOW_FAST
    int i;
#endif
#endif

    /* Sanity check:
     * - If there is no active request, then nothing to do.
     */
    if( NULL == current_collreq ) {
        return num_processed;
    }

    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic:iagreement) (2phase) Progress: Progressing...",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);
    log_entry = (mca_coll_ftbasic_agreement_log_entry_t*)current_collreq->log_entry;

    /* Sanity check */
    if( TWO_PHASE_NB_STAGE_DONE == current_collreq->stage ) {
        goto done;
    }

    /*
     * Test for completion of the previous phase
     */
    num_processed++;
    is_finished = false;
    test_exit_status = two_phase_test_all(comm,
                                          ftbasic_module,
                                          current_collreq->num_reqs,
                                          &is_finished,
                                          true);
    if( OMPI_SUCCESS != test_exit_status ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (2phase) Failed (%2d) - two_phase_test_all()",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status));
        /* Continue and decide later */
    }

    /* If previous phase is not yet finished, then nothing to do */
    if( !is_finished && OMPI_SUCCESS == test_exit_status ) {
        OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (2phase) Progress: ***** Waiting",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
        exit_status = OMPI_SUCCESS;
        goto cleanup;
    }
    current_collreq->num_reqs = 0;

    /*
     * Determine the current state
     */
    /********************************
     * Coordinator
     ********************************/
    if( rank == current_collreq->coordinator ) {
        /* Coordinator can safely ignore the value of test_exit_status */

        /********************************
         * Request Broadcast finished, start Gather
         ********************************/
        if( TWO_PHASE_NB_STAGE_COORD_WAIT_BCAST_REQ == current_collreq->stage ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (2phase) Progress: ***** Coord: Finished Bcast Req.",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_COORD_PRE_GATHER
            /* TEST: Failure before gather
             * Expected behavior:
             *   participants termination protocol, decide abort - Retry 2 phase commit
             */
            if( test_phase_active ) {
                if( two_phase_test_counter <= 0 ) {
                    sleep(2);
                    kill(getpid(), SIGKILL);
                }
            }
#endif
#endif
            AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
            AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER);

            /************ Gather Decisions ***************/
            if( OMPI_SUCCESS != (ret = two_phase_coordinator_start_gather(comm,
                                                                          ftbasic_module,
                                                                          current_collreq->local_bitmap,
                                                                          &(current_collreq->num_reqs) ))) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                     "%s ftbasic:iagreement) (2phase) Coordinator Failed (%2d) - Start Gather",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret));
                exit_status = ret;
                goto cleanup;
            }

            current_collreq->stage = TWO_PHASE_NB_STAGE_COORD_WAIT_GATHER;
        }
        /********************************
         * Gather finished, Decide, and Bcast Decision
         ********************************/
        else if( TWO_PHASE_NB_STAGE_COORD_WAIT_GATHER == current_collreq->stage ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (2phase) Progress: ***** Coord: Finished Gather",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

            AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER);
            AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE);

            log_entry->state   = AGREEMENT_STATE_VOTED;

            /************ Decision Phase ***************/
            two_phase_coordinator_commit_final_list(comm,
                                                    ftbasic_module,
                                                    current_collreq->local_bitmap);

            /* Add it to the local log */
            log_entry->state = AGREEMENT_STATE_COMMIT;
            opal_bitmap_copy(log_entry->commit_bitmap, current_collreq->local_bitmap);

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_COORD_PRE_DECIDE
            /* TEST: Failure before bcast of result
             * Expected behavior:
             *   participants termination protocol, decide abort - Retry 2 phase commit
             */
            if( test_phase_active ) {
                if( two_phase_test_counter <= 0 ) {
                    sleep(2);
                    kill(getpid(), SIGKILL);
                }
            }
#endif
#endif

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_PART_ROLL_PRE_DECIDE
            /* TEST: Failure before bcast of result rolling 0, 1 fail
             * Expected behavior:
             *   participants termination protocol, decide commit after 2 elected.
             */
            if( test_phase_active ) {
                if( two_phase_test_counter <= 1 ) {
                    sleep(2);
                    kill(getpid(), SIGKILL);
                }
            }
#endif
#endif

            AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE);
            AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST);

            /************ Bcast Decision ***************/
            if( OMPI_SUCCESS != (ret = two_phase_coordinator_start_bcast(comm,
                                                                         ftbasic_module,
                                                                         current_collreq->local_bitmap,
                                                                         &(current_collreq->num_reqs) ))) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                     "%s ftbasic:iagreement) (2phase) Coordinator Failed (%2d) - Start Bcast",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret));
                exit_status = ret;
                goto cleanup;
            }

            current_collreq->stage = TWO_PHASE_NB_STAGE_COORD_WAIT_BCAST;
        }
        /********************************
         * Bcast Finished, all done
         ********************************/
        else if( TWO_PHASE_NB_STAGE_COORD_WAIT_BCAST == current_collreq->stage ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (2phase) Progress: ***** Coord: Finished Bcast",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_COORD_MID_DECIDE
            /* TEST: Failure mid braodcast (***paired with above***)
             * Expected behavior:
             *   participants termination protocol, decide commit
             */
            if( test_phase_active ) {
                if( two_phase_test_counter <= 0 ) {
                    sleep(2);
                    kill(getpid(), SIGKILL);
                }
            }
#endif
#endif

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_PART_SLOW_FAST
            /* TEST: (See above)
             * Expected behavior:
             *   participants termination protocol, decide commit
             */
            if( test_phase_active ) {
                if( two_phase_test_counter <= 0 ) {
                    sleep(1);
                    kill(getpid(), SIGKILL);
                }
            }
#endif
#endif

            AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST);

            current_collreq->stage = TWO_PHASE_NB_STAGE_DONE;
            current_collreq->mpi_error = OMPI_SUCCESS;
        }
    }
    /********************************
     * Participant
     ********************************/
    else {
        if( TWO_PHASE_NB_STAGE_PART_WAIT_RECV_REQ == current_collreq->stage ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (2phase) Progress: ***** Part: Finished Recv Req. (%2d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

            /* If the coordinator failed, then Abort */
            if( OMPI_SUCCESS != test_exit_status ) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic:iagreement) (2phase) Participant: Error: Coordinator Failure (%2d = %3d) - Abort",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), coordinator, test_exit_status);
                exit_status = test_exit_status;

                log_entry->state = AGREEMENT_STATE_ABORT;
                current_collreq->stage = TWO_PHASE_NB_STAGE_DONE;
                current_collreq->mpi_error  = exit_status;
                current_collreq->mpi_source = coordinator;

                goto done;
            }

            ret = two_phase_participant_start_send(comm,
                                                   ftbasic_module,
                                                   current_collreq->local_bitmap,
                                                   &(current_collreq->num_reqs) );
            if( OMPI_SUCCESS != ret ) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic:iagreement) (2phase) Participant: Error: Coordinator Failure %2d - Abort",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    coordinator);
                exit_status = ret;

                log_entry->state = AGREEMENT_STATE_ABORT;
                current_collreq->stage = TWO_PHASE_NB_STAGE_DONE;
                current_collreq->mpi_error  = ret;
                current_collreq->mpi_source = coordinator;

                goto done;
            }

            current_collreq->stage = TWO_PHASE_NB_STAGE_PART_WAIT_SEND;
        }
        else if( TWO_PHASE_NB_STAGE_PART_WAIT_SEND == current_collreq->stage ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (2phase) Progress: ***** Part: Finished Send (%2d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

            /* If the coordinator failed, then Abort */
            if( OMPI_SUCCESS != test_exit_status ) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic:iagreement) (2phase) Participant: Error: Coordinator Failure (%2d = %3d) - Abort",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), coordinator, test_exit_status);
                exit_status = test_exit_status;

                log_entry->state = AGREEMENT_STATE_ABORT;
                current_collreq->stage = TWO_PHASE_NB_STAGE_DONE;
                current_collreq->mpi_error  = exit_status;
                current_collreq->mpi_source = coordinator;

                goto done;
            }

            log_entry->state = AGREEMENT_STATE_VOTED;

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_PART_UNCERTAIN
            if( test_phase_active ) {
                if( two_phase_test_counter <= 0 && rank == 1) {
                    for( i = 0; i < 100; ++i ) {
                        usleep(100000);
                        opal_progress();
                    }
                }
            }
#endif
#endif

            ret = two_phase_participant_start_recv(comm,
                                                   ftbasic_module,
                                                   current_collreq->local_bitmap,
                                                   &(current_collreq->num_reqs) );
#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_PART_SLOW_FAST
            if( test_phase_active ) {
                if( two_phase_test_counter <= 0 && rank == 1) {
                    for( i = 0; i < 100; ++i ) {
                        usleep(100000);
                        opal_progress();
                    }
                    ret = OMPI_ERROR; /* Simulate failure of the call */
                }
            }
#endif
#endif
            if( OMPI_SUCCESS != ret ) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic:iagreement) (2phase) Participant: Error: Coordinator Failure %2d - Term. Protocol",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), coordinator);

                current_collreq->mpi_error  = ret;
                current_collreq->mpi_source = coordinator;

                /*
                 * JJH TODO:
                 * Improve this to be nonblocking to reduce blocking in progress.
                 */
                if( OMPI_SUCCESS != ( ret = two_phase_term_initiator(comm,
                                                                     current_collreq->local_bitmap,
                                                                     ftbasic_module,
                                                                     log_entry)) ) {
                    opal_output_verbose(1, ompi_ftmpi_output_handle,
                                        "%s ftbasic:iagreement) (2phase) Participant: Error: Term. Protocol failure - Abort",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                    exit_status = ret;

                    log_entry->state = AGREEMENT_STATE_ABORT;
                    current_collreq->stage = TWO_PHASE_NB_STAGE_DONE;
                    current_collreq->mpi_error  = ret;
                    current_collreq->mpi_source = coordinator;

                    goto done;
                }
                /* We have decided something, so skip the Recv (since it will fail) and decide */
                goto uncertain;
            }

            current_collreq->stage = TWO_PHASE_NB_STAGE_PART_WAIT_RECV;
        }
        else if( TWO_PHASE_NB_STAGE_PART_WAIT_RECV == current_collreq->stage ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (2phase) Progress: ***** Part: Finished Recv (%2d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

            /* If the coordinator failed, then enter termination protocol  */
            if( OMPI_SUCCESS != test_exit_status ) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic:iagreement) (2phase) Participant: Error: Coordinator Failure %2d - Term. Protocol",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), coordinator);

                current_collreq->mpi_error  = test_exit_status;
                current_collreq->mpi_source = coordinator;

                /*
                 * JJH TODO:
                 * Improve this to be nonblocking to reduce blocking in progress.
                 */
                if( OMPI_SUCCESS != ( ret = two_phase_term_initiator(comm,
                                                                     current_collreq->local_bitmap,
                                                                     ftbasic_module,
                                                                     log_entry)) ) {
                    opal_output_verbose(1, ompi_ftmpi_output_handle,
                                        "%s ftbasic:iagreement) (2phase) Participant: Error: Term. Protocol failure - Abort",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                    exit_status = ret;

                    log_entry->state = AGREEMENT_STATE_ABORT;
                    current_collreq->stage = TWO_PHASE_NB_STAGE_DONE;
                    current_collreq->mpi_error  = ret;
                    current_collreq->mpi_source = coordinator;

                    goto done;
                }
            }

        uncertain:
            /*
             * If we reponded to a peer before this, then we need to ignore latent
             * messages from the coordinator, and decide with the group.
             */
            if( AGREEMENT_STATE_UNCERTAIN == log_entry->state ) {
                if( OMPI_SUCCESS != ( ret = two_phase_term_initiator(comm,
                                                                     current_collreq->local_bitmap,
                                                                     ftbasic_module,
                                                                     log_entry)) ) {
                    opal_output_verbose(1, ompi_ftmpi_output_handle,
                                        "%s ftbasic:iagreement) (2phase) Participant: Error: Term. Protocol failure - Abort",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                    exit_status = ret;

                    log_entry->state = AGREEMENT_STATE_ABORT;
                    current_collreq->stage = TWO_PHASE_NB_STAGE_DONE;
                    current_collreq->mpi_error  = ret;
                    current_collreq->mpi_source = coordinator;

                    goto done;
                }
            }

            /* Abort could have been decided in the termination protocol */
            if( AGREEMENT_STATE_ABORT == log_entry->state ) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                     "%s ftbasic:iagreement) (2phase) Participant: ***** Decide Abort.",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
                exit_status = OMPI_EXISTS;

                log_entry->state = AGREEMENT_STATE_ABORT;
                current_collreq->stage = TWO_PHASE_NB_STAGE_DONE;
                current_collreq->mpi_error  = exit_status;
                current_collreq->mpi_source = coordinator;

                goto done;
            }

            /************ Commit Phase ***************/
#if OPAL_ENABLE_DEBUG
            tmp_bitstr = opal_bitmap_get_string(current_collreq->local_bitmap);

            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (2phase) Participant: ***** Commit the list [%s]",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), tmp_bitstr));
            if( NULL != tmp_bitstr ) {
                free(tmp_bitstr);
                tmp_bitstr = NULL;
            }
#endif
            log_entry->state = AGREEMENT_STATE_COMMIT;
            opal_bitmap_copy(log_entry->commit_bitmap, current_collreq->local_bitmap);

            current_collreq->stage = TWO_PHASE_NB_STAGE_DONE;
            current_collreq->mpi_error  = OMPI_SUCCESS;
            current_collreq->mpi_source = MPI_PROC_NULL;
        }
    }

 done:
    num_processed++;
    /********************************
     * Check to see if we are all done
     ********************************/
    if( TWO_PHASE_NB_STAGE_DONE == current_collreq->stage ) {
#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST != TWO_PHASE_TEST_NONE
        if( test_phase_active ) {
            two_phase_test_counter++;
        }
#endif
#endif
        /*
         * If we decided 'abort' then try again
         */
        if( log_entry->state == AGREEMENT_STATE_ABORT ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (2phase) Progress: ***** Abort - Retry",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
            two_phase_inside_progress = false;
            internal_iagreement_two_phase(comm,
                                          &current_collreq->group,
                                          current_collreq->flag,
                                          current_collreq->local_bitmap,
                                          ftbasic_module,
                                          current_collreq);
            two_phase_inside_progress = true;
            goto cleanup;
        }
        /*
         * Otherwise we decided commit, so finish the request
         */
        else {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (2phase) Progress: ***** Done",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

            ftbasic_module->agreement_info->cur_request->mpi_error = OMPI_SUCCESS;
            mca_coll_ftbasic_agreement_base_finish_nonblocking(comm,
                                                               /*&tmp_newfailures, true, JJH DOUBLE CHECK*/
                                                               &current_collreq->group,
                                                               current_collreq->flag,
                                                               current_collreq->local_bitmap,
                                                               ftbasic_module);
        }
    }

 cleanup:
#if OPAL_ENABLE_DEBUG
    if( NULL != tmp_bitstr ) {
        free(tmp_bitstr);
        tmp_bitstr = NULL;
    }
#endif

    return num_processed;
}


/*************************************
 * Supporting functions
 *************************************/
static int two_phase_test_all(ompi_communicator_t* comm,
                              mca_coll_ftbasic_module_t *ftbasic_module,
                              int num_reqs,
                              bool *is_finished,
                              bool return_error)
{
    int ret, exit_status = OMPI_SUCCESS;
    int i;
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    int done;

    /*
     * Test for all the requests to complete
     */
    ret = ompi_request_test_all(num_reqs, reqs, &done, statuses);
    *is_finished = OPAL_INT_TO_BOOL(done);

    if(*is_finished ) {
        /* ret = ompi_request_wait_all(num_reqs, reqs, statuses); */
        for(i = 0; i < num_reqs; ++i ) {
            /* Finish waiting on the outstanding requests */
            if( MPI_ERR_PENDING == statuses[i].MPI_ERROR ) {
                ret = ompi_request_wait(&reqs[i], &(statuses[i]));
                if( MPI_SUCCESS != ret ) {
                    opal_output_verbose(1, ompi_ftmpi_output_handle,
                                        "%s ftbasic: agreement) (2phase) Error: Coordinator: Wait failed for peer %2d - skip",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        statuses[i].MPI_SOURCE);
                    if( return_error ) {
                        exit_status = statuses[i].MPI_ERROR;
                    }
                }
            }
            else if( MPI_SUCCESS != statuses[i].MPI_ERROR ) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (2phase) Error: Coordinator: Wait failed for peer %2d - skip",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    statuses[i].MPI_SOURCE);
                if( return_error ) {
                    exit_status = statuses[i].MPI_ERROR;
                }
            }
        }

        mca_coll_ftbasic_free_reqs(reqs, num_reqs);
        num_reqs = 0;
    }

    return exit_status;
}

/*************************************
 * Coordinator functionality
 *************************************/
static int two_phase_elect_coordinator(ompi_communicator_t* comm, mca_coll_ftbasic_module_t *ftbasic_module)
{
    int root, i;
    int rank = ompi_comm_rank(comm);
    int size = ompi_comm_size(comm);
    mca_coll_ftbasic_remote_bitmap_t *remote_bitmap = NULL;

    /*
     * Determine who is the coordinator
     * - Lowest, non-failed process
     */
    root = -1;
    COLL_FTBASIC_FIND_LOWEST_ALIVE_BASE(comm, size, root);

    /*
     * Create a remote bitmaps array, if needed
     */
    if( rank == root ) {
        if( 0 >= opal_list_get_size(&(ftbasic_module->agreement_info->remote_bitmaps)) ) {
            for( i = 0; i < size; ++i) {
                REMOTE_BITMAP_ALLOC(remote_bitmap, size);
                remote_bitmap->rank = -1;
                opal_list_append(&(ftbasic_module->agreement_info->remote_bitmaps), &(remote_bitmap->super.super));
            }
        }
    }
    REMOTE_BITMAP_RESET_NEXT(ftbasic_module);

    return root;
}

static int two_phase_coordinator(ompi_communicator_t* comm,
                                 opal_bitmap_t *local_bitmap,
                                 mca_coll_ftbasic_module_t *ftbasic_module,
                                 mca_coll_ftbasic_agreement_log_entry_t *log_entry)
{
    int ret, exit_status = OMPI_SUCCESS;
    int i, num_reqs = 0;
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;

    /************ Voting Phase ***************/

    /************************************************
     * Send List Request (broadcast)
     * - Skip failed peers
     ************************************************/
    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Coordinator: Bcast Request.",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
    two_phase_coordinator_start_bcast_req(comm, ftbasic_module, &num_reqs);

    /*
     * Wait for all the sends to complete
     */
    ret = ompi_request_wait_all(num_reqs, reqs, statuses);
    if( MPI_SUCCESS != ret ) {
        for(i = 0; i < num_reqs; ++i ) {
            /* Finish waiting on the outstanding requests */
            if( MPI_ERR_PENDING == statuses[i].MPI_ERROR ) {
                ret = ompi_request_wait(&reqs[i], &(statuses[i]));
                if( MPI_SUCCESS != ret ) {
                    opal_output_verbose(1, ompi_ftmpi_output_handle,
                                        "%s ftbasic: agreement) (2phase) Error: Coordinator: Wait failed for peer %2d - skip",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        statuses[i].MPI_SOURCE);
                }
            }
            else if( MPI_SUCCESS != statuses[i].MPI_ERROR ) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (2phase) Error: Coordinator: Wait failed for peer %2d - skip",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    statuses[i].MPI_SOURCE);
            }
        }
    }
    mca_coll_ftbasic_free_reqs(reqs, num_reqs);
    num_reqs = 0;

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_COORD_PRE_GATHER
    /* TEST: Failure before gather
     * Expected behavior:
     *   participants termination protocol, decide abort - Retry 2 phase commit
     */
    if( test_phase_active ) {
        if( two_phase_test_counter <= 0 ) {
            sleep(2);
            kill(getpid(), SIGKILL);
        }
    }
#endif
#endif

    /************************************************
     * Receive Lists from all alive processes (gather)
     * - Skip failed peers
     ************************************************/
    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Coordinator: Receive Lists.",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

    two_phase_coordinator_start_gather(comm, ftbasic_module,
                                       local_bitmap, &num_reqs);

    /*
     * Wait for all the receives to complete
     */
    ret = ompi_request_wait_all(num_reqs, reqs, statuses);
    if( MPI_SUCCESS != ret ) {
        for(i = 0; i < num_reqs; ++i ) {
            /* Finish waiting on the outstanding requests */
            if( MPI_ERR_PENDING == statuses[i].MPI_ERROR ) {
                ret = ompi_request_wait(&reqs[i], &(statuses[i]));
                if( MPI_SUCCESS != ret ) {
                    opal_output_verbose(1, ompi_ftmpi_output_handle,
                                        "%s ftbasic: agreement) (2phase) Error: Coordinator: Wait failed for peer %2d - skip",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        statuses[i].MPI_SOURCE);
                }
            }
            else if( MPI_SUCCESS != statuses[i].MPI_ERROR ) {
                opal_output_verbose(5, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (2phase) Error: Coordinator: Wait failed for peer %2d - skip",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    statuses[i].MPI_SOURCE);
            }
        }
    }
    mca_coll_ftbasic_free_reqs(reqs, num_reqs);
    num_reqs = 0;

    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER);
    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE);

    /************ Decision Phase ***************/
    log_entry->state   = AGREEMENT_STATE_VOTED;

    /************************************************
     * Create the final list (commit list)
     ************************************************/
    two_phase_coordinator_commit_final_list(comm, ftbasic_module, local_bitmap);

    /* Add it to the local log */
    log_entry->state = AGREEMENT_STATE_COMMIT;
    opal_bitmap_copy(log_entry->commit_bitmap, local_bitmap);

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_COORD_PRE_DECIDE
    /* TEST: Failure before bcast of result
     * Expected behavior:
     *   participants termination protocol, decide abort - Retry 2 phase commit
     */
    if( test_phase_active ) {
        if( two_phase_test_counter <= 0 ) {
            sleep(2);
            kill(getpid(), SIGKILL);
        }
    }
#endif
#endif

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_PART_ROLL_PRE_DECIDE
    /* TEST: Failure before bcast of result rolling 0, 1 fail
     * Expected behavior:
     *   participants termination protocol, decide commit after 2 elected.
     */
    if( test_phase_active ) {
        if( two_phase_test_counter <= 1 ) {
            sleep(2);
            kill(getpid(), SIGKILL);
        }
    }
#endif
#endif


    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE);
    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST);

    /************************************************
     * Send the commit list to all peers (broadcast)
     * - Skip failed peers
     * - If peer fails during the broadcast then mark operation as successful,
     *   but make it look like the peer failed just after the protocol finished.
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Coordinator: Bcast final list.",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
    two_phase_coordinator_start_bcast(comm, ftbasic_module,
                                      local_bitmap, &num_reqs);

    /*
     * Wait for all the sends to complete
     */
    ret = ompi_request_wait_all(num_reqs, reqs, statuses);
    if( MPI_SUCCESS != ret ) {
        for(i = 0; i < num_reqs; ++i ) {
            /* Finish waiting on the outstanding requests */
            if( MPI_ERR_PENDING == statuses[i].MPI_ERROR ) {
                ret = ompi_request_wait(&reqs[i], &(statuses[i]));
                if( MPI_SUCCESS != ret ) {
                    opal_output_verbose(1, ompi_ftmpi_output_handle,
                                        "%s ftbasic: agreement) (2phase) Error: Coordinator: Wait failed for peer %2d - skip",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        statuses[i].MPI_SOURCE);
                }
            }
            else if( MPI_SUCCESS != statuses[i].MPI_ERROR ) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (2phase) Error: Coordinator: Wait failed for peer %2d - skip",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    statuses[i].MPI_SOURCE);
            }
        }
    }
    mca_coll_ftbasic_free_reqs(reqs, num_reqs);
    num_reqs = 0;

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_COORD_MID_DECIDE
    /* TEST: Failure mid braodcast (***paired with above***)
     * Expected behavior:
     *   participants termination protocol, decide commit
     */
    if( test_phase_active ) {
        if( two_phase_test_counter <= 0 ) {
            sleep(2);
            kill(getpid(), SIGKILL);
        }
    }
#endif
#endif

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_PART_SLOW_FAST
    /* TEST: (See above)
     * Expected behavior:
     *   participants termination protocol, decide commit
     */
    if( test_phase_active ) {
        if( two_phase_test_counter <= 0 ) {
            sleep(1);
            kill(getpid(), SIGKILL);
        }
    }
#endif
#endif

    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST);

    /*
     * Cleanup
     */
    mca_coll_ftbasic_free_reqs(reqs, num_reqs);

    return exit_status;
}

/*************************************
 * Coordinator: Bcast Request
 *************************************/
static int two_phase_coordinator_start_bcast_req(ompi_communicator_t* comm,
                                                 mca_coll_ftbasic_module_t *ftbasic_module,
                                                 int *num_reqs)
{
    int ret, i;
    int rank = ompi_comm_rank(comm);
    int size = ompi_comm_size(comm);
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;

    *num_reqs = 0;
    for (i = 0; i < size; ++i) {
        if (i == rank) {
            continue;
        }

        /*
         * Make sure the peer is alive
         * - Skip/Ignore all failed processes
         */
        COLL_FTBASIC_CHECK_SKIP_ALL_FAILED_BASE(comm, i);

        /*
         * Post all sends
         */
        ret = MCA_PML_CALL(isend( NULL, 0, MPI_BYTE,
                                  i, MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                                  MCA_PML_BASE_SEND_STANDARD,
                                  comm, &(reqs[*num_reqs])));
        if (MPI_SUCCESS != ret) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (2phase) Error: Coordinator: Peer %2d failed in isend(%2d) - skip",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 i, ret));
            continue;
        }

        (*num_reqs)++;
    }

    return OMPI_SUCCESS;
}

/*************************************
 * Coordinator: Gather
 *************************************/
static int two_phase_coordinator_start_gather(ompi_communicator_t* comm,
                                              mca_coll_ftbasic_module_t *ftbasic_module,
                                              opal_bitmap_t *local_bitmap,
                                              int *num_reqs)
{
    int ret, i;
    int rank = ompi_comm_rank(comm);
    int size = ompi_comm_size(comm);
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    int packet_size = 0;
    mca_coll_ftbasic_remote_bitmap_t *remote_bitmap = NULL;

    packet_size = local_bitmap->array_size;

    *num_reqs = 0;
    for (i = 0; i < size; ++i) {
        REMOTE_BITMAP_GET_NEXT(ftbasic_module, remote_bitmap, size);
        remote_bitmap->rank = i;
        if (i == rank) {
            opal_bitmap_copy(remote_bitmap->bitmap, local_bitmap);
            continue;
        }

        /*
         * Make sure the peer is alive
         * - Skip all failed
         */
        COLL_FTBASIC_CHECK_SKIP_ALL_FAILED_BASE(comm, i);

        /*
         * Post all receives
         */
        ret = MCA_PML_CALL(irecv( (remote_bitmap->bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                  i, MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                                  comm, &(reqs[*num_reqs]) ));
        if (MPI_SUCCESS != ret) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (2phase) Error: Coordinator: Peer %2d failed in irecv(%2d) - skip",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 i, ret));
            continue;
        }

        (*num_reqs)++;
    }

    return OMPI_SUCCESS;
}

/*************************************
 * Coordinator: Bcast Commit
 *************************************/
static int two_phase_coordinator_commit_final_list(ompi_communicator_t* comm,
                                                   mca_coll_ftbasic_module_t *ftbasic_module,
                                                   opal_bitmap_t *local_bitmap)
{
    int rank = ompi_comm_rank(comm);
    mca_coll_ftbasic_remote_bitmap_t *remote_bitmap = NULL;
    opal_list_item_t* item = NULL;
    bool local_flag, remote_flag;

    for( item  = opal_list_get_first(&(ftbasic_module->agreement_info->remote_bitmaps));
         item != opal_list_get_end(&(ftbasic_module->agreement_info->remote_bitmaps));
         item  = opal_list_get_next(item) ) {
        remote_bitmap = (mca_coll_ftbasic_remote_bitmap_t*)item;

        if( remote_bitmap->rank == rank ) {
            continue;
        }

        /*
         * Make sure the peer is alive
         * - Skip all failed procs
         */
        if( !ompi_comm_is_proc_active(comm, remote_bitmap->rank, false) ) {
            /* Make sure to mark them just in case they are not already marked */
            opal_bitmap_set_bit(local_bitmap, remote_bitmap->rank);
            continue;
        }

        /*
         * The 'flag' bit is not or'ed with everything else, so pull it out so
         * we can handle it separately.
         */
        if( opal_bitmap_is_set_bit(local_bitmap, FTBASIC_AGREEMENT_FLAG_BIT_LOC(local_bitmap)) ) {
            local_flag = true;
        } else {
            local_flag = false;
        }
        if( opal_bitmap_is_set_bit(remote_bitmap->bitmap, FTBASIC_AGREEMENT_FLAG_BIT_LOC(remote_bitmap->bitmap)) ) {
            remote_flag = true;
        } else {
            remote_flag = false;
        }

        /*
         * Bitwise OR over the whole bitmap
         */
        opal_bitmap_bitwise_or_inplace(local_bitmap, remote_bitmap->bitmap);

        /*
         * Set the 'flag' bit as the logical AND of the two values
         */
        if( remote_flag && local_flag ) {
            opal_bitmap_set_bit(local_bitmap, FTBASIC_AGREEMENT_FLAG_BIT_LOC(local_bitmap));
        } else {
            opal_bitmap_clear_bit(local_bitmap, FTBASIC_AGREEMENT_FLAG_BIT_LOC(local_bitmap));
        }
    }
    REMOTE_BITMAP_RESET_NEXT(ftbasic_module);

    return OMPI_SUCCESS;
}

/*************************************
 * Coordinator: Bcast
 *************************************/
static int two_phase_coordinator_start_bcast(ompi_communicator_t* comm,
                                             mca_coll_ftbasic_module_t *ftbasic_module,
                                             opal_bitmap_t *local_bitmap,
                                             int *num_reqs)
{
    int ret, i;
    int rank = ompi_comm_rank(comm);
    int size = ompi_comm_size(comm);
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    int packet_size = 0;

    packet_size = local_bitmap->array_size;

    *num_reqs = 0;
    for (i = 0; i < size; ++i) {
        if (i == rank) {
            continue;
        }

        /*
         * Make sure the peer is alive
         * - Skip/Ignore all failed processes
         */
        COLL_FTBASIC_CHECK_SKIP_ALL_FAILED_BASE(comm, i);

        /*
         * Post all sends
         */
        ret = MCA_PML_CALL(isend( (local_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                  i, MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                                  MCA_PML_BASE_SEND_STANDARD,
                                  comm, &(reqs[*num_reqs])));
        if (MPI_SUCCESS != ret) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (2phase) Error: Coordinator: Peer %2d failed in isend(%2d) - skip",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 i, ret));
            continue;
        }

        (*num_reqs)++;

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_COORD_MID_DECIDE
        /* TEST: Failure mid braodcast (***paired with below***)
         * Expected behavior:
         *   participants termination protocol, decide commit
         */
        if( test_phase_active ) {
            if( two_phase_test_counter <= 0 ) {
                if( (*num_reqs) > 1 ) {
                    break;
                }
            }
        }
#endif
#endif

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_PART_UNCERTAIN
        /* TEST: Failure of coordinator after bcast to 1.
         *       1 is slow so it becomes uncertian of 0 after peers start to
         *       ask it for a decision. This message may or may not be
         *       delivered, but if it is delivered 1 will ignore it.
         * Expected behavior:
         *   participants termination protocol, decide abort
         */
        if( test_phase_active ) {
            if( two_phase_test_counter <= 0 ) {
                if( (*num_reqs) > 0 ) {
                    sleep(1);
                    kill(getpid(), SIGKILL);
                }
            }
        }
#endif
#endif

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_PART_SLOW_FAST
        /* TEST: 1 is slow in receiving the message, 2 receives it, but 0 dies
         *       before sending to anyone else. 2 must lead the commit procedure.
         * Expected behavior:
         *   participants termination protocol, decide commit
         */
        if( test_phase_active ) {
            if( two_phase_test_counter <= 0 ) {
                if( (*num_reqs) > 1 ) {
                    break;
                }
            }
        }
#endif
#endif
    }

    return OMPI_SUCCESS;
}

/*************************************
 * Participant
 *************************************/
static int two_phase_participant(ompi_communicator_t* comm,
                                 opal_bitmap_t *local_bitmap,
                                 mca_coll_ftbasic_module_t *ftbasic_module,
                                 mca_coll_ftbasic_agreement_log_entry_t *log_entry)
{
    int ret, exit_status = OMPI_SUCCESS;
    int num_reqs = 0;
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
#if OPAL_ENABLE_DEBUG
    char *tmp_bitstr = NULL;
#if TWO_PHASE_TEST != TWO_PHASE_TEST_NONE
    int rank = ompi_comm_rank(comm);
    int i;
#endif
#endif

    /************ Not Voted Phase ***************/

    /************************************************
     * Wait for the List Request
     * - coordinator failure = abort
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Participant: Wait for request",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
    /* Passing NULL for the num_reqs makes this a blocking Recv */
    ret = two_phase_participant_start_recv_req(comm, ftbasic_module, NULL );

    if (MPI_SUCCESS != ret) {
        opal_output_verbose(1, ompi_ftmpi_output_handle,
                            "%s ftbasic: agreement) (2phase) Participant: Error: Coordinator Failure (%2d = %3d) - Abort",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), coordinator, ret);
        log_entry->state = AGREEMENT_STATE_ABORT;
        exit_status = ret;
        goto cleanup;
    }

    /************************************************
     * Send list of failed processes
     * - coordinator failure = abort
     * Note: Enter uncertainty region
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Participant: Send list",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /* Passing NULL for the num_reqs makes this a blocking Send */
    ret = two_phase_participant_start_send(comm, ftbasic_module, local_bitmap, NULL );

    if (MPI_SUCCESS != ret) {
        opal_output_verbose(1, ompi_ftmpi_output_handle,
                            "%s ftbasic: agreement) (2phase) Participant: Error: Coordinator Failure %2d - Abort",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                            coordinator);
        log_entry->state = AGREEMENT_STATE_ABORT;
        exit_status = ret;
        goto cleanup;
    }


    /************ Voted Phase / Uncertainty Period ***************/
    log_entry->state = AGREEMENT_STATE_VOTED;

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_PART_UNCERTAIN
    if( test_phase_active ) {
        if( two_phase_test_counter <= 0 && rank == 1) {
            for( i = 0; i < 100; ++i ) {
                usleep(100000);
                opal_progress();
            }
        }
    }
#endif
#endif

    /************************************************
     * Receive the final list
     * - coordinator failure = termination protocol
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Participant: Wait for final list",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
    /* Passing NULL for the num_reqs makes this a blocking Recv */
    ret = two_phase_participant_start_recv(comm, ftbasic_module, local_bitmap, NULL );

#if OPAL_ENABLE_DEBUG
#if TWO_PHASE_TEST == TWO_PHASE_TEST_PART_SLOW_FAST
    if( test_phase_active ) {
        if( two_phase_test_counter <= 0 && rank == 1) {
            for( i = 0; i < 100; ++i ) {
                usleep(100000);
                opal_progress();
            }
            ret = OMPI_ERROR; /* Simulate failure of the call */
        }
    }
#endif
#endif
    if (MPI_SUCCESS != ret) {
        opal_output_verbose(1, ompi_ftmpi_output_handle,
                            "%s ftbasic: agreement) (2phase) Participant: Error: Coordinator Failure %2d - Term. Protocol",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), coordinator);

        if( OMPI_SUCCESS != ( ret = two_phase_term_initiator(comm, local_bitmap, ftbasic_module, log_entry)) ) {
            opal_output_verbose(1, ompi_ftmpi_output_handle,
                                "%s ftbasic: agreement) (2phase) Participant: Error: Term. Protocol failure - Abort",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            log_entry->state = AGREEMENT_STATE_ABORT;
            exit_status = ret;
            goto cleanup;
        }
    }


    /*
     * If we reponded to a peer before this, then we need to ignore latent
     * messages from the coordinator, and decide with the group.
     */
    if( AGREEMENT_STATE_UNCERTAIN == log_entry->state ) {
        if( OMPI_SUCCESS != ( ret = two_phase_term_initiator(comm, local_bitmap, ftbasic_module, log_entry)) ) {
            opal_output_verbose(1, ompi_ftmpi_output_handle,
                                "%s ftbasic: agreement) (2phase) Participant: Error: Term. Protocol failure - Abort",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            log_entry->state = AGREEMENT_STATE_ABORT;
            exit_status = ret;
            goto cleanup;
        }
    }

    /* Abort could have been decided in the termination protocol */
    if( AGREEMENT_STATE_ABORT == log_entry->state ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (2phase) Participant: Decide Abort.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
        exit_status = OMPI_EXISTS;
        goto cleanup;
    }

    /************ Commit Phase ***************/

    /************************************************
     * Commit this list
     ************************************************/
#if OPAL_ENABLE_DEBUG
    tmp_bitstr = opal_bitmap_get_string(local_bitmap);
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Participant: Commit the list [%s]",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), tmp_bitstr));
#endif

    log_entry->state = AGREEMENT_STATE_COMMIT;
    opal_bitmap_copy(log_entry->commit_bitmap, local_bitmap);

 cleanup:
    mca_coll_ftbasic_free_reqs(reqs, num_reqs);

#if OPAL_ENABLE_DEBUG
    if( NULL != tmp_bitstr ) {
        free(tmp_bitstr);
        tmp_bitstr = NULL;
    }
#endif

    return exit_status;
}

/*************************************
 * Participant: Recv
 *************************************/
static int two_phase_participant_start_recv_req(ompi_communicator_t* comm,
                                                mca_coll_ftbasic_module_t *ftbasic_module,
                                                int *num_reqs)
{
    int ret;
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;

    /*
     * If we are not given a 'num_reqs' then just block here
     */
    if( NULL == num_reqs ) {
        return MCA_PML_CALL(recv( NULL, 0, MPI_BYTE,
                                  coordinator, MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                                  comm, &(statuses[0]) ));
    }

    *num_reqs = 0;
    ret = MCA_PML_CALL(irecv( NULL, 0, MPI_BYTE,
                              coordinator, MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                              comm, &(reqs[*num_reqs]) ));
    (*num_reqs)++;

    return ret;
}

/*************************************
 * Participant: Send
 *************************************/
static int two_phase_participant_start_send(ompi_communicator_t* comm,
                                            mca_coll_ftbasic_module_t *ftbasic_module,
                                            opal_bitmap_t *local_bitmap,
                                            int *num_reqs)
{
    int ret;
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    int packet_size = 0;

    packet_size = local_bitmap->array_size;

    if( NULL == num_reqs ) {
        return MCA_PML_CALL(send( (local_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                  coordinator, MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                                  MCA_PML_BASE_SEND_STANDARD,
                                  comm));
    }

    *num_reqs = 0;
    ret = MCA_PML_CALL(isend( (local_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                              coordinator, MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                              MCA_PML_BASE_SEND_STANDARD,
                              comm, &(reqs[*num_reqs]) ));
    (*num_reqs)++;

    return ret;
}

/*************************************
 * Participant: Recv
 *************************************/
static int two_phase_participant_start_recv(ompi_communicator_t* comm,
                                            mca_coll_ftbasic_module_t *ftbasic_module,
                                            opal_bitmap_t *local_bitmap,
                                            int *num_reqs)
{
    int ret;
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    int packet_size = 0;

    packet_size = local_bitmap->array_size;

    /*
     * If we are not given a 'num_reqs' then just block here
     */
    if( NULL == num_reqs ) {
        return MCA_PML_CALL(recv( (local_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                  coordinator, MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                                  comm, &(statuses[0]) ));
    }

    *num_reqs = 0;
    ret = MCA_PML_CALL(irecv( (local_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                              coordinator, MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                              comm, &(reqs[*num_reqs]) ));
    (*num_reqs)++;

    return ret;
}

/*************************************
 * Termination Protocol: Initiator
 *************************************/
static int two_phase_term_initiator(ompi_communicator_t* comm,
                                    opal_bitmap_t *local_bitmap,
                                    mca_coll_ftbasic_module_t *ftbasic_module,
                                    mca_coll_ftbasic_agreement_log_entry_t *log_entry)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank, size, i;
    int num_reqs = 0;
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    int packet_size = 0;
    int loc_state = AGREEMENT_STATE_NONE;

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);
    packet_size = local_bitmap->array_size;

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Term: Initiator = %2d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), rank));

    log_entry->state = AGREEMENT_STATE_UNCERTAIN;

    /************************************************
     * For each alive peer
     * We must go linearly to avoid some race conditions that may come up
     * if we used a technique such as:
     *       for(i = (rank+1)%size; i != rank; i = (i+1)%size )
     * This does flood the ranks in order, but hopefully we can improve on
     * that in the future.
     * Notice that if we respond to a request then we know that the peer
     * has already asked all of the ranks before us and those ranks were
     * either dead or undecided.
     ************************************************/
    for( i = 0; i < size; ++i ) {
        /* Skip self - Should not happen, but just in case*/
        if( i == rank ) {
            continue;
        }

        /* Communicate to only suspected alive processes */
        COLL_FTBASIC_CHECK_SKIP_ALL_FAILED_BASE(comm, i);

        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (2phase) Term: Initiator: Asking Rank %2d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i));

        /************************************************
         * Send a Decision Request message
         * Send both the seq_num and attempt_num so the peer can decide if it
         * needs to move to the next round.
         ************************************************/
        ret = MCA_PML_CALL(send( &(log_entry->seq_num), 1, MPI_INT,
                                 i, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                                 MCA_PML_BASE_SEND_STANDARD,
                                 comm));
        if (MPI_SUCCESS != ret) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (2phase) Error: Term: Initiator: Peer %2d failed in send(%2d) - skip",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 i, ret));
            continue;
        }

        ret = MCA_PML_CALL(send( &(log_entry->attempt_num), 1, MPI_INT,
                                 i, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                                 MCA_PML_BASE_SEND_STANDARD,
                                 comm));
        if (MPI_SUCCESS != ret) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (2phase) Error: Term: Initiator: Peer %2d failed in send(%2d) - skip",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 i, ret));
            continue;
        }

        /************************************************
         * Receive decision
         * - On peer failure, skip and try another
         * - NOTE: If no peer has decision, then the protocol fails. (Block)
         * - Decide on the list provided by peer, or keep asking
         ************************************************/
        loc_state = AGREEMENT_STATE_NONE;
        ret = MCA_PML_CALL(recv( &loc_state, 1, MPI_INT,
                                 i, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP,
                                 comm, &(statuses[0]) ));
        if (MPI_SUCCESS != ret) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (2phase) Error: Term: Initiator: Peer %2d failed in recv(%2d) - skip",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 i, ret));
            continue;
        }

        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (2phase) Term: Initiator: Rank %2d replied %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i,
                             (AGREEMENT_STATE_COMMIT      == loc_state ? "Commit" :
                              (AGREEMENT_STATE_ABORT      == loc_state ? "Abort" :
                               (AGREEMENT_STATE_UNCERTAIN == loc_state ? "Uncertian" :
                                (AGREEMENT_STATE_VOTED    == loc_state ? "Voted" :
                                 (AGREEMENT_STATE_NONE    == loc_state ? "None" : "Unknown")))))
                             ));

        /*
         * If they decided abort, then do the same.
         */
        if( AGREEMENT_STATE_ABORT == loc_state ) {
            log_entry->state = AGREEMENT_STATE_ABORT;
            /* Done with this function */
            exit_status = OMPI_SUCCESS;
            goto cleanup;
        }
        /*
         * If they committed already, accept their list
         */
        else if( AGREEMENT_STATE_COMMIT == loc_state ) {
            ret = MCA_PML_CALL(recv( (local_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                     i, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP,
                                     comm, &(statuses[0]) ));
            if (MPI_SUCCESS != ret) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                     "%s ftbasic: agreement) (2phase) Error: Term: Initiator: Peer %2d failed in recv(%2d) - skip",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     i, ret));
                continue;
            }

            /* Done with this function */
            exit_status = OMPI_SUCCESS;
            goto cleanup;
        }
        /*
         * If they have not committed or aborted, then keep searching.
         *
         * It is also possible that they started the next round, but then
         * they just wait for us to join. So after we are done asking everyone
         * we will independently decide ABORT and start the next round.
         */
        else {
            loc_state = AGREEMENT_STATE_NONE;
        }
    }

    /*
     * If there were no peers that knew the result, then we fail
     * Note: This is the 'blocking' step of this protocol. This process
     *       is supposed to block until some process that knows the result
     *       is restarted.
     */
    if( AGREEMENT_STATE_NONE == loc_state ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (2phase) Term: Initiator: Unknown state - Give up",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
        exit_status = OMPI_EXISTS;
    }

 cleanup:
    mca_coll_ftbasic_free_reqs(reqs, num_reqs);

    return exit_status;
}

/*************************************
 * Termination Protocol: Responder
 *************************************/
static int two_phase_term_responder(ompi_communicator_t* comm, int peer,
                                    mca_coll_ftbasic_module_t *ftbasic_module)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank;
    int num_reqs = 0;
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    int packet_size = 0;
    int loc_state, loc_val_iter, loc_attempt_num;
    mca_coll_ftbasic_agreement_log_entry_t *log_entry = NULL;

    rank = ompi_comm_rank(comm);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Term: Responder = %2d Reply to %2d ***********************************",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), rank, peer));

    /************************************************
     * Finish receiving the message
     * - ignore peer failure
     ************************************************/
    ret = MCA_PML_CALL(recv( &loc_val_iter, 1, MPI_INT,
                             peer, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                             comm, &(statuses[0]) ));
    if (MPI_SUCCESS != ret) {
        opal_output_verbose(1, ompi_ftmpi_output_handle,
                            "%s ftbasic: agreement) (2phase) Term: Responder: Error: Peer Failure %2d - Ignore",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer);
        goto cleanup;
    }

    ret = MCA_PML_CALL(recv( &loc_attempt_num, 1, MPI_INT,
                             peer, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                             comm, &(statuses[0]) ));
    if (MPI_SUCCESS != ret) {
        opal_output_verbose(1, ompi_ftmpi_output_handle,
                            "%s ftbasic: agreement) (2phase) Term: Responder: Error: Peer Failure %2d - Ignore",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer);
        goto cleanup;
    }

    /* Find this log entry */
    loc_state = AGREEMENT_STATE_NONE;
    log_entry = mca_coll_ftbasic_agreement_log_entry_find(comm, loc_val_iter, false, ftbasic_module);

    /*
     * If we have not locally decided for this attempt of the transaction
     * then we need to become uncertian, unless we already decided.
     */
    if( log_entry->attempt_num <= loc_attempt_num ) {
        if( AGREEMENT_STATE_ABORT  != log_entry->state &&
            AGREEMENT_STATE_COMMIT != log_entry->state ) {
            log_entry->state = AGREEMENT_STATE_UNCERTAIN;
        }
    }

    loc_state = log_entry->state;

    /************************************************
     * Determine if we have decided or not
     * - ignore peer failure
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (2phase) Term: Responder: Reply to Rank %2d State %s [%2d/%2d]",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer,
                         (AGREEMENT_STATE_COMMIT      == loc_state ? "Commit" :
                          (AGREEMENT_STATE_ABORT      == loc_state ? "Abort" :
                           (AGREEMENT_STATE_UNCERTAIN == loc_state ? "Uncertian" :
                            (AGREEMENT_STATE_VOTED    == loc_state ? "Voted" :
                             (AGREEMENT_STATE_NONE    == loc_state ? "None" : "Unknown"))))),
                         loc_attempt_num, log_entry->attempt_num
                         ));

    ret = MCA_PML_CALL(send( &loc_state, 1, MPI_INT,
                             peer, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP,
                             MCA_PML_BASE_SEND_STANDARD,
                             comm));
    if (MPI_SUCCESS != ret) {
        opal_output_verbose(1, ompi_ftmpi_output_handle,
                            "%s ftbasic: agreement) (2phase) Term: Responder: Error: Peer Failure %2d - Ignore",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer);
        goto cleanup;
    }

    if( AGREEMENT_STATE_COMMIT != loc_state ) {
        goto cleanup;
    }

    /************************************************
     * Send back response
     * - ignore peer failure
     ***********************************************/
    packet_size = log_entry->commit_bitmap->array_size;

    ret = MCA_PML_CALL(send( (log_entry->commit_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                             peer, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP,
                             MCA_PML_BASE_SEND_STANDARD,
                             comm));
    if (MPI_SUCCESS != ret) {
        opal_output_verbose(1, ompi_ftmpi_output_handle,
                            "%s ftbasic: agreement) (2phase) Term: Responder: Error: Peer Failure %2d - Ignore",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer);
        goto cleanup;
    }

 cleanup:
    mca_coll_ftbasic_free_reqs(reqs, num_reqs);
    return exit_status;
}
