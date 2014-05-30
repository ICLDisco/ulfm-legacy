/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 *
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */
#ifndef MCA_COLL_FTBASIC_AGREEMENT_EXPORT_H
#define MCA_COLL_FTBASIC_AGREEMENT_EXPORT_H

#include "coll_ftbasic.h"

BEGIN_C_DECLS

/* Match this with opal_bitmap.c */
#define SIZE_OF_CHAR ((int) (sizeof(char) * 8))

/* Extra bits added to bitmap
 * +1            : COMMIT / ABORT Bit
 * +SIZE_OF_CHAR : flag (as a separate segment in bitmap)
 */
#define FTBASIC_AGREEMENT_EXTRA_BITS (1 + SIZE_OF_CHAR)

#define FTBASIC_AGREEMENT_FLAG_BIT_LOC(bm) ( ( (bm->array_size - 1)*SIZE_OF_CHAR + 1) )

    /*
     * Base supporting functions
     */
    extern int mca_coll_ftbasic_agreement_num_active_nonblocking;
    extern int mca_coll_ftbasic_agreement_help_num_asking;
    extern int mca_coll_ftbasic_agreement_help_wait_cycles;

    int mca_coll_ftbasic_agreement_init(mca_coll_ftbasic_module_t *module);
    int mca_coll_ftbasic_agreement_finalize(mca_coll_ftbasic_module_t *module);

    int mca_coll_ftbasic_agreement_base_setup_blocking(ompi_communicator_t* comm,
                                                       ompi_group_t **group,
                                                       int *flag,
                                                       opal_bitmap_t *local_bitmap,
                                                       mca_coll_ftbasic_module_t *ftbasic_module);
    int mca_coll_ftbasic_agreement_base_setup_nonblocking(ompi_communicator_t* comm,
                                                          ompi_group_t **group,
                                                          int *flag,
                                                          opal_bitmap_t *local_bitmap,
                                                          mca_coll_ftbasic_module_t *ftbasic_module,
                                                          ompi_request_t **request);

    int mca_coll_ftbasic_agreement_base_finish_blocking(ompi_communicator_t* comm,
                                                        ompi_group_t **group,
                                                        int *flag,
                                                        opal_bitmap_t *local_bitmap,
                                                        mca_coll_ftbasic_module_t *ftbasic_module);

    int mca_coll_ftbasic_agreement_base_finish_nonblocking(ompi_communicator_t* comm,
                                                           ompi_group_t **group,
                                                           int *flag,
                                                           opal_bitmap_t *local_bitmap,
                                                           mca_coll_ftbasic_module_t *ftbasic_module);

    int mca_coll_ftbasic_agreement_base_term_request_help(ompi_communicator_t* comm,
                                                          mca_coll_ftbasic_module_t *ftbasic_module);


    /*
     * Initialize the local bitmap from locally known failure set
     */
    int agreement_init_local_bitmap(ompi_communicator_t* comm, opal_bitmap_t *local_bitmap);

    /*
     * Allreduce specific
     */
    int mca_coll_ftbasic_agreement_allreduce_init(mca_coll_ftbasic_module_t *module);
    int mca_coll_ftbasic_agreement_allreduce_finalize(mca_coll_ftbasic_module_t *module);

    /*
     * Two phase specific
     */
    int mca_coll_ftbasic_agreement_two_phase_init(mca_coll_ftbasic_module_t *module);
    int mca_coll_ftbasic_agreement_two_phase_finalize(mca_coll_ftbasic_module_t *module);

    extern int mca_coll_ftbasic_agreement_two_phase_progress(void);
    extern int mca_coll_ftbasic_agreement_two_phase_term_progress(void);

    /*
     * Log two phase specific
     */
    int mca_coll_ftbasic_agreement_log_two_phase_init(mca_coll_ftbasic_module_t *module);
    int mca_coll_ftbasic_agreement_log_two_phase_finalize(mca_coll_ftbasic_module_t *module);

    extern int mca_coll_ftbasic_agreement_log_two_phase_progress(void);
    extern int mca_coll_ftbasic_agreement_log_two_phase_term_progress(void);
    int mca_coll_ftbasic_agreement_log_two_phase_refresh_tree(opal_bitmap_t *local_bitmap,
                                                                   ompi_communicator_t* comm,
                                                                   mca_coll_ftbasic_module_t *ftbasic_module);

    /*
     * Early Returning Specific
     */
    int mca_coll_ftbasic_agreement_era_init(mca_coll_ftbasic_module_t *module);
    int mca_coll_ftbasic_agreement_era_fini(mca_coll_ftbasic_module_t *module);

    /*
     * Log entry structure
     */
typedef enum {
    /*
     * Protocol has not started yet
     */
    AGREEMENT_STATE_NONE      = 0,
    /*
     * Vate has been requested of the children
     */
    AGREEMENT_STATE_VOTE_REQ  = 1,
    /*
     * Vate has been sent to the coordinator
     */
    AGREEMENT_STATE_VOTED     = 2,
    /*
     * Previously voted, but not yet commit/abort.
     * A peer has asked for the result indicating that the coordinator
     * has failed, but I may or may not know it yet. So I need to
     * ignore any message from the coordinator that may be buffered
     * and decide with the remaining group.
     */
    AGREEMENT_STATE_UNCERTAIN = 3,
    /*
     * Decided Commit from the coordinator
     */
    AGREEMENT_STATE_COMMIT    = 4,
    /*
     * Decided Commit from the coordinator (finished bcast)
     */
    AGREEMENT_STATE_COMMITTED = 5,
    /*
     * Decided Abort
     */
    AGREEMENT_STATE_ABORT     = 6,
    /*
     * Decided Abort (finished bcast)
     */
    AGREEMENT_STATE_ABORTED   = 7
} agreement_state_t;

#define AGREEMENT_STATE_STR(loc_state)                                   \
    (AGREEMENT_STATE_COMMIT         == loc_state ? "Commit" :            \
     (AGREEMENT_STATE_COMMITTED     == loc_state ? "Committed" :         \
      (AGREEMENT_STATE_ABORT        == loc_state ? "Abort" :             \
       (AGREEMENT_STATE_ABORTED     == loc_state ? "Aborted" :           \
        (AGREEMENT_STATE_UNCERTAIN  == loc_state ? "Uncertian" :         \
         (AGREEMENT_STATE_VOTED     == loc_state ? "Voted" :             \
          (AGREEMENT_STATE_VOTE_REQ == loc_state ? "Voted Req." :        \
           (AGREEMENT_STATE_NONE    == loc_state ? "None" : "Unknown")   \
           )))))))                                                      \

struct mca_coll_ftbasic_agreement_log_entry_t {
    /** This is a list object */
    opal_list_item_t super;

    /** Agreement Seq Number */
    int seq_num;

    /** State of the protocol */
    agreement_state_t state;

    /** Committed bitmap */
    opal_bitmap_t *commit_bitmap;

    /** Attempt number - For debugging only */
    int attempt_num;
};
typedef struct mca_coll_ftbasic_agreement_log_entry_t mca_coll_ftbasic_agreement_log_entry_t;
OBJ_CLASS_DECLARATION(mca_coll_ftbasic_agreement_log_entry_t);

    mca_coll_ftbasic_agreement_log_entry_t *
    mca_coll_ftbasic_agreement_log_entry_find(ompi_communicator_t* comm,
                                             int seq_num,
                                             bool create,
                                             mca_coll_ftbasic_module_t *ftbasic_module);


    /*
     * Remote bitmap list item
     * Must be declared here so we can setup the free list
     */
struct mca_coll_ftbasic_remote_bitmap_t {
    /** This is a list object */
    ompi_free_list_item_t super;

    /* Rank of the process */
    int rank;

    /* Bitmap associated */
    opal_bitmap_t *bitmap;
};
typedef struct mca_coll_ftbasic_remote_bitmap_t mca_coll_ftbasic_remote_bitmap_t;
OBJ_CLASS_DECLARATION(mca_coll_ftbasic_remote_bitmap_t);

extern ompi_free_list_t mca_coll_ftbasic_remote_bitmap_free_list;
extern int mca_coll_ftbasic_remote_bitmap_num_modules;

    /*
     * Remote bitmap free list accessor macros
     */
#define REMOTE_BITMAP_ALLOC(rbm, size)                                  \
    do {                                                                \
        ompi_free_list_item_t* item;                                    \
        int rc;                                                         \
                                                                        \
        OMPI_FREE_LIST_GET(&mca_coll_ftbasic_remote_bitmap_free_list,   \
                           item, rc);                                   \
        rbm = (mca_coll_ftbasic_remote_bitmap_t*)item;                  \
        opal_bitmap_init(rbm->bitmap, size + FTBASIC_AGREEMENT_EXTRA_BITS); \
    } while(0)

#define REMOTE_BITMAP_RETURN(rbm)                                       \
    do {                                                                \
        OMPI_FREE_LIST_RETURN(&mca_coll_ftbasic_remote_bitmap_free_list, \
                              (ompi_free_list_item_t*)rbm);             \
    } while(0)

#define REMOTE_BITMAP_GET_NEXT(ftmodule, rbm, size)                     \
    do {                                                                \
        /* Otherwise return the 'next' value */                         \
        if( 0 < opal_list_get_size(&(ftmodule->agreement_info->remote_bitmaps)) ) {     \
            if( NULL == ftmodule->agreement_info->last_used ) {                         \
                ftmodule->agreement_info->last_used = opal_list_get_first(&(ftmodule->agreement_info->remote_bitmaps)); \
            }                                                           \
            else if( ftmodule->agreement_info->last_used != opal_list_get_last(&(ftmodule->agreement_info->remote_bitmaps)) ) { \
                ftmodule->agreement_info->last_used = opal_list_get_next(ftmodule->agreement_info->last_used); \
            }                                                           \
            else {                                                      \
                REMOTE_BITMAP_ALLOC(rbm, size);                         \
                opal_list_append(&(ftmodule->agreement_info->remote_bitmaps),           \
                                 (opal_list_item_t*)rbm);               \
                ftmodule->agreement_info->last_used = opal_list_get_last(&(ftmodule->agreement_info->remote_bitmaps)); \
            }                                                           \
            rbm = (mca_coll_ftbasic_remote_bitmap_t*)ftmodule->agreement_info->last_used; \
        }                                                               \
        /* If the list is empty, then allocate one */                   \
        else {                                                          \
            REMOTE_BITMAP_ALLOC(rbm, size);                             \
            opal_list_append(&(ftmodule->agreement_info->remote_bitmaps),               \
                             (opal_list_item_t*)rbm);                   \
            ftmodule->agreement_info->last_used = opal_list_get_last(&(ftmodule->agreement_info->remote_bitmaps)); \
        }                                                               \
    } while(0)

#define REMOTE_BITMAP_RESET_NEXT(ftmodule)                              \
    if( ftmodule->agreement_info ) {                                    \
        opal_list_item_t* item = NULL;                                  \
        mca_coll_ftbasic_remote_bitmap_t *rbm = NULL;                   \
                                                                        \
        for( item  = opal_list_get_first(&(ftmodule->agreement_info->remote_bitmaps));  \
             item != opal_list_get_end(&(ftmodule->agreement_info->remote_bitmaps));    \
             item  = opal_list_get_next(item) ) {                       \
            rbm = (mca_coll_ftbasic_remote_bitmap_t*)item;              \
            rbm->rank = -1;                                             \
        }                                                               \
        ftmodule->agreement_info->last_used = NULL;                                     \
    }


    /*
     * Agreement Specific Request Object
     * Used by the nonblocking agreement operation
     */
struct mca_coll_ftbasic_request_t {
    /* Base request */
    ompi_request_t req_ompi;

    /* Source */
    int mpi_source;
    /* Error Code */
    int mpi_error;
    /* If free was called on this request */
    bool free_called;

    /* Local bitmap to start with */
    opal_bitmap_t *local_bitmap;
    /* Pointer to group */
    ompi_group_t *group;
    /* Point to flag passed by user */
    int *flag;

    /* Coordinator in the collective */
    int coordinator;
    /* Stage in the collective */
    int stage;
    /* Pointer to the log entry */
    opal_list_item_t *log_entry;
    /* Current number of outstanding requests */
    int num_reqs;
};
typedef struct mca_coll_ftbasic_request_t mca_coll_ftbasic_request_t;
OMPI_DECLSPEC OBJ_CLASS_DECLARATION(mca_coll_ftbasic_request_t);

    /*
     * Agreement Request Support macros
     */
#define MCA_COLL_FTBASIC_REQUEST_ALLOC(collreq, comm)                   \
    {                                                                   \
        collreq = OBJ_NEW(mca_coll_ftbasic_request_t);                  \
                                                                        \
        OMPI_REQUEST_INIT(&(collreq)->req_ompi, false);                 \
        collreq->req_ompi.req_mpi_object.comm = comm;                   \
        collreq->req_ompi.req_complete        = false;                  \
        collreq->req_ompi.req_state           = OMPI_REQUEST_ACTIVE;    \
    }

#define MCA_COLL_FTBASIC_REQUEST_FREE(collreq)                         \
    {                                                                  \
        OMPI_REQUEST_FINI(collreq->req_ompi);                          \
    }

#define MCA_COLL_FTBASIC_REQUEST_COMPLETE(collreq)                      \
    {                                                                   \
        OPAL_THREAD_LOCK(&ompi_request_lock);                           \
        if( false == collreq->req_ompi.req_complete ) {                 \
            collreq->req_ompi.req_status.MPI_SOURCE = collreq->mpi_source; \
            collreq->req_ompi.req_status.MPI_TAG    = -1;               \
            collreq->req_ompi.req_status.MPI_ERROR  = collreq->mpi_error; \
            collreq->req_ompi.req_status._ucount    = 0;                \
            ompi_request_complete(&(collreq)->req_ompi, true);          \
        }                                                               \
        OPAL_THREAD_UNLOCK(&ompi_request_lock);                         \
    }

    /*
     * Support functions for process state checking
     */
    /*
     * Check the true state of the process (not what is cached on the comm)
     * If failed then continue in the loop.
     */
#define COLL_FTBASIC_CHECK_SKIP_ALL_FAILED_BASE(comm, peer) {           \
        if( !ompi_comm_is_proc_active(comm, peer, false) ) {            \
            continue;                                                   \
        }                                                               \
    }


    /*
     * Check the true state of the process (not what is cached on the comm)
     * Find the lowest known alive peer - leader election
     */
#define COLL_FTBASIC_FIND_LOWEST_ALIVE_BASE(comm, size, peer) {         \
        int i;                                                          \
        for(i = 0; i < size; ++i ) {                                    \
            if( ompi_comm_is_proc_active(comm, i, false) ) {            \
                peer = i;                                               \
                break;                                                  \
            }                                                           \
        }                                                               \
    }


/*
 * Agreement timing support
 */
#define AGREEMENT_ENABLE_TIMING OPAL_ENABLE_DEBUG

#if AGREEMENT_ENABLE_TIMING == 1
#define COLL_FTBASIC_AGREEMENT_TIMER_2P_SETUP      0
#define COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ  1
#define COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER     2
#define COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE     3
#define COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST      4
#define COLL_FTBASIC_AGREEMENT_TIMER_AR_SETUP      5
#define COLL_FTBASIC_AGREEMENT_TIMER_AR_GATHER     6
#define COLL_FTBASIC_AGREEMENT_TIMER_AR_DECIDE     7
#define COLL_FTBASIC_AGREEMENT_TIMER_AR_BCAST      8
#define COLL_FTBASIC_AGREEMENT_TIMER_PROTOCOL      9
#define COLL_FTBASIC_AGREEMENT_TIMER_DECIDE       10
#define COLL_FTBASIC_AGREEMENT_TIMER_REBALANCE    11
#define COLL_FTBASIC_AGREEMENT_TIMER_TOTAL        12
#define COLL_FTBASIC_AGREEMENT_TIMER_OTHER        13
#define COLL_FTBASIC_AGREEMENT_TIMER_MAX          14

double agreement_get_time(void);
void agreement_start_time(int idx);
void agreement_end_time(int idx);
void agreement_display_all_timers(void);
void agreement_clear_timers(void);

#define AGREEMENT_START_TIMER(idx)                                       \
    {                                                                   \
        if(OPAL_UNLIKELY( mca_coll_ftbasic_use_agreement_timer )) {    \
            agreement_start_time(idx);                                   \
        }                                                               \
    }

#define AGREEMENT_END_TIMER(idx)                                         \
    {                                                                   \
        if(OPAL_UNLIKELY( mca_coll_ftbasic_use_agreement_timer )) {    \
            agreement_end_time(idx);                                     \
        }                                                               \
    }

#define AGREEMENT_CLEAR_ALL_TIMERS()                                     \
    {                                                                   \
        if(OPAL_UNLIKELY( mca_coll_ftbasic_use_agreement_timer )) {    \
            agreement_clear_timers();                                    \
        }                                                               \
    }

#define AGREEMENT_DISPLAY_ALL_TIMERS()                                   \
    {                                                                   \
        if(OPAL_UNLIKELY( mca_coll_ftbasic_use_agreement_timer )) {    \
            agreement_display_all_timers();                              \
        }                                                               \
    }

#else
#define AGREEMENT_START_TIMER(idx) ;
#define AGREEMENT_END_TIMER(idx) ;
#define AGREEMENT_CLEAR_ALL_TIMERS() ;
#define AGREEMENT_DISPLAY_ALL_TIMERS() ;
#endif /* AGREEMENT_ENABLE_TIMING */

END_C_DECLS

#endif /* MCA_COLL_FTBASIC_AGREEMENT_EXPORT_H */
