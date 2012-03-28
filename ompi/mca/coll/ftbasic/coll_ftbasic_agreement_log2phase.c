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

#include MCA_timer_IMPLEMENTATION_HEADER

/*************************************
 * Testing Globals
 *************************************/
#if OPAL_ENABLE_DEBUG
#define DEBUG_WITH_STR 1
#else
#define DEBUG_WITH_STR 0
#endif

#if OPAL_ENABLE_DEBUG
/*#define DEBUG_DECIDE_SLEEP 1*/
#define DEBUG_DECIDE_SLEEP 0
#else
#define DEBUG_DECIDE_SLEEP 0
#endif

#if OPAL_ENABLE_DEBUG
#define LOG_TWO_PHASE_ENABLE_TESTING 1

/* Coordinator Rank
 * - Must not be a leaf in the tree
 */
#define LOG_TWO_PHASE_TEST_RANK_COORD  0
/* Parent Rank
 * - Must not be the root in the tree
 * - Must not be a leaf in the tree
 */
#define LOG_TWO_PHASE_TEST_RANK_PARENT 1
/* Child Rank
 * - Must be a leaf in the tree connected to the 'parent'
 */
#define LOG_TWO_PHASE_TEST_RANK_CHILD  3

/* No testing enabled */
#define LOG_TWO_PHASE_TEST_NONE                          0

/****** Coordinator: ******/
/* Coordinator: (np = 4, Rank 1 fails)
 * - Fail before Bcast(vote_req)
 * - Vote_req eliminated since it is not needed
 *
 * NOTE: Not implemented since Vote Req is skipped
 */
#define LOG_TWO_PHASE_TEST_COORD_BEFORE_BCAST_VOTE_REQ   1

/* Coordinator: (np = 8, Rank 1 fails - between send(1) and send(2))
 * - Fail during Bcast(vote_req)
 * - Vote_req eliminated since it is not needed
 *
 * NOTE: Not implemented since Vote Req is skipped
 */
#define LOG_TWO_PHASE_TEST_COORD_DURING_BCAST_VOTE_REQ   2

/* Coordinator: (np = 4, Rank 1 fails)
 * - Fail before Gather(vote)
 * - None have voted
 * - Child ("Parent_D", "Child_B")
 *   - Escalate to "RootFailure_B" or "RootFailure_E" respectively
 *   - Decide Abort
 *   - if( Parent ) Bcast(Abort) to all children
 */
#define LOG_TWO_PHASE_TEST_COORD_BEFORE_GATHER_VOTE      3

/* Coordinator: (np = 8, Rank 1 fails - between recv(1) and recv(2))
 * - Fail during Gather(vote)
 * - Some have voted, other have not
 * - Child ("Parent_D", "Child_B") - Those that have not voted yet
 *   - Escalate to "RootFailure_B" or "RootFailure_E" respectively
 *   - Decide Abort
 *   - if( Parent ) Bcast(Abort) to all children
 * - Child ("Parent_E", "Child_C") - Those that have voted
 *   - Escalate to "RootFailure_C" or "RootFailure_F" respectively
 *   - Uncertain about state
 *   - Term protocol
 *     - Decide Abort
 *   - if( Parent ) Bcast(Abort) to all children
 */
#define LOG_TWO_PHASE_TEST_COORD_DURING_GATHER_VOTE      4

/* Coordinator: (np = 4, Rank 1 fails)
 * - Fail before Bcast(Commit)
 * - All have voted, all are waiting on a decision
 * - **** Technically we need to decide 'uncertain' and abort the set of procs
 *        Instead we decide Abort and move on. (JJH May need to revisit)
 * - Child ("Parent_E", "Child_C")
 *   - Escalate to "RootFailure_C" or "RootFailure_F" respectively
 *   - Uncertain about state
 *   - Term protocol
 *     - Decide Abort
 *   - if( Parent ) Bcast(Abort) to all children
 */
#define LOG_TWO_PHASE_TEST_COORD_BEFORE_BCAST_COMMIT     5

/* Coordinator: (np = 8, Rank 1 fails - between recv(1) and recv(2))
 * - Fail during Bcast(Commit)
 * - All have voted, some finished and some are waiting on a decision
 * - Child ("Done")
 *   - Respond to peer when they ask for the decision via Term protocol
 * - Child ("Parent_E", "Child_C")
 *   - Escalate to "RootFailure_C" or "RootFailure_F" respectively
 *   - Uncertain about state
 *   - Term protocol
 *     - Decide Commit
 *   - if( Parent ) Bcast(Commit) to all children
 */
#define LOG_TWO_PHASE_TEST_COORD_DURING_BCAST_COMMIT     6


/****** Participant: Parent ******/
/* Participant: Parent: (np = 4, Rank 1 fails)
 * - Fail before Recv(vote_req)
 * - Vote_req eliminated since it is not needed
 *
 * NOTE: Not implemented since Vote Req is skipped
 */
#define LOG_TWO_PHASE_TEST_PART_P_BEFORE_RECV_VOTE_REQ   7

/* Participant: Parent: (np = 4, Rank 1 fails)
 * - Fail before Bcast(vote_req)
 * - Vote_req eliminated since it is not needed
 *
 * NOTE: Not implemented since Vote Req is skipped
 */
#define LOG_TWO_PHASE_TEST_PART_P_BEFORE_BCAST_VOTE_REQ  8

/* Participant: Parent: (np = 8, Rank 1 fails - between send(3) and send(5))
 * - Fail during Bcast(vote_req)
 * - Vote_req eliminated since it is not needed
 *
 * NOTE: Not implemented since Vote Req is skipped
 */
#define LOG_TWO_PHASE_TEST_PART_P_DURING_BCAST_VOTE_REQ  9

/* Participant: Parent: (np = 4, Rank 1 fails)
 * - Fail before Gather(vote)
 * - Child ("Child_B") - Did not gather my vote yet
 *   - Detect failure of parent in Send(Vote)
 *   - Find grandparent (Know that we are still 'voting')
 *   - Recv(Vote_req) from grandparent
 *   - Send(Vote) to grandparent
 * - Grandparent ("Root_B", "Parent_C")
 *   - Detect failure of child in Gather(Vote)
 *   - Find grandchildren
 *   - Send(Vote_req) to grandchildren
 *   - Recv(Vote) from grandchildren
 */
#define LOG_TWO_PHASE_TEST_PART_P_BEFORE_GATHER_VOTE    10

/* Participant: Parent: (np = 8, Rank 1 fails - between recv(3) and recv(5))
 * - Fail during Gather(vote)
 * - Child ("Parent_D", "Child_B") - Did not gather my vote yet [Rank 5]
 *   - Detect failure of parent in Send(Vote)
 *   - Find grandparent (Know that we are still 'voting')
 *   - Recv(Vote_req) from grandparent
 *   - Send(Vote) to grandparent
 * - Child ("Parent_E", "Child_C") - Did gather my vote = Uncertain [Rank 3]
 *   - Detect failure of parent in Recv(Commit)
 *   - Find grandparent
 *   - Send(Query) grandparent for current state
 *     (Do not know if still 'voting' or if 'committing')
 *     - if 'voting'
 *       - Recv(Vote_req) from grandparent
 *       - Send(Vote) to grandparent
 *     - if 'committing'
 *       - Recv(Commit) from grandparent
 * - Grandparent ("Root_B", "Parent_C") - knows that we are still 'voting'
 *   - Detect failure of child in Gather(Vote)
 *   - Find grandchildren
 *   - Send(Vote_req) to grandchildren
 *   - Recv(Vote) from grandchildren
 */
#define LOG_TWO_PHASE_TEST_PART_P_DURING_GATHER_VOTE    11

/* Participant: Parent: (np = 4, Rank 1 fails)
 * - Fail before Send(vote)
 * - Child ("Parent_E", "Child_C") - Did gather my vote = Uncertain
 *   - Detect failure of parent in Recv(Commit)
 *   - Find grandparent
 *   - Send(Query) grandparent for current state
 *     (Do not know if still 'voting' or if 'committing')
 *     - if 'voting'
 *       - Recv(Vote_req) from grandparent
 *       - Send(Vote) to grandparent
 *     - if 'committing'
 *       - Recv(Commit) from grandparent
 * - Grandparent ("Root_B", "Parent_C") - knows that we are still 'voting'
 *   - Detect failure of child in Gather(Vote)
 *   - Find grandchildren
 *   - Send(Vote_req) to grandchildren
 *   - Recv(Vote) from grandchildren
 */
#define LOG_TWO_PHASE_TEST_PART_P_BEFORE_SEND_VOTE      12

/* Participant: Parent: (np = 4, Rank 1 fails)
 * - Fail before Recv(Commit)
 * - Child ("Parent_E", "Child_C") - Did gather my vote = Uncertain
 *   - Detect failure of parent in Recv(Commit)
 *   - Find grandparent
 *   - Send(Query) grandparent for current state
 *     (Do not know if still 'voting' or if 'committing')
 *     - if 'voting'
 *       - Recv(Vote_req) from grandparent
 *       - Send(Vote) to grandparent
 *     - if 'committing'
 *       - Recv(Commit) from grandparent
 * - Grandparent ("Root_C", "Parent_F") - knows that we are 'committing'
 *   - Detect failure of child in Bcast(Commit)
 *   - Find grandchildren
 *   - Send(Commit) to grandchildren
 */
#define LOG_TWO_PHASE_TEST_PART_P_BEFORE_RECV_COMMIT    13

/* Participant: Parent: (np = 4, Rank 1 fails)
 * - Fail before Bcast(Commit)
 * - Child ("Parent_E", "Child_C") - Did gather my vote = Uncertain
 *   - Detect failure of parent in Recv(Commit)
 *   - Find grandparent
 *   - Send(Query) grandparent for current state
 *     (Do not know if still 'voting' or if 'committing')
 *     - if 'voting'
 *       - Recv(Vote_req) from grandparent
 *       - Send(Vote) to grandparent
 *     - if 'committing'
 *       - Recv(Commit) from grandparent
 * - Grandparent ("Root_C", "Parent_F") - knows that we are 'committing'
 *   - Detect failure of child in Bcast(Commit)
 *   - Find grandchildren
 *   - Send(Commit) to grandchildren
 * - Grandparent ("Done")
 *   - Recv(Query) message from grandchildren
 *   - Send(Commit) to grandchildren
 */
#define LOG_TWO_PHASE_TEST_PART_P_BEFORE_BCAST_COMMIT   14

/* Participant: Parent: (np = 8, Rank 1 fails - between recv(3) and recv(5))
 * - Fail during Bcast(Commit)
 * - Child ("Parent_E", "Child_C") - Did gather my vote = Uncertain
 *   - Detect failure of parent in Recv(Commit)
 *   - Find grandparent
 *   - Send(Query) grandparent for current state
 *     (Do not know if still 'voting' or if 'committing')
 *     - if 'voting'
 *       - Recv(Vote_req) from grandparent
 *       - Send(Vote) to grandparent
 *     - if 'committing'
 *       - Recv(Commit) from grandparent
 * - Child ("Done")
 *   - Do nothing will detect failure in next Recv(Vote_req)
 * - Grandparent ("Done")
 *   - Recv(Query) message from grandchildren
 *   - Send(Commit) to grandchildren
 */
#define LOG_TWO_PHASE_TEST_PART_P_DURING_BCAST_COMMIT   15


/****** Participant: Child ******/
/* Participant: Child: (np = 4, Rank 3 fails)
 * - Fail before Recv(vote_req)
 * - Grandparent ("Root_A", "Root_B", "Parent_B", "Parent_C")
 *   - Detect failure of child in Bcast(Recv_req) or Gather(Vote)
 *   - Find grandchildren (none will be found)
 *   - Basecase: return...
 *
 * NOTE: Not implemented since Vote Req is skipped
 */
#define LOG_TWO_PHASE_TEST_PART_C_BEFORE_VOTE_REQ       16

/* Participant: Child: (np = 4, Rank 3 fails)
 * - Fail after Recv(vote_req)
 * - Grandparent ("Root_B", "Parent_C")
 *   - Detect failure of child in Gather(Vote)
 *   - Find grandchildren (none will be found)
 *   - Basecase: return...
 */
#define LOG_TWO_PHASE_TEST_PART_C_AFTER_VOTE_REQ        17

/* Participant: Child: (np = 4, Rank 3 fails)
 * - Fail after Send(vote)
 * - Grandparent ("Root_C", "Parent_F")
 *   - Detect failure of child in Gather(Vote)
 *   - Find grandchildren (none will be found)
 *   - Basecase: return...
 */
#define LOG_TWO_PHASE_TEST_PART_C_AFTER_VOTE            18


#define LOG_TWO_PHASE_TEST LOG_TWO_PHASE_TEST_NONE

#else /* !OPAL_ENABLE_DEBUG */
#define LOG_TWO_PHASE_ENABLE_TESTING 0
#endif /* OPAL_ENABLE_DEBUG */


/*************************************
 * Command Globals and Macros
 *************************************/
#define RANK_UNDEF -1
#define IS_INVALID_RANK(rank) (rank < 0)
#define GET_RANK_OF_INVALID_RANK(rank) (IS_INVALID_RANK(rank) ? ((rank * -1) + RANK_UNDEF) : rank)
#define GET_INVALID_RANK_OF_RANK(rank) (IS_INVALID_RANK(rank) ? rank : ((rank * -1) + RANK_UNDEF))


/*************************************
 * Query Protocol Support
 *************************************/
#define LOG_TWO_PHASE_CMD_QUERY_PROTOCOL 1
#define LOG_TWO_PHASE_CMD_TERM_PROTOCOL  2

#define LOG_TWO_PHASE_QUERY_CMD_DECIDE_COMMIT 1
#define LOG_TWO_PHASE_QUERY_CMD_DECIDE_ABORT  2
#define LOG_TWO_PHASE_QUERY_CMD_GATHER        3
#define LOG_TWO_PHASE_QUERY_CMD_BCAST         4

#define LOG_QUERY_PHASE_STR(cmd_msg)                    \
    (LOG_TWO_PHASE_QUERY_CMD_DECIDE_COMMIT   == cmd_msg ? "Commit" :    \
     (LOG_TWO_PHASE_QUERY_CMD_DECIDE_ABORT   == cmd_msg ? "Abort"  :    \
      (LOG_TWO_PHASE_QUERY_CMD_GATHER        == cmd_msg ? "Gather" :    \
       (LOG_TWO_PHASE_QUERY_CMD_BCAST        == cmd_msg ? "Bcast"  :    \
        "Unknown"))))


struct log_two_phase_query_entry_t {
    /** This is a list object */
    opal_list_item_t super;

    int peer;
    int seq_num;
    int attempt_num;

    bool done;
};
typedef struct log_two_phase_query_entry_t log_two_phase_query_entry_t;
OBJ_CLASS_DECLARATION(log_two_phase_query_entry_t);

void log_two_phase_query_entry_construct(log_two_phase_query_entry_t *entry);
void log_two_phase_query_entry_destruct(log_two_phase_query_entry_t *entry);


/*************************************
 * Nonblocking states
 *************************************/
enum {
    /*
     * Collective not active state
     */
    LOG_TWO_PHASE_NB_STAGE_NULL = 0,

    /*
     * Root Coordinator: Bcast Decision Request
     */
    LOG_TWO_PHASE_NB_STAGE_COORD_WAIT_BCAST_REQ = 1,
    /*
     * Root Coordinator: Gather lists
     */
    LOG_TWO_PHASE_NB_STAGE_COORD_WAIT_GATHER = 2,
    /*
     * Root Coordinator: Bcast final list
     */
    LOG_TWO_PHASE_NB_STAGE_COORD_WAIT_BCAST = 3,

    /*
     * Parent Coordinator: Bcast Decision Request
     */
    LOG_TWO_PHASE_NB_STAGE_COORD_PARENT_WAIT_BCAST_REQ = 4,
    /*
     * Parent Coordinator: Gather lists
     */
    LOG_TWO_PHASE_NB_STAGE_COORD_PARENT_WAIT_GATHER = 5,
    /*
     * Parent Coordinator: Bcast final list
     */
    LOG_TWO_PHASE_NB_STAGE_COORD_PARENT_WAIT_BCAST = 6,

    /*
     * Parent Participant: Receive Request
     */
    LOG_TWO_PHASE_NB_STAGE_PART_PARENT_WAIT_RECV_REQ = 7,
    /*
     * Parent Participant: Send List
     */
    LOG_TWO_PHASE_NB_STAGE_PART_PARENT_WAIT_SEND = 8,
    /*
     * Parent Participant: Receive final list
     */
    LOG_TWO_PHASE_NB_STAGE_PART_PARENT_WAIT_RECV = 9,

    /*
     * Child Participant: Receive Request
     */
    LOG_TWO_PHASE_NB_STAGE_PART_CHILD_WAIT_RECV_REQ = 10,
    /*
     * Child Participant: Send List
     */
    LOG_TWO_PHASE_NB_STAGE_PART_CHILD_WAIT_SEND = 11,
    /*
     * Child Participant: Receive final list
     */
    LOG_TWO_PHASE_NB_STAGE_PART_CHILD_WAIT_RECV = 12,

    /*
     * (Term Protocol)
     * Initiator: Sending request to peer
     */
    LOG_TWO_PHASE_NB_STAGE_TERM_WAIT_SEND = 13,
    /*
     * (Term Protocol)
     * Initiator: Recv information from peer
     */
    LOG_TWO_PHASE_NB_STAGE_TERM_WAIT_RECV = 14,

    /*
     * All done
     */
    LOG_TWO_PHASE_NB_STAGE_DONE = 15
};


/*************************************
 * A tree structure for quick lookup
 *************************************/
struct mca_coll_ftbasic_agreement_tree_t {
    int root;
    int parent;
    int num_children;
    int num_children_alloc;
    int *children;
    opal_bitmap_t *state_bitmap;
    int bkmrk_idx;
};
typedef struct mca_coll_ftbasic_agreement_tree_t mca_coll_ftbasic_agreement_tree_t;

/*
 * Agreement 'tree' support (for Log Two Phase)
 */
#define INIT_AGREE_TREE_STRUCT(agree_tree) {       \
        agree_tree->root = 0;                     \
        agree_tree->parent = 0;                   \
        agree_tree->num_children = 0;             \
        agree_tree->num_children_alloc = 0;       \
        agree_tree->children = NULL;              \
        agree_tree->state_bitmap = NULL;          \
        agree_tree->bkmrk_idx = 0;                \
    }

#define FINALIZE_AGREE_TREE_STRUCT(agree_tree) {    \
        agree_tree->root = 0;                      \
        agree_tree->parent = 0;                    \
        agree_tree->num_children = 0;              \
        agree_tree->num_children_alloc = 0;        \
        if( NULL != agree_tree->children ) {       \
            free(agree_tree->children);            \
            agree_tree->children = NULL;           \
        }                                         \
        if( NULL != agree_tree->state_bitmap) {    \
            OBJ_RELEASE(agree_tree->state_bitmap); \
            agree_tree->state_bitmap = NULL;       \
        }                                         \
        agree_tree->bkmrk_idx = 0;                 \
    }

/*************************************
 * Algorithm specific agreement structure
 *************************************/
struct mca_coll_ftbasic_agreement_logtwophase_t {
    /* Base object */
    mca_coll_ftbasic_agreement_t super;

    /* Rebalance struct: Agreement Tree */
    mca_coll_ftbasic_agreement_tree_t *agreement_tree;

    /* Agreement Query List */
    opal_list_t *query_queue;
};
typedef struct mca_coll_ftbasic_agreement_logtwophase_t mca_coll_ftbasic_agreement_logtwophase_t;
OBJ_CLASS_DECLARATION(mca_coll_ftbasic_agreement_logtwophase_t);

static void mca_coll_ftbasic_agreement_logtwophase_construct(mca_coll_ftbasic_agreement_logtwophase_t *v_info)
{
    v_info->agreement_tree = (mca_coll_ftbasic_agreement_tree_t*)malloc(sizeof(mca_coll_ftbasic_agreement_tree_t));
    INIT_AGREE_TREE_STRUCT(v_info->agreement_tree);

    v_info->query_queue = OBJ_NEW(opal_list_t);
}

static void mca_coll_ftbasic_agreement_logtwophase_destruct(mca_coll_ftbasic_agreement_logtwophase_t *v_info)
{
    opal_list_item_t* item = NULL;

    if( NULL != v_info->agreement_tree ) {
        FINALIZE_AGREE_TREE_STRUCT(v_info->agreement_tree);
        free(v_info->agreement_tree);
        v_info->agreement_tree = NULL;
    }

    if( NULL != v_info->query_queue ) {
        while (NULL != (item = opal_list_remove_first(v_info->query_queue)) ) {
            OBJ_RELEASE(item);
        }
        OBJ_RELEASE(v_info->query_queue);
        v_info->query_queue = NULL;
    }
}
OBJ_CLASS_INSTANCE(mca_coll_ftbasic_agreement_logtwophase_t,
                   mca_coll_ftbasic_agreement_t,
                   mca_coll_ftbasic_agreement_logtwophase_construct,
                   mca_coll_ftbasic_agreement_logtwophase_destruct);


/***************************************
 * Support: Tree Maintenance
 ***************************************/
static int log_two_phase_update_root(ompi_communicator_t* comm,
                                     mca_coll_ftbasic_module_t *ftbasic_module);
static int log_two_phase_update_parent(ompi_communicator_t* comm,
                                       mca_coll_ftbasic_module_t *ftbasic_module);
static int log_two_phase_update_children(ompi_communicator_t* comm,
                                         mca_coll_ftbasic_module_t *ftbasic_module);

static int log_two_phase_append_children(ompi_communicator_t* comm,
                                         mca_coll_ftbasic_module_t *ftbasic_module,
                                         int *children, int num_children, bool notice_if_new);
static int log_two_phase_append_child(ompi_communicator_t* comm,
                                      mca_coll_ftbasic_module_t *ftbasic_module,
                                      int child, bool notice_if_new);
static int log_two_phase_remove_child(ompi_communicator_t* comm,
                                      mca_coll_ftbasic_module_t *ftbasic_module,
                                      int child);

static int log_two_phase_mark_need_notice(ompi_communicator_t* comm,
                                          mca_coll_ftbasic_module_t *ftbasic_module,
                                          int peer, bool notice);
static bool log_two_phase_if_needs_notice(ompi_communicator_t* comm,
                                          mca_coll_ftbasic_module_t *ftbasic_module,
                                          int peer);
static bool log_two_phase_if_should_skip(ompi_communicator_t* comm,
                                         mca_coll_ftbasic_module_t *ftbasic_module,
                                         int peer,
                                         int of_rank,
                                         int num_prev_children);

static bool log_two_phase_my_child(ompi_communicator_t* comm,
                                   mca_coll_ftbasic_module_t *ftbasic_module,
                                   int peer);

static int log_two_phase_get_vsize(ompi_communicator_t* comm,
                                   int *vsize,
                                   mca_coll_ftbasic_module_t *ftbasic_module);
static int log_two_phase_get_vrank(ompi_communicator_t* comm,
                                   int rank, int *vrank,
                                   mca_coll_ftbasic_module_t *ftbasic_module);
static int log_two_phase_get_rank(ompi_communicator_t* comm,
                                  int vrank, int *rank,
                                  mca_coll_ftbasic_module_t *ftbasic_module);

static int log_two_phase_get_root_of(ompi_communicator_t* comm,
                                     mca_coll_ftbasic_module_t *ftbasic_module,
                                     int *root,
                                     bool *found);
static int log_two_phase_get_parent_of(ompi_communicator_t* comm,
                                       mca_coll_ftbasic_module_t *ftbasic_module,
                                       int rank,
                                       int *parent);
static int log_two_phase_get_children_of(ompi_communicator_t* comm,
                                         mca_coll_ftbasic_module_t *ftbasic_module,
                                         int rank,
                                         int *children[],
                                         int *num_children,
                                         int *num_children_alloc);
static void append_child_to_array(int *children[],
                                  int *num_children,
                                  int *num_children_alloc,
                                  int rank);



/***************************************
 * Support: Termination Protocol
 ***************************************/
static int log_two_phase_term_initiator(ompi_communicator_t* comm,
                                        mca_coll_ftbasic_module_t *ftbasic_module,
                                        opal_bitmap_t *local_bitmap,
                                        mca_coll_ftbasic_agreement_log_entry_t *log_entry);
static int log_two_phase_term_responder(ompi_communicator_t* comm,
                                        mca_coll_ftbasic_module_t *ftbasic_module,
                                        int peer);


/***************************************
 * Support: Normal Protocol: General
 ***************************************/
/* (A)
 * Broadcast a message to all children.
 * If 'local_bitmap' is null then this is the 'vote request' and
 * no data is explicitly sent.
 */
static int log_two_phase_protocol_bcast_to_children(ompi_communicator_t* comm,
                                                    mca_coll_ftbasic_module_t *ftbasic_module,
                                                    opal_bitmap_t *local_bitmap,
                                                    int *num_reqs,
                                                    int of_rank,
                                                    int num_prev_children,
                                                    mca_coll_ftbasic_agreement_log_entry_t *log_entry);

/* (B)
 * Receive a message from parent
 * If 'local_bitmap' is null then this is the 'vote request' and
 * no data is explicitly received.
 */
static int log_two_phase_protocol_recv_from_parent(ompi_communicator_t* comm,
                                                   mca_coll_ftbasic_module_t *ftbasic_module,
                                                   opal_bitmap_t *local_bitmap,
                                                   int *num_reqs,
                                                   int of_rank);

/* (C)
 * Send a message to the parent
 */
static int log_two_phase_protocol_send_to_parent(ompi_communicator_t* comm,
                                                 mca_coll_ftbasic_module_t *ftbasic_module,
                                                 opal_bitmap_t *local_bitmap,
                                                 int *num_reqs,
                                                 int of_rank);

/* (D)
 * Gather from all children
 */
static int log_two_phase_protocol_gather_from_children(ompi_communicator_t* comm,
                                                       mca_coll_ftbasic_module_t *ftbasic_module,
                                                       opal_bitmap_t *local_bitmap,
                                                       int *num_reqs,
                                                       int of_rank,
                                                       int num_prev_children,
                                                       mca_coll_ftbasic_agreement_log_entry_t *log_entry);

/* (E)
 * Accumulate the list
 */
static int log_two_phase_protocol_accumulate_list(ompi_communicator_t* comm,
                                                  mca_coll_ftbasic_module_t *ftbasic_module,
                                                  opal_bitmap_t *local_bitmap);

/* (F)
 * Query the state of a new parent
 * This is an operation to augment the termination protocol.
 * Keep it is a separate function for clarity of implementation.
 */
static int log_two_phase_protocol_query_parent(ompi_communicator_t* comm,
                                               mca_coll_ftbasic_module_t *ftbasic_module,
                                               opal_bitmap_t *local_bitmap,
                                               mca_coll_ftbasic_agreement_log_entry_t *log_entry,
                                               bool root_fail_do_term_protocol);

/***************************************
 * Support: Query operations
 ***************************************/
static int log_two_phase_query_responder(ompi_communicator_t* comm,
                                         mca_coll_ftbasic_module_t *ftbasic_module,
                                         int peer);
static int log_two_phase_query_send_notice(ompi_communicator_t* comm,
                                           mca_coll_ftbasic_module_t *ftbasic_module,
                                           int peer,
                                           int cmd_msg,
                                           int seq_num,
                                           int attempt_num);
static int log_two_phase_query_recv_notice(ompi_communicator_t* comm,
                                           mca_coll_ftbasic_module_t *ftbasic_module,
                                           int peer,
                                           int *cmd_msg,
                                           int *seq_num,
                                           int *attempt_num);

static int log_two_phase_query_purge_queue(ompi_communicator_t* comm,
                                           mca_coll_ftbasic_module_t *ftbasic_module);
static int log_two_phase_query_append_queue(ompi_communicator_t* comm,
                                            mca_coll_ftbasic_module_t *ftbasic_module,
                                            int peer,
                                            int seq_num,
                                            int attempt_num,
                                            mca_coll_ftbasic_agreement_log_entry_t *log_entry);
static int log_two_phase_query_process_all_entries(ompi_communicator_t* comm,
                                                   mca_coll_ftbasic_module_t *ftbasic_module,
                                                   mca_coll_ftbasic_agreement_log_entry_t *log_entry);
static int log_two_phase_query_process_entry(ompi_communicator_t* comm,
                                             mca_coll_ftbasic_module_t *ftbasic_module,
                                             int peer,
                                             int seq_num,
                                             int attempt_num,
                                             mca_coll_ftbasic_agreement_log_entry_t *log_entry);

/***************************************
 * Support: Normal Protocol: Coordinator
 ***************************************/
static int log_two_phase_coordinator(ompi_communicator_t* comm,
                                     opal_bitmap_t *local_bitmap,
                                     mca_coll_ftbasic_module_t *ftbasic_module,
                                     mca_coll_ftbasic_agreement_log_entry_t *log_entry);

static int log_two_phase_coordinator_progress(ompi_communicator_t* comm,
                                              mca_coll_ftbasic_module_t *ftbasic_module,
                                              int test_exit_status,
                                              int num_failed);


/***************************************
 * Support: Normal Protocol: Participant
 ***************************************/
static int log_two_phase_participant_parent(ompi_communicator_t* comm,
                                            opal_bitmap_t *local_bitmap,
                                            mca_coll_ftbasic_module_t *ftbasic_module,
                                            mca_coll_ftbasic_agreement_log_entry_t *log_entry);

static int log_two_phase_participant_parent_progress(ompi_communicator_t* comm,
                                                     mca_coll_ftbasic_module_t *ftbasic_module,
                                                     int test_exit_status,
                                                     int num_failed);

static int log_two_phase_participant_child(ompi_communicator_t* comm,
                                           opal_bitmap_t *local_bitmap,
                                           mca_coll_ftbasic_module_t *ftbasic_module,
                                           mca_coll_ftbasic_agreement_log_entry_t *log_entry);

static int log_two_phase_participant_child_progress(ompi_communicator_t* comm,
                                                    mca_coll_ftbasic_module_t *ftbasic_module,
                                                    int test_exit_status);


/***************************************
 * Support: Term/Normal Progress Classes
 ***************************************/
static bool log_two_phase_inside_progress = false;
static bool log_two_phase_inside_term_progress = false;
static int  log_two_phase_progress_num_active = 0;

int mca_coll_ftbasic_agreement_log_two_phase_progress(void);
static int mca_coll_ftbasic_agreement_log_two_phase_progress_comm(ompi_communicator_t *comm,
                                                             mca_coll_ftbasic_module_t *ftbasic_module);
int mca_coll_ftbasic_agreement_log_two_phase_term_progress(void);
static int mca_coll_ftbasic_agreement_log_two_phase_term_progress_comm(ompi_communicator_t *comm);


/***************************************
 * Support: Other
 ***************************************/
static int internal_agreement_log_two_phase(ompi_communicator_t* comm,
                                            ompi_group_t **group,
                                            int *flag,
                                            opal_bitmap_t *local_bitmap,
                                            mca_coll_ftbasic_module_t *ftbasic_module);
static int internal_iagreement_log_two_phase(ompi_communicator_t* comm,
                                             ompi_group_t **group,
                                             int *flag,
                                             opal_bitmap_t *local_bitmap,
                                             mca_coll_ftbasic_module_t *ftbasic_module,
                                             struct mca_coll_ftbasic_request_t *collreq);

static int log_two_phase_test_all(ompi_communicator_t* comm,
                                  mca_coll_ftbasic_module_t *ftbasic_module,
                                  int num_reqs,
                                  bool *is_finished,
                                  int *num_failed,
                                  bool return_error);


/***************************************
 * Object Support
 ***************************************/
void log_two_phase_query_entry_construct(log_two_phase_query_entry_t *entry)
{
    entry->peer = -1;
    entry->seq_num = -1;
    entry->attempt_num = -1;
    entry->done = false;
}

void log_two_phase_query_entry_destruct(log_two_phase_query_entry_t *entry)
{
    entry->peer = -1;
    entry->seq_num = -1;
    entry->attempt_num = -1;
    entry->done = false;
}
OBJ_CLASS_INSTANCE(log_two_phase_query_entry_t,
                   opal_list_item_t,
                   log_two_phase_query_entry_construct,
                   log_two_phase_query_entry_destruct);

/*************************************
 * Initialization and Finalization
 *************************************/
int mca_coll_ftbasic_agreement_log_two_phase_init(mca_coll_ftbasic_module_t *module)
{
    mca_coll_ftbasic_agreement_logtwophase_t *loc_info;

    loc_info = OBJ_NEW(mca_coll_ftbasic_agreement_logtwophase_t);
    module->agreement_info = (mca_coll_ftbasic_agreement_t*)loc_info;

    /*
     * Register the termination progress function
     * But only once for the module, not each communicator
     */
    if( log_two_phase_progress_num_active == 0 ) {
        log_two_phase_inside_term_progress = false;
        log_two_phase_inside_progress = false;
        opal_progress_register(mca_coll_ftbasic_agreement_log_two_phase_progress);
        opal_progress_register(mca_coll_ftbasic_agreement_log_two_phase_term_progress);
    }
    log_two_phase_progress_num_active++;

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Initialize ",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

    return OMPI_SUCCESS;
}

/***************************************
 * Finalize Module
 ***************************************/
int mca_coll_ftbasic_agreement_log_two_phase_finalize(mca_coll_ftbasic_module_t *module)
{
    mca_coll_ftbasic_agreement_logtwophase_t *loc_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(module->agreement_info);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Finalize",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

    /*
     * Deregister the termination progress function
     * But only once per job.
     */
    log_two_phase_progress_num_active--;
    if( log_two_phase_progress_num_active == 0 || ompi_mpi_finalized ) {
        OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Finalize - Cancel Progress handler (%s)",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             (ompi_mpi_finalized ? "Finalizing" : "") ));

        opal_progress_unregister(mca_coll_ftbasic_agreement_log_two_phase_progress);
        opal_progress_unregister(mca_coll_ftbasic_agreement_log_two_phase_term_progress);

        log_two_phase_inside_progress = false;
        log_two_phase_inside_term_progress = false;
    }

    OBJ_RELEASE(loc_info);
    module->agreement_info = NULL;

    return OMPI_SUCCESS;
}


/*************************************
 * High level methods
 *************************************/
int mca_coll_ftbasic_agreement_log_two_phase(ompi_communicator_t* comm,
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
        ret = internal_agreement_log_two_phase(comm, group, flag,
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

int mca_coll_ftbasic_iagreement_log_two_phase(ompi_communicator_t* comm,
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
        ret = internal_iagreement_log_two_phase(comm, group, flag,
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
static int internal_agreement_log_two_phase(ompi_communicator_t* comm,
                                            ompi_group_t **group,
                                            int *flag,
                                            opal_bitmap_t *local_bitmap,
                                            mca_coll_ftbasic_module_t *ftbasic_module)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank = ompi_comm_rank(comm);
    mca_coll_ftbasic_agreement_log_entry_t *log_entry = NULL;
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
#if OPAL_ENABLE_DEBUG
    int size = ompi_comm_size(comm);
#endif

    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_SETUP);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Rank %2d/%2d Starting agreement...",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank, size));

    /*
     * Is this the first call to agreement?
     * If so setup the tree structure.
     * Do not use the local_bitmap since it is not agreed upon.
     * Instead contruct a tree assuming every process is alive (something
     * that all processes can say).
     */
    if( NULL == ftbasic_module->agreement_info->fail_bitmap ) {
        mca_coll_ftbasic_agreement_log_two_phase_refresh_tree(NULL, comm, ftbasic_module);
    }

    /*
     * Sanity check: If it is just me, then return directly
     */
    if( 1 == ompi_comm_num_active_local(comm) ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) All alone, skip agreement.",
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
    log_entry = mca_coll_ftbasic_agreement_log_entry_find(comm, ftbasic_module->agreement_info->agreement_seq_num,
                                                         true, ftbasic_module);

    /************************************************
     * Elect a coordinator - Set as part of the 'tree'
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Elected Coordinator is %2d for [%2d/%2d]",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), agreement_tree->root,
                         log_entry->seq_num, log_entry->attempt_num));
#if OPAL_ENABLE_DEBUG
    if( log_entry->attempt_num > ompi_comm_size(comm) ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Oops. It seems that we have restarted to many times...",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
        opal_output(0,
                    "%s ftbasic: agreement) (log2phase) ****************** STOP ************************",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        {
            int sleeper = 1;
            while(sleeper == 1 ) {
                sleep(1);
            }
        }
    }
#endif /* OPAL_ENABLE_DEBUG */

    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_SETUP);


    /************************************************
     * Coordinator
     ************************************************/
    if( rank == agreement_tree->root ) {
        if( OMPI_SUCCESS != (ret = log_two_phase_coordinator(comm, local_bitmap, ftbasic_module, log_entry)) ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) Coordinator Protocol Failed (%2d) - Should never happen",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret));
            exit_status = ret;
            goto cleanup;
        }
    }
    /************************************************
     * Participant (Parent)
     ************************************************/
    else if( 0 < agreement_tree->num_children ) {
        ret = log_two_phase_participant_parent(comm, local_bitmap, ftbasic_module, log_entry);
        if( OMPI_SUCCESS != ret ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) Participant (Parent) Protocol Failed (%2d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret));
            /*
             * This is a special return code used to indicate that we decided
             * 'Abort' in the last round so try again. The only reason we would
             * have decided abort is if the root failed. So mark the root as
             * failed, and rebalance the tree so that next time we have a shot
             * at getting to 'Commit' with a new root.
             */
            if( OMPI_EXISTS == ret ) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                     "%s ftbasic: agreement) (log2phase) Participant (Parent) Refreshing Tree (Root %2d failed)",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), agreement_tree->root));
                opal_bitmap_set_bit(ftbasic_module->agreement_info->fail_bitmap,
                                    agreement_tree->root);
                mca_coll_ftbasic_agreement_log_two_phase_refresh_tree(ftbasic_module->agreement_info->fail_bitmap,
                                                                      comm,
                                                                      ftbasic_module);
            }
            exit_status = ret;
            goto cleanup;
        }
    }
    /************************************************
     * Participant (Child)
     ************************************************/
    else {
        if( OMPI_SUCCESS != (ret = log_two_phase_participant_child(comm, local_bitmap, ftbasic_module, log_entry)) ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) Participant (Child) Protocol Failed (%2d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret));
            /*
             * This is a special return code used to indicate that we decided
             * 'Abort' in the last round so try again. The only reason we would
             * have decided abort is if the root failed. So mark the root as
             * failed, and rebalance the tree so that next time we have a shot
             * at getting to 'Commit' with a new root.
             */
            if( OMPI_EXISTS == ret ) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                     "%s ftbasic: agreement) (log2phase) Participant (Child) Refreshing Tree (Root %2d failed)",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), agreement_tree->root));
                opal_bitmap_set_bit(ftbasic_module->agreement_info->fail_bitmap,
                                    agreement_tree->root);
                mca_coll_ftbasic_agreement_log_two_phase_refresh_tree(ftbasic_module->agreement_info->fail_bitmap,
                                                                      comm,
                                                                      ftbasic_module);
            }

            exit_status = ret;
            goto cleanup;
        }
    }

    /*
     * All done
     */
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Rank %2d/%2d Finished agreement. [%2d/%2d]",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank, size,
                         log_entry->seq_num, log_entry->attempt_num));

 cleanup:
    return exit_status;
}

static int internal_iagreement_log_two_phase(ompi_communicator_t* comm,
                                             ompi_group_t **group,
                                             int *flag,
                                             opal_bitmap_t *local_bitmap,
                                             mca_coll_ftbasic_module_t *ftbasic_module,
                                             struct mca_coll_ftbasic_request_t *collreq)
{
    int exit_status = OMPI_SUCCESS;
    int rank = ompi_comm_rank(comm);
    mca_coll_ftbasic_agreement_log_entry_t *log_entry = NULL;
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
#if OPAL_ENABLE_DEBUG
    int size = ompi_comm_size(comm);
#endif

    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_SETUP);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic:iagreement) (log2phase) Rank %2d/%2d Starting agreement...",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank, size));

    /*
     * Is this the first call to agreement?
     * If so setup some structures.
     * Do not use the local_bitmap since it is not agreed upon.
     * Instead contruct a tree assuming every process is alive (something
     * that all processes can say).
     */
    if( NULL == agreement_info->super.fail_bitmap ) {
        mca_coll_ftbasic_agreement_log_two_phase_refresh_tree(NULL, comm, ftbasic_module);
    }
    REMOTE_BITMAP_RESET_NEXT(ftbasic_module);

    /*
     * Make sure our progress function does not get accidentally triggered while
     * setting up this new request.
     */
    if( log_two_phase_inside_progress ) {
        opal_output(0, "%s ftbasic:iagreement) (log2phase) "
                    "Warning: Inside progress flag set. Re-entering iagreement - should not happen!",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
    }
    log_two_phase_inside_progress = true;

    ftbasic_module->agreement_info->cur_request->stage = LOG_TWO_PHASE_NB_STAGE_NULL;

    /*
     * Sanity check: If it is just me, then return directly
     */
    if( 1 == ompi_comm_num_active_local(comm) ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) All alone, skip agreement.",
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
    log_entry = mca_coll_ftbasic_agreement_log_entry_find(comm, ftbasic_module->agreement_info->agreement_seq_num,
                                                         true, ftbasic_module);
    ftbasic_module->agreement_info->cur_request->log_entry = (opal_list_item_t*)log_entry;
    OBJ_RETAIN(ftbasic_module->agreement_info->cur_request->log_entry);

    /************************************************
     * Elect a coordinator - Set as part of the 'tree'
     ************************************************/
    ftbasic_module->agreement_info->cur_request->coordinator = agreement_tree->root;

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic:iagreement) (log2phase) Elected Coordinator is %2d [Log %2d / %2d]",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ftbasic_module->agreement_info->cur_request->coordinator, 
                         log_entry->seq_num,
                         log_entry->attempt_num));

#if OPAL_ENABLE_DEBUG
    if( log_entry->attempt_num > ompi_comm_size(comm) ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Oops. It seems that we have restarted to many times...",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
        opal_output(0,
                    "%s ftbasic:iagreement) (log2phase) ****************** STOP ************************",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
        {
            int sleeper = 1;
            while(sleeper == 1 ) {
                sleep(1);
            }
        }
    }
#endif /* OPAL_ENABLE_DEBUG */

    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_SETUP);


    /************************************************
     * Coordinator vs. Participant (Parent vs. Child)
     ************************************************/
    /* Starting state */
    ftbasic_module->agreement_info->cur_request->stage = LOG_TWO_PHASE_NB_STAGE_NULL;
    mca_coll_ftbasic_agreement_log_two_phase_progress_comm(comm, ftbasic_module);

 cleanup:
    /*
     * Progress function will take it from here
     */
    log_two_phase_inside_progress = false;

    return exit_status;
}


/***************************************
 * Refresh the agreement tree
 ***************************************/
int mca_coll_ftbasic_agreement_log_two_phase_refresh_tree(opal_bitmap_t *local_bitmap,
                                                               ompi_communicator_t* comm,
                                                               mca_coll_ftbasic_module_t *ftbasic_module)
{
    bool free_bitmap = false;
    int size = ompi_comm_size(comm);
    mca_coll_ftbasic_remote_bitmap_t *remote_bitmap = NULL;
    int i;
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
#if DEBUG_WITH_STR == 1
    char *tree_str = NULL;
    int int_sep = 2;
#endif


    if( NULL == local_bitmap ) {
        free_bitmap = true;
        local_bitmap = OBJ_NEW(opal_bitmap_t);
        opal_bitmap_init(local_bitmap, ompi_comm_size(comm) + FTBASIC_AGREEMENT_EXTRA_BITS);
        opal_bitmap_clear_all_bits(local_bitmap);
    }

    /* ftbasic_module->agreement_info->fail_bitmap is allocated by main agreement function */

#if DEBUG_WITH_STR == 1
    tree_str = opal_bitmap_get_string(local_bitmap);
    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Refresh Tree... [%s]",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), tree_str ));
    if( NULL != tree_str ) {
        free(tree_str);
        tree_str = NULL;
    }
#else 
    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Refresh Tree...",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
#endif

    /* Copy the agreed upon local bitmap into the cached bitmap on the module.
     * We need to make sure we use the cached version to construct the tree
     * since we need all processes to be working with the same tree. So as
     * things fail they all make the same routing decisions.
     *
     * Compare Objects since it is possible to pass in the 'fail_bitmap' object
     */
    if( ftbasic_module->agreement_info->fail_bitmap != local_bitmap ) {
        opal_bitmap_copy(ftbasic_module->agreement_info->fail_bitmap, local_bitmap);
    }

    /* Reset these to '0' so we re-assess them during the update */
    agreement_tree->root = 0;
    agreement_tree->parent = 0;
    agreement_tree->num_children = 0;

    /* Find the new values */
    log_two_phase_update_root(comm, ftbasic_module);
    log_two_phase_update_parent(comm, ftbasic_module);
    log_two_phase_update_children(comm, ftbasic_module);

    /* Allocate initial remote_bitmaps array */
    if( 0 < agreement_tree->num_children &&
        0 >= opal_list_get_size(&(ftbasic_module->agreement_info->remote_bitmaps)) ) {
        for( i = 0; i < agreement_tree->num_children; ++i) {
            REMOTE_BITMAP_ALLOC(remote_bitmap, size);
            remote_bitmap->rank = RANK_UNDEF;
            opal_list_append(&(ftbasic_module->agreement_info->remote_bitmaps),
                             (opal_list_item_t*)remote_bitmap);
        }
    }
    REMOTE_BITMAP_RESET_NEXT(ftbasic_module);

#if DEBUG_WITH_STR == 1
    /*
     * Display the tree
     * {Root; Parent; Children, ...}
     */
    tree_str = (char *)malloc(sizeof(char) *
                              (1 + ((int_sep + 2)*3) +
                               (agreement_tree->num_children * (int_sep + 2)) + 2));
    tree_str[0] = '{';
    sprintf((tree_str+(1  )), "%*d; ", int_sep, agreement_tree->root);
    sprintf((tree_str+(1+(int_sep+2))), "%*d; ", int_sep, agreement_tree->parent);
    for( i = 0; i < agreement_tree->num_children; ++i) {
        if( (i+1) == agreement_tree->num_children ) {
            sprintf((tree_str+(1+(int_sep+2)+(int_sep+2)+(i*(int_sep+2)))), "%c %*d}%c",
                    (0 == i ? ' ' : ','), int_sep,
                    agreement_tree->children[i], '\0');
        }
        else if( 0 == i ) {
            sprintf((tree_str+(1+(int_sep+2)+(int_sep+2)+(i*(int_sep+2)))), "  %*d",
                    int_sep,
                    agreement_tree->children[i]);
        }
        else {
            sprintf((tree_str+(1+(int_sep+2)+(int_sep+2)+(i*(int_sep+2)))), ", %*d",
                    int_sep,
                    agreement_tree->children[i]);
        }
    }
    if( agreement_tree->num_children <= 0 ) {
        sprintf((tree_str+(1+(int_sep+2)+(int_sep+2))), "-}%c", '\0');
    }

    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Tree: %s",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), tree_str));
    if( NULL != tree_str ) {
        free(tree_str);
        tree_str = NULL;
    }
#endif

    if( free_bitmap ) {
        OBJ_RELEASE(local_bitmap);
        local_bitmap = NULL;
    }

    return OMPI_SUCCESS;
}

/***************************************
 * Termination Protocol Progress
 ***************************************/
int mca_coll_ftbasic_agreement_log_two_phase_term_progress(void)
{
    ompi_communicator_t *comm = NULL;
    int max_num_comm = 0, i;
    int num_processed = 0;

    if( ompi_mpi_finalized ) {
        return 0;
    }

    if( log_two_phase_inside_term_progress ) {
        return num_processed;
    }
    log_two_phase_inside_term_progress = true;

    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Listener: Polling...",
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
        if( mca_coll_ftbasic_iagreement_log_two_phase != comm->c_coll.coll_iagreement ) {
            continue;
        }

        num_processed += mca_coll_ftbasic_agreement_log_two_phase_term_progress_comm(comm);
    }

    /************************************************
     * Setup the timer again
     ************************************************/
    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Listener: Polling... (%3d Processed)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), num_processed ));

    log_two_phase_inside_term_progress = false;

    return num_processed;
}


/***************************************
 * Normal Protocol Progress
 ***************************************/
int mca_coll_ftbasic_agreement_log_two_phase_progress(void)
{
    ompi_communicator_t *comm = NULL;
    int max_num_comm = 0, i;
    int num_processed = 0;
    mca_coll_ftbasic_module_t *ftbasic_module = NULL;

    if( ompi_mpi_finalized ) {
        return 0;
    }

    /* Sanity check:
     * - Do not recursively enter this function from progress
     * - Do not enter this function if we are executing the termination protocol
     *   (could cause a deadlock)
     */
    if( log_two_phase_inside_progress ||
        log_two_phase_inside_term_progress ) {
        return num_processed;
    }

    log_two_phase_inside_progress = true;

    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic:iagreement) (log2phase) Progress: Progressing...",
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
        if( mca_coll_ftbasic_agreement_log_two_phase != comm->c_coll.coll_agreement ) {
            continue;
        }
        ftbasic_module = (mca_coll_ftbasic_module_t*) comm->c_coll.coll_agreement_module;
        /* Skip those without an active request */
        if( NULL == ftbasic_module->agreement_info->cur_request ) {
            continue;
        }

        num_processed += mca_coll_ftbasic_agreement_log_two_phase_progress_comm(comm,
                                                                               ftbasic_module);
    }

    /************************************************
     * Done
     ************************************************/
    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic:iagreement) (log2phase) Listener: Progressing... (%3d Processed)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), num_processed ));

    log_two_phase_inside_progress = false;

    return num_processed;
}


/************************* Support: Normal Protocol: General *****************************/
/* (A)
 * Broadcast a message to all children.
 * If 'local_bitmap' is null then this is the 'vote request' and
 * no data is explicitly sent.
 *
 * Upon failure of a child -> Recursive bcast down the tree
 * - 'adopt' the grandchildren recursively
 * - re-send to the grandchildren
 */
static int log_two_phase_protocol_bcast_to_children(ompi_communicator_t* comm,
                                                    mca_coll_ftbasic_module_t *ftbasic_module,
                                                    opal_bitmap_t *local_bitmap,
                                                    int *num_reqs,
                                                    int of_rank,
                                                    int num_prev_children,
                                                    mca_coll_ftbasic_agreement_log_entry_t *log_entry)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank = ompi_comm_rank(comm);
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    int loc_num_reqs, i, packet_size;
    int loc_num_children = 0, loc_alloc = 0, loc_prev_num_children;
    int *loc_children = NULL;
    bool locally_allocated_array = false;
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
#if DEBUG_WITH_STR == 1
    char *tmp_bitstr = NULL;
#endif

    if( IS_INVALID_RANK(of_rank) ) {
        return OMPI_SUCCESS;
    }

#if DEBUG_WITH_STR == 1
    tmp_bitstr = opal_bitmap_get_string(local_bitmap);
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) bcast_to_children: "
                         "Self %3d of_rank %3d [%s]",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank, of_rank, tmp_bitstr ));
    if( NULL != tmp_bitstr ) {
        free(tmp_bitstr);
        tmp_bitstr = NULL;
    }
#endif

    /**************************************
     * Determine which children to send to
     * If we are sending to our children, then use the cached list.
     * Otherwise create the list of children.
     **************************************/
    if( of_rank == rank ) {
        loc_num_children = agreement_tree->num_children;
        loc_children = agreement_tree->children;
        loc_prev_num_children = num_prev_children = loc_num_children;
        loc_alloc = agreement_tree->num_children_alloc;
        locally_allocated_array = false;
    }
    else {
        /* 'adopt' children */
        log_two_phase_get_children_of(comm, ftbasic_module, of_rank,
                                      &loc_children, &loc_num_children, &loc_alloc);
        /* Append adopted children - Mark for notice as needed */
        log_two_phase_append_children(comm, ftbasic_module,
                                      loc_children, loc_num_children, true);
        loc_prev_num_children = num_prev_children + loc_num_children;
        locally_allocated_array = true;
    }

    /*
     * Basecase: If no children to send to, then this rank is a leaf and can
     * return success immediately.
     */
    if( 0 == loc_num_children ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) bcast_to_children: "
                             "(%3d) Basecase.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             of_rank ));
        exit_status = OMPI_SUCCESS;
        goto cleanup;
    }

    /*
     * If this is a non-blocking request, be sure to start from the 'num_req'
     * location as their might be other requests outstanding in the list.
     */
    if( NULL == num_reqs ) {
        loc_num_reqs = 0;
    } else {
        loc_num_reqs = *num_reqs;
    }


    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_OTHER);

    for(i = 0; i < loc_num_children; ++i ) {
        if( IS_INVALID_RANK(loc_children[i]) ) {
            continue;
        }

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_COORD_DURING_BCAST_COMMIT
        /* TEST:
         * Complete the first request
         * Die between first and second send to child
         */
        if( NULL != local_bitmap && rank == LOG_TWO_PHASE_TEST_RANK_COORD ) {
            if( i != 0 ) {
                ret = ompi_request_wait(&reqs[loc_num_reqs-1], &(statuses[loc_num_reqs-1]));
                sleep(2);
                kill(getpid(), SIGKILL);
            }
        }
#endif /* LOG_TWO_PHASE_TEST_COORD_DURING_BCAST_COMMIT */

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_P_DURING_BCAST_COMMIT
        /* TEST:
         * Complete the first request
         * Die between first and second send to child
         */
        if( NULL != local_bitmap && rank == LOG_TWO_PHASE_TEST_RANK_PARENT ) {
            if( i != 0 ) {
                ret = ompi_request_wait(&reqs[loc_num_reqs-1], &(statuses[loc_num_reqs-1]));
                sleep(2);
                kill(getpid(), SIGKILL);
            }
        }
#endif /* LOG_TWO_PHASE_TEST_PART_P_DURING_BCAST_COMMIT */

        /*
         * Make sure the peer is alive
         */
        if( !ompi_comm_is_proc_active(comm, loc_children[i], false) ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) bcast_to_children: "
                                 "(%3d) Failed Child %3d, adopt children.",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 of_rank, loc_children[i] ));
            ret = log_two_phase_protocol_bcast_to_children(comm, ftbasic_module, local_bitmap,
                                                           &loc_num_reqs, loc_children[i],
                                                           loc_prev_num_children,
                                                           log_entry);
            if( of_rank == rank ) {
                log_two_phase_remove_child(comm, ftbasic_module, loc_children[i]);
            }
            continue;
        }

        if( log_two_phase_if_should_skip(comm, ftbasic_module, loc_children[i], of_rank, num_prev_children) ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) bcast_to_children: "
                                 "Send Child %3d - Dup Skip. (prev = %2d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 loc_children[i], num_prev_children ));
            continue;
        }

        /*
         * If 'new' child and the first time we are interacting with it
         * then send_oob(cmd = 'bcast')
         */
        if( log_two_phase_if_needs_notice(comm, ftbasic_module, loc_children[i] ) ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) bcast_to_children: "
                                 "Send Notice to Child %3d.",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 loc_children[i] ));

            ret = log_two_phase_query_send_notice(comm, ftbasic_module, loc_children[i],
                                                  LOG_TWO_PHASE_QUERY_CMD_BCAST,
                                                  log_entry->seq_num,
                                                  log_entry->attempt_num);
            if( MPI_SUCCESS != ret ) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                     "%s ftbasic: agreement) (log2phase) bcast_to_children: "
                                     "(%3d) Failed Child %3d, adopt children.",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     of_rank, loc_children[i] ));
                ret = log_two_phase_protocol_bcast_to_children(comm, ftbasic_module, local_bitmap,
                                                               &loc_num_reqs, loc_children[i],
                                                               loc_prev_num_children,
                                                               log_entry);
                if( of_rank == rank ) {
                    log_two_phase_remove_child(comm, ftbasic_module, loc_children[i]);
                }
                continue;
            }
        }

        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) bcast_to_children: "
                             "(%3d) Send Child %3d (%s) [%s].",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             of_rank, loc_children[i],
                             (NULL == local_bitmap ? "NULL" : "List"),
                             (NULL == local_bitmap ? "" :
                              ( AGREEMENT_STATE_ABORT == log_entry->state ? "Abort" : "Commit")) ));

        /*
         * If no bitmap was passed, then this is the 'vote req' message
         */
        if( NULL == local_bitmap ) {
            ret = MCA_PML_CALL(isend( NULL, 0, MPI_BYTE,
                                      loc_children[i],
                                      MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                                      MCA_PML_BASE_SEND_SYNCHRONOUS,
                                      comm, &(reqs[loc_num_reqs]) ));
        }
        /*
         * Otherwise this is the final list
         */
        else {
            /* Encode decision into the bitfield - at end */
            if( AGREEMENT_STATE_ABORT == log_entry->state ) {
                opal_bitmap_set_bit(local_bitmap, ompi_comm_size(comm));
            } else {
                opal_bitmap_clear_bit(local_bitmap, ompi_comm_size(comm));
            }

            /* Always send the local_bitmap even if 'abort' since remote side
             * needs to post non-blocking receives for both
             */
            packet_size = local_bitmap->array_size;
            ret = MCA_PML_CALL(isend( (local_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                      loc_children[i],
                                      MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                                      MCA_PML_BASE_SEND_SYNCHRONOUS,
                                      comm, &(reqs[loc_num_reqs]) ));
        }
        loc_num_reqs++;
    }
    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_OTHER);

    /*
     * If we are not given a 'num_reqs' then just block here
     */
    if( NULL == num_reqs ) {
        ret = ompi_request_wait_all(loc_num_reqs, reqs, statuses);
        if( MPI_SUCCESS != ret ) {
            if( of_rank == rank ) {
                loc_children = NULL;
                loc_children = (int*)malloc(sizeof(int) * 1);
                loc_alloc = 1;
                locally_allocated_array = true;
            }
            else if( loc_alloc > 0 && NULL != loc_children ) {
                free(loc_children);
                loc_children = (int*)malloc(sizeof(int) * 1);
                loc_alloc = 1;
                locally_allocated_array = true;
            }
            loc_num_children = 0;

            /*
             * Finish any outstanding requests and find failed procs
             */
            for(i = 0; i < loc_num_reqs; ++i ) {
                if( MPI_ERR_PENDING == statuses[i].MPI_ERROR ) {
                    ret = ompi_request_wait(&reqs[i], &(statuses[i]));
                    if( MPI_SUCCESS != ret ) {
                        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                             "%s ftbasic: agreement) (log2phase) bcast_to_children: "
                                             "Proc. %3d Failed!",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             statuses[i].MPI_SOURCE));
                        append_child_to_array(&loc_children, &loc_num_children, &loc_alloc, statuses[i].MPI_SOURCE);
                    }
                }
                else if( MPI_SUCCESS != statuses[i].MPI_ERROR ) {
                    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                         "%s ftbasic: agreement) (log2phase) bcast_to_children: "
                                         "Proc. %3d Failed!",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         statuses[i].MPI_SOURCE));
                    append_child_to_array(&loc_children, &loc_num_children, &loc_alloc, statuses[i].MPI_SOURCE);
                }
                else if( MPI_SUCCESS == statuses[i].MPI_ERROR ) {
                    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                         "%s ftbasic: agreement) (log2phase) bcast_to_children: "
                                         "Proc. %3d Wait Success!",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         statuses[i].MPI_SOURCE));
                }
            }

            mca_coll_ftbasic_free_reqs(reqs, loc_num_reqs);

            /*
             * All failed requests are recursively re-broadcasted
             */
            loc_alloc = loc_num_children;
            if( loc_num_children > 0 ) {
                for( i = 0; i < loc_num_children; ++i ) {
                    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                         "%s ftbasic: agreement) (log2phase) bcast_to_children: "
                                         "Proc. %3d Failed! Bcast to Children...",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         statuses[i].MPI_SOURCE));

                    /* Do this operation blocking
                     * JJH TODO:
                     * May want to do this non-blocking for large numbers of
                     * failures since the children may flood us with unexpected
                     * receives while we are waiting on other children.
                     */
                    ret = log_two_phase_protocol_bcast_to_children(comm, ftbasic_module, local_bitmap,
                                                                   NULL, loc_children[i],
                                                                   loc_prev_num_children,
                                                                   log_entry);
                    if( of_rank == rank ) {
                        log_two_phase_remove_child(comm, ftbasic_module, loc_children[i]);
                    }
                }
            }
        }
    }
    /*
     * If a non-blocking request then advance the num_reqs index.
     */
    else {
        (*num_reqs) = loc_num_reqs;
        /* Keep an index in case we have to call this in the non-blocking
         * operation to recover from the loss of a child.
         */
        agreement_tree->bkmrk_idx = loc_prev_num_children;
    }

 cleanup:
#if DEBUG_WITH_STR == 1
    if( NULL != tmp_bitstr ) {
        free(tmp_bitstr);
        tmp_bitstr = NULL;
    }
#endif

    if( locally_allocated_array && NULL != loc_children ) {
        free(loc_children);
        loc_children = NULL;
    }

    return exit_status;
}

/* (B)
 * Receive a message from parent
 * If 'local_bitmap' is null then this is the 'vote request' and
 * no data is explicitly received.
 *
 * If parent is failed -> recursively recv from grandparent
 * - Find grandparent
 * - Recv from grandparent
 *
 * If grandparent is root, and root failed
 * - 
 * - 
 */
static int log_two_phase_protocol_recv_from_parent(ompi_communicator_t* comm,
                                                   mca_coll_ftbasic_module_t *ftbasic_module,
                                                   opal_bitmap_t *local_bitmap,
                                                   int *num_reqs,
                                                   int of_rank)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank = ompi_comm_rank(comm);
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    int loc_num_reqs = 0;
    int packet_size = 0;
    int loc_parent;
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
#if DEBUG_WITH_STR == 1
    char *tmp_bitstr = NULL;
#endif

    if( IS_INVALID_RANK(of_rank) ) {
        return OMPI_SUCCESS;
    }

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) recv_from_parent: "
                         "Self %3d of_rank %3d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank, of_rank ));

    /**************************************
     * Determine which parent to recv from
     **************************************/
    if( of_rank == rank ) {
        loc_parent = agreement_tree->parent;
    }
    else {
        /* Find grandparent */
        log_two_phase_get_parent_of(comm, ftbasic_module, agreement_tree->parent, &loc_parent);
    }

    /*
     * Basecase: 'root' becomes the parent.
     * If root fails, then that is detected below.
     */

    /*
     * If this is a non-blocking request, be sure to start from the 'num_req'
     * location as their might be other requests outstanding in the list.
     */
    if( NULL == num_reqs ) {
        loc_num_reqs = 0;
    } else {
        loc_num_reqs = *num_reqs;
    }

    /*
     * Make sure the parent is alive
     */
    if( !ompi_comm_is_proc_active(comm, loc_parent, false) ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) recv_from_parent: "
                             "(%3d) Failed Parent %3d! Return to query.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             rank, loc_parent ));
        exit_status = MPI_ERR_PROC_FAILED;
        goto cleanup;
    }

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) recv_from_parent: "
                         "Recv Parent %3d (%s).",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         loc_parent,
                         (NULL == local_bitmap ? "NULL" : "List") ));

    /*
     * 'vote req'
     */
    if( NULL == local_bitmap ) {
        ret = MCA_PML_CALL(irecv( NULL, 0, MPI_BYTE,
                                  loc_parent,
                                  MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                                  comm, &(reqs[loc_num_reqs]) ));
    }
    /*
     * 'final list'
     */
    else {
        packet_size = local_bitmap->array_size;
        ret = MCA_PML_CALL(irecv( (local_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                  loc_parent,
                                  MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                                  comm, &(reqs[loc_num_reqs]) ));
    }

    /*
     * If we are not given a 'num_reqs' then just block here
     */
    if( NULL == num_reqs ) {
        ret = ompi_request_wait(&reqs[loc_num_reqs], &(statuses[loc_num_reqs]));
        if( MPI_SUCCESS != ret ) {
            OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) recv_from_parent: "
                                 "Error: Wait failed for peer %2d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 statuses[loc_num_reqs].MPI_SOURCE));
            exit_status = MPI_ERR_PROC_FAILED;
            goto cleanup;
        }
    }
    /*
     * If a non-blocking request then advance the num_reqs index.
     */
    else {
        (*num_reqs)++;
    }

    /*
     * If we had to switch parents mid-way through the protocol then make the
     * update to the tree structure so that:
     * 1) we don't get into a situation where we block
     * 2) we don't have to search the next time.
     */
    if( of_rank != rank && agreement_tree->parent != loc_parent) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:log2phase) (log2phase) recv_from_parent: "
                             "Replace Parent %2d with %2d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             agreement_tree->parent, loc_parent));
        agreement_tree->parent = loc_parent;
    }

 cleanup:
#if DEBUG_WITH_STR == 1
    if( NULL == num_reqs && NULL != local_bitmap ) {
        tmp_bitstr = opal_bitmap_get_string(local_bitmap);
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) recv_from_parent: "
                             "Self %3d of_rank %3d [%s]",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             rank, of_rank, tmp_bitstr ));
        if( NULL != tmp_bitstr ) {
            free(tmp_bitstr);
            tmp_bitstr = NULL;
        }
    }
#endif

    return exit_status;
}


/* (C)
 * Send a message to the parent
 *
 * If parent is failed
 * - Find grandparent
 * - Wait for grandparent to re-send request
 * - re-sent to grandparent
 *
 * If grandparent is root, and root failed
 * - 
 * - 
 */
static int log_two_phase_protocol_send_to_parent(ompi_communicator_t* comm,
                                                 mca_coll_ftbasic_module_t *ftbasic_module,
                                                 opal_bitmap_t *local_bitmap,
                                                 int *num_reqs,
                                                 int of_rank)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank = ompi_comm_rank(comm);
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    int loc_num_reqs = 0;
    int packet_size = 0;
    int loc_parent;
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
#if DEBUG_WITH_STR == 1
    char *tmp_bitstr = NULL;
#endif

    if( IS_INVALID_RANK(of_rank) ) {
        return OMPI_SUCCESS;
    }

#if DEBUG_WITH_STR == 1
    tmp_bitstr = opal_bitmap_get_string(local_bitmap);
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) send_to_parent: "
                         "Self %3d of_rank %3d [%s]",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank, of_rank, tmp_bitstr ));
    if( NULL != tmp_bitstr ) {
        free(tmp_bitstr);
        tmp_bitstr = NULL;
    }
#endif

    /**************************************
     * Determine which parent to send to
     **************************************/
    if( of_rank == rank ) {
        loc_parent = agreement_tree->parent;
    }
    else {
        /*
         * Receive 'vote req' from parent
         * Not using the 'vote req' it is not needed
         *
         * log_two_phase_protocol_recv_from_parent(comm, ftbasic_module, NULL, NULL, agreement_tree->parent);
         */

        /* Find grandparent */
        log_two_phase_get_parent_of(comm, ftbasic_module, agreement_tree->parent, &loc_parent);

        /* Continue to re-send */
    }

    /*
     * Basecase: 'root' becomes the parent.
     * If root fails, then that is detected below.
     */

    /*
     * If this is a non-blocking request, be sure to start from the 'num_req'
     * location as their might be other requests outstanding in the list.
     */
    if( NULL == num_reqs ) {
        loc_num_reqs = 0;
    } else {
        loc_num_reqs = *num_reqs;
    }

    /*
     * Make sure the parent is alive
     */
    if( !ompi_comm_is_proc_active(comm, loc_parent, false) ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) send_to_parent: "
                             "(%3d) Failed Parent %3d, return to query.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             rank, loc_parent ));
        exit_status = MPI_ERR_PROC_FAILED;
        goto cleanup;
    }

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) send_to_parent: "
                         "Send Parent %3d.",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         loc_parent ));

    packet_size = local_bitmap->array_size;
    /*
     * Needs to be a 'synchronous' send since we need ot make sure that we are
     * not buffering which would delay our reaction to a failed parent. The
     * delayed reaction could cause us to make an expensive termination
     * protocol decision when we do not need to.
     */
    ret = MCA_PML_CALL(isend( (local_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                              loc_parent,
                              MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                              MCA_PML_BASE_SEND_SYNCHRONOUS,
                              comm, &(reqs[loc_num_reqs]) ));

    /*
     * If we are not given a 'num_reqs' then just block here
     */
    if( NULL == num_reqs ) {
        ret = ompi_request_wait(&reqs[loc_num_reqs], &(statuses[loc_num_reqs]));
        if( MPI_SUCCESS != ret ) {
            OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) send_to_parent: "
                                 "Error: Wait failed for peer %2d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 statuses[loc_num_reqs].MPI_SOURCE));
            exit_status = MPI_ERR_PROC_FAILED;
            goto cleanup;
        }
    }
    /*
     * If a non-blocking request then advance the num_reqs index.
     */
    else {
        (*num_reqs)++;
    }

    /*
     * If we had to switch parents mid-way through the protocol then make the
     * update to the tree structure so that:
     * 1) we don't get into a situation where we block
     * 2) we don't have to search the next time.
     */
    if( of_rank != rank && agreement_tree->parent != loc_parent ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:log2phase) (log2phase) send_to_parent: "
                             "Replace Parent %2d with %2d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             agreement_tree->parent, loc_parent));
        agreement_tree->parent = loc_parent;
    }

 cleanup:
#if DEBUG_WITH_STR == 1
    if( NULL != tmp_bitstr) {
        free(tmp_bitstr);
        tmp_bitstr = NULL;
    }
#endif

    return exit_status;
}


/* (D)
 * Gather from all children
 * Upon failure of a child -> Recursive gather down the tree
 * - 'adopt' the grandchildren recursively
 * - re-send vote-req to the grandchildren
 * - re-recv vote from grandchildren
 */
static int log_two_phase_protocol_gather_from_children(ompi_communicator_t* comm,
                                                       mca_coll_ftbasic_module_t *ftbasic_module,
                                                       opal_bitmap_t *local_bitmap,
                                                       int *num_reqs,
                                                       int of_rank,
                                                       int num_prev_children,
                                                       mca_coll_ftbasic_agreement_log_entry_t *log_entry)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank = ompi_comm_rank(comm);
    int size = ompi_comm_size(comm);
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    int loc_num_reqs, i, packet_size;
    int loc_num_children = 0, loc_alloc = 0, loc_prev_num_children;
    int *loc_children = NULL;
    mca_coll_ftbasic_remote_bitmap_t *remote_bitmap = NULL;
    bool locally_allocated_array = false;
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;

    if( IS_INVALID_RANK(of_rank) ) {
        return OMPI_SUCCESS;
    }

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) gather_from_children: "
                         "Self %3d of_rank %3d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank, of_rank ));

    /**************************************
     * Determine which children to recv from
     * If we are recv'ing from our children, then use the cached list.
     * Otherwise create the list of children.
     **************************************/
    if( of_rank == rank ) {
        loc_num_children = agreement_tree->num_children;
        loc_children = agreement_tree->children;
        loc_prev_num_children = num_prev_children = loc_num_children;
        loc_alloc = agreement_tree->num_children_alloc;
        locally_allocated_array = false;
    }
    else {
        /*
         * Re-send 'vote-req'
         * - Not using the 'vote-req' since it is not needed
         *
         * ret = log_two_phase_protocol_bcast_to_children(comm, ftbasic_module, NULL, NULL, of_rank);
         */

        /* 'adopt' children */
        log_two_phase_get_children_of(comm, ftbasic_module, of_rank,
                                      &loc_children, &loc_num_children, &loc_alloc);
        /* Append adopted children - Mark for notice as needed */
        log_two_phase_append_children(comm, ftbasic_module,
                                      loc_children, loc_num_children, true);
        loc_prev_num_children = num_prev_children + loc_num_children;

        locally_allocated_array = true;
    }

    /*
     * Basecase: If no children to send to, then this rank is a leaf and can
     * return success immediately.
     */
    if( 0 == loc_num_children ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) gather_from_children: "
                             "(%3d) Basecase.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             of_rank ));
        exit_status = OMPI_SUCCESS;
        goto cleanup;
    }

    /*
     * If this is a non-blocking request, be sure to start from the 'num_req'
     * location as their might be other requests outstanding in the list.
     */
    if( NULL == num_reqs ) {
        loc_num_reqs = 0;
    } else {
        loc_num_reqs = *num_reqs;
    }

    for(i = 0; i < loc_num_children; ++i ) {
        if( IS_INVALID_RANK(loc_children[i]) ) {
            continue;
        }

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_COORD_DURING_GATHER_VOTE
        /* TEST:
         * Complete the first request
         * Die between first and second send to child
         */
        if( rank == LOG_TWO_PHASE_TEST_RANK_COORD ) {
            if( i != 0 ) {
                ret = ompi_request_wait(&reqs[loc_num_reqs-1], &(statuses[loc_num_reqs-1]));
                sleep(2);
                kill(getpid(), SIGKILL);
            }
        }
#endif /* LOG_TWO_PHASE_TEST_COORD_DURING_GATHER_VOTE */

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_P_DURING_GATHER_VOTE
        /* TEST:
         * Die between first and second send to child
         * Make sure to wait for the first 'recv' so that the child is
         * progressing to the next stage.
         */
        if( rank == LOG_TWO_PHASE_TEST_RANK_PARENT ) {
            if( i != 0 ) {
                ret = ompi_request_wait(&reqs[loc_num_reqs-1], &(statuses[loc_num_reqs-1]));
                sleep(2);
                kill(getpid(), SIGKILL);
            }
        }
#endif /* LOG_TWO_PHASE_TEST_PART_P_DURING_GATHER_VOTE */

        /*
         * Make sure the peer is alive
         */
        if( !ompi_comm_is_proc_active(comm, loc_children[i], false) ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) gather_from_children: "
                                 "(%3d) Recv Child %3d, adopt children.",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 of_rank, loc_children[i] ));
            ret = log_two_phase_protocol_gather_from_children(comm, ftbasic_module, local_bitmap,
                                                              &loc_num_reqs, loc_children[i], loc_prev_num_children,
                                                              log_entry);
            if( of_rank == rank ) {
                log_two_phase_remove_child(comm, ftbasic_module, loc_children[i]);
            }
            continue;
        }

        if(log_two_phase_if_should_skip(comm, ftbasic_module, loc_children[i], of_rank, num_prev_children) ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) gather_from_children: "
                                 "Recv Child %3d - Dup Skip.",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 loc_children[i] ));
            continue;
        }

        /*
         * If 'new' child and the first time we are interacting with it
         * then send_oob(cmd = 'gather')
         */
        if( log_two_phase_if_needs_notice(comm, ftbasic_module, loc_children[i] ) ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) gather_from_children: "
                                 "Send Notice to Child %3d.",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 loc_children[i] ));

            ret = log_two_phase_query_send_notice(comm, ftbasic_module, loc_children[i],
                                                  LOG_TWO_PHASE_QUERY_CMD_GATHER,
                                                  log_entry->seq_num,
                                                  log_entry->attempt_num);
            if( MPI_SUCCESS != ret ) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                     "%s ftbasic: agreement) (log2phase) gather_from_children: "
                                     "(%3d) Failed Child %3d, adopt children.",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     of_rank, loc_children[i] ));
                ret = log_two_phase_protocol_gather_from_children(comm, ftbasic_module, local_bitmap,
                                                                  &loc_num_reqs, loc_children[i], loc_prev_num_children,
                                                                  log_entry);
                if( of_rank == rank ) {
                    log_two_phase_remove_child(comm, ftbasic_module, loc_children[i]);
                }
                continue;
            }
        }

        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) gather_from_children: "
                             "Recv Child %3d.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             loc_children[i] ));

        REMOTE_BITMAP_GET_NEXT(ftbasic_module, remote_bitmap, size);
        remote_bitmap->rank = loc_children[i];

        packet_size = local_bitmap->array_size;
        ret = MCA_PML_CALL(irecv( (remote_bitmap->bitmap->bitmap),
                                  packet_size, MPI_UNSIGNED_CHAR,
                                  loc_children[i],
                                  MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                                  comm, &(reqs[loc_num_reqs]) ));
        loc_num_reqs++;
    }

    /*
     * If we are not given a 'num_reqs' then just block here
     */
    if( NULL == num_reqs ) {
        ret = ompi_request_wait_all(loc_num_reqs, reqs, statuses);

        if( MPI_SUCCESS != ret ) {
            if( of_rank == rank ) {
                loc_children = (int*)malloc(sizeof(int) * 1);
                loc_alloc = 1;
                locally_allocated_array = true;
            }
            else if( loc_alloc > 0 && NULL != loc_children ) {
                free(loc_children);
                loc_children = (int*)malloc(sizeof(int) * 1);
                loc_alloc = 1;
                locally_allocated_array = true;
            }
            loc_num_children = 0;

            /*
             * Finish any outstanding requests and find failed procs
             */
            for(i = 0; i < loc_num_reqs; ++i ) {
                if( MPI_ERR_PENDING == statuses[i].MPI_ERROR ) {
                    ret = ompi_request_wait(&reqs[i], &(statuses[i]));
                    if( MPI_SUCCESS != ret ) {
                        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                             "%s ftbasic: agreement) (log2phase) gather_from_children: "
                                             "Proc. %3d Failed! (A)",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             statuses[i].MPI_SOURCE));
                        append_child_to_array(&loc_children, &loc_num_children, &loc_alloc, statuses[i].MPI_SOURCE);
                    }
                }
                else if( MPI_SUCCESS != statuses[i].MPI_ERROR ) {
                    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                         "%s ftbasic: agreement) (log2phase) gather_from_children: "
                                         "Proc. %3d Failed! (B)",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         statuses[i].MPI_SOURCE));
                    append_child_to_array(&loc_children, &loc_num_children, &loc_alloc, statuses[i].MPI_SOURCE);
                }
                else if( MPI_SUCCESS == statuses[i].MPI_ERROR ) {
                    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                         "%s ftbasic: agreement) (log2phase) gather_from_children: "
                                         "Proc. %3d Wait Success!",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         statuses[i].MPI_SOURCE));
                }
            }

            mca_coll_ftbasic_free_reqs(reqs, loc_num_reqs);

            /*
             * All failed requests are recursively re-gathered
             */
            loc_alloc = loc_num_children;
            if( loc_num_children > 0 ) {
                for( i = 0; i < loc_num_children; ++i ) {
                    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                         "%s ftbasic: agreement) (log2phase) gather_from_children: "
                                         "Proc. %3d Failed! Gather from Children... [%3d : %3d]",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         loc_children[i], rank, of_rank));

                    /* Do this operation blocking
                     * JJH TODO:
                     * May want to do this non-blocking for large numbers of
                     * failures since the children may flood us with unexpected
                     * receives while we are waiting on other children.
                     */
                    ret = log_two_phase_protocol_gather_from_children(comm, ftbasic_module, local_bitmap,
                                                                      NULL, loc_children[i], loc_prev_num_children,
                                                                      log_entry);
                    if( of_rank == rank ) {
                        log_two_phase_remove_child(comm, ftbasic_module, loc_children[i]);
                    }
                }
            }
        }
    }
    /*
     * If a non-blocking request then advance the num_reqs index.
     */
    else {
        (*num_reqs) = loc_num_reqs;
        /* Keep an index in case we have to call this in the non-blocking
         * operation to recover from the loss of a child.
         */
        agreement_tree->bkmrk_idx = loc_prev_num_children;
    }

 cleanup:
    if( locally_allocated_array && NULL != loc_children ) {
        free(loc_children);
        loc_children = NULL;
    }

    return exit_status;
}


/* (E)
 * Accumulate the list
 */
static int log_two_phase_protocol_accumulate_list(ompi_communicator_t* comm,
                                                  mca_coll_ftbasic_module_t *ftbasic_module,
                                                  opal_bitmap_t *local_bitmap)
{
    mca_coll_ftbasic_remote_bitmap_t *remote_bitmap = NULL;
    opal_list_item_t* item = NULL;
    bool local_flag, remote_flag;
#if DEBUG_WITH_STR == 1
    char *my_bitstr = NULL, *remote_bitstr = NULL;
#endif

    /* JJH PERFORMANCE NOTE:
     * This is an optimization step for the algorithm, but since this
     * operation is currently linear scaling.
     *
     * A first pass to push all of our locally known failures into the local list.
     * These could have occured during the protocol.
     */
    agreement_init_local_bitmap(comm, local_bitmap);

    /*
     * Now to accumulate the other lists
     * Put the remote_bitmaps back on the free list
     */
    while( NULL != (item = opal_list_remove_first(&(ftbasic_module->agreement_info->remote_bitmaps)) ) ) {
        remote_bitmap = (mca_coll_ftbasic_remote_bitmap_t*)item;

        if( RANK_UNDEF == remote_bitmap->rank ) {
            continue;
        }

        if( IS_INVALID_RANK(remote_bitmap->rank) ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) accumulate_list: "
                                 "Child %3d: Failed During Protocol (%3d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 GET_RANK_OF_INVALID_RANK(remote_bitmap->rank), remote_bitmap->rank ));
            opal_bitmap_set_bit(local_bitmap, GET_RANK_OF_INVALID_RANK(remote_bitmap->rank));
            continue;
        }

        /*
         * Make sure the peer is alive
         * We cannot ignore the contribution of failed processes if the original
         * vote was successfully received. Because that may have been the only
         * opportunity that a child had to vote in this round.
         */
        if( !ompi_comm_is_proc_active(comm, remote_bitmap->rank, false) ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) accumulate_list: "
                                 "Child %3d: Failed After Protocol",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 GET_RANK_OF_INVALID_RANK(remote_bitmap->rank) ));
            /*
             * Make sure to mark them just in case they are not already marked.
             * It is fine to add new locally known failures to the commit list
             * here since the list is being accumulated up the tree. We cannot
             * add anything to the list once the root has commited it and
             * started sending it down the tree. So since this is only used
             * going up the tree it is fine.
             */
            opal_bitmap_set_bit(local_bitmap, remote_bitmap->rank);

            log_two_phase_remove_child(comm, ftbasic_module, GET_RANK_OF_INVALID_RANK(remote_bitmap->rank));
        }

#if DEBUG_WITH_STR == 1
        my_bitstr     = opal_bitmap_get_string(local_bitmap);
        remote_bitstr = opal_bitmap_get_string(remote_bitmap->bitmap);
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) accumulate_list: "
                             "Child %3d: Compare [%s] to [%s]",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             remote_bitmap->rank, my_bitstr, remote_bitstr));
        if( NULL != my_bitstr ) {
            free(my_bitstr);
            my_bitstr = NULL;
        }
        if( NULL != remote_bitstr ) {
            free(remote_bitstr);
            remote_bitstr = NULL;
        }
#endif

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

        REMOTE_BITMAP_RETURN(remote_bitmap);
    }
    REMOTE_BITMAP_RESET_NEXT(ftbasic_module);

#if DEBUG_WITH_STR == 1
    if( NULL != my_bitstr ) {
        free(my_bitstr);
        my_bitstr = NULL;
    }
    if( NULL != remote_bitstr ) {
        free(remote_bitstr);
        remote_bitstr = NULL;
    }
#endif

    return OMPI_SUCCESS;
}


/************************* Support: Normal Protocol: Coordinator*****************************/
static int log_two_phase_coordinator(ompi_communicator_t* comm,
                                     opal_bitmap_t *local_bitmap,
                                     mca_coll_ftbasic_module_t *ftbasic_module,
                                     mca_coll_ftbasic_agreement_log_entry_t *log_entry)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank = ompi_comm_rank(comm);

    /******************************* Voting Phase *****************************/

    /************************************************
     * Send List Request (broadcast)
     * Not required so skip...
     ************************************************/
    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Coordinator: "
                         "Bcast Request.",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

    log_entry->state   = AGREEMENT_STATE_VOTE_REQ;


#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_COORD_BEFORE_GATHER_VOTE
    /* TEST:
     */
    if( rank == LOG_TWO_PHASE_TEST_RANK_COORD ) {
        sleep(2);
        kill(getpid(), SIGKILL);
    }
#endif /* LOG_TWO_PHASE_TEST_COORD_BEFORE_GATHER_VOTE */

    /************************************************
     * Receive Lists from children processes (gather)
     ************************************************/
    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Coordinator: "
                         "Receive Lists.",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

    ret = log_two_phase_protocol_gather_from_children(comm, ftbasic_module, local_bitmap, NULL, rank, 0, log_entry);

    /***************************** Decision Phase *****************************/
    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER);
    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE);

    log_entry->state   = AGREEMENT_STATE_VOTED;


    /************************************************
     * Create the final list (commit list)
     ************************************************/
    ret = log_two_phase_protocol_accumulate_list(comm, ftbasic_module, local_bitmap);

    /* Add it to the local log */
    log_entry->state = AGREEMENT_STATE_COMMIT;
    opal_bitmap_copy(log_entry->commit_bitmap, local_bitmap);

    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE);
    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST);

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_COORD_BEFORE_BCAST_COMMIT
    /* TEST:
     */
    if( rank == LOG_TWO_PHASE_TEST_RANK_COORD ) {
        sleep(2);
        kill(getpid(), SIGKILL);
    }
#endif /* LOG_TWO_PHASE_TEST_COORD_BEFORE_BCAST_COMMIT */

    /************************************************
     * Send the commit list to all peers (broadcast)
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Coordinator: "
                         "Bcast final list.",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
    ret = log_two_phase_protocol_bcast_to_children(comm, ftbasic_module, local_bitmap,
                                                   NULL, rank, 0, log_entry);

    /* log_entry->suspend_query  = true; */
    ret = log_two_phase_query_process_all_entries(comm, ftbasic_module, log_entry);
    /* log_entry->suspend_query  = false; */
    if( OMPI_SUCCESS != ret ) {
        /* JJH TODO: process_queue error state (should never happen) */
    }

    log_entry->state = AGREEMENT_STATE_COMMITTED;
    log_two_phase_query_purge_queue(comm, ftbasic_module);

    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST);

    return exit_status;
}


/************************* Support: Normal Protocol: Participant - General *****************************/

/************************* Support: Normal Protocol: Participant - Parent  *****************************/
static int log_two_phase_participant_parent(ompi_communicator_t* comm,
                                            opal_bitmap_t *local_bitmap,
                                            mca_coll_ftbasic_module_t *ftbasic_module,
                                            mca_coll_ftbasic_agreement_log_entry_t *log_entry)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank = ompi_comm_rank(comm);
#if DEBUG_WITH_STR == 1
    char *tmp_bitstr = NULL;
#endif

    /***************************** Not Voted Phase ****************************/

    /************************************************
     * Wait for the List Request from parent
     * 'Vote_req' is not required, so skip
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                         "Wait for request",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    /************************************************
     * Send List Request to Children
     * 'Vote_req' is not required, so skip
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                         "Send request to children",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    log_entry->state   = AGREEMENT_STATE_VOTE_REQ;

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_P_BEFORE_GATHER_VOTE
    /* TEST:
     */
    if( rank == LOG_TWO_PHASE_TEST_RANK_PARENT ) {
        sleep(2);
        kill(getpid(), SIGKILL);
    }
#endif /* LOG_TWO_PHASE_TEST_PART_P_BEFORE_GATHER_VOTE */

    /************************************************
     * Receive lists from all Children
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                         "Recv list from children",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    ret = log_two_phase_protocol_gather_from_children(comm, ftbasic_module, local_bitmap, NULL, rank, 0, log_entry);

    /************************************************
     * Aggregate the list before sending forward
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                         "Reduce lists",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    ret = log_two_phase_protocol_accumulate_list(comm, ftbasic_module, local_bitmap);


#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_P_BEFORE_SEND_VOTE
    /* TEST:
     */
    if( rank == LOG_TWO_PHASE_TEST_RANK_PARENT ) {
        sleep(2);
        kill(getpid(), SIGKILL);
    }
#endif /* LOG_TWO_PHASE_TEST_PART_P_BEFORE_SEND_VOTE */

    /************************************************
     * Send list of failed processes
     * - coordinator failure = abort
     * Note: Enter uncertainty region
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                         "Send list",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    ret = log_two_phase_protocol_send_to_parent(comm, ftbasic_module, local_bitmap, NULL, rank);
    if( OMPI_SUCCESS != ret ) {
        ret = log_two_phase_protocol_query_parent(comm, ftbasic_module, local_bitmap, log_entry, false);
        if( OMPI_SUCCESS != ret ) {
            opal_output_verbose(1, ompi_ftmpi_output_handle,
                                "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                                "Error: Root failure. Decide Abort***",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            log_entry->state = AGREEMENT_STATE_ABORT;
            goto decide_abort;
        }
        else {
            goto decide;
        }
    }

    /***************************** Not Voted Phase / Uncertainty Period  ******/
    log_entry->state = AGREEMENT_STATE_VOTED;


#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_P_BEFORE_RECV_COMMIT
    /* TEST:
     */
    if( rank == LOG_TWO_PHASE_TEST_RANK_PARENT ) {
        sleep(2);
        kill(getpid(), SIGKILL);
    }
#endif /* LOG_TWO_PHASE_TEST_PART_P_BEFORE_RECV_COMMIT */

    /************************************************
     * Receive the final list
     * - coordinator failure = termination protocol
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                         "Wait for final list",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    ret = log_two_phase_protocol_recv_from_parent(comm, ftbasic_module, local_bitmap, NULL, rank);
    if( OMPI_SUCCESS != ret ) {
        ret = log_two_phase_protocol_query_parent(comm, ftbasic_module, local_bitmap, log_entry, true);
        if( OMPI_SUCCESS != ret ) {
            opal_output_verbose(1, ompi_ftmpi_output_handle,
                                "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                                "Error: Term. Protocol failure - Abort***",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            log_entry->state = AGREEMENT_STATE_ABORT;
            goto decide_abort;
        }
    }
    else if( opal_bitmap_is_set_bit(local_bitmap, ompi_comm_size(comm) ) ) {
        /* JJH TODO: Verify this is ok */
        log_entry->state = AGREEMENT_STATE_ABORT;
    }

 decide:
    /*
     * If we reponded to a peer before this, then we need to ignore latent
     * messages from the coordinator, and decide with the group.
     *
     * See rational in the child match of this.
     */
    if( AGREEMENT_STATE_UNCERTAIN == log_entry->state ) {
        if( OMPI_SUCCESS != ( ret = log_two_phase_term_initiator(comm, ftbasic_module, local_bitmap, log_entry)) ) {
            opal_output_verbose(1, ompi_ftmpi_output_handle,
                                "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                                "Error: Term. Protocol failure - Abort***",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            log_entry->state = AGREEMENT_STATE_ABORT;
            goto decide_abort;
        }
    }

 decide_abort:
    /*
     * Abort could have been decided in the termination protocol
     */
    if( AGREEMENT_STATE_ABORT == log_entry->state ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                             "Decide Abort.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
        /*
         * Broadcast decision to all children
         */
        ret = log_two_phase_protocol_bcast_to_children(comm, ftbasic_module, local_bitmap,
                                                       NULL, rank, 0, log_entry);

        /* log_entry->suspend_query  = true; */
        ret = log_two_phase_query_process_all_entries(comm, ftbasic_module, log_entry);
        /* log_entry->suspend_query  = false; */
        if( OMPI_SUCCESS != ret ) {
            /* JJH TODO: process_queue error state (should never happen) */
        }

        log_entry->state = AGREEMENT_STATE_ABORTED;
        log_two_phase_query_purge_queue(comm, ftbasic_module);

        exit_status = OMPI_EXISTS;
        goto cleanup;
    }

    /***************************** Commit Phase  ******************************/

    /************************************************
     * Commit this list
     ************************************************/
#if DEBUG_WITH_STR == 1
    tmp_bitstr = opal_bitmap_get_string(local_bitmap);
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                         "Commit the list [%s]",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), tmp_bitstr));
    if( NULL != tmp_bitstr ) {
        free(tmp_bitstr);
        tmp_bitstr = NULL;
    }
#endif

    log_entry->state = AGREEMENT_STATE_COMMIT;
    opal_bitmap_copy(log_entry->commit_bitmap, local_bitmap);


#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_C_AFTER_VOTE
    /* TEST:
     */
    if( rank == LOG_TWO_PHASE_TEST_RANK_PARENT ) {
        int i;
        for( i = 0; i < 30; ++i ) {
            usleep(100000);
            opal_progress();
        }
    }
#endif /* LOG_TWO_PHASE_TEST_PART_C_AFTER_VOTE */


#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_P_BEFORE_BCAST_COMMIT
    /* TEST:
     */
    if( rank == LOG_TWO_PHASE_TEST_RANK_PARENT ) {
        sleep(2);
        kill(getpid(), SIGKILL);
    }
#endif /* LOG_TWO_PHASE_TEST_PART_P_BEFORE_BCAST_COMMIT */


    /************************************************
     * Send final list to all children
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                         "Send final list to children",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    ret = log_two_phase_protocol_bcast_to_children(comm, ftbasic_module, local_bitmap,
                                                   NULL, rank, 0, log_entry);

    /* log_entry->suspend_query  = true; */
    ret = log_two_phase_query_process_all_entries(comm, ftbasic_module, log_entry);
    /* log_entry->suspend_query  = false; */
    if( OMPI_SUCCESS != ret ) {
        /* JJH TODO: process_queue error state (should never happen) */
    }

    log_entry->state = AGREEMENT_STATE_COMMITTED;
    log_two_phase_query_purge_queue(comm, ftbasic_module);

    /*
     * Cleanup
     */
 cleanup:
#if DEBUG_WITH_STR == 1
    if( NULL != tmp_bitstr ) {
        free(tmp_bitstr);
        tmp_bitstr = NULL;
    }
#endif

    return exit_status;
}


/************************* Support: Normal Protocol: Participant - Child   *****************************/
static int log_two_phase_participant_child(ompi_communicator_t* comm,
                                           opal_bitmap_t *local_bitmap,
                                           mca_coll_ftbasic_module_t *ftbasic_module,
                                           mca_coll_ftbasic_agreement_log_entry_t *log_entry)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank = ompi_comm_rank(comm);
#if DEBUG_WITH_STR == 1
    char *tmp_bitstr = NULL;
#endif

    /***************************** Not Voted Phase  ***************************/

    /************************************************
     * Wait for the List Request
     * 'Vote_req' is not required, so skip
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Participant: (Child) "
                         "Wait for request",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_C_AFTER_VOTE_REQ
    /* TEST:
     */
    if( rank == LOG_TWO_PHASE_TEST_RANK_CHILD ) {
        sleep(2);
        kill(getpid(), SIGKILL);
    }
#endif /* LOG_TWO_PHASE_TEST_PART_C_AFTER_VOTE_REQ */

    /************************************************
     * Send list of failed processes
     * - coordinator failure = abort
     * Note: Enter uncertainty region
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Participant: (Child) "
                         "Send list",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    ret = log_two_phase_protocol_send_to_parent(comm, ftbasic_module, local_bitmap, NULL, rank);
    if( OMPI_SUCCESS != ret ) {
        ret = log_two_phase_protocol_query_parent(comm, ftbasic_module, local_bitmap, log_entry, false);
        if( OMPI_SUCCESS != ret ) {
            opal_output_verbose(1, ompi_ftmpi_output_handle,
                                "%s ftbasic: agreement) (log2phase) Participant: (Child) "
                                "Error: Root failure. Decide Abort***",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            log_entry->state = AGREEMENT_STATE_ABORT;
            goto decide_abort;
        } else {
            goto decide;
        }
    }

    /***************************** Voted Phase / Uncertainty Period  **********/
    log_entry->state = AGREEMENT_STATE_VOTED;

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_C_AFTER_VOTE
    /* TEST:
     */
    if( rank == LOG_TWO_PHASE_TEST_RANK_CHILD ) {
        sleep(2);
        kill(getpid(), SIGKILL);
    }
#endif /* LOG_TWO_PHASE_TEST_PART_C_AFTER_VOTE */

    /************************************************
     * Receive the final list
     * - coordinator failure = termination protocol
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Participant: (Child) "
                         "Wait for final list",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    ret = log_two_phase_protocol_recv_from_parent(comm, ftbasic_module, local_bitmap, NULL, rank);
    if( OMPI_SUCCESS != ret ) {
#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_P_DURING_BCAST_COMMIT
        /* TEST:
         * Complete the first request
         * Die between first and second send to child
         */
        if( rank == 5 ) {
            sleep(3);
        }
#endif
        ret = log_two_phase_protocol_query_parent(comm, ftbasic_module, local_bitmap, log_entry, true);
        if( OMPI_SUCCESS != ret ) {
            opal_output_verbose(1, ompi_ftmpi_output_handle,
                                "%s ftbasic: agreement) (log2phase) Participant: (Child) "
                                "Error: Term. Protocol failure - Abort***",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            log_entry->state = AGREEMENT_STATE_ABORT;
            goto decide_abort;
        }
    }
    else if( opal_bitmap_is_set_bit(local_bitmap, ompi_comm_size(comm) ) ) {
        /* JJH TODO: Agreement this is correct */
        log_entry->state = AGREEMENT_STATE_ABORT;
    }

 decide:
    /*
     * If we reponded to a peer before this, then we need to ignore latent
     * messages from the coordinator, and decide with the group.
     *
     * If we did not do this then it is possible that we could decide 'Commit'
     * while another process decides 'Uncertain' which escalates to 'Abort'.
     * The 'Abort' decided process will rebalance the tree with the root gone
     * and I will not. Then we will ahve a mismatch of protocol with some
     * ranks re-entering the agreement for another attempt while other ranks
     * (namely those like me that decided 'Commit') will not.
     */
    if( AGREEMENT_STATE_UNCERTAIN == log_entry->state ) {
        if( OMPI_SUCCESS != ( ret = log_two_phase_term_initiator(comm, ftbasic_module, local_bitmap, log_entry)) ) {
            opal_output_verbose(1, ompi_ftmpi_output_handle,
                                "%s ftbasic: agreement) (log2phase) Participant: (Child) "
                                "Error: Term. Protocol failure - Abort***",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            log_entry->state = AGREEMENT_STATE_ABORT;
            exit_status = ret;
            goto cleanup;
        }
    }

 decide_abort:
    /*
     * Abort could have been decided in the termination protocol
     */
    if( AGREEMENT_STATE_ABORT == log_entry->state ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Participant: (Child) "
                             "Decide Abort.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
        exit_status = OMPI_EXISTS;
        goto cleanup;
    }

    /***************************** Commit Phase  ******************************/

    /************************************************
     * Commit this list
     ************************************************/
#if DEBUG_WITH_STR == 1
    tmp_bitstr = opal_bitmap_get_string(local_bitmap);
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Participant: (Child) "
                         "Commit the list [%s]",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), tmp_bitstr));
    if( NULL != tmp_bitstr ) {
        free(tmp_bitstr);
        tmp_bitstr = NULL;
    }
#endif

    log_entry->state = AGREEMENT_STATE_COMMIT;
    opal_bitmap_copy(log_entry->commit_bitmap, local_bitmap);


    /*
     * Cleanup
     */
 cleanup:
#if DEBUG_WITH_STR == 1
    if( NULL != tmp_bitstr ) {
        free(tmp_bitstr);
        tmp_bitstr = NULL;
    }
#endif

    return exit_status;
}


/************************* Support: Termination Protocol *****************************/
/* (F)
 * Query the state of a new parent
 * This is an operation to augment the termination protocol.
 * Keep it is a separate function for clarity of implementation.
 *
 * - Send_oob(Context)
 * - Send_oob(Waiting) -> Redundant with 'context' so drop
 * - Recv_oob(cmd_msg)
 * - if( cmd_msg == 'decide' )
 *   - Recv_oob(Commit)
 * - if( cmd_msg == 'gather' )
 *   - Send(Vote)   for gather
 *   - Recv(Commit) from bcast
 * - if( cmd_msg == 'bcast' )
 *   - Do not Send(Vote), they don't need it
 *   - Recv(Commit) from bcast
 */
static int log_two_phase_protocol_query_parent(ompi_communicator_t* comm,
                                               mca_coll_ftbasic_module_t *ftbasic_module,
                                               opal_bitmap_t *local_bitmap,
                                               mca_coll_ftbasic_agreement_log_entry_t *log_entry,
                                               bool root_fail_do_term_protocol)
{
    int ret, exit_status = OMPI_SUCCESS;
    int size = ompi_comm_size(comm);
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    int packet_size = 0;
    int loc_parent;
    int loc_cmd = LOG_TWO_PHASE_CMD_QUERY_PROTOCOL;
    opal_bitmap_t *loc_commit_bitmap = NULL;
    int cmd_msg, cmd_seq_num, cmd_attempt_num;
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;

    packet_size = local_bitmap->array_size;

    log_entry->state = AGREEMENT_STATE_UNCERTAIN;

    /**************************************
     * Determine which parent to recv from
     **************************************/
 parent_election:
    log_two_phase_get_parent_of(comm, ftbasic_module, agreement_tree->parent, &loc_parent);
    agreement_tree->parent = loc_parent;

    /*
     * If parent is 'root' and the root is failed, then go to the termination protocol
     */
    if( !ompi_comm_is_proc_active(comm, loc_parent, false) ) {
        if( loc_parent == agreement_tree->root ) {
            opal_output_verbose(5, ompi_ftmpi_output_handle,
                                "%s ftbasic: agreement) (log2phase) Query: "
                                "Root Failed (%3d), %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), loc_parent,
                                (root_fail_do_term_protocol ? "Termination Protocol" : "Return to Abort") );
            if( root_fail_do_term_protocol ) {
                exit_status = log_two_phase_term_initiator(comm, ftbasic_module, local_bitmap, log_entry);
            } else {
                exit_status = MPI_ERR_PROC_FAILED;
            }
            goto cleanup;
        } else {
            /* This should not happen here, but put it here for completeness */
            goto parent_election;
        }
    }

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Query: "
                         "Asking Parent = %2d [%2d/%2d]",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), loc_parent,
                         log_entry->seq_num, log_entry->attempt_num));

    /**************************************
     * Send term protocol context information
     *  - Cmd: query_req
     *  - seq_num
     *  - attempt_num
     * Send both the seq_num and attempt_num so the peer can decide if it
     * needs to move to the next round.
     **************************************/
    ret = MCA_PML_CALL(send( &(loc_cmd), 1, MPI_INT,
                             loc_parent,
                             MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                             MCA_PML_BASE_SEND_STANDARD,
                             comm));
    if( MPI_SUCCESS != ret ) {
        goto error;
    }

    ret = MCA_PML_CALL(send( &(log_entry->seq_num), 1, MPI_INT,
                             loc_parent,
                             MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                             MCA_PML_BASE_SEND_STANDARD,
                             comm));
    if( MPI_SUCCESS != ret ) {
        goto error;
    }

    ret = MCA_PML_CALL(send( &(log_entry->attempt_num), 1, MPI_INT,
                             loc_parent,
                             MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                             MCA_PML_BASE_SEND_STANDARD,
                             comm));
    if( MPI_SUCCESS != ret ) {
        goto error;
    }


    /**************************************
     * Receive the command message from the parent
     *
     * If the parent is in the next iteration of the agreement then just
     * drop the notice. It will be 'gather' and we will do that next time
     * we enter agreement.
     *
     * In that case then we need to keep receiving until the parent
     * sends us the 'commit' for for previous iteration of the agreement.
     **************************************/
    do {
        ret = log_two_phase_query_recv_notice(comm, ftbasic_module, loc_parent,
                                              &cmd_msg, &cmd_seq_num, &cmd_attempt_num);
        if( MPI_SUCCESS != ret ) {
            goto error;
        }

        opal_output_verbose(5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Query: "
                            "Rank %2d replied %s [P: %2d/%2d vs M: %2d/%2d]",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), loc_parent,
                             (LOG_TWO_PHASE_QUERY_CMD_DECIDE_COMMIT == cmd_msg ? "Committed" :
                              (LOG_TWO_PHASE_QUERY_CMD_DECIDE_ABORT == cmd_msg ? "Aborted" :
                               (LOG_TWO_PHASE_QUERY_CMD_GATHER      == cmd_msg ? "Gather" :
                                (LOG_TWO_PHASE_QUERY_CMD_BCAST       == cmd_msg ? "Bcast" : "Unknown" )))),
                             cmd_seq_num, cmd_attempt_num,
                             log_entry->seq_num, log_entry->attempt_num
                             );
        /* If the parent is on the same sequence, but a different attempt then
         * they must have decided abort previously and are attempting once more.
         * So decide abort
         *
         * JJH TODO: Double check this
         */
        if( cmd_seq_num == log_entry->seq_num &&
            cmd_attempt_num != log_entry->attempt_num) {
            cmd_msg = LOG_TWO_PHASE_QUERY_CMD_DECIDE_COMMIT;
        }
    } while (cmd_seq_num != log_entry->seq_num );

    /**************************************
     * If Parent has Decided, decide with parent
     **************************************/
    if( LOG_TWO_PHASE_QUERY_CMD_DECIDE_COMMIT == cmd_msg ||
        LOG_TWO_PHASE_QUERY_CMD_DECIDE_ABORT  == cmd_msg ) {

        if( LOG_TWO_PHASE_QUERY_CMD_DECIDE_COMMIT == cmd_msg ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) Query: "
                                 "Decide Commit with Rank %2d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), loc_parent));

            if( NULL == loc_commit_bitmap ) {
                loc_commit_bitmap = OBJ_NEW(opal_bitmap_t);
                opal_bitmap_init(loc_commit_bitmap, size + FTBASIC_AGREEMENT_EXTRA_BITS);
            }
            ret = MCA_PML_CALL(recv( (loc_commit_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                     loc_parent,
                                     MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP,
                                     comm, &(statuses[0]) ));
            if( MPI_SUCCESS != ret ) {
                goto error;
            }

            opal_bitmap_copy(local_bitmap, loc_commit_bitmap);
            log_entry->state = AGREEMENT_STATE_COMMIT;
        } else {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) Query: "
                                 "Decide Abort with Rank %2d",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), loc_parent));
            log_entry->state = AGREEMENT_STATE_ABORT;
        }
    }
    /**************************************
     * If Parent in 'gather' that particpate from there
     **************************************/
    else if( LOG_TWO_PHASE_QUERY_CMD_GATHER == cmd_msg ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Query: "
                             "Participate in Gather with Rank %2d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), loc_parent));

        packet_size = local_bitmap->array_size;
        ret = MCA_PML_CALL(send( (local_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                 loc_parent,
                                 MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                                 MCA_PML_BASE_SEND_SYNCHRONOUS,
                                 comm ));
        if( MPI_SUCCESS != ret ) {
            goto error;
        }

        if( NULL == loc_commit_bitmap ) {
            loc_commit_bitmap = OBJ_NEW(opal_bitmap_t);
            opal_bitmap_init(loc_commit_bitmap, size + FTBASIC_AGREEMENT_EXTRA_BITS);
        }
        ret = MCA_PML_CALL(recv( (loc_commit_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                 loc_parent,
                                 MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                                 comm, &(statuses[0]) ));
        if( MPI_SUCCESS != ret ) {
            goto error;
        }

        opal_bitmap_copy(local_bitmap, loc_commit_bitmap);
        log_entry->state = AGREEMENT_STATE_COMMIT;
    }
    /**************************************
     * If Parent in 'bcast' that particpate from there
     **************************************/
    else if( LOG_TWO_PHASE_QUERY_CMD_BCAST == cmd_msg ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Query: "
                             "Participate in Bcast with Rank %2d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), loc_parent));

        if( NULL == loc_commit_bitmap ) {
            loc_commit_bitmap = OBJ_NEW(opal_bitmap_t);
            opal_bitmap_init(loc_commit_bitmap, size + FTBASIC_AGREEMENT_EXTRA_BITS);
        }
        ret = MCA_PML_CALL(recv( (loc_commit_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                 loc_parent,
                                 MCA_COLL_FTBASIC_TAG_AGREEMENT(ftbasic_module),
                                 comm, &(statuses[0]) ));
        if( MPI_SUCCESS != ret ) {
            goto error;
        }

        opal_bitmap_copy(local_bitmap, loc_commit_bitmap);
        log_entry->state = AGREEMENT_STATE_COMMIT;
    }
    /**************************************
     * Else so other state, should never happen
     **************************************/
    else {
        OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Query: "
                             "Unkown response from Rank %2d -- Should never happen",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), loc_parent));
        goto error;
    }

 error:
    /*
     * Parent failed during the query.
     * Search for the next parent
     */
    if( MPI_SUCCESS != ret ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Query: "
                             "Error: Parent %2d failed, trying next parent (ret = %3d)",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             loc_parent, ret));
#if OPAL_ENABLE_DEBUG
        /* Sanity check */
        if( ompi_comm_is_proc_active(comm, loc_parent, false) ) {
            if( loc_parent == agreement_tree->root ) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                     "%s ftbasic: agreement) (log2phase) Query: "
                                     "Root identified as incorrectly Failed (%3d)...",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), loc_parent));
                opal_output_verbose(0, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (log2phase) ****************** STOP ************************",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) );
                {
                    int sleeper = 1;
                    while(sleeper == 1 ) {
                        sleep(1);
                    }
                }
            }
        }
#endif /* OPAL_ENABLE_DEBUG */
        goto parent_election;
    } else {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Query: "
                             "Decided with Parent %2d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             loc_parent ));
        exit_status = OMPI_SUCCESS;
    }
   
 cleanup:
   if( NULL != loc_commit_bitmap ) {
       OBJ_RELEASE(loc_commit_bitmap);
       loc_commit_bitmap = NULL;
   }

    return exit_status;
}

/*
 * Query responder
 * Only called in a 'parent' or 'root' process by a child recovering from
 * a failed parent.
 *
 * - Recv_oob(Waiting)
 * - if( !postpone )
 *   - if( committed or aborted )
 *     Responding about a previous round, so reply directly
 *     - Send_oob('Decide')
 *     - Send_oob(Commit)
 *   - else
 *     - push_onto_queue
 * - else
 *   - push_onto_queue
 */
static int log_two_phase_query_responder(ompi_communicator_t* comm,
                                         mca_coll_ftbasic_module_t *ftbasic_module,
                                         int peer)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    int loc_seq_num, loc_attempt_num;
    mca_coll_ftbasic_agreement_log_entry_t *log_entry = NULL;

    rank = ompi_comm_rank(comm);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Query: "
                         "Responder %2d, Reply to %2d ***********************************",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), rank, peer));

    /************************************************
     * Finish receiving the message
     * - ignore peer failure
     ************************************************/
    ret = MCA_PML_CALL(recv( &loc_seq_num, 1, MPI_INT,
                             peer, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                             comm, &(statuses[0]) ));
    if (MPI_SUCCESS != ret) {
        opal_output_verbose(1, ompi_ftmpi_output_handle,
                            "%s ftbasic: agreement) (log2phase) Query: Responder: "
                            "Error: Peer Failure %2d - Ignore",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer);
        goto cleanup;
    }

    ret = MCA_PML_CALL(recv( &loc_attempt_num, 1, MPI_INT,
                             peer, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                             comm, &(statuses[0]) ));
    if (MPI_SUCCESS != ret) {
        opal_output_verbose(1, ompi_ftmpi_output_handle,
                            "%s ftbasic: agreement) (log2phase) Query: Responder: "
                            "Error: Peer Failure %2d - Ignore",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer);
        goto cleanup;
    }

    /************************************************
     * Find this log entry
     ************************************************/
    log_entry = mca_coll_ftbasic_agreement_log_entry_find(comm, loc_seq_num, false, ftbasic_module);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Query: Responder: "
                         "Peer %2d [%2d/%2d, Log %2d/%2d] State %s ***",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer,
                         loc_seq_num, loc_attempt_num, log_entry->seq_num, log_entry->attempt_num,
                         AGREEMENT_STATE_STR(log_entry->state) ));

    /*
     * Append the child so we do not mistakenly see it as 'new' later
     */
    log_two_phase_append_child(comm, ftbasic_module, peer, true);

    /************************************************
     * If this entry was finished then send the response directly to the peer
     ************************************************/
    if( AGREEMENT_STATE_COMMITTED == log_entry->state ||
        AGREEMENT_STATE_ABORTED   == log_entry->state ) {

        /* Process Entry checks for this case, so re-use so that we do not duplicate code */
        exit_status = log_two_phase_query_process_entry(comm, ftbasic_module, peer, loc_seq_num, loc_attempt_num, log_entry);
        goto cleanup;
    }
    /************************************************
     * Query suspended -> processing the queue
     * So just add this entry to the end of the queue and
     * it will be processed then.
     * -or-
     * Currently working in the protocol and will either
     * be delt with during the protocol or afterwards.
     * In either case add it to the queue
     ************************************************/
    else {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Query: Responder: "
                             "Processing... Not decided, delay reply to peer %2d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             peer ));
        /* Add to the queue */
        log_two_phase_query_append_queue(comm, ftbasic_module, peer, loc_seq_num, loc_attempt_num, log_entry);
    }

 cleanup:
    return exit_status;
}

static int log_two_phase_query_append_queue(ompi_communicator_t* comm,
                                            mca_coll_ftbasic_module_t *ftbasic_module,
                                            int peer,
                                            int seq_num,
                                            int attempt_num,
                                            mca_coll_ftbasic_agreement_log_entry_t *log_entry)
{
    log_two_phase_query_entry_t *entry = NULL;
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);

    entry = OBJ_NEW(log_two_phase_query_entry_t);
    entry->peer = peer;
    entry->seq_num = seq_num;
    entry->attempt_num = attempt_num;

    opal_list_append(agreement_info->query_queue, &(entry->super));

    return OMPI_SUCCESS;
}

static int log_two_phase_query_purge_queue(ompi_communicator_t* comm,
                                           mca_coll_ftbasic_module_t *ftbasic_module)
{
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    log_two_phase_query_entry_t *entry = NULL;
    opal_list_item_t *item = NULL;
    opal_list_item_t *next_item = NULL;

    item      = opal_list_get_first(agreement_info->query_queue);
    next_item = opal_list_get_next(item);

    for( ; item != opal_list_get_end(agreement_info->query_queue); ) {
        entry = (log_two_phase_query_entry_t*)item;
        /* Remove all done entries */
        if( entry->done ) {
            opal_list_remove_item(agreement_info->query_queue, item);
            OBJ_RELEASE(item);
        }
        item      = next_item;
        next_item = opal_list_get_next(item);
    }

    return OMPI_SUCCESS;
}

static int log_two_phase_query_send_notice(ompi_communicator_t* comm,
                                           mca_coll_ftbasic_module_t *ftbasic_module,
                                           int peer,
                                           int cmd_msg,
                                           int seq_num,
                                           int attempt_num)
{
    int ret = OMPI_SUCCESS;
    int msg_buffer[3];

    log_two_phase_mark_need_notice(comm, ftbasic_module, peer, false);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) send_notice: "
                         "Child %3d Cmd %s [%2d/%2d].",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         peer,
                         LOG_QUERY_PHASE_STR(cmd_msg),
                         seq_num, attempt_num ));

    msg_buffer[0] = cmd_msg;
    msg_buffer[1] = seq_num;
    msg_buffer[2] = attempt_num;

    ret = MCA_PML_CALL(send( &(msg_buffer), 3, MPI_INT,
                             peer,
                             MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP,
                             MCA_PML_BASE_SEND_SYNCHRONOUS,
                             comm));
    if( OMPI_SUCCESS != ret ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) send_notice: "
                             "Error: Child %3d Cmd %s [%2d/%2d] - failed.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             peer,
                             LOG_QUERY_PHASE_STR(cmd_msg),
                             seq_num, attempt_num ));
        goto cleanup;
    }

 cleanup:
    return ret;
}

static int log_two_phase_query_recv_notice(ompi_communicator_t* comm,
                                           mca_coll_ftbasic_module_t *ftbasic_module,
                                           int peer,
                                           int *cmd_msg,
                                           int *seq_num,
                                           int *attempt_num)
{
    int ret = OMPI_SUCCESS;
    MPI_Status status;
    int msg_buffer[3];

    ret = MCA_PML_CALL(recv( &(msg_buffer), 3, MPI_INT,
                             peer,
                             MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP,
                             comm, &status ));
    if( MPI_SUCCESS != ret ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) recv_notice: "
                             "Error: Parent %3d Cmd %s [%2d/%2d] - failed.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             peer,
                             LOG_QUERY_PHASE_STR(*cmd_msg),
                             *seq_num, *attempt_num ));
        goto cleanup;
    }

    *cmd_msg     = msg_buffer[0];
    *seq_num     = msg_buffer[1];
    *attempt_num = msg_buffer[2];

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) recv_notice: "
                         "Parent %3d Cmd %s [%2d/%2d].",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         peer,
                         LOG_QUERY_PHASE_STR(*cmd_msg),
                         *seq_num, *attempt_num ));
 cleanup:
    return ret;
}

static int log_two_phase_query_process_all_entries(ompi_communicator_t* comm,
                                                   mca_coll_ftbasic_module_t *ftbasic_module,
                                                   mca_coll_ftbasic_agreement_log_entry_t *log_entry)
{
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    log_two_phase_query_entry_t *entry = NULL;
    opal_list_item_t *item = NULL;

    for (item  = opal_list_get_first(agreement_info->query_queue);
         item != opal_list_get_end(agreement_info->query_queue);
         item  = opal_list_get_next(item)) {
        entry = (log_two_phase_query_entry_t*)item;
        if( entry->done ) {
            continue;
        }
        log_two_phase_query_process_entry(comm,
                                          ftbasic_module,
                                          entry->peer,
                                          entry->seq_num,
                                          entry->attempt_num,
                                          NULL);
        entry->done = true;
    }

    return OMPI_SUCCESS;
}

/*
 * process_entry
 * - if( log = 'committed' or 'aborted')
 *   Already finished that round
 *   - Send_oob('decide')
 *   - Send_oob(Commit)
 * - Else if( child in list of children )
 *   Already responded to this child, so skip
 * - Else
 *   Child detected a parent failure that I have not yet
 *   - Send_oob('decide')
 *   - Send_oob(Commit)
 */
static int log_two_phase_query_process_entry(ompi_communicator_t* comm,
                                             mca_coll_ftbasic_module_t *ftbasic_module,
                                             int peer,
                                             int seq_num,
                                             int attempt_num,
                                             mca_coll_ftbasic_agreement_log_entry_t *log_entry)
{
    int ret, exit_status = OMPI_SUCCESS;
    int packet_size;
    int loc_state;
    int cmd_msg;

    if( NULL == log_entry ) {
        log_entry = mca_coll_ftbasic_agreement_log_entry_find(comm, seq_num, false, ftbasic_module);
    }

    loc_state = log_entry->state;

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Query: Process Entry: "
                         "Peer %2d [%2d/%2d] State %s ***********",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer, seq_num, attempt_num,
                         AGREEMENT_STATE_STR(loc_state) ));

    /************************************************
     * If already finished the round in question, just decide
     ************************************************/
    if( loc_state == AGREEMENT_STATE_COMMITTED ||
        loc_state == AGREEMENT_STATE_ABORTED ) {

        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Query: Process Entry: "
                             "Peer %2d Decide %s ***",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer,
                             AGREEMENT_STATE_STR(loc_state) ));

        cmd_msg = (loc_state == AGREEMENT_STATE_COMMITTED ?
                   LOG_TWO_PHASE_QUERY_CMD_DECIDE_COMMIT :
                   LOG_TWO_PHASE_QUERY_CMD_DECIDE_ABORT );

        ret = log_two_phase_query_send_notice(comm, ftbasic_module, peer,
                                              cmd_msg, seq_num, attempt_num);
        if (MPI_SUCCESS != ret) {
            opal_output_verbose(1, ompi_ftmpi_output_handle,
                                "%s ftbasic: agreement) (log2phase) Query: Process Entry: "
                                "Error: Peer Failure %2d - Ignore",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer);
            goto cleanup;
        }

        if( loc_state == AGREEMENT_STATE_COMMITTED ) {
            packet_size = log_entry->commit_bitmap->array_size;
            ret = MCA_PML_CALL(send( (log_entry->commit_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                     peer,
                                     MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP,
                                     MCA_PML_BASE_SEND_SYNCHRONOUS,
                                     comm));
            if (MPI_SUCCESS != ret) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (log2phase) Query: Process Entry: "
                                    "Error: Peer Failure %2d - Ignore",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer);
                goto cleanup;
            }
        }
    }
    /************************************************
     * If child is in our list of children then we know it
     * has already been caught up in the course of the protocol.
     * So just skip it.
     ************************************************/
    else if( log_two_phase_my_child(comm, ftbasic_module, peer) ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Query: Process Entry: "
                             "Peer %2d Already Decided - Skip ***",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer));
        goto cleanup;
    }
    /************************************************
     * If child detected its parent fail, but I have not detected
     * that that child failed yet - because nearly finished with the protocol
     * then respond with the decide vote.
     ************************************************/
    else {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Query: Process Entry: "
                             "Peer %2d Decide %s - Unknown Failure ***",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer,
                             AGREEMENT_STATE_STR(loc_state) ));

        cmd_msg = (loc_state == AGREEMENT_STATE_COMMIT ?
                   LOG_TWO_PHASE_QUERY_CMD_DECIDE_COMMIT :
                   LOG_TWO_PHASE_QUERY_CMD_DECIDE_ABORT );

        ret = log_two_phase_query_send_notice(comm, ftbasic_module, peer,
                                              cmd_msg, seq_num, attempt_num);
        if (MPI_SUCCESS != ret) {
            opal_output_verbose(1, ompi_ftmpi_output_handle,
                                "%s ftbasic: agreement) (log2phase) Query: Process Entry: "
                                "Error: Peer Failure %2d - Ignore",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer);
            goto cleanup;
        }

        if( loc_state == AGREEMENT_STATE_COMMITTED ) {
            packet_size = log_entry->commit_bitmap->array_size;
            ret = MCA_PML_CALL(send( (log_entry->commit_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                     peer,
                                     MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP,
                                     MCA_PML_BASE_SEND_SYNCHRONOUS,
                                     comm));
            if (MPI_SUCCESS != ret) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (log2phase) Query: Process Entry: "
                                    "Error: Peer Failure %2d - Ignore",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer);
                goto cleanup;
            }
        }
    }

 cleanup:
    return exit_status;
}


/*
 * Child will enter this function when it has successfully completed Send(Vote),
 *  and was waiting in Recv(Commit). So we are uncertain if the new parent is 
 *  in the 'voting' or 'committing' phase of the protocol. We are in the
 *  'committing' phase, so if the parent is in the 'voting' phase then
 *  re-Send(Vote) and wait again for the commit message.
 *
 * Child
 *  - Find grandparent
 *  - Send(Query)
 *  - Recv(Phase)
 *  - if( Phase = 'voting' ) {
 *     - Recv(Vote_req)
 *     - Send(Vote)
 *  - Recv(Vote)
 *
 * If grandparent fails during the protocol keep calling this function until
 *  A) grandparent succeeds
 *  B) Root failure is discovered...
 */
static int log_two_phase_term_initiator(ompi_communicator_t* comm,
                                        mca_coll_ftbasic_module_t *ftbasic_module,
                                        opal_bitmap_t *local_bitmap,
                                        mca_coll_ftbasic_agreement_log_entry_t *log_entry)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank, size, i;
    int num_reqs = 0;
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    int packet_size = 0;
    int loc_state = AGREEMENT_STATE_NONE;
    int loc_cmd = LOG_TWO_PHASE_CMD_TERM_PROTOCOL;

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);
    packet_size = local_bitmap->array_size;

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Term: Initiator = %2d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), rank));

    log_entry->state = AGREEMENT_STATE_UNCERTAIN;

    /*
     * If the 'root' is failed then do a linear search for a decided, alive peer
     *
     * JJH TODO:
     * Finish implementing a top level search of the tree instead of
     * the linear search.
     */

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
        /* Skip self */
        if( i == rank ) {
            continue;
        }

        /* Communicate to only suspected alive processes */
        COLL_FTBASIC_CHECK_SKIP_ALL_FAILED_BASE(comm, i);

        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Term: Initiator: "
                             "Asking Rank %2d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i));

        /************************************************
         * Send a Decision Request message
         * Send both the seq_num and attempt_num so the peer can decide if it
         * needs to move to the next round.
         ************************************************/
        ret = MCA_PML_CALL(send( &(loc_cmd), 1, MPI_INT,
                                 i, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                                 MCA_PML_BASE_SEND_STANDARD,
                                 comm));
        if (MPI_SUCCESS != ret) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) Term: Initiator: "
                                 "Error: Peer %2d failed in send(%2d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 i, ret));
            continue;
        }

        ret = MCA_PML_CALL(send( &(log_entry->seq_num), 1, MPI_INT,
                                 i, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                                 MCA_PML_BASE_SEND_STANDARD,
                                 comm));
        if (MPI_SUCCESS != ret) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) Term: Initiator: "
                                 "Error: Peer %2d failed in send(%2d)",
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
                                 "%s ftbasic: agreement) (log2phase) Term: Initiator: "
                                 "Error: Peer %2d failed in send(%2d)",
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
                                 "%s ftbasic: agreement) (log2phase) Term: Initiator: "
                                 "Error: Peer %2d failed in recv(%2d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 i, ret));
            continue;
        }

        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Term: Initiator: "
                             "Rank %2d replied %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i,
                             AGREEMENT_STATE_STR(loc_state) ));

        /*
         * If they decided abort, then do the same.
         */
        if( AGREEMENT_STATE_ABORT   == loc_state ||
            AGREEMENT_STATE_ABORTED == loc_state ) {
            log_entry->state = AGREEMENT_STATE_ABORT;
            /* Done with this function */
            exit_status = OMPI_SUCCESS;
            goto cleanup;
        }
        /*
         * If they committed already, accept their list
         */
        else if( AGREEMENT_STATE_COMMIT    == loc_state ||
                 AGREEMENT_STATE_COMMITTED == loc_state ) {
            ret = MCA_PML_CALL(recv( (local_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                     i, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP,
                                     comm, &(statuses[0]) ));
            if (MPI_SUCCESS != ret) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                     "%s ftbasic: agreement) (log2phase) Term: Initiator: "
                                     "Error: Peer %2d failed in recv(%2d)",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     i, ret));
                continue;
            }

            /* Done with this function */
            log_entry->state = AGREEMENT_STATE_COMMIT;
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
                             "%s ftbasic: agreement) (log2phase) Term: Initiator: "
                             "Unknown state - Give up",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
        exit_status = OMPI_EXISTS;
    }

 cleanup:
    mca_coll_ftbasic_free_reqs(reqs, num_reqs);

    return exit_status;
}

static int log_two_phase_term_responder(ompi_communicator_t* comm,
                                        mca_coll_ftbasic_module_t *ftbasic_module,
                                        int peer)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank;
    int num_reqs = 0;
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    int packet_size = 0;
    int loc_state, loc_seq_num, loc_attempt_num;
    mca_coll_ftbasic_agreement_log_entry_t *log_entry = NULL;

    rank = ompi_comm_rank(comm);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Term: "
                         "Responder = %2d Reply to %2d ***********************************",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), rank, peer));

    /************************************************
     * Finish receiving the message
     * - ignore peer failure
     ************************************************/
    ret = MCA_PML_CALL(recv( &loc_seq_num, 1, MPI_INT,
                             peer, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                             comm, &(statuses[0]) ));
    if (MPI_SUCCESS != ret) {
        opal_output_verbose(1, ompi_ftmpi_output_handle,
                            "%s ftbasic: agreement) (log2phase) Term: Responder: "
                            "Error: Peer Failure %2d - Ignore",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer);
        goto cleanup;
    }

    ret = MCA_PML_CALL(recv( &loc_attempt_num, 1, MPI_INT,
                             peer, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                             comm, &(statuses[0]) ));
    if (MPI_SUCCESS != ret) {
        opal_output_verbose(1, ompi_ftmpi_output_handle,
                            "%s ftbasic: agreement) (log2phase) Term: Responder: "
                            "Error: Peer Failure %2d - Ignore",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer);
        goto cleanup;
    }

    /* Find this log entry */
    loc_state = AGREEMENT_STATE_NONE;
    log_entry = mca_coll_ftbasic_agreement_log_entry_find(comm, loc_seq_num, false, ftbasic_module);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Term: Responder: "
                         "Log for [%2d/%2d] state = %s ***",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         log_entry->seq_num, log_entry->attempt_num,
                         AGREEMENT_STATE_STR(log_entry->state) ));

    /*
     * If we have not locally decided for this attempt of the transaction
     * then we need to become uncertian, unless we already decided.
     */
    if( log_entry->attempt_num <= loc_attempt_num ) {
        if( AGREEMENT_STATE_ABORT     != log_entry->state &&
            AGREEMENT_STATE_ABORTED   != log_entry->state &&
            AGREEMENT_STATE_COMMIT    != log_entry->state &&
            AGREEMENT_STATE_COMMITTED != log_entry->state ) {
            log_entry->state = AGREEMENT_STATE_UNCERTAIN;
        }
    }

    if( log_entry->attempt_num == loc_attempt_num ) {
        loc_state = log_entry->state;
    } else {
        loc_state = AGREEMENT_STATE_ABORTED;
    }

    /************************************************
     * Determine if we have decided or not
     * - ignore peer failure
     ************************************************/
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Term: Responder: "
                         "Reply to Rank %2d State %s [R: %2d/%2d vs L: %2d/%2d]",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer,
                         AGREEMENT_STATE_STR(loc_state),
                         loc_seq_num, loc_attempt_num,
                         log_entry->seq_num, log_entry->attempt_num
                         ));

    ret = MCA_PML_CALL(send( &loc_state, 1, MPI_INT,
                             peer, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP,
                             MCA_PML_BASE_SEND_STANDARD,
                             comm));
    if (MPI_SUCCESS != ret) {
        opal_output_verbose(1, ompi_ftmpi_output_handle,
                            "%s ftbasic: agreement) (log2phase) Term: Responder: "
                            "Error: Peer Failure %2d - Ignore",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer);
        goto cleanup;
    }

    if( AGREEMENT_STATE_COMMIT    != loc_state &&
        AGREEMENT_STATE_COMMITTED != loc_state ) {
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
                            "%s ftbasic: agreement) (log2phase) Term: Responder: "
                            "Error: Peer Failure %2d - Ignore",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), peer);
        goto cleanup;
    }

 cleanup:
    mca_coll_ftbasic_free_reqs(reqs, num_reqs);

    return exit_status;
}

/************************* Support: Progress Classes *****************************/
static int log_two_phase_test_all(ompi_communicator_t* comm,
                                  mca_coll_ftbasic_module_t *ftbasic_module,
                                  int num_reqs,
                                  bool *is_finished,
                                  int *num_failed,
                                  bool return_error)
{
    int ret, exit_status = OMPI_SUCCESS;
    int i;
    ompi_request_t **reqs = ftbasic_module->mccb_reqs;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    int done;

    *num_failed = 0;

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
                                        "%s ftbasic: agreement) (log2phase) test_all: "
                                        "Error: Wait failed for peer %2d",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        statuses[i].MPI_SOURCE);
                    *num_failed += 1;
                    if( return_error ) {
                        exit_status = statuses[i].MPI_ERROR;
                    }
                }
            }
            else if( MPI_SUCCESS != statuses[i].MPI_ERROR ) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (log2phase) test_all: "
                                    "Error: Wait failed for peer %2d",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    statuses[i].MPI_SOURCE);
                *num_failed += 1;
                if( return_error ) {
                    exit_status = statuses[i].MPI_ERROR;
                }
            }
        }

        mca_coll_ftbasic_free_reqs(reqs, num_reqs);
    }

    return exit_status;
}

/************************* Support: Normal Progress Classes *****************************/
static int mca_coll_ftbasic_agreement_log_two_phase_progress_comm(ompi_communicator_t *comm,
                                                                 mca_coll_ftbasic_module_t *ftbasic_module)
{
    int ret, exit_status = OMPI_SUCCESS, test_exit_status = OMPI_SUCCESS;
    mca_coll_ftbasic_request_t *current_collreq = ftbasic_module->agreement_info->cur_request;
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
    int rank, size;
    bool is_finished;
    mca_coll_ftbasic_agreement_log_entry_t *log_entry = NULL;
    int num_processed = 0;
    int num_failed = 0;

    /* Sanity check:
     * - If there is no active request, then nothing to do.
     */
    if( NULL == current_collreq ) {
        return num_processed;
    }

    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic:iagreement) (log2phase) Progress: Progressing...",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);
    log_entry = (mca_coll_ftbasic_agreement_log_entry_t*)current_collreq->log_entry;

    /* Sanity check */
    if( LOG_TWO_PHASE_NB_STAGE_DONE == current_collreq->stage ) {
        goto done;
    }

    /* Initial state, go to start state */
    if( LOG_TWO_PHASE_NB_STAGE_NULL == current_collreq->stage ) {
        test_exit_status = OMPI_SUCCESS;
        num_failed = 0;
        goto initial_state;
    }

    /*
     * Test for completion of the previous phase
     */
    num_processed++;
    is_finished = false;
    test_exit_status = log_two_phase_test_all(comm,
                                              ftbasic_module,
                                              current_collreq->num_reqs,
                                              &is_finished,
                                              &num_failed,
                                              true);
    if( OMPI_SUCCESS != test_exit_status ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Failed (%2d) - log_two_phase_test_all()",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status));
        /* Continue and decide later */
    }

    /* If previous phase is not yet finished, then nothing to do */
    if( !is_finished && OMPI_SUCCESS == test_exit_status ) {
        OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Progress: ***** Waiting",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
        exit_status = OMPI_SUCCESS;
        goto cleanup;
    }
    /*
     * Only reset the number of request if success.
     * On error the individual progress functions will reset the value
     * as needed.
     */
    if( OMPI_SUCCESS == test_exit_status ) {
        current_collreq->num_reqs = 0;
    }

    /*
     * Determine the current state
     */
 initial_state:
    /********************************
     * Coordinator (Root)
     ********************************/
    if( rank == current_collreq->coordinator ) {
        if( LOG_TWO_PHASE_NB_STAGE_NULL == current_collreq->stage ) {
            /* Start at the end of Bcast_Req */
            ftbasic_module->agreement_info->cur_request->stage = LOG_TWO_PHASE_NB_STAGE_COORD_WAIT_BCAST_REQ;
        }
        ret = log_two_phase_coordinator_progress(comm, ftbasic_module, test_exit_status, num_failed);
    }
    /********************************
     * Participant (Parent)
     ********************************/
    else if(0 < agreement_tree->num_children ) {
        if( LOG_TWO_PHASE_NB_STAGE_NULL == current_collreq->stage ) {
            /* Start at the end of Bcast_Req */
            ftbasic_module->agreement_info->cur_request->stage = LOG_TWO_PHASE_NB_STAGE_COORD_PARENT_WAIT_BCAST_REQ;
        }
        ret = log_two_phase_participant_parent_progress(comm, ftbasic_module, test_exit_status, num_failed);
    }
    /********************************
     * Participant (Child)
     ********************************/
    else {
        if( LOG_TWO_PHASE_NB_STAGE_NULL == current_collreq->stage ) {
            /* Start at the end of Recv_Req */
            ftbasic_module->agreement_info->cur_request->stage = LOG_TWO_PHASE_NB_STAGE_PART_CHILD_WAIT_RECV_REQ;
        }
        ret = log_two_phase_participant_child_progress(comm, ftbasic_module, test_exit_status);
    }

 done:
    num_processed++;
    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic:iagreement) (log2phase) Progress: Check Done: (%s) (%2d)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (LOG_TWO_PHASE_NB_STAGE_DONE == current_collreq->stage ? "Done" : "Not Done"),
                         log_entry->state ));
    /********************************
     * Check to see if we are all done
     ********************************/
    if( LOG_TWO_PHASE_NB_STAGE_DONE == current_collreq->stage ) {
        /*
         * If we decided 'abort' then try again
         */
        if( log_entry->state == AGREEMENT_STATE_ABORT || 
            log_entry->state == AGREEMENT_STATE_ABORTED ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (log2phase) Progress: ***** Abort - Retry",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
            log_two_phase_inside_progress = false;

            /* New round:
             * - remove the 'abort' bit in the local_bitmap
             * - Rebalance the tree (coordinator must have failed)
             */
            opal_bitmap_clear_bit(current_collreq->local_bitmap, ompi_comm_size(comm) );

            opal_bitmap_set_bit(ftbasic_module->agreement_info->fail_bitmap,
                                agreement_tree->root);
            mca_coll_ftbasic_agreement_log_two_phase_refresh_tree(ftbasic_module->agreement_info->fail_bitmap,
                                                                       comm,
                                                                       ftbasic_module);
            /* Try again */
            internal_iagreement_log_two_phase(comm,
                                              &current_collreq->group,
                                              current_collreq->flag,
                                              current_collreq->local_bitmap,
                                              ftbasic_module,
                                              current_collreq);
            log_two_phase_inside_progress = true;
            goto cleanup;
        }
        /*
         * Otherwise we decided commit, so finish the request
         */
        else {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (log2phase) Progress: ***** Done",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

            ftbasic_module->agreement_info->cur_request->mpi_error = OMPI_SUCCESS;
            mca_coll_ftbasic_agreement_base_finish_nonblocking(comm,
                                                               /* &tmp_newfailures, true, JJH DOUBLE CHECK*/
                                                               &current_collreq->group,
                                                               current_collreq->flag,
                                                               current_collreq->local_bitmap,
                                                               ftbasic_module);
        }
    }

 cleanup:
    return num_processed;
}

static int log_two_phase_coordinator_progress(ompi_communicator_t* comm,
                                              mca_coll_ftbasic_module_t *ftbasic_module,
                                              int test_exit_status,
                                              int num_failed)
{
    int ret, exit_status = OMPI_SUCCESS;
    mca_coll_ftbasic_request_t *current_collreq = ftbasic_module->agreement_info->cur_request;
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    mca_coll_ftbasic_agreement_log_entry_t *log_entry = NULL;
    int rank, size;
    int i, j;
    int *failed_ranks = NULL;

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);
    log_entry = (mca_coll_ftbasic_agreement_log_entry_t*)current_collreq->log_entry;

    /*
     * If there was an error:
     * - Create a list of failed processes
     * - reset 'num_reqs'
     */
    if( OMPI_SUCCESS != test_exit_status ) {
        /* Sanity check: If this value is not set, then calculate it */
        if( num_failed <= 0 ) {
            num_failed = 0;
            for( i = 0, j = 0; i < current_collreq->num_reqs; ++i ) {
                if( MPI_SUCCESS != statuses[i].MPI_ERROR ) {
                    ++num_failed;
                }
            }
        }

        /* Create a list of failed processes */
        failed_ranks = (int*)malloc(sizeof(int) * num_failed);
        for( i = 0, j = 0; i < current_collreq->num_reqs; ++i ) {
            if( MPI_SUCCESS != statuses[i].MPI_ERROR ) {
                opal_output_verbose(5, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (log2phase) Coordinator: "
                                    "Failed peer %2d",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    statuses[i].MPI_SOURCE);
                failed_ranks[j] = statuses[i].MPI_SOURCE;
                ++j;
            }
        }

        /* Reset the 'num_reqs' */
        current_collreq->num_reqs = 0;
    }

    /********************************
     * Coordinator: Request Broadcast Finished -> Start Gather
     * Note: This state transition is only used for initialization.
     *       It is safe to skip the error case.
     ********************************/
    if( LOG_TWO_PHASE_NB_STAGE_COORD_WAIT_BCAST_REQ == current_collreq->stage ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Coordinator: "
                             "[%2d] START -> Bcast Req. -> Gather",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

        /********************************
         * If test failed:
         * JJH TODO:
         * Although this state is skipped, we should finish writing it for
         * the sake of completeness.
         ********************************/
        if( OMPI_SUCCESS != test_exit_status ) {
            opal_output_verbose(0, ompi_ftmpi_output_handle,
                                "%s ftbasic:iagreement) (log2phase) Coordinator: "
                                "[%2d] Error: Not Implemented (Should never happen)",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status );
        }

        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
        log_entry->state   = AGREEMENT_STATE_VOTE_REQ;

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_COORD_BEFORE_GATHER_VOTE
        /* TEST:
         */
        if( rank == LOG_TWO_PHASE_TEST_RANK_COORD ) {
            sleep(2);
            kill(getpid(), SIGKILL);
        }
#endif /* LOG_TWO_PHASE_TEST_COORD_BEFORE_GATHER_VOTE */

        /************************************************
         * Receive Lists from children processes (gather)
         ************************************************/
        AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER);

        ret = log_two_phase_protocol_gather_from_children(comm,
                                                          ftbasic_module,
                                                          current_collreq->local_bitmap,
                                                          &(current_collreq->num_reqs),
                                                          rank, 0, log_entry);

        current_collreq->stage = LOG_TWO_PHASE_NB_STAGE_COORD_WAIT_GATHER;

        if( OMPI_SUCCESS != ret ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (log2phase) Coordinator: "
                                 "Failed (%2d) - Receive Lists.",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret ));
            exit_status = log_two_phase_coordinator_progress(comm, ftbasic_module, ret, -1);
            goto cleanup;
        }

    }
    /********************************
     * Coordinator: Gather finished -> Decide, and Bcast Decision
     ********************************/
    else if( LOG_TWO_PHASE_NB_STAGE_COORD_WAIT_GATHER == current_collreq->stage ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Coordinator: "
                             "[%2d] Gather -> Bcast",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

        /********************************
         * If test failed...
         * Some child failed, gather from grandchildren
         ********************************/
        if( OMPI_SUCCESS != test_exit_status ) {
            for( i = 0; i < num_failed; ++i ) {
                opal_output_verbose(5, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (log2phase) Coordinator: "
                                    "Proc. %3d Failed! Gather from Children...",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    failed_ranks[i] );

                /* Do this operation blocking
                 * JJH TODO:
                 * Improve this to be nonblocking to reduce blocking in progress.
                 */
                ret = log_two_phase_protocol_gather_from_children(comm,
                                                                  ftbasic_module,
                                                                  current_collreq->local_bitmap,
                                                                  NULL,
                                                                  failed_ranks[i],
                                                                  agreement_tree->bkmrk_idx,
                                                                  log_entry);
                /* Remove the dead process from our tree */
                log_two_phase_remove_child(comm, ftbasic_module, failed_ranks[i]);
            }
            /* Since the gather above is blocking (for now) it is safe to just
             * continue from here instead of going through another progress
             * loop.
             */
        }

        /***************************** Decision Phase *****************************/
        AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER);
        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE);

        log_entry->state   = AGREEMENT_STATE_VOTED;


        /************************************************
         * Create the final list (commit list)
         ************************************************/
        ret = log_two_phase_protocol_accumulate_list(comm,
                                                     ftbasic_module,
                                                     current_collreq->local_bitmap);

        /* Add it to the local log */
        log_entry->state = AGREEMENT_STATE_COMMIT;
        opal_bitmap_copy(log_entry->commit_bitmap, current_collreq->local_bitmap);

#if DEBUG_DECIDE_SLEEP == 1
        sleep(2);
#endif
        AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_DECIDE);
        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST);

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_COORD_BEFORE_BCAST_COMMIT
        /* TEST:
         */
        if( rank == LOG_TWO_PHASE_TEST_RANK_COORD ) {
            sleep(2);
            kill(getpid(), SIGKILL);
        }
#endif /* LOG_TWO_PHASE_TEST_COORD_BEFORE_BCAST_COMMIT */

        /************************************************
         * Send the commit list to all peers (broadcast)
         ************************************************/
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Coordinator: "
                             "Bcast final list.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
        ret = log_two_phase_protocol_bcast_to_children(comm,
                                                       ftbasic_module,
                                                       current_collreq->local_bitmap,
                                                       &(current_collreq->num_reqs),
                                                       rank, 0, log_entry);

        current_collreq->stage = LOG_TWO_PHASE_NB_STAGE_COORD_WAIT_BCAST;

        if( OMPI_SUCCESS != ret ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (log2phase) Coordinator: "
                                 "Failed (%2d) - Bcast final list.",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret ));
            exit_status = log_two_phase_coordinator_progress(comm, ftbasic_module, ret, -1);
            goto cleanup;
        }
    }
    /********************************
     * Coordinator: Bcast Finished -> Commit/DONE
     ********************************/
    else if( LOG_TWO_PHASE_NB_STAGE_COORD_WAIT_BCAST == current_collreq->stage ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Coordinator: "
                             "[%2d] Bcast -> DONE",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

        /********************************
         * If test failed...
         * Some child failed, bcast to grandchildren
         ********************************/
        if( OMPI_SUCCESS != test_exit_status ) {
            for( i = 0; i < num_failed; ++i ) {
                opal_output_verbose(5, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (log2phase) Coordinator: "
                                    "Proc. %3d Failed! Bcast to Children...",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    failed_ranks[i] );

                /* Do this operation blocking
                 * JJH TODO:
                 * Improve this to be nonblocking to reduce blocking in progress.
                 */
                ret = log_two_phase_protocol_bcast_to_children(comm,
                                                               ftbasic_module,
                                                               current_collreq->local_bitmap,
                                                               NULL,
                                                               failed_ranks[i],
                                                               agreement_tree->bkmrk_idx,
                                                               log_entry);
                /* Remove the dead process from our tree */
                log_two_phase_remove_child(comm, ftbasic_module, failed_ranks[i]);
            }
            /* Since the bcast above is blocking (for now) it is safe to just
             * continue from here instead of going through another progress
             * loop.
             */
        }

        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Coordinator: "
                             "Finished.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

        /* log_entry->suspend_query  = true; */
        ret = log_two_phase_query_process_all_entries(comm, ftbasic_module, log_entry);
        /* log_entry->suspend_query  = false; */
        if( OMPI_SUCCESS != ret ) {
            /* JJH TODO: process_queue error state (should never happen) */
        }

        log_entry->state = AGREEMENT_STATE_COMMITTED;
        log_two_phase_query_purge_queue(comm, ftbasic_module);

        AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST);

        current_collreq->stage = LOG_TWO_PHASE_NB_STAGE_DONE;
        current_collreq->mpi_error = OMPI_SUCCESS;
    }

 cleanup:
    if( NULL != failed_ranks ) {
        free(failed_ranks);
        failed_ranks = NULL;
    }

    return exit_status;
}

static int log_two_phase_participant_parent_progress(ompi_communicator_t* comm,
                                                     mca_coll_ftbasic_module_t *ftbasic_module,
                                                     int test_exit_status,
                                                     int num_failed)
{
    int ret, exit_status = OMPI_SUCCESS;
    mca_coll_ftbasic_request_t *current_collreq = ftbasic_module->agreement_info->cur_request;
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
    ompi_status_public_t *statuses = ftbasic_module->mccb_statuses;
    mca_coll_ftbasic_agreement_log_entry_t *log_entry = NULL;
    int rank, size;
    int i, j;
    int *failed_ranks = NULL;
#if DEBUG_WITH_STR == 1
    char *tmp_bitstr = NULL;
#endif

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);
    log_entry = (mca_coll_ftbasic_agreement_log_entry_t*)current_collreq->log_entry;

    /*
     * If there was an error:
     * - Create a list of failed processes
     * - reset 'num_reqs'
     */
    if( OMPI_SUCCESS != test_exit_status ) {
        /* Sanity check: If this value is not set, then calculate it */
        if( num_failed <= 0 ) {
            num_failed = 0;
            for( i = 0, j = 0; i < current_collreq->num_reqs; ++i ) {
                if( MPI_SUCCESS != statuses[i].MPI_ERROR ) {
                    ++num_failed;
                }
            }
        }

        /*
         * It is possible that even here 'num_failed' is 0.
         * This is when the parent of this process failes (when acting as a child)
         * So we know it was the parent that failed, and it is handled in the
         * error routine for the send/recv functionality below.
         */
        if( num_failed != 0 ) {
            /* Create a list of failed processes */
            failed_ranks = (int*)malloc(sizeof(int) * num_failed);
            for( i = 0, j = 0; i < current_collreq->num_reqs; ++i ) {
                if( MPI_SUCCESS != statuses[i].MPI_ERROR ) {
                    opal_output_verbose(5, ompi_ftmpi_output_handle,
                                        "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                                        "Failed peer %2d",
                                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                        statuses[i].MPI_SOURCE);
                    failed_ranks[j] = statuses[i].MPI_SOURCE;
                    ++j;
                }
            }
        }

        /* Reset the 'num_reqs' */
        current_collreq->num_reqs = 0;
    }

    /********************************
     * (Parent) Recv Req. Finished -> Bcast down tree
     * Note: This state transition is skipped!
     *
     * JJH TODO:
     * Although this state is skipped, we should finish writing it for
     * the sake of completeness.
     ********************************/
    if( LOG_TWO_PHASE_NB_STAGE_PART_PARENT_WAIT_RECV_REQ == current_collreq->stage ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                             "[%2d] Recv Req -> Bcast Req.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

        /********************************
         * If test failed...
         * JJH TODO:
         * Although this state is skipped, we should finish writing it for
         * the sake of completeness.
         ********************************/
        if( OMPI_SUCCESS != test_exit_status ) {
            opal_output_verbose(0, ompi_ftmpi_output_handle,
                                "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                                "[%2d] Error: Not Implemented (Should never happen)",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status );
        }

        /* ----- Skipped ------ */

        current_collreq->stage = LOG_TWO_PHASE_NB_STAGE_COORD_PARENT_WAIT_BCAST_REQ;
    }
    /********************************
     * (Parent) Bcast Req. Finished -> Gather 
     * Note: This state transition is only used for initialization.
     *       It is safe to skip the error case.
     ********************************/
    else if( LOG_TWO_PHASE_NB_STAGE_COORD_PARENT_WAIT_BCAST_REQ == current_collreq->stage ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                             "[%2d] Bcast Req. -> Gather",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

        /********************************
         * If test failed...
         * JJH TODO:
         * Although this state is skipped, we should finish writing it for
         * the sake of completeness.
         ********************************/
        if( OMPI_SUCCESS != test_exit_status ) {
            opal_output_verbose(0, ompi_ftmpi_output_handle,
                                "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                                "[%2d] Error: Not Implemented (Should never happen)",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status );
        }

        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
        log_entry->state   = AGREEMENT_STATE_VOTE_REQ;

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_P_BEFORE_GATHER_VOTE
        /* TEST:
         */
        if( rank == LOG_TWO_PHASE_TEST_RANK_PARENT ) {
            sleep(2);
            kill(getpid(), SIGKILL);
        }
#endif /* LOG_TWO_PHASE_TEST_PART_P_BEFORE_GATHER_VOTE */

        /************************************************
         * Receive Lists from children processes (gather)
         ************************************************/
        AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_GATHER);

        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                             "Receive Lists.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

        ret = log_two_phase_protocol_gather_from_children(comm,
                                                          ftbasic_module,
                                                          current_collreq->local_bitmap,
                                                          &(current_collreq->num_reqs),
                                                          rank, 0, log_entry);

        current_collreq->stage = LOG_TWO_PHASE_NB_STAGE_COORD_PARENT_WAIT_GATHER;

        if( OMPI_SUCCESS != ret ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                                 "Failed (%2d) - Receive Lists.",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret ));
            exit_status = log_two_phase_participant_parent_progress(comm, ftbasic_module, ret, -1);
            goto cleanup;
        }
    }
    /********************************
     * (Parent) Gather Finished -> Accumulate and send up tree
     ********************************/
    else if( LOG_TWO_PHASE_NB_STAGE_COORD_PARENT_WAIT_GATHER == current_collreq->stage ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Parent) [%2d] "
                             "Gather -> Send up",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

        /********************************
         * If test failed...
         * Some child failed, gather from grandchildren
         ********************************/
        if( OMPI_SUCCESS != test_exit_status ) {
            for( i = 0; i < num_failed; ++i ) {
                opal_output_verbose(5, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                                    "Proc. %3d Failed! Gather from Children...",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    failed_ranks[i] );

                /* Do this operation blocking
                 * JJH TODO:
                 * Improve this to be nonblocking to reduce blocking in progress.
                 */
                ret = log_two_phase_protocol_gather_from_children(comm,
                                                                  ftbasic_module,
                                                                  current_collreq->local_bitmap,
                                                                  NULL,
                                                                  failed_ranks[i],
                                                                  agreement_tree->bkmrk_idx,
                                                                  log_entry);
                /* Remove the dead process from our tree */
                log_two_phase_remove_child(comm, ftbasic_module, failed_ranks[i]);
            }
            /* Since the gather above is blocking (for now) it is safe to just
             * continue from here instead of going through another progress
             * loop.
             */
        }

        /************************************************
         * Create the pre-final list (commit list)
         ************************************************/
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                             "Accumulate Lists.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

        ret = log_two_phase_protocol_accumulate_list(comm,
                                                     ftbasic_module,
                                                     current_collreq->local_bitmap);

#if DEBUG_DECIDE_SLEEP == 1
        sleep(2);
#endif

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_P_BEFORE_SEND_VOTE
        /* TEST:
         */
        if( rank == LOG_TWO_PHASE_TEST_RANK_PARENT ) {
            sleep(2);
            kill(getpid(), SIGKILL);
        }
#endif /* LOG_TWO_PHASE_TEST_PART_P_BEFORE_SEND_VOTE */

        /************************************************
         * Send list of failed processes to parent
         ************************************************/
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                             "Send list",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

        ret = log_two_phase_protocol_send_to_parent(comm,
                                                    ftbasic_module,
                                                    current_collreq->local_bitmap,
                                                    &(current_collreq->num_reqs),
                                                    rank);

        current_collreq->stage = LOG_TWO_PHASE_NB_STAGE_PART_PARENT_WAIT_SEND;

        if( OMPI_SUCCESS != ret ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                                 "Failed (%2d) - Send list",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret ));
            exit_status = log_two_phase_participant_parent_progress(comm, ftbasic_module, ret, -1);
            goto cleanup;
        }
    }
    /********************************
     * (Parent) Send Finished -> Recv Decision
     ********************************/
    else if( LOG_TWO_PHASE_NB_STAGE_PART_PARENT_WAIT_SEND == current_collreq->stage ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                             "[%2d] Send up -> Recv",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

        /********************************
         * If test failed...
         * Parent failed, seek out grandparent or new coordinator
         ********************************/
        if( OMPI_SUCCESS != test_exit_status ) {
            ret = log_two_phase_protocol_query_parent(comm,
                                                      ftbasic_module,
                                                      current_collreq->local_bitmap,
                                                      log_entry, false);
            if( OMPI_SUCCESS != ret ) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                                    "Error: Root failure. Decide Abort***",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                log_entry->state = AGREEMENT_STATE_ABORT;
                goto parent_decide_abort;
            }

            goto parent_decide;
        }

        log_entry->state = AGREEMENT_STATE_VOTED;

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_P_BEFORE_RECV_COMMIT
        /* TEST:
         */
        if( rank == LOG_TWO_PHASE_TEST_RANK_PARENT ) {
            sleep(2);
            kill(getpid(), SIGKILL);
        }
#endif /* LOG_TWO_PHASE_TEST_PART_P_BEFORE_RECV_COMMIT */

        /************************************************
         * Receive the final list
         ************************************************/
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                             "Wait for final list",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

        ret = log_two_phase_protocol_recv_from_parent(comm,
                                                      ftbasic_module,
                                                      current_collreq->local_bitmap,
                                                      &(current_collreq->num_reqs),
                                                      rank);

        current_collreq->stage = LOG_TWO_PHASE_NB_STAGE_PART_PARENT_WAIT_RECV;

        if( OMPI_SUCCESS != ret ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                                 "Failed (%2d) - Recv list",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret ));
            exit_status = log_two_phase_participant_parent_progress(comm, ftbasic_module, ret, -1);
            goto cleanup;
        }
    }
    /********************************
     * (Parent) Recv Finished -> Bcast down the tree
     ********************************/
    else if( LOG_TWO_PHASE_NB_STAGE_PART_PARENT_WAIT_RECV == current_collreq->stage ) {
    parent_done:
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                             "[%2d] Recv -> Bcast",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

        /********************************
         * If test failed...
         * Parent failed, seek out grandparent or new coordinator
         ********************************/
        if( OMPI_SUCCESS != test_exit_status ) {
            ret = log_two_phase_protocol_query_parent(comm,
                                                      ftbasic_module,
                                                      current_collreq->local_bitmap,
                                                      log_entry, true);
            if( OMPI_SUCCESS != ret ) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                                    "Error: Root failure. Decide Abort***",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                log_entry->state = AGREEMENT_STATE_ABORT;
                goto parent_decide_abort;
            }

            goto parent_decide;
        }

        if( opal_bitmap_is_set_bit(current_collreq->local_bitmap, ompi_comm_size(comm) ) ) {
            /* JJH TODO: Verify this logic */
            log_entry->state = AGREEMENT_STATE_ABORT;
            goto parent_decide_abort;
        }

        if( AGREEMENT_STATE_UNCERTAIN == log_entry->state ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                                 "Uncertain... Query peers",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
            goto parent_decide;
        }

        if( AGREEMENT_STATE_ABORT == log_entry->state ) {
            goto parent_decide_abort;
        }

        /***************************** Commit Phase  ******************************/

        /************************************************
         * Commit this list
         ************************************************/
#if DEBUG_WITH_STR == 1
        tmp_bitstr = opal_bitmap_get_string(current_collreq->local_bitmap);
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                             "Commit the list [%s]",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), tmp_bitstr));
        if( NULL != tmp_bitstr ) {
            free(tmp_bitstr);
            tmp_bitstr = NULL;
        }
#endif

        log_entry->state = AGREEMENT_STATE_COMMIT;
        opal_bitmap_copy(log_entry->commit_bitmap, current_collreq->local_bitmap);

#if DEBUG_DECIDE_SLEEP == 1
        sleep(2);
#endif

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_C_AFTER_VOTE
        /* TEST:
         */
        if( rank == LOG_TWO_PHASE_TEST_RANK_PARENT ) {
            int i;
            for( i = 0; i < 30; ++i ) {
                usleep(100000);
                opal_progress();
            }
        }
#endif /* LOG_TWO_PHASE_TEST_PART_C_AFTER_VOTE */

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_P_BEFORE_BCAST_COMMIT
        /* TEST:
         */
        if( rank == LOG_TWO_PHASE_TEST_RANK_PARENT ) {
            sleep(2);
            kill(getpid(), SIGKILL);
        }
#endif /* LOG_TWO_PHASE_TEST_PART_P_BEFORE_BCAST_COMMIT */

        /************************************************
         * Send final list to all children
         ************************************************/
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                             "Send final list to children",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

        ret = log_two_phase_protocol_bcast_to_children(comm,
                                                       ftbasic_module,
                                                       current_collreq->local_bitmap,
                                                       &(current_collreq->num_reqs),
                                                       rank, 0, log_entry);

        current_collreq->stage = LOG_TWO_PHASE_NB_STAGE_COORD_PARENT_WAIT_BCAST;

        if( OMPI_SUCCESS != ret ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                                 "Failed (%2d) - Bcast decision",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret ));
            exit_status = log_two_phase_participant_parent_progress(comm, ftbasic_module, ret, -1);
            goto cleanup;
        }
    }
    /********************************
     * (Parent) Bcst Finished -> Done
     ********************************/
    else if( LOG_TWO_PHASE_NB_STAGE_COORD_PARENT_WAIT_BCAST == current_collreq->stage ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Parent) "
                             "[%2d] Bcast -> DONE",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

        /********************************
         * If test failed...
         * Some child failed, bcast to grandchildren
         ********************************/
        if( OMPI_SUCCESS != test_exit_status ) {
            for( i = 0; i < num_failed; ++i ) {
                opal_output_verbose(5, ompi_ftmpi_output_handle,
                                    "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                                    "Proc. %3d Failed! Gather from Children...",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                    failed_ranks[i] );

                /* Do this operation blocking
                 * JJH TODO:
                 * Improve this to be nonblocking to reduce blocking in progress.
                 */
                ret = log_two_phase_protocol_bcast_to_children(comm,
                                                               ftbasic_module,
                                                               current_collreq->local_bitmap,
                                                               NULL,
                                                               failed_ranks[i],
                                                               agreement_tree->bkmrk_idx,
                                                               log_entry);
                /* Remove the dead process from our tree */
                log_two_phase_remove_child(comm, ftbasic_module, failed_ranks[i]);
            }
            /* Since the bcast above is blocking (for now) it is safe to just
             * continue from here instead of going through another progress
             * loop.
             */
        }

        /* log_entry->suspend_query  = true; */
        ret = log_two_phase_query_process_all_entries(comm, ftbasic_module, log_entry);
        /* log_entry->suspend_query  = false; */
        if( OMPI_SUCCESS != ret ) {
            /* JJH TODO: process_queue error state (should never happen) */
        }

        log_entry->state = AGREEMENT_STATE_COMMITTED;
        log_two_phase_query_purge_queue(comm, ftbasic_module);

        current_collreq->stage = LOG_TWO_PHASE_NB_STAGE_DONE;
        current_collreq->mpi_error = OMPI_SUCCESS;
    }

    goto cleanup;

 parent_decide:
    if( AGREEMENT_STATE_UNCERTAIN == log_entry->state ) {
        if( OMPI_SUCCESS != ( ret = log_two_phase_term_initiator(comm,
                                                                 ftbasic_module,
                                                                 current_collreq->local_bitmap,
                                                                 log_entry)) ) {
            opal_output_verbose(1, ompi_ftmpi_output_handle,
                                "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                                "Error: Term. Protocol failure - Abort***",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            log_entry->state = AGREEMENT_STATE_ABORT;
            goto parent_decide_abort;
        }

        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                             "Decided with a peer. (%s)",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ( AGREEMENT_STATE_ABORT   == log_entry->state || 
                               AGREEMENT_STATE_ABORTED == log_entry->state ? "Abort" : "Commit") ));
    }

 parent_decide_abort:
    if( AGREEMENT_STATE_ABORT == log_entry->state ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Participant: (Parent) "
                             "Decide Abort.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

        /*
         * Broadcast decision to all children
         */
        ret = log_two_phase_protocol_bcast_to_children(comm,
                                                       ftbasic_module,
                                                       current_collreq->local_bitmap,
                                                       NULL, rank, 0, log_entry);

        /* log_entry->suspend_query  = true; */
        ret = log_two_phase_query_process_all_entries(comm, ftbasic_module, log_entry);
        /* log_entry->suspend_query  = false; */
        if( OMPI_SUCCESS != ret ) {
            /* JJH TODO: process_queue error state (should never happen) */
        }

        log_entry->state = AGREEMENT_STATE_ABORTED;
        log_two_phase_query_purge_queue(comm, ftbasic_module);

        current_collreq->stage = LOG_TWO_PHASE_NB_STAGE_DONE;
        current_collreq->mpi_error  = exit_status;
        current_collreq->mpi_source = agreement_tree->parent;

        exit_status = OMPI_EXISTS;
        goto cleanup;
    }

    /*
     * Fixed it, now decide with the new parent
     */
    test_exit_status = OMPI_SUCCESS;
    goto parent_done;


 cleanup:
    if( NULL != failed_ranks ) {
        free(failed_ranks);
        failed_ranks = NULL;
    }

#if DEBUG_WITH_STR == 1
    if( NULL != tmp_bitstr ) {
        free(tmp_bitstr);
        tmp_bitstr = NULL;
    }
#endif

    return exit_status;
}

static int log_two_phase_participant_child_progress(ompi_communicator_t* comm,
                                                    mca_coll_ftbasic_module_t *ftbasic_module,
                                                    int test_exit_status)
{
    int ret, exit_status = OMPI_SUCCESS;
    mca_coll_ftbasic_request_t *current_collreq = ftbasic_module->agreement_info->cur_request;
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
    mca_coll_ftbasic_agreement_log_entry_t *log_entry = NULL;
    int rank, size;
#if DEBUG_WITH_STR == 1
    char *tmp_bitstr = NULL;
#endif

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);
    log_entry = (mca_coll_ftbasic_agreement_log_entry_t*)current_collreq->log_entry;

    /********************************
     * (Child) Recv Req. Finished -> Send list
     * Note: This state transition is only used for initialization.
     *       It is safe to skip the error case.
     ********************************/
    if( LOG_TWO_PHASE_NB_STAGE_PART_CHILD_WAIT_RECV_REQ == current_collreq->stage ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Child) "
                             "[%2d] Recv Req -> Send up",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

        /********************************
         * If test failed...
         * JJH TODO:
         * Although this state is skipped, we should finish writing it for
         * the sake of completeness.
         ********************************/
        if( OMPI_SUCCESS != test_exit_status ) {
            opal_output_verbose(0, ompi_ftmpi_output_handle,
                                "%s ftbasic:iagreement) (log2phase) Participant: (Child) "
                                "[%2d] Error: Not Implemented (Should never happen)",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status );
        }

        AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_2P_BCAST_REQ);
        log_entry->state   = AGREEMENT_STATE_VOTE_REQ;

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_C_AFTER_VOTE_REQ
        /* TEST:
         */
        if( rank == LOG_TWO_PHASE_TEST_RANK_CHILD ) {
            sleep(2);
            kill(getpid(), SIGKILL);
        }
#endif /* LOG_TWO_PHASE_TEST_PART_C_AFTER_VOTE_REQ */

        /************************************************
         * Send list of failed processes
         ************************************************/
        ret = log_two_phase_protocol_send_to_parent(comm,
                                                    ftbasic_module,
                                                    current_collreq->local_bitmap,
                                                    &(current_collreq->num_reqs),
                                                    rank);

        ftbasic_module->agreement_info->cur_request->stage = LOG_TWO_PHASE_NB_STAGE_PART_CHILD_WAIT_SEND;

        if( OMPI_SUCCESS != ret ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (log2phase) Participant: (Child) "
                                 "Failed (%2d) - Send list",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret));
            exit_status = log_two_phase_participant_child_progress(comm, ftbasic_module, ret);
            goto cleanup;
        }

    }
    /********************************
     * (Child) Send Finished -> Wait for commit list
     ********************************/
    else if( LOG_TWO_PHASE_NB_STAGE_PART_CHILD_WAIT_SEND == current_collreq->stage ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Child) "
                             "[%2d] Send up -> Recv",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

        /********************************
         * If test failed...
         * Parent failed, find new parent
         ********************************/
        if( OMPI_SUCCESS != test_exit_status ) {
            ret = log_two_phase_protocol_query_parent(comm, ftbasic_module,
                                                      current_collreq->local_bitmap,
                                                      log_entry, false);
            if( OMPI_SUCCESS != ret ) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic:iagreement) (log2phase) Participant: (Child) "
                                    "Error: Root failure. Decide Abort***",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                log_entry->state = AGREEMENT_STATE_ABORT;
                exit_status = test_exit_status;
                goto child_decide_abort;
            }

            goto child_decide;
        }

        /***************************** Voted Phase / Uncertainty Period  **********/
        log_entry->state = AGREEMENT_STATE_VOTED;

#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_C_AFTER_VOTE
        /* TEST:
         */
        if( rank == LOG_TWO_PHASE_TEST_RANK_CHILD ) {
            sleep(2);
            kill(getpid(), SIGKILL);
        }
#endif /* LOG_TWO_PHASE_TEST_PART_C_AFTER_VOTE */

        /************************************************
         * Receive the final list
         ************************************************/
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Child) "
                             "Wait for final list",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

        ret = log_two_phase_protocol_recv_from_parent(comm,
                                                      ftbasic_module,
                                                      current_collreq->local_bitmap,
                                                      &(current_collreq->num_reqs),
                                                      rank);

        current_collreq->stage = LOG_TWO_PHASE_NB_STAGE_PART_CHILD_WAIT_RECV;

        if( OMPI_SUCCESS != ret ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:iagreement) (log2phase) Participant: (Child) "
                                 "Failed (%2d) - Recv list",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret));
            exit_status = log_two_phase_participant_child_progress(comm, ftbasic_module, ret);
            goto cleanup;
        }
    }
    /********************************
     * (Child) Wait for commit list Finished -> Commit/DONE
     ********************************/
    else if( LOG_TWO_PHASE_NB_STAGE_PART_CHILD_WAIT_RECV == current_collreq->stage ) {
    child_done:
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Child) "
                             "[%2d] Recv -> DONE",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), test_exit_status ));

        /********************************
         * If test failed...
         * Parent failed, find new parent
         ********************************/
        if( OMPI_SUCCESS != test_exit_status ) {
#if LOG_TWO_PHASE_ENABLE_TESTING == 1 && LOG_TWO_PHASE_TEST == LOG_TWO_PHASE_TEST_PART_P_DURING_BCAST_COMMIT
        /* TEST:
         * Complete the first request
         * Die between first and second send to child
         */
            if( rank == 5 ) {
                sleep(3);
            }
#endif
            /*
             * Parent failed, find new parent
             */
            ret = log_two_phase_protocol_query_parent(comm,
                                                      ftbasic_module,
                                                      current_collreq->local_bitmap,
                                                      log_entry, true);
            if( OMPI_SUCCESS != ret ) {
                opal_output_verbose(1, ompi_ftmpi_output_handle,
                                    "%s ftbasic:iagreement) (log2phase) Participant: (Child) "
                                    "Error: Root failure. Decide Abort***",
                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
                log_entry->state = AGREEMENT_STATE_ABORT;
                exit_status = test_exit_status;
                goto child_decide_abort;
            }

            goto child_decide;
        }

        if( opal_bitmap_is_set_bit(current_collreq->local_bitmap, ompi_comm_size(comm) ) ) {
            /* JJH TODO: Double check this */
            log_entry->state = AGREEMENT_STATE_ABORT;
            goto child_decide_abort;
        }

        if( AGREEMENT_STATE_UNCERTAIN == log_entry->state ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic: agreement) (log2phase) Participant: (Child) "
                                 "Uncertain... Query peers",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));
            goto child_decide;
        }

        if( AGREEMENT_STATE_ABORT == log_entry->state ) {
            goto child_decide_abort;
        }

        /***************************** Commit Phase  ******************************/

        /************************************************
         * Commit this list
         ************************************************/
#if DEBUG_WITH_STR == 1
        tmp_bitstr = opal_bitmap_get_string(current_collreq->local_bitmap);
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Child) "
                             "Commit the list [%s]",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), tmp_bitstr));
        if( NULL != tmp_bitstr ) {
            free(tmp_bitstr);
            tmp_bitstr = NULL;
        }
#endif

        log_entry->state = AGREEMENT_STATE_COMMIT;
        opal_bitmap_copy(log_entry->commit_bitmap, current_collreq->local_bitmap);

        current_collreq->stage = LOG_TWO_PHASE_NB_STAGE_DONE;
        current_collreq->mpi_error  = OMPI_SUCCESS;
        current_collreq->mpi_source = MPI_PROC_NULL;
    }

    goto cleanup;

 child_decide:
    if( AGREEMENT_STATE_UNCERTAIN == log_entry->state ) {
        if( OMPI_SUCCESS != ( ret = log_two_phase_term_initiator(comm,
                                                                 ftbasic_module,
                                                                 current_collreq->local_bitmap,
                                                                 log_entry)) ) {
            opal_output_verbose(1, ompi_ftmpi_output_handle,
                                "%s ftbasic:iagreement) (log2phase) Participant: (Child) "
                                "Error: Term. Protocol failure - Abort***",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME));
            log_entry->state = AGREEMENT_STATE_ABORT;

            goto child_decide_abort;
        }

        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic: agreement) (log2phase) Participant: (Child) "
                             "Decided with a peer. (%s)",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ( AGREEMENT_STATE_ABORT   == log_entry->state || 
                               AGREEMENT_STATE_ABORTED == log_entry->state ? "Abort" : "Commit") ));
    }

 child_decide_abort:
    if( AGREEMENT_STATE_ABORT == log_entry->state ) {
        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                             "%s ftbasic:iagreement) (log2phase) Participant: (Child) "
                             "Decide Abort.",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) ));

        log_entry->state = AGREEMENT_STATE_ABORT;

        current_collreq->stage = LOG_TWO_PHASE_NB_STAGE_DONE;
        current_collreq->mpi_error  = exit_status;
        current_collreq->mpi_source = agreement_tree->parent;

        goto cleanup;
    }

    /*
     * Fixed it, now decide with the new parent
     */
    test_exit_status = OMPI_SUCCESS;
    goto child_done;

 cleanup:
#if DEBUG_WITH_STR == 1
    if( NULL != tmp_bitstr ) {
        free(tmp_bitstr);
        tmp_bitstr = NULL;
    }
#endif

    return exit_status;
}


/************************* Support: Term Progress Classes *****************************/
static int mca_coll_ftbasic_agreement_log_two_phase_term_progress_comm(ompi_communicator_t *comm)
{
    int ret, flag;
    MPI_Status status;
    int num_processed = 0;
    int loc_cmd;

    /*
     * Process pending messages until there are no more
     */
    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Listener: Polling Comm %3d",
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
                                "%s ftbasic: agreement) (log2phase) Listener: Error: "
                                "Probe failed (%3d)",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret );
            goto cleanup;
        }

        if( flag ) {
            num_processed++;

            ret = MCA_PML_CALL(recv( &loc_cmd, 1, MPI_INT,
                                     status.MPI_SOURCE, MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ,
                                     comm, &status ));
            if( LOG_TWO_PHASE_CMD_QUERY_PROTOCOL == loc_cmd ) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                     "%s ftbasic: agreement) (log2phase) Listener: "
                                     "Responding to Peer %3d on Comm %3d [Num %3d] - Query",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), status.MPI_SOURCE, comm->c_contextid,
                                     num_processed ));
                log_two_phase_query_responder(comm,
                                              (mca_coll_ftbasic_module_t*)comm->c_coll.coll_agreement_module,
                                              status.MPI_SOURCE);
            }
            else if( LOG_TWO_PHASE_CMD_TERM_PROTOCOL == loc_cmd ) {
                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                     "%s ftbasic: agreement) (log2phase) Listener: "
                                     "Responding to Peer %3d on Comm %3d [Num %3d] - Term",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), status.MPI_SOURCE, comm->c_contextid,
                                     num_processed ));
                log_two_phase_term_responder(comm,
                                             (mca_coll_ftbasic_module_t*)comm->c_coll.coll_agreement_module,
                                             status.MPI_SOURCE);
            }
        }
    } while(flag);

 cleanup:
    return num_processed;
}


/************************* Support: Tree Maintenance *****************************/
static int log_two_phase_update_root(ompi_communicator_t* comm,
                                     mca_coll_ftbasic_module_t *ftbasic_module)
{
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
    bool found = false;

    return log_two_phase_get_root_of(comm, ftbasic_module,
                                     &(agreement_tree->root),
                                     &found);
}

static int log_two_phase_get_root_of(ompi_communicator_t* comm,
                                     mca_coll_ftbasic_module_t *ftbasic_module,
                                     int *root,
                                     bool *found)
{
    opal_bitmap_t *fail_bitmap = ftbasic_module->agreement_info->fail_bitmap;
    int exit_status = MPI_SUCCESS;
    int i, child, dim, hibit, mask;
    int size = ompi_comm_size(comm);
    int suggested_root;

    suggested_root = *root;
    *found = false;

    OPAL_OUTPUT_VERBOSE((6, ompi_ftmpi_output_handle,
                         "%s ftbasic: agreement) (log2phase) Refresh Tree: Root Suggested (%2d)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), *root ));

    /*
     * See if this root is good to go.
     * Check against the fail_bitmap so all procs make the same decision
     * on which is the root.
     */
    if( !opal_bitmap_is_set_bit(fail_bitmap, suggested_root) ) {
        *found = true;
        exit_status = OMPI_SUCCESS;
        goto cleanup;
    }

    /* If not then:
     * Depth-First Search 'across' the tree until you find the peer with the
     * least number of children to elect.
     */
    dim = comm->c_cube_dim;
    hibit = opal_hibit(suggested_root, dim);
    --dim;

    for (i = dim, mask = 1 << i; i > hibit; --i, mask >>= 1) {
        child = suggested_root | mask;
        *root = child;
        if (child < size) {
            log_two_phase_get_root_of(comm, ftbasic_module, root, found);
            if( *found  ) {
                *root = child;
                exit_status = OMPI_SUCCESS;
                goto cleanup;
            }
        }
    }

    /* All done */
 cleanup:
    return exit_status;
}

static int log_two_phase_update_parent(ompi_communicator_t* comm,
                                       mca_coll_ftbasic_module_t *ftbasic_module)
{
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
    int rank = ompi_comm_rank(comm);

    return log_two_phase_get_parent_of(comm, ftbasic_module,
                                       rank,
                                       &(agreement_tree->parent));
}

static int log_two_phase_update_children(ompi_communicator_t* comm,
                                         mca_coll_ftbasic_module_t *ftbasic_module)
{
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
    int rank = ompi_comm_rank(comm);

    if( NULL == agreement_tree->children ) {
        agreement_tree->num_children_alloc = 256;
        agreement_tree->children = (int*)malloc(sizeof(int) * agreement_tree->num_children_alloc);
    }

    agreement_tree->num_children = 0;

    return log_two_phase_get_children_of(comm,ftbasic_module, rank,
                                         &(agreement_tree->children),
                                         &(agreement_tree->num_children),
                                         &(agreement_tree->num_children_alloc));
}

static int log_two_phase_append_children(ompi_communicator_t* comm,
                                         mca_coll_ftbasic_module_t *ftbasic_module,
                                         int *children, int num_children, bool notice_if_new)
{
    int i;

    for(i = 0; i < num_children; ++i) {
        log_two_phase_append_child(comm, ftbasic_module, children[i], notice_if_new);
    }

    return OMPI_SUCCESS;
}

static int log_two_phase_append_child(ompi_communicator_t* comm,
                                      mca_coll_ftbasic_module_t *ftbasic_module,
                                      int child, bool notice_if_new)
{
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
    int p;
#if OPAL_ENABLE_DEBUG
    int rank = ompi_comm_rank(comm);
#endif

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic:log2phase) (tree  ) Append Child %2d (num %2d / rank %2d)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         child,
                         agreement_tree->num_children,
                         rank));

    /*
     * Watchout for duplicates
     */
    for(p = 0; p < agreement_tree->num_children; ++p) {
        if( agreement_tree->children[p] == child ) {
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:log2phase) (tree  ) Append Child %2d -- DUP -> skip",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 child));
            return OMPI_SUCCESS;
        }
    }

    /*
     * Add to the child list
     */
    append_child_to_array(&(agreement_tree->children),
                          &(agreement_tree->num_children),
                          &(agreement_tree->num_children_alloc),
                          child);

    if( notice_if_new ) {
        log_two_phase_mark_need_notice(comm, ftbasic_module, child, true);
    }

    return OMPI_SUCCESS;
}

static int log_two_phase_remove_child(ompi_communicator_t* comm,
                                      mca_coll_ftbasic_module_t *ftbasic_module,
                                      int child)
{
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
    mca_coll_ftbasic_remote_bitmap_t *remote_bitmap = NULL;
    opal_list_item_t* item = NULL;
    int p;

    if( IS_INVALID_RANK(child) ) {
        return OMPI_SUCCESS;
    }

    /*
     * Find the child
     */
    for(p = 0; p < agreement_tree->num_children; ++p) {
        if( agreement_tree->children[p] == child ) {
            agreement_tree->children[p] = GET_INVALID_RANK_OF_RANK(agreement_tree->children[p]);
            OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                 "%s ftbasic:log2phase) (tree  ) Remove Child %2d (%2d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 child, agreement_tree->children[p]));
            break;
        }
    }

    /*
     * Inagreement it's entry in the remote bitmap
     */
    for( item  = opal_list_get_first(&(ftbasic_module->agreement_info->remote_bitmaps));
         item != opal_list_get_end(&(ftbasic_module->agreement_info->remote_bitmaps));
         item  = opal_list_get_next(item) ) {
        remote_bitmap = (mca_coll_ftbasic_remote_bitmap_t*)item;
        if( remote_bitmap->rank == child ) {
            remote_bitmap->rank = GET_INVALID_RANK_OF_RANK(child);
            break;
        }
    }

    return OMPI_SUCCESS;
}

static int log_two_phase_mark_need_notice(ompi_communicator_t* comm,
                                          mca_coll_ftbasic_module_t *ftbasic_module,
                                          int peer, bool notice)
{
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;

    if( NULL == agreement_tree->state_bitmap ) {
        agreement_tree->state_bitmap = OBJ_NEW(opal_bitmap_t);
        opal_bitmap_init(agreement_tree->state_bitmap,
                         ompi_comm_size(comm) + FTBASIC_AGREEMENT_EXTRA_BITS);
    }
    if( notice ) {
        opal_bitmap_set_bit(agreement_tree->state_bitmap, peer);
    } else {
        opal_bitmap_clear_bit(agreement_tree->state_bitmap, peer);
    }

    return OMPI_SUCCESS;
}

static bool log_two_phase_if_needs_notice(ompi_communicator_t* comm,
                                          mca_coll_ftbasic_module_t *ftbasic_module,
                                          int peer)
{
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;

    if( NULL == agreement_tree->state_bitmap ) {
        return false;
    }
    if( !opal_bitmap_is_set_bit(agreement_tree->state_bitmap, peer) ) {
        return false;
    }
    return true;
}

static bool log_two_phase_if_should_skip(ompi_communicator_t* comm,
                                         mca_coll_ftbasic_module_t *ftbasic_module,
                                         int peer,
                                         int of_rank,
                                         int num_prev_children)
{
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
    int p;

    /* Do not skip if these are our children */
    if( ompi_comm_rank(comm) == of_rank ) {
        return false;
    }
    /* Skip if they:
     * 1) Are included in the previous iteration of the recursion. (indicated by 'num_prev_children')
     * 2) ... that's about it ...
     */
    for(p = 0; p < num_prev_children; ++p) {
        if( agreement_tree->children[p] == peer) {
            return true;
        }
    }

    return false;
}

static bool log_two_phase_my_child(ompi_communicator_t* comm,
                                   mca_coll_ftbasic_module_t *ftbasic_module,
                                   int peer)
{
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
    int p;

    for(p = 0; p < agreement_tree->num_children; ++p) {
        if( agreement_tree->children[p] == peer) {
            return true;
        }
    }
    return false;
}

static int log_two_phase_get_vsize(ompi_communicator_t* comm,
                                   int *vsize,
                                   mca_coll_ftbasic_module_t *ftbasic_module)
{
    opal_bitmap_t *fail_bitmap = ftbasic_module->agreement_info->fail_bitmap;
    int i = 0, size = ompi_comm_size(comm);

    *vsize = 0;

    /* Find the number of alive processes */
    for( i = 0; i < size; ++i ) {
        if( !opal_bitmap_is_set_bit(fail_bitmap, i) ) {
            *vsize += 1;
        }
    }

    return OMPI_SUCCESS;
}

static int log_two_phase_get_vrank(ompi_communicator_t* comm,
                                   int rank, int *vrank,
                                   mca_coll_ftbasic_module_t *ftbasic_module)
{
    opal_bitmap_t *fail_bitmap = ftbasic_module->agreement_info->fail_bitmap;
    int i, before = 0;

    *vrank = rank;

    /*
     * Find number of failed before this rank
     */
    for(i = 0; i < rank; ++i ) {
        if( opal_bitmap_is_set_bit(fail_bitmap, i) ) {
            before += 1;
        }
    }

    /* vrank = rank - #_failed_before_rank */
    *vrank = rank - before;

    return OMPI_SUCCESS;
}

static int log_two_phase_get_rank(ompi_communicator_t* comm,
                                  int vrank, int *rank,
                                  mca_coll_ftbasic_module_t *ftbasic_module)
{
    opal_bitmap_t *fail_bitmap = ftbasic_module->agreement_info->fail_bitmap;
    int i = 0, num_failed = 0;
    int size = ompi_comm_size(comm);

    /*
     * Iterate through the list to find the real rank
     *
     * rank = vrank + vrank'th_nonfailed
     */
    *rank = -1;
    for( i = 0; i < size; ++i ) {
        if( opal_bitmap_is_set_bit(fail_bitmap, i) ) {
            num_failed++;
        }

        *rank += 1;
        if( vrank == (*rank - num_failed) ) {
            return OMPI_SUCCESS;
        }
    }

    return OMPI_SUCCESS;
}

static int log_two_phase_get_parent_of(ompi_communicator_t* comm,
                                       mca_coll_ftbasic_module_t *ftbasic_module,
                                       int rank,
                                       int *parent)
{
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
    int dim, hibit, peer;
    int vsize, vroot, vrank;

    log_two_phase_get_vsize(comm, &vsize, ftbasic_module);
    log_two_phase_get_vrank(comm, agreement_tree->root, &vroot, ftbasic_module);
    log_two_phase_get_vrank(comm, rank, &vrank, ftbasic_module);
    vrank = (vrank + vsize - vroot) % vsize; /* Shift virtual rank */

    dim = comm->c_cube_dim;
    hibit = opal_hibit(vrank, dim);
    --dim;

    if( rank == agreement_tree->root ) {
        *parent = rank;
    } else {
        /* Calculate virtual parent and shift virtual parent rank */
        peer = ((vrank & ~(1 << hibit)) + vroot) % vsize;
        /* Get actual parent rank */
        log_two_phase_get_rank(comm,
                               peer,
                               parent,
                               ftbasic_module);
        /* If this parent is failed, then keep searching
         * Optimization: To find closest parent faster instead of waiting on
         *               failed comm.
         */
        if( !ompi_comm_is_proc_active(comm, *parent, false) ) {
            rank = *parent;
            return log_two_phase_get_parent_of(comm, ftbasic_module, rank, parent);
        }
    }

    return OMPI_SUCCESS;
}


static int log_two_phase_get_children_of(ompi_communicator_t* comm,
                                         mca_coll_ftbasic_module_t *ftbasic_module,
                                         int rank,
                                         int *children[],
                                         int *num_children,
                                         int *num_children_alloc)
{
    mca_coll_ftbasic_agreement_logtwophase_t *agreement_info = (mca_coll_ftbasic_agreement_logtwophase_t*)(ftbasic_module->agreement_info);
    mca_coll_ftbasic_agreement_tree_t *agreement_tree = agreement_info->agreement_tree;
    int i, p, dim, hibit, mask, child;
    int vsize, vroot, vrank, real_rank;

    log_two_phase_get_vsize(comm, &vsize, ftbasic_module);
    log_two_phase_get_vrank(comm, agreement_tree->root, &vroot, ftbasic_module);
    log_two_phase_get_vrank(comm, rank, &vrank, ftbasic_module);
    vrank = (vrank + vsize - vroot) % vsize; /* Shift virtual rank */

    dim = comm->c_cube_dim;
    hibit = opal_hibit(vrank, dim);
    --dim;

    for (i = hibit + 1, mask = 1 << i; i <= dim; ++i, mask <<= 1) {
        child = vrank | mask;
        /* Skip self */
        if( child == vrank ) {
            continue;
        }
        else if (child < vsize) {
            child = (child + vroot) % vsize;
            log_two_phase_get_rank(comm,
                                   child,
                                   &real_rank,
                                   ftbasic_module);

            /* If child is failed, then inherit its children
             * Optimization: To get as many children in this pass as possible
             *               without waiting on failed comm.
             */
            if( !ompi_comm_is_proc_active(comm, real_rank, false) ) {
                log_two_phase_get_children_of(comm, ftbasic_module, real_rank,
                                              children, num_children, num_children_alloc);
            } else {
                /* Check for duplicates */
                for(p = 0; p < (*num_children); ++p) {
                    if( (*children)[p] == real_rank ) {
                        OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                             "%s ftbasic:log2phase) (tree  ) "
                                             "Add Child %2d (virt %2d / real %2d / num %2d) -- DUP",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             (*children)[p],
                                             child, real_rank, (*num_children) ));
                        continue;
                    }
                }

                /* Add this child */
                append_child_to_array(children, num_children, num_children_alloc, real_rank);

                OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                                     "%s ftbasic:log2phase) (tree  ) "
                                     "Add Child %2d (virt %2d / real %2d / num %2d) -- NEW",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     (*children)[(*num_children)-1],
                                     child, real_rank, (*num_children) ));
            }
        }
    }

    return OMPI_SUCCESS;
}

static void append_child_to_array(int *children[],
                                  int *num_children,
                                  int *num_children_alloc,
                                  int rank)
{
    (*num_children) += 1;
    if( (*num_children) > (*num_children_alloc) ) {
        (*num_children_alloc) = (*num_children) + 255;
        (*children) = (int*)realloc((*children),
                                    sizeof(int)*(*num_children_alloc) );
    }

    (*children)[(*num_children)-1] = rank;

    return;
}
