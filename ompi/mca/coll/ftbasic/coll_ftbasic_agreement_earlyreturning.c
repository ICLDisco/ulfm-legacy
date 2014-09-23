/*
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

#include "mpi.h"
#include "ompi/constants.h"
#include "ompi/datatype/ompi_datatype.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/btl/btl.h"
#include "ompi/mca/bml/bml.h"
#include "ompi/op/op.h"
#include "ompi/mca/bml/base/base.h"
#include "orte/mca/errmgr/errmgr.h"

#include MCA_timer_IMPLEMENTATION_HEADER
#include "coll_ftbasic.h"
static opal_hash_table_t era_passed_agreements;
static opal_hash_table_t era_ongoing_agreements;
static int era_inited = 0;
static ompi_comm_rank_failure_callback_t *ompi_stacked_rank_failure_callback_fct = NULL;

int coll_ftbasic_era_debug_rank_may_fail;

typedef enum {
    MSG_UP = 1,
    MSG_DOWN,
    MSG_RESULT_REQUEST
} era_msg_type_t;

/**
 * This enum defines the status of processes
 * when the consensus has not been decided yet
 */
enum {
    /* These values are for the current process */
    NOT_CONTRIBUTED = 1,
    GATHERING,
    BROADCASTING
};
typedef uint32_t era_proc_status_t;
#define OP_NOT_DEFINED     (1<<31)

typedef struct {
    union {
        /** Order is performance critical:
         *   Hash tables (as of June 11, 2014) consider only the lower x bits for
         *   hash value (key & mask). As a consequence, having mostly stable entries
         *   like epoch or contextid at the low part of the 64bits key anihilates the
         *   performance of the hash tables. The most varying 16 bits should be kept
         *   first (assuming little endian).
         */
        struct {
            uint16_t agreementid;
            uint16_t contextid;    /**< Although context ids are 32 bit long, only the lower 16 bits are used */
            uint32_t epoch;
        } fields;
        uint64_t uint64;
    } u;
} era_identifier_t;

#define ERAID_KEY    u.uint64
#define ERAID_FIELDS u.fields

#define MAX_ACK_FAILED_SIZE 2
#define MAX_NEW_DEAD_SIZE   2
#define MAX_FLAG_LENGTH    64 /**< Must be greater than sizeof(int) per the standard
                               *   and 2*sizeof(int) per internal requirements
                               */

#if defined(OPAL_ENABLE_DEBUG)
#define PROGRESS_FAILURE_PROB .05
#endif

typedef struct {
    int      operand;                        /**< operand applied on bytes. 
                                              *   One of OMPI_OP_BASE_FORTRAN_* values in mca/op/op.h */
    int      dt_count;                       /**< The number of datatypes in bytes */
    int      datatype;                       /**< Fortran index of predefined basic datatype in bytes */
    uint8_t  bytes[MAX_FLAG_LENGTH];         /**< content of flags. */
    int32_t  ret;                            /**< Return code */
    int      new_dead[MAX_NEW_DEAD_SIZE];    /**< -1 terminated bounded list of newly
                                              *      discovered dead ranks */
} era_value_t;

typedef struct {
    era_msg_type_t      msg_type;
    era_identifier_t    agreement_id;
    era_value_t         agreement_value;

    /** We give the source ID as both the rank in the communicator
     *  and the proc_name_t, because 90% of the time, the receiver
     *  will understand what the rank means and this is a faster
     *  operation than using the proc_name, but 10% of the time
     *  the receiver will not understand what the rank means
     *  and it needs to answer.
     */
    int                 src_comm_rank;
    orte_process_name_t src_proc_name;

    /* The following fields are defined only for messages of type MSG_UP */
    int      nb_up_messages;                  /**< Informs how many of these messages types 
                                               *   will be sent upward (in case ack_failed must
                                               *   be split in multiple messages. */
    int      ack_failed[MAX_ACK_FAILED_SIZE]; /**< -1 terminated list of ranks that are
                                                *  known to be failed by the processes in
                                                *  that branch of the tree */
} era_msg_t;

typedef struct {
    opal_object_t   super;
    era_value_t     agreement_value;
} era_passed_agreement_t;

OBJ_CLASS_INSTANCE(era_passed_agreement_t, opal_object_t, NULL, NULL);

/**
 * Direct descendents provide information that is reduced as we go up.
 * This information may be transmitted in multiple messages (when lists of
 *  dead ack processes > MAX_ACK_FAILED_SIZE).
 * How many messages / if the message has been received by a given
 * descendent is kept in a list with elements of this type
 */
typedef struct {
    opal_list_item_t super;
    int32_t          rank;        /**< The rank of the descendent that provided information */
    int32_t          counter;     /**< How many additional messages are expected from this descendent */
} era_rank_counter_item_t;

OBJ_CLASS_INSTANCE(era_rank_counter_item_t, opal_list_item_t, NULL, NULL /*print_destructor*/);

/**
 * Main structure to remember the current status of an agreement that
 *  was started.
 */
typedef struct {
    opal_object_t         super;
    era_identifier_t      agreement_id;
    era_value_t           current_value;
    era_proc_status_t     status;            /**< status of this process in that agreement. */
    ompi_communicator_t  *comm;              /**< Communicator related to that agreement. Might be NULL
                                              *   if this process has not entered the agreement yet.*/
    int                   alive_size;        /**< Size of the "alive" group = comm->c_local_group \ comm->agreed_failed_ranks
                                              *   Also the size of the translation array below. */
    int                  *alive_ta;          /**< Alive translation array: maps ranks in 0 - alive_size-1
                                              *   to ranks in c_local_group. Used to compute the trees on
                                              *   the alive group but return ranks in the communicator. */
    ompi_group_t         *acked_group;       /**< Group of the last acknowledged process when entering
                                              *   the agreement. Used to compare with the descendents
                                              *   acknowledged processes */
    opal_list_t           gathered_info;     /**< The list of direct descendents that provided information (even partially) */
    opal_list_t           union_of_dead_ack; /**< The list of dead processes acknowledged by the descendents */
    opal_list_t           waiting_res_from;  /**< A list of ranks and status, from which we requested to "restart" the
                                              *   consensus (if the current process became root in the middle of one) */
    opal_list_t           early_requesters;  /**< Remember anybody who requests to receive the decision as
                                              *   the FD may be too slow to provide a symmetric view of the world,
                                              *   and passing over the passed agreements after the decision is taken
                                              *   is too slow */
    int                   waiting_down_from; /**< If in BROADCAST state: who is supposed to send me the info */
} era_agreement_info_t;

static void  era_agreement_info_constructor (era_agreement_info_t *agreement_info)
{
    int r;

    agreement_info->agreement_id.ERAID_KEY = 0;
    agreement_info->status = NOT_CONTRIBUTED | OP_NOT_DEFINED;
    for(r = 0; r < MAX_NEW_DEAD_SIZE; r++)
        agreement_info->current_value.new_dead[r] = -1;
    agreement_info->current_value.ret = 0;
    agreement_info->comm = NULL;
    agreement_info->alive_size = 0;
    agreement_info->alive_ta   = NULL;
    agreement_info->waiting_down_from = -1;
    OBJ_CONSTRUCT(&agreement_info->gathered_info, opal_list_t);
    OBJ_CONSTRUCT(&agreement_info->union_of_dead_ack, opal_list_t);
    OBJ_CONSTRUCT(&agreement_info->waiting_res_from, opal_list_t);
    OBJ_CONSTRUCT(&agreement_info->early_requesters, opal_list_t);
    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Constructing Agreement Info %p\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (void*)agreement_info));
}

static void  era_agreement_info_destructor (era_agreement_info_t *agreement_info)
{
    opal_list_item_t *li;
    while( NULL != (li = opal_list_remove_first(&agreement_info->early_requesters)) ) {
        OBJ_RELEASE(li);
    }
    OBJ_DESTRUCT(&agreement_info->early_requesters);
    while( NULL != (li = opal_list_remove_first(&agreement_info->gathered_info)) ) {
        OBJ_RELEASE(li);
    }
    OBJ_DESTRUCT(&agreement_info->gathered_info);
    while( NULL != (li = opal_list_remove_first(&agreement_info->union_of_dead_ack)) ) {
        OBJ_RELEASE(li);
    }
    OBJ_DESTRUCT(&agreement_info->union_of_dead_ack);
    while( NULL != (li = opal_list_remove_first(&agreement_info->waiting_res_from)) ) {
        OBJ_RELEASE(li);
    }
    OBJ_DESTRUCT(&agreement_info->waiting_res_from);
    if( NULL != agreement_info->comm ) {
        OBJ_RELEASE(agreement_info->comm);
        assert( NULL != agreement_info->alive_ta );
        free(agreement_info->alive_ta);
        agreement_info->alive_ta = NULL;
        agreement_info->alive_size =  0;
    }
}

OBJ_CLASS_INSTANCE(era_agreement_info_t,
                   opal_object_t, 
                   era_agreement_info_constructor, 
                   era_agreement_info_destructor);

typedef struct {
    opal_object_t super;
    uint32_t      max_seq_num;
} era_seq_num_record_t;

OBJ_CLASS_INSTANCE(era_seq_num_record_t, opal_object_t, NULL, NULL);


#if OPAL_ENABLE_DEBUG
static char *era_status_to_string(era_proc_status_t s) {
    switch(s) {
    case NOT_CONTRIBUTED:
        if( s & OP_NOT_DEFINED )
            return "NOT_CONTRIBUTED | OP_NOT_DEFINED";
        else
            return "NOT_CONTRIBUTED";
    case GATHERING:
        if( s & OP_NOT_DEFINED )
            return "GATHERING | OP_NOT_DEFINED";
        else
            return "GATHERING";
    case BROADCASTING:
        if( s & OP_NOT_DEFINED )
            return "BROADCASTING | OP_NOT_DEFINED";
        else
            return "BROADCASTING";
    }
    return "UNDEFINED STATUS";
}

static char *era_msg_type_to_string(int type) {
    switch(type) {
    case MSG_UP:
        return "UP";
    case MSG_DOWN:
        return "DOWN";
    case MSG_RESULT_REQUEST:
        return "RESULT REQUEST";
    }
    return "UNDEFINED MESSAGE TYPE";
}
#endif /* OPAL_ENABLE_DEBUG */

static era_agreement_info_t *era_lookup_agreeement_info(era_identifier_t agreement_id)
{
    void *value;

    if( opal_hash_table_get_value_uint64(&era_ongoing_agreements,
                                         agreement_id.ERAID_KEY,
                                         &value) == OPAL_SUCCESS ) {
        return (era_agreement_info_t *)value;
    } else {
        return NULL;
    }
}

static era_agreement_info_t *era_create_agreement_info(era_identifier_t agreement_id)
{
    era_agreement_info_t *ci;
#if OPAL_ENABLE_DEBUG
    void *value;
    assert( opal_hash_table_get_value_uint64(&era_ongoing_agreements, 
                                             agreement_id.ERAID_KEY, 
                                             &value) != OPAL_SUCCESS );
#endif
    ci = OBJ_NEW(era_agreement_info_t);
    ci->agreement_id.ERAID_KEY = agreement_id.ERAID_KEY;
    opal_hash_table_set_value_uint64(&era_ongoing_agreements,
                                     agreement_id.ERAID_KEY,
                                     ci);
    return ci;
}

#if defined(OPAL_ENABLE_DEBUG)
static void era_debug_print_group(int lvl, ompi_group_t *group, ompi_communicator_t *comm, char *info)
{
    int *gra = NULL;
    int *cra = NULL;
    int i, n, s, p;
    char *str;

    if( (n = ompi_group_size(group)) > 0 ) {
        gra = (int*)malloc( n * sizeof(int) );
        for(i = 0; i < n; i++)
            gra[i] = i;
        cra = (int*)malloc( n * sizeof(int) );
        ompi_group_translate_ranks(group, n, gra, comm->c_local_group, gra);
    }
    s = 128 + n * 16;
    str = (char*)malloc(s);
    sprintf(str, "Group of size %d. Ranks in %d.%d: (", n, comm->c_contextid, comm->c_epoch);
    p = strlen(str);
    for(i = 0; i < n; i++) {
        snprintf(str + p, s - p, "%d%s", gra[i], i==n-1 ? "" : ", ");
        p = strlen(str);
    }
    snprintf(str + p, s-p, ")");
    OPAL_OUTPUT_VERBOSE((lvl, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) %s: %s\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         info,
                         str));
    free(str);
    if( NULL != cra )
        free(cra);
    if( NULL != gra )
        free(gra);
}
#else
#define era_debug_print_group(g, c, i) do {} while(0)
#endif

static void era_agreement_info_set_comm(era_agreement_info_t *ci, ompi_communicator_t *comm, ompi_group_t *acked_group)
{
    ompi_group_t *tmp_grp;
    int *src_ra;
    int *dst_ra;
    int r, p, nd_size, min, t;

    assert( comm->c_contextid == ci->agreement_id.ERAID_FIELDS.contextid );
    assert( comm->c_epoch     == ci->agreement_id.ERAID_FIELDS.epoch     );
    assert( ci->comm          == NULL                                    );
    ci->comm = comm;
    OBJ_RETAIN(comm);

    assert( comm->agreed_failed_ranks != NULL );

    ci->acked_group = acked_group; /**< We assume that it is not necessary to copy the group, just keep the pointer */
    era_debug_print_group(30, acked_group, comm, "Acked group before Agreement");

    tmp_grp = OBJ_NEW(ompi_group_t);
    ompi_group_difference(ompi_group_all_failed_procs, comm->agreed_failed_ranks, &tmp_grp);
    nd_size = ompi_group_size(tmp_grp);
    if( nd_size > 0 ) {
        src_ra = (int*)malloc(nd_size * sizeof(int));
        dst_ra = (int*)malloc(nd_size * sizeof(int));
        for(r = 0 ; r < nd_size; r++)
            src_ra[r] = r;
        ompi_group_translate_ranks(tmp_grp, nd_size, src_ra, comm->c_local_group, dst_ra);
        for(p = 0; p < MAX_NEW_DEAD_SIZE; p++) {
            min = MPI_UNDEFINED;
            for(r = 0; r < nd_size; r++) {
                if( dst_ra[r] == MPI_UNDEFINED )
                    continue; /**< This new dead process does not belong to the associated communicator */
                if( min == MPI_UNDEFINED || dst_ra[r] < min ) {
                    t = dst_ra[r];
                    dst_ra[r] = min;
                    min = t;
                }
            }
            if( min != MPI_UNDEFINED ) {
                OPAL_OUTPUT_VERBOSE((15, ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ERA) add rank %d as a new discovered dead process #%d on agreement (%d.%d).%d\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     min,
                                     p,
                                     ci->agreement_id.ERAID_FIELDS.contextid,
                                     ci->agreement_id.ERAID_FIELDS.epoch,                         
                                     ci->agreement_id.ERAID_FIELDS.agreementid));
                ci->current_value.new_dead[p++] = min;
            } else {
                break;
            }
        }
        free(src_ra);
        free(dst_ra);
    }
    OBJ_RELEASE(tmp_grp);

    tmp_grp = OBJ_NEW(ompi_group_t);
    ompi_group_difference(comm->c_local_group, comm->agreed_failed_ranks, &tmp_grp);
    ci->alive_size = ompi_group_size(tmp_grp);
    assert(NULL == ci->alive_ta);
    ci->alive_ta = (int*)malloc(ci->alive_size * sizeof(int));
    src_ra = (int*)malloc(ci->alive_size * sizeof(int));
    for(r = 0 ; r < ci->alive_size; r++)
        src_ra[r] = r;
    ompi_group_translate_ranks(tmp_grp, ci->alive_size, src_ra, comm->c_local_group, ci->alive_ta);
#if defined(OPAL_ENABLE_DEBUG)
    /* Check: in order to keep the memory overhead small(er), we assume that
     *   ranks in ci->alive_ta are ordered from small to big.
     *   This loop checks this before we base the conversion on a wrong assumption
     */
    for(r = 1; r < ci->alive_size; r++)
        assert(ci->alive_ta[r-1] < ci->alive_ta[r]);
#endif
    free(src_ra);
    OBJ_RELEASE(tmp_grp);
}

int mca_coll_ftbasic_agreement_era_comm_init(ompi_communicator_t *comm, mca_coll_ftbasic_module_t *module)
{
    mca_coll_ftbasic_agreement_t *comm_ag_info;

    comm_ag_info = OBJ_NEW(mca_coll_ftbasic_agreement_t);
    comm_ag_info->agreement_seq_num = 0;

    module->agreement_info = comm_ag_info;

    return OMPI_SUCCESS;
}

int mca_coll_ftbasic_agreement_era_comm_finalize(mca_coll_ftbasic_module_t *module)
{
    mca_coll_ftbasic_agreement_t *comm_ag_info;
    comm_ag_info = module->agreement_info;
    OBJ_RELEASE(comm_ag_info);
    return OMPI_SUCCESS;
}

static  void send_up_msg(era_agreement_info_t *ci, int rank);
static void send_msg(ompi_communicator_t *comm,
                     int dst,
                     orte_process_name_t *proc_name,
                     era_identifier_t agreement_id,
                     era_msg_type_t type,
                     era_value_t *value,
                     int          nb_up_msg,
                     int         *ack_failed);

#define ERA_TAG_AGREEMENT MCA_COLL_BASE_TAG_AGREEMENT

static void era_combine_agreement_values(era_agreement_info_t *ni, era_value_t *value)
{
    int r, ir, to_insert, tmp;
    ompi_op_t *op;
    ompi_datatype_t *dt;
#if defined(OPAL_ENABLE_DEBUG)
    int pr = 0;
#endif

    if( ni->status & OP_NOT_DEFINED ) {
        ni->current_value.operand = value->operand;
        ni->current_value.dt_count = value->dt_count;
        ni->current_value.datatype = value->datatype;
        memcpy(ni->current_value.bytes, value->bytes, MAX_FLAG_LENGTH);
        ni->current_value.ret = value->ret;
        ni->status &= ~OP_NOT_DEFINED;
    } else {
        assert( ni->current_value.operand == value->operand );
        assert( ni->current_value.dt_count == value->dt_count );
        assert( ni->current_value.datatype == value->datatype );
        op = opal_pointer_array_get_item(ompi_op_f_to_c_table,
                                         ni->current_value.operand);
        assert(NULL != op);
        dt = opal_pointer_array_get_item(&ompi_datatype_f_to_c_table,
                                         ni->current_value.datatype);
        assert(NULL != dt);
        ompi_op_reduce( op, value->bytes, ni->current_value.bytes,
                        ni->current_value.dt_count, dt);
    }

    if( value->ret > ni->current_value.ret )
        ni->current_value.ret = value->ret;

#if defined(OPAL_ENABLE_DEBUG)
    if( (ni->current_value.new_dead[0] != -1) ||
        (ni->current_value.new_dead[1] != -1) ||
        (value->new_dead[0] != -1) ||
        (value->new_dead[1] != -1) ) {
        OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) COMBINE %d,%d with %d,%d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ni->current_value.new_dead[0], ni->current_value.new_dead[1],
                             value->new_dead[0], value->new_dead[1]));
        pr = 1;
    } 
#endif

    /* Merge the new_dead lists: keep the lowest ranks that belong to both,
     * knowing that -1 means end of list
     */
    for(r = 0, ir = 0; r < MAX_NEW_DEAD_SIZE && ir < MAX_NEW_DEAD_SIZE; r++) {
        if( (to_insert = value->new_dead[r]) == -1 ) {
            break;
        }
        for(; ir < MAX_NEW_DEAD_SIZE; ir++) {
            if( ni->current_value.new_dead[ir] == -1 ) {
                ni->current_value.new_dead[ir] = to_insert;
                ir++;
                break;
            } else if( ni->current_value.new_dead[ir] > to_insert ) {
                tmp = ni->current_value.new_dead[ir];
                ni->current_value.new_dead[ir] = to_insert;
                to_insert = tmp;
            } else if( ni->current_value.new_dead[ir] == to_insert ) {
                /* No dupplicate! */
                break;
            }
        }
    }

#if defined(OPAL_ENABLE_DEBUG)
    if( pr )
        OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) COMBINE GIVES %d,%d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ni->current_value.new_dead[0], ni->current_value.new_dead[1]));
#endif
}

/**
 * This search to find r such that ci->alive_ta[r] == r_in_comm
 * should be (in average) linear in the number of failures, so it's
 * better than translate_ranks which is linear in the communicator
 * size
 */
static int era_comm_to_alive(era_agreement_info_t *ci, int r_in_comm)
{
    int r;
    assert(NULL != ci->alive_ta);
    assert(0 < ci->alive_size);
    r = r_in_comm >= ci->alive_size ? ci->alive_size - 1 : r_in_comm;
    while( r > 0 && ci->alive_ta[r] > r_in_comm ) {
        r--;
    }
    if( r < 0 || ci->alive_ta[r] < r_in_comm )
        return MPI_UNDEFINED;
    else {
        assert(r >= 0 && r < ci->alive_size );
        assert(ci->alive_ta[r] == r_in_comm);
        return r;
    }
}

#define ERA_TOPOLOGY_BINARY_TREE

#if defined ERA_TOPOLOGY_BINARY_TREE
static int era_parent_of(era_agreement_info_t *ci, int r_in_comm)
{
    ompi_communicator_t *comm = ci->comm;
    int p_in_alive;
    int r_in_alive = era_comm_to_alive(ci, r_in_comm);
    assert(MPI_UNDEFINED != r_in_alive);

    if(r_in_comm == 0) {
        /** We never ask the parent of a dead process */
        assert( ompi_comm_is_proc_active(comm, r_in_comm, false) );
        return 0;
    }

    p_in_alive = (r_in_alive-1) / 2;

    while( !ompi_comm_is_proc_active(comm, ci->alive_ta[p_in_alive], false) && (0 != p_in_alive) ) {
        p_in_alive = (p_in_alive-1)/2;
    }

    if(!ompi_comm_is_proc_active(comm, ci->alive_ta[p_in_alive], false)) {
        assert( p_in_alive==0 );
        for(p_in_alive = 0; p_in_alive < r_in_alive; p_in_alive++)
            if(ompi_comm_is_proc_active(comm, ci->alive_ta[p_in_alive], false))
                break;
    }

    return ci->alive_ta[p_in_alive];
}

static int era_parent(era_agreement_info_t *ci)
{
    return era_parent_of(ci, ompi_comm_rank(ci->comm));
}

static int era_next_child(era_agreement_info_t *ci, int prev_child_in_comm)
{
    ompi_communicator_t *comm = ci->comm;
    int prev_child_in_alive = prev_child_in_comm == -1 ? -1 : era_comm_to_alive(ci, prev_child_in_comm);
    int next_child_in_alive;
    int r_in_comm = ompi_comm_rank(ci->comm);
    int r_in_alive = era_comm_to_alive(ci, r_in_comm);
    assert(MPI_UNDEFINED != r_in_alive);

    if( prev_child_in_alive == -1 )
        next_child_in_alive = r_in_alive + 1;
    else
        next_child_in_alive = prev_child_in_alive + 1;

    /** Anybody with an ID > r is a potentation child */
    while(next_child_in_alive < ci->alive_size) {
        if( ompi_comm_is_proc_active(comm, ci->alive_ta[next_child_in_alive], false) && 
            r_in_comm == era_parent_of(ci, ci->alive_ta[next_child_in_alive]) )
            break;
        next_child_in_alive++;
    }

    if( next_child_in_alive == ci->alive_size )
        return ompi_comm_size(ci->comm);

    return ci->alive_ta[next_child_in_alive];
}
#endif

#if defined ERA_TOPOLOGY_STAR
static int era_parent(era_agreement_info_t *ci)
{
    int r;
    for(r = 0; r < ci->comm->c_my_rank; r++) {
        if( ompi_comm_is_proc_active(ci->comm, r, false) )
            break;
    }
    return r;
}

static int era_next_child(era_agreement_info_t *ci, int prev_child)
{
    ompi_communicator_t *comm = ci->comm;
    if( era_parent(ci) == comm->c_my_rank ) {
        if( prev_child == -1 ) 
            r = comm->c_my_rank+1;
        else 
            r = prev_child + 1;
        while( r < ompi_comm_size(comm) ) {
            if( ompi_comm_is_proc_active(comm, r, false) )
                break;
            r++;
        }
        return r;
    } else {
        return ompi_comm_size(comm);
    }
}
#endif

#if defined ERA_TOPOLOGY_STRING
static int era_parent(era_agreement_info_t *ci)
{
    int r;
    ompi_communicator_t *comm = ci->comm;
    for(r = comm->c_my_rank - 1; r >= 0; r--)
        if( ompi_comm_is_proc_active(comm, r, false) )
            break;
    if( r < 0 )
        return comm->c_my_rank; /* I'm root! */
    return r;
}

static int era_next_child(era_agreement_info_t *ci, int prev_child)
{
    ompi_communicator_t *comm = ci->comm;
    int r;

    if( prev_child == -1 ) {
        for(r = comm->c_my_rank + 1; r < ompi_comm_size(comm); r++)
            if( ompi_comm_is_proc_active(comm, r, false) )
                break;
        return r;
    } else {
        return ompi_comm_size(comm);
    }
}
#endif

static void era_decide(era_value_t *decided_value, era_agreement_info_t *ci)
{
    era_passed_agreement_t *da;
    ompi_communicator_t *comm;
    ompi_group_t *new_deads_group, *new_agreed;
    era_rank_counter_item_t *rl;
    int r;

    da = OBJ_NEW(era_passed_agreement_t);
    memcpy(&da->agreement_value, decided_value, sizeof(era_value_t));

    OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) decide %08x.%d.%d.. on agreement (%d.%d).%d\n",                         
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         *(int*)decided_value->bytes,
                         decided_value->ret,
                         decided_value->new_dead[0],
                         ci->agreement_id.ERAID_FIELDS.contextid,
                         ci->agreement_id.ERAID_FIELDS.epoch,                         
                         ci->agreement_id.ERAID_FIELDS.agreementid));

    opal_hash_table_remove_value_uint64(&era_ongoing_agreements, ci->agreement_id.ERAID_KEY);
    opal_hash_table_set_value_uint64(&era_passed_agreements,
                                     ci->agreement_id.ERAID_KEY,
                                     da);

    comm = ci->comm;
    assert( NULL != comm );

    if( decided_value->new_dead[0] != -1 ) {
        for(r = 0; r < MAX_NEW_DEAD_SIZE && decided_value->new_dead[r] != -1; r++)
            /*nothing*/;

        OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) decide %08x.%d.%d.. on agreement (%d.%d).%d: adding %d processes to the list of agreed deaths\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             *(int*)decided_value->bytes,
                             decided_value->ret,
                             decided_value->new_dead[0],
                             ci->agreement_id.ERAID_FIELDS.contextid,
                             ci->agreement_id.ERAID_FIELDS.epoch,                         
                             ci->agreement_id.ERAID_FIELDS.agreementid,
                             r));

#if defined(OPAL_ENABLE_DEBUG)
        {
            int _i, _j;
            for(_i = 0; _i < r; _i++) {
                assert(decided_value->new_dead[_i] >= 0 &&
                       decided_value->new_dead[_i] < ompi_comm_size(comm));
                for(_j = _i+1; _j < r; _j++) {
                    assert(decided_value->new_dead[_i] < decided_value->new_dead[_j]);
                }
            }
        }
#endif /*OPAL_ENABLE_DEBUG*/

        new_deads_group = OBJ_NEW(ompi_group_t);
        new_agreed      = OBJ_NEW(ompi_group_t);
        ompi_group_incl(comm->c_local_group, r, decided_value->new_dead, &new_deads_group);
        ompi_group_union(comm->agreed_failed_ranks, new_deads_group, &new_agreed);
        OBJ_RELEASE(comm->agreed_failed_ranks);
        comm->agreed_failed_ranks = new_agreed;
        era_debug_print_group(30, comm->agreed_failed_ranks, comm, "Added elements in agreed_failed_Groups");
        OBJ_RELEASE(new_deads_group);
    }
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) decide %08x.%d.%d.. on agreement (%d.%d).%d: group of agreed deaths is of size %d\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         *(int*)decided_value->bytes,
                         decided_value->ret,
                         decided_value->new_dead[0],
                         ci->agreement_id.ERAID_FIELDS.contextid,
                         ci->agreement_id.ERAID_FIELDS.epoch,                         
                         ci->agreement_id.ERAID_FIELDS.agreementid,
                         ompi_group_size(comm->agreed_failed_ranks)));

    r = -1;
    while( (r = era_next_child(ci, r)) < ompi_comm_size(comm) ) {

        /** Cleanup the early_requesters list, to avoid sending unecessary dupplicate messages */
        if( opal_list_get_size(&ci->early_requesters) > 0 ) {
            for(rl = (era_rank_counter_item_t*)opal_list_get_first(&ci->early_requesters);
                rl != (era_rank_counter_item_t*)opal_list_get_end(&ci->early_requesters);
                rl = (era_rank_counter_item_t*)opal_list_get_next(&rl->super)) {
                if( rl->rank == r ) {
                    opal_list_remove_item(&ci->early_requesters, &rl->super);
                    break;
                }
            }
        }

        send_msg(comm, r, NULL, ci->agreement_id, MSG_DOWN, &da->agreement_value, 0, NULL);
    }

    /* In case we have some child we haven't detected yet */
    if( opal_list_get_size(&ci->early_requesters) > 0 ) {
        for(rl = (era_rank_counter_item_t*)opal_list_get_first(&ci->early_requesters);
            rl != (era_rank_counter_item_t*)opal_list_get_end(&ci->early_requesters);
            rl = (era_rank_counter_item_t*)opal_list_get_next(&rl->super)) {
            send_msg(comm, rl->rank, NULL, ci->agreement_id, MSG_DOWN, &da->agreement_value, 0, NULL);
        }
    }

    OBJ_RELEASE(ci); /* This will take care of the content of ci too */
}

static void era_compute_local_return_value(era_agreement_info_t *ci) {
    int r, found;
    era_rank_counter_item_t *rl;
    ompi_group_t 
        *ack_descendent_union_group, 
        *ack_after_agreement_group,
        *tmp_sub_group;
    int *adug_array, nb_adug, abag_array[3];

    /* Simplest case: some children has decided MPI_ERR_PROC_FAILED already 
     * OR I had children during the run, and they died and were not acknowledged before I entered
     * the agreement (see era_mark_process_failed). Then, the return value was set by
     * era_combine_agreement_values or era_mark_process_failed
     */
    if( ci->current_value.ret == MPI_ERR_PROC_FAILED ) {
        OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) compute local return value for Agreement ID = (%d.%d).%d: decide FAILED at line %s:%d\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ci->agreement_id.ERAID_FIELDS.contextid,
                             ci->agreement_id.ERAID_FIELDS.epoch,
                             ci->agreement_id.ERAID_FIELDS.agreementid,
                             __FILE__, __LINE__));
        return;
    }

    if( era_next_child(ci, -1) != ompi_comm_size(ci->comm) ) {
        /* I still have some children. Let's count them */
        r = -1;
        found = 0;
        while( r != ompi_comm_size(ci->comm) ) {
            r = era_next_child(ci, r);
            if( r != ompi_comm_size(ci->comm) )
                found++;
        }

        /* First case: are there two children that didn't acknowledge the same list of ranks?
         * Use that time to count the size of the ADUG */
        nb_adug = 0;
        for( rl = (era_rank_counter_item_t *)opal_list_get_first( &ci->union_of_dead_ack );
             rl != (era_rank_counter_item_t *)opal_list_get_end( &ci->union_of_dead_ack );
             rl = (era_rank_counter_item_t *)opal_list_get_next( &rl->super ) ) {
            if( found != rl->counter ) {
                /* That's it: at least one of my children did not acknowledge this rank.
                 * It could be because the child died before it could tell it acknowledged the rank.
                 * But then, it died during the agreement, so I have marked the operation
                 * FAILED already. So, we should return ERR_PROC_FAILED in any case. */
                OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ERA) compute local return value for Agreement ID = (%d.%d).%d: decide FAILED at line %s:%d: I should have %d children, and %d reported rank %d as dead.\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ci->agreement_id.ERAID_FIELDS.contextid,
                                     ci->agreement_id.ERAID_FIELDS.epoch,
                                     ci->agreement_id.ERAID_FIELDS.agreementid,
                                     __FILE__, __LINE__,
                                     found, rl->counter, rl->rank));                  
                ci->current_value.ret = MPI_ERR_PROC_FAILED;
                return;
            }
            nb_adug++;
        }

        if( nb_adug > 0 /**< There is at least a failure acknowledged by the children. */ ) {
            /* Okay.. We may need to compare the group I acknowledged (when entering the agreement)
             * and the 'group' my children acknowledged... */

            if( ompi_group_size(ci->acked_group) == 0 ) {
                /* My children have some acknowledged, failures, but not me */
                OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ERA) compute local return value for Agreement ID = (%d.%d).%d: decide FAILED at line %s:%d\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ci->agreement_id.ERAID_FIELDS.contextid,
                                     ci->agreement_id.ERAID_FIELDS.epoch,
                                     ci->agreement_id.ERAID_FIELDS.agreementid,
                                     __FILE__, __LINE__));
                ci->current_value.ret = MPI_ERR_PROC_FAILED;
                return;
            }
            
            /** Did my children acknowledge the same number of ranks as me? */
            if( nb_adug != ompi_group_size(ci->acked_group) ) {
                OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ERA) compute local return value for Agreement ID = (%d.%d).%d: decide FAILED at line %s:%d\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ci->agreement_id.ERAID_FIELDS.contextid,
                                     ci->agreement_id.ERAID_FIELDS.epoch,
                                     ci->agreement_id.ERAID_FIELDS.agreementid,
                                     __FILE__, __LINE__));
                ci->current_value.ret = MPI_ERR_PROC_FAILED;
                return;
            }

            /* We need to do some hard work and actually compare the groups, as they can still be different */
            ack_descendent_union_group = OBJ_NEW(ompi_group_t);
            adug_array = (int*)malloc(nb_adug * sizeof(int));
            nb_adug = 0;
            for( rl = (era_rank_counter_item_t *)opal_list_get_first( &ci->union_of_dead_ack );
                 rl != (era_rank_counter_item_t *)opal_list_get_end( &ci->union_of_dead_ack );
                 rl = (era_rank_counter_item_t *)opal_list_get_next( &rl->super ) ) {
                adug_array[nb_adug++] = rl->rank;
            }
            ompi_group_incl(ci->comm->c_local_group, nb_adug, adug_array, &ack_descendent_union_group);
            
            ompi_group_compare(ack_descendent_union_group, ci->acked_group, &r);
            if( MPI_UNEQUAL == r ) {
                OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ERA) compute local return value for Agreement ID = (%d.%d).%d: decide FAILED at line %s:%d\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ci->agreement_id.ERAID_FIELDS.contextid,
                                     ci->agreement_id.ERAID_FIELDS.epoch,
                                     ci->agreement_id.ERAID_FIELDS.agreementid,
                                     __FILE__, __LINE__));
                ci->current_value.ret = MPI_ERR_PROC_FAILED;
            }

            OBJ_RELEASE(ack_descendent_union_group);
            free(adug_array);
        }
    }

    /** At this point, I acknowledged the same failures as my children,
     *  and no failure prevented us to move forward. However, if I noticed
     *  a failure in the communicator that has not been acknowledged, I must
     *  still report an ERR_PROC_FAILED.
     */
    if( ompi_group_size(ompi_group_all_failed_procs) > ompi_group_size(ci->acked_group) ) {
        /* New failures have been reported since I started the agreement */
        ack_after_agreement_group = OBJ_NEW(ompi_group_t);
        tmp_sub_group = OBJ_NEW(ompi_group_t);
        abag_array[0] = 0;
        abag_array[1] = ompi_group_size(ompi_group_all_failed_procs) - 1; /**< >=0 since gpr_proc_count > ompi_group_size(ci->acked_group) >= 0 */
        abag_array[2] = 1;
        ompi_group_range_incl(ompi_group_all_failed_procs, 1, &abag_array, &tmp_sub_group);
        ompi_group_intersection(tmp_sub_group,
                                ci->comm->c_local_group,
                                &ack_after_agreement_group);
        OBJ_RELEASE(tmp_sub_group);

        if( ompi_group_size(ack_after_agreement_group) != ompi_group_size(ci->acked_group) ) {
            OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) compute local return value for Agreement ID = (%d.%d).%d: decide FAILED at line %s:%d\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ci->agreement_id.ERAID_FIELDS.contextid,
                                 ci->agreement_id.ERAID_FIELDS.epoch,
                                 ci->agreement_id.ERAID_FIELDS.agreementid,
                                 __FILE__, __LINE__));
            era_debug_print_group(3, ack_after_agreement_group, ci->comm, "ack_after_agreement");
            era_debug_print_group(3, ci->acked_group, ci->comm, "acked");
            ci->current_value.ret = MPI_ERR_PROC_FAILED;
        } else {
            OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) compute local return value for Agreement ID = (%d.%d).%d: decide SUCCESS at line %s:%d\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ci->agreement_id.ERAID_FIELDS.contextid,
                                 ci->agreement_id.ERAID_FIELDS.epoch,
                                 ci->agreement_id.ERAID_FIELDS.agreementid,
                                 __FILE__, __LINE__));
        }
        OBJ_RELEASE(ack_after_agreement_group);
    } else {
        OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) compute local return value for Agreement ID = (%d.%d).%d: decide SUCCESS at line %s:%d\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ci->agreement_id.ERAID_FIELDS.contextid,
                             ci->agreement_id.ERAID_FIELDS.epoch,
                             ci->agreement_id.ERAID_FIELDS.agreementid,
                             __FILE__, __LINE__));
    }
}

static void era_check_status(era_agreement_info_t *ci)
{
    int r;
    int found;
    era_rank_counter_item_t *rl;

    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) check_status for Agreement ID = (%d.%d).%d, current status = %s\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         ci->agreement_id.ERAID_FIELDS.contextid,
                         ci->agreement_id.ERAID_FIELDS.epoch,
                         ci->agreement_id.ERAID_FIELDS.agreementid,
                         era_status_to_string(ci->status)));

    if( ci->status == NOT_CONTRIBUTED ) {
        /* Well, I haven't contributed to this agreement yet, and you'll not make a decision without me */
        return;
    }

    if( ci->status == GATHERING ) {
        /* I contributed myself, and I may just have received a contribution from a child */
        /* Let's see if it's time to pass up */
        r = -1;
        while( (r = era_next_child(ci, r)) < ompi_comm_size(ci->comm) ) {
            OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) check_status for Agreement ID = (%d.%d).%d, child %d is supposed to contribute\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                 ci->agreement_id.ERAID_FIELDS.contextid,
                                 ci->agreement_id.ERAID_FIELDS.epoch,
                                 ci->agreement_id.ERAID_FIELDS.agreementid,
                                 r));
            found = 0;
            for( rl =  (era_rank_counter_item_t*)opal_list_get_first(&ci->gathered_info);
                 rl != (era_rank_counter_item_t*)opal_list_get_end(&ci->gathered_info);
                 rl =  (era_rank_counter_item_t*)opal_list_get_next(&rl->super) ) {
                if( rl->rank == r ) {
                    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                                         "%s ftbasic:agreement (ERA) check_status for Agreement ID = (%d.%d).%d, child %d has still %d messages to send\n",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                         ci->agreement_id.ERAID_FIELDS.contextid,
                                         ci->agreement_id.ERAID_FIELDS.epoch,
                                         ci->agreement_id.ERAID_FIELDS.agreementid,
                                         r,
                                         rl->counter));
                    /* Found it, but maybe it needs to send me more messages? */
                    found = (rl->counter == 0);
                    break;
                }
            }
            if( !found ) {
                OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ERA) check_status for Agreement ID = (%d.%d).%d, some children have not contributed\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                     ci->agreement_id.ERAID_FIELDS.contextid,
                                     ci->agreement_id.ERAID_FIELDS.epoch,
                                     ci->agreement_id.ERAID_FIELDS.agreementid));
                /* We are still waiting for a message from at least a child. Let's wait */
                return;
            }
        }

        /* Left that loop? We're good to decide locally */
        era_compute_local_return_value(ci);

        if( ci->comm->c_my_rank == (r = era_parent(ci)) ) {
            OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) check_status for Agreement ID = (%d.%d).%d, all children of root have contributed\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                 ci->agreement_id.ERAID_FIELDS.contextid,
                                 ci->agreement_id.ERAID_FIELDS.epoch,
                                 ci->agreement_id.ERAID_FIELDS.agreementid));

            /* I'm root. I have to decide now. */
            era_decide(&ci->current_value, ci);
        } else {
            OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) check_status for Agreement ID = (%d.%d).%d, all children of non-root have contributed\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                 ci->agreement_id.ERAID_FIELDS.contextid,
                                 ci->agreement_id.ERAID_FIELDS.epoch,
                                 ci->agreement_id.ERAID_FIELDS.agreementid));

            /* Let's forward up and wait for the DOWN messages */
            send_up_msg(ci, r);
            ci->status = BROADCASTING;
            ci->waiting_down_from = r;
        }
        return;
    }

    opal_output(0, "%s: %s (%s:%d) -- Need to implement function for that case (status = %d)\n",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                __FUNCTION__,
                __FILE__, __LINE__,
                ci->status);
    assert(0);
}

static void restart_agreement_from_me(era_agreement_info_t *ci)
{
    int r;
    era_rank_counter_item_t *rc;
    assert( NULL != ci->comm );
    assert( 0 == opal_list_get_size(&ci->waiting_res_from) );

    /* First of all, we start gathering information again */
    ci->status = GATHERING;

    /* Then, we request all the living guys that could have
     * received the information to send it back, or send the UP
     * back to their parent.
     * Eventually, this information will reach me and all children
     * will have contributed. OR somebody will have sent back
     * the DOWN message directly to me because it got the 
     * lost result
     */
    r = -1;
    while( (r = era_next_child(ci, r)) != ompi_comm_size(ci->comm) ) {
        rc = OBJ_NEW(era_rank_counter_item_t);
        rc->rank = r;
        rc->counter = 0;
        opal_list_append(&ci->waiting_res_from, &rc->super);
        send_msg(ci->comm, r, NULL, ci->agreement_id, MSG_RESULT_REQUEST, &ci->current_value, 
                 0, NULL);
    }

    /** I can become the root when all other living processes are
     *  already in my subtree. In that case, I might be able to decide
     *  now... 
     */
    era_check_status(ci);
}

static void era_mark_process_failed(era_agreement_info_t *ci, int rank)
{
    int r;
    era_rank_counter_item_t *rl;
    era_identifier_t era_id;

    if( ci->status > NOT_CONTRIBUTED ) {
        /* I may not have sent up yet (or I'm going to re-send up because of failures), 
         * and since I already contributed, this failure is not acknowledged yet
         * So, the return value should be MPI_ERR_PROC_FAILED.
         * Of course, if I have already contributed upward, the final return might still
         * be MPI_SUCCESS
         */
        OPAL_OUTPUT_VERBOSE((30,  ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Handling failure of process %d: Agreement (%d.%d).%d will have to return ERR_PROC_FAILED.\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             rank,
                             ci->agreement_id.ERAID_FIELDS.contextid,
                             ci->agreement_id.ERAID_FIELDS.epoch,
                             ci->agreement_id.ERAID_FIELDS.agreementid));
        ci->current_value.ret = MPI_ERR_PROC_FAILED;
    }
    if( ci->status == BROADCASTING ) {
        OPAL_OUTPUT_VERBOSE((20,  ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Handling failure of process %d: Agreement (%d.%d).%d is in the BROADCASTING state.\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             rank,
                             ci->agreement_id.ERAID_FIELDS.contextid,
                             ci->agreement_id.ERAID_FIELDS.epoch,
                             ci->agreement_id.ERAID_FIELDS.agreementid));
        /* We are waiting from the parent on that agreement...
         * Is it the one that died? */
        if( rank == ci->waiting_down_from ) {
            /* OK, let's send my contribution again to the new parent and see if that's better */
            r = era_parent(ci);
            if( r == ci->comm->c_my_rank ) {
                OPAL_OUTPUT_VERBOSE((20,  ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ERA) Handling failure of process %d: Restarting Agreement (%d.%d).%d as I am the new root.\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     rank,
                                     ci->agreement_id.ERAID_FIELDS.contextid,
                                     ci->agreement_id.ERAID_FIELDS.epoch,
                                     ci->agreement_id.ERAID_FIELDS.agreementid));
                /* Trouble: I'm becoming root, while I was waiting for this answer... 
                 * We need to check that nobody decided before, or if they connected to
                 * me as a child, to ask them to re-send their up message, because I might
                 * have ignored it, not knowing why they sent the message in the first place.
                 */
                restart_agreement_from_me(ci);
            } else {
                OPAL_OUTPUT_VERBOSE((20,  ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Handling failure of process %d: My parent changed to %d for Agreement (%d.%d).%d, sending the UP message to it\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     rank, r,
                                     ci->agreement_id.ERAID_FIELDS.contextid,
                                     ci->agreement_id.ERAID_FIELDS.epoch,
                                     ci->agreement_id.ERAID_FIELDS.agreementid));
                ci->waiting_down_from = r;
                send_up_msg(ci, r);
            }
        }
    } else if( ci->status == GATHERING ) {
        OPAL_OUTPUT_VERBOSE((20,  ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Handling failure of process %d: Agreement (%d.%d).%d is in the GATHERING state.\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             rank,
                             ci->agreement_id.ERAID_FIELDS.contextid,
                             ci->agreement_id.ERAID_FIELDS.epoch,
                             ci->agreement_id.ERAID_FIELDS.agreementid));
        OPAL_OUTPUT_VERBOSE((20,  ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Retaining agreement info for (%d.%d).%d while resolving failure during agreement.\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             ci->agreement_id.ERAID_FIELDS.contextid,
                             ci->agreement_id.ERAID_FIELDS.epoch,
                             ci->agreement_id.ERAID_FIELDS.agreementid));
        OBJ_RETAIN(ci);
        /* It could be one of the guys that we contacted about a restarting agreement. */
        for(rl = (era_rank_counter_item_t*)opal_list_get_first(&ci->waiting_res_from);
            rl != (era_rank_counter_item_t*)opal_list_get_end(&ci->waiting_res_from);
            rl = (era_rank_counter_item_t*)opal_list_get_next(&rl->super)) {
            if(rl->rank == rank) {
                OPAL_OUTPUT_VERBOSE((20,  ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ERA) Handling failure of process %d: I was waiting for the contribution of that process for Agreement (%d.%d).%d.\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     rank,
                                     ci->agreement_id.ERAID_FIELDS.contextid,
                                     ci->agreement_id.ERAID_FIELDS.epoch,
                                     ci->agreement_id.ERAID_FIELDS.agreementid));

                /* In that case, it could be bad, as it could create a new guy waiting for the
                 * result, or worse, the result previously computed could be under that subtree.
                 * Remove the guy from the list of waiting_res_from,
                 */
                opal_list_remove_item(&ci->waiting_res_from, &rl->super);

                /* and add its living children, requesting the result if it was
                 * not done before
                 */
                r = -1;
                while( (r = era_next_child(ci, r)) != ompi_comm_size(ci->comm) ) {
                    for(rl = (era_rank_counter_item_t*)opal_list_get_first(&ci->waiting_res_from);
                        rl != (era_rank_counter_item_t*)opal_list_get_end(&ci->waiting_res_from);
                        rl = (era_rank_counter_item_t*)opal_list_get_next(&rl->super)) {
                        if( rl->rank == r ) {
                            break;
                        }
                    }

                    if( rl == (era_rank_counter_item_t*)opal_list_get_end(&ci->waiting_res_from) ) {
                        OPAL_OUTPUT_VERBOSE((20,  ompi_ftmpi_output_handle,
                                             "%s ftbasic:agreement (ERA) Handling failure of process %d: Requesting contribution of process %d for Agreement (%d.%d).%d.\n",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             rank, r,
                                             ci->agreement_id.ERAID_FIELDS.contextid,
                                             ci->agreement_id.ERAID_FIELDS.epoch,
                                             ci->agreement_id.ERAID_FIELDS.agreementid));
                        rl = OBJ_NEW(era_rank_counter_item_t);
                        rl->rank = r;
                        rl->counter = 0;
                        opal_list_append(&ci->waiting_res_from, &rl->super);
                        send_msg(ci->comm, r, NULL, ci->agreement_id, MSG_RESULT_REQUEST, &ci->current_value, 
                                 0, NULL);
                    }
                }
                break;
            }
        }

        /* It could also be a child, that's also important but taken care of by check_status */
        era_check_status(ci);

        era_id = ci->agreement_id;
        OBJ_RELEASE(ci);
    }
}

static void message_sent(struct mca_btl_base_module_t* module,
                         struct mca_btl_base_endpoint_t* endpoint,
                         struct mca_btl_base_descriptor_t* descriptor,
                         int status)
{
    (void)module;
    (void)endpoint;
    (void)descriptor;
    (void)status;
}

static  void send_up_msg(era_agreement_info_t *ci, int rank)
{
    int *ack_rank_array, *co_rank_array;
    int  r, upsize;
    int  nb_afa;

    assert( NULL != ci->comm );

    nb_afa = ompi_group_size(ci->acked_group);

    upsize = MAX_ACK_FAILED_SIZE * (nb_afa/MAX_ACK_FAILED_SIZE + 1);

    co_rank_array = (int*)malloc(upsize * sizeof(int));
    for(r = nb_afa; r < upsize; r++)
        co_rank_array[r] = -1;

    if( nb_afa > 0 ) {
        ack_rank_array = (int*)malloc(nb_afa * sizeof(int));
        for(r = 0 ; r < nb_afa; r++)
            ack_rank_array[r] = r;
        ompi_group_translate_ranks(ci->acked_group, nb_afa, ack_rank_array, ci->comm->c_local_group, co_rank_array);
        free(ack_rank_array);
    }

    for(r = 0; r < upsize; r += MAX_ACK_FAILED_SIZE) {
        send_msg(ci->comm, rank, NULL, ci->agreement_id, MSG_UP, &ci->current_value, 
                 upsize / MAX_ACK_FAILED_SIZE, &co_rank_array[r]);
    }
    free(co_rank_array);
}

static void send_msg(ompi_communicator_t *comm,
                     int dst,
                     orte_process_name_t *proc_name,
                     era_identifier_t agreement_id,
                     era_msg_type_t type,
                     era_value_t *value,
                     int          nb_up_msg,
                     int         *ack_failed)
{
    mca_btl_base_descriptor_t *des;
    era_msg_t *src;
    ompi_proc_t *peer;
    mca_bml_base_endpoint_t *proc_bml;
    mca_bml_base_btl_t *bml_btl;
    struct mca_btl_base_endpoint_t *btl_endpoint;
    mca_btl_base_module_t *btl;

#if defined(PROGRESS_FAILURE_PROB)
#pragma message("Hard coded probability of failure inside the agreement")
    if( coll_ftbasic_era_debug_rank_may_fail &&
        (double)rand() / (double)RAND_MAX < PROGRESS_FAILURE_PROB ) {
        OPAL_OUTPUT_VERBOSE((0, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Killing myself just before sending message [(%d.%d).%d, %s, %08x.%d.%d..] to %d/%s\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             agreement_id.ERAID_FIELDS.contextid,
                             agreement_id.ERAID_FIELDS.epoch,
                             agreement_id.ERAID_FIELDS.agreementid,
                             era_msg_type_to_string(type),
                             *(int*)value->bytes,
                             value->ret,
                             value->new_dead[0],
                             dst,
                             NULL != proc_name ? ORTE_NAME_PRINT(proc_name) : "(null)"));
        raise(SIGKILL);
    }
#endif

    if( MSG_UP == type ) {
        OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) send message [(%d.%d).%d, %s, %08x.%d.%d.. %d UP msg, ADR: %d...] to %d/%s\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             agreement_id.ERAID_FIELDS.contextid,
                             agreement_id.ERAID_FIELDS.epoch,
                             agreement_id.ERAID_FIELDS.agreementid,
                             era_msg_type_to_string(type),
                             *(int*)value->bytes,
                             value->ret,
                             value->new_dead[0],
                             nb_up_msg,
                             ack_failed[0],
                             dst,
                             NULL != proc_name ? ORTE_NAME_PRINT(proc_name) : "(null)"));
    } else {
        OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) send message [(%d.%d).%d, %s, %08x.%d.%d..] to %d/%s\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             agreement_id.ERAID_FIELDS.contextid,
                             agreement_id.ERAID_FIELDS.epoch,
                             agreement_id.ERAID_FIELDS.agreementid,
                             era_msg_type_to_string(type),
                             *(int*)value->bytes,
                             value->ret,
                             value->new_dead[0],
                             dst,
                             NULL != proc_name ? ORTE_NAME_PRINT(proc_name) : "(null)"));
    }

    assert( NULL == comm || agreement_id.ERAID_FIELDS.contextid == ompi_comm_get_cid(comm) );
    assert( NULL == comm || agreement_id.ERAID_FIELDS.epoch == comm->c_epoch );

    if( NULL == comm ) {
        assert(NULL != proc_name);
        peer = ompi_proc_find ( proc_name );
    } else {
        peer = ompi_comm_peer_lookup(comm, dst);
    }
    assert(NULL != peer);

    proc_bml = peer->proc_bml;
    assert(NULL != proc_bml);
    bml_btl = mca_bml_base_btl_array_get_index(&proc_bml->btl_eager, 0);
    assert(NULL != bml_btl);
    btl_endpoint = bml_btl->btl_endpoint;
    assert(NULL != btl_endpoint);
    btl = bml_btl->btl;
    assert(NULL != btl);

    if( MSG_UP == type ) {
        des = btl->btl_alloc(btl, btl_endpoint, MCA_BTL_NO_ORDER, sizeof(era_msg_t),
                             MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
    } else {
        des = btl->btl_alloc(btl, btl_endpoint, MCA_BTL_NO_ORDER, 
                             sizeof(era_msg_t) - sizeof(int) * (MAX_ACK_FAILED_SIZE + 1),
                             MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
    }
    des->des_cbfunc = message_sent;
    des->des_cbdata = NULL;
    src = (era_msg_t*)des->des_src->seg_addr.pval;
    src->msg_type = type;
    src->agreement_id.ERAID_KEY = agreement_id.ERAID_KEY;
    if( NULL != comm ) {
        src->src_comm_rank = ompi_comm_rank(comm);
    } else {
        src->src_comm_rank = -1;
    }
    src->src_proc_name = *ORTE_PROC_MY_NAME;
    memcpy(&src->agreement_value, value, sizeof(era_value_t));
    if( MSG_UP == type ) {
        src->nb_up_messages = nb_up_msg;
        memcpy(&src->ack_failed, ack_failed, MAX_ACK_FAILED_SIZE * sizeof(int));
    }
    btl->btl_send(btl, btl_endpoint, des, MCA_BTL_TAG_FT_AGREE);
}

static void result_request(era_msg_t *msg)
{
    void *value;
    era_value_t *old_agreement_value;
    era_agreement_info_t *ci;
    int r;

    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received RESULT_REQUEST Message: Agreement ID = (%d.%d).%d, sender: %d/%s\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         msg->agreement_id.ERAID_FIELDS.contextid,
                         msg->agreement_id.ERAID_FIELDS.epoch,
                         msg->agreement_id.ERAID_FIELDS.agreementid,
                         msg->src_comm_rank,
                         ORTE_NAME_PRINT(&msg->src_proc_name)));

    if( opal_hash_table_get_value_uint64(&era_passed_agreements,
                                         msg->agreement_id.ERAID_KEY,
                                         &value) == OPAL_SUCCESS ) {
        old_agreement_value = &((era_passed_agreement_t*)value)->agreement_value;
        send_msg(NULL, msg->src_comm_rank, &msg->src_proc_name, msg->agreement_id, MSG_DOWN, old_agreement_value, 0, NULL);
    } else {
        /** I should be a descendent of msg->src (since RESULT_REQUEST messages are sent to
         *  people below the caller.
         *  So, the caller is the current root (or it is dead now and a new root was selected)
         *  Two cases: */
        
        ci = era_lookup_agreeement_info(msg->agreement_id);
        if( NULL != ci &&
            ci->status == BROADCASTING ) {
            /** if I am in this agreement, in the BROADCASTING state, then I need
             *  to start working with my parent again, so that the info reaches the root, eventually.
             *  There is in fact a good chance that this guy is my parent, but not in all cases,
             *  so I send UP again to my parent, and we'll see what happens.
             */
            assert(ci->comm != NULL);
            r = era_parent(ci);
            if(r == ci->comm->c_my_rank) {
                /** OK, weird case: a guy sent me that request, but died before I answered, and I receive it now... */
                /** I will deal with that when I deal with the failure notification, and start again */
                return;
            }
            send_up_msg(ci, r);
            ci->waiting_down_from = r;
        } else {
            /** Or, I have not started this agreement, or I have started this agreement, but a child
             *  has not given me its contribution. So, I need to wait for it to send it to me, and
             *  then I will send my UP message to the parent, so it can wait the normal step in 
             *  the protocol
             */
            return;
        }
    }
}

static void msg_up(era_msg_t *msg)
{
    era_agreement_info_t *ci;
    era_rank_counter_item_t *rank_item;
    void *value;
    era_value_t *old_agreement_value;
    int r;
 
    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received UP Message: Agreement ID = (%d.%d).%d, sender: %d/%s, msg value: %08x.%d.%d.. %d up msg, ADR: %d...\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         msg->agreement_id.ERAID_FIELDS.contextid,
                         msg->agreement_id.ERAID_FIELDS.epoch,
                         msg->agreement_id.ERAID_FIELDS.agreementid,
                         msg->src_comm_rank,
                         ORTE_NAME_PRINT(&msg->src_proc_name),
                         *(int*)msg->agreement_value.bytes,
                         msg->agreement_value.ret,
                         msg->agreement_value.new_dead[0],
                         msg->nb_up_messages,
                         msg->ack_failed[0]));

    /** It could be an UP message about a decided agreement:
     *  a child gives me its contribution, I broadcast and receive
     *  the decision, or decide myself, and then the child dies before
     *  it could transmit the decision to its own children. The children
     *  will contact me as their new parent, still in their BROADCAST phase,
     *  so what this UP message really means is "give me the decision." */
    if( opal_hash_table_get_value_uint64(&era_passed_agreements,
                                         msg->agreement_id.ERAID_KEY,
                                         &value) == OPAL_SUCCESS ) {
        old_agreement_value = &((era_passed_agreement_t*)value)->agreement_value;
        send_msg(NULL, msg->src_comm_rank, &msg->src_proc_name, msg->agreement_id, MSG_DOWN, old_agreement_value, 
                 0, NULL);
        return;
    }

    ci = era_lookup_agreeement_info( msg->agreement_id );

    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Managing UP Message, agreement is %s\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ci == NULL ? "unknown" : "known"));

    if( NULL == ci ) {
        ci = era_create_agreement_info( msg->agreement_id );
        /* We will attach the communicator when we contribute to it */
    }

    if( ci->status == BROADCASTING ) {
        /** This can happen: a child gives me its contribution,
         *  I enter the broadcast phase, then it dies; its children 
         *  have not received the decision yet, I haven't received the
         *  decision yet, so they send me their contribution again,
         *  and I receive this UP message while in BROADCASTING state.
         *  The children contributions have been taken into account already.
         *  Just in case we are slow at having the same view parent / child
         *  as this guy, let's remember it requested to receive the answer
         *  directly.
         */
        OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Managing UP Message -- Already in BROADCASTING state: ignoring message, adding %d in the requesters\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             msg->src_comm_rank));

        /** We could receive multiple messages from msg->src_comm_rank, because the messages are split */
        for(rank_item = (era_rank_counter_item_t*)opal_list_get_first(&ci->early_requesters);
            rank_item != (era_rank_counter_item_t*)opal_list_get_end(&ci->early_requesters);
            rank_item = (era_rank_counter_item_t*)opal_list_get_next(&rank_item->super)) {
            if( rank_item->rank == msg->src_comm_rank )
                return;
        }

        /** If not, add it */
        rank_item = OBJ_NEW(era_rank_counter_item_t);
        rank_item->rank = msg->src_comm_rank;
        rank_item->counter = 0;
        opal_list_append(&ci->early_requesters, &rank_item->super);
        return;
    }

    /** Did we receive enough contributions from that rank already?
     *  He could be re-sending his data, because of some failure that
     *  was discovered, and I requested it because I became root (because
     *  of another failure), but it discovered the first failure at the 
     *  same time, and started sending without me requesting.
     */
    for( rank_item = (era_rank_counter_item_t *)opal_list_get_first( &ci->gathered_info );
         rank_item != (era_rank_counter_item_t *)opal_list_get_end( &ci->gathered_info );
         rank_item = (era_rank_counter_item_t *)opal_list_get_next( &rank_item->super ) ) {
        if( rank_item->rank == msg->src_comm_rank ) {
            break; /* Ah ah: we heard from you already */
        }
    }
    if( rank_item != (era_rank_counter_item_t *)opal_list_get_end( &ci->gathered_info ) ) {
        if( rank_item->counter == 0 ) {
            /* We are not waiting from more messages, thank you */
            /* NOTE: there may be a race here... What if we received twice the packet 0, and no of the packets 1?...
             * We can have the wrong list of ADR...
             * Check this carefully.
             */
            era_check_status(ci);
            return;
        }
    }

    /* ci holds the current agreement information structure */
    era_combine_agreement_values(ci, &msg->agreement_value);

    /* Handle the list of acknowledged dead processes: merge all the upcoming information,
     * and remember that this message was received
     */
    for(r = 0; r < MAX_ACK_FAILED_SIZE; r++) {
        if( msg->ack_failed[r] == -1 ) {
            break;
        }
        for(rank_item = (era_rank_counter_item_t *)opal_list_get_first( &ci->union_of_dead_ack );
            rank_item != (era_rank_counter_item_t *)opal_list_get_end( &ci->union_of_dead_ack );
            rank_item = (era_rank_counter_item_t *)opal_list_get_next( &rank_item->super ) ) {
            if( rank_item->rank == msg->ack_failed[r] ) {
                rank_item->counter++;
                break;
            }
        }
        if( rank_item == (era_rank_counter_item_t *)opal_list_get_end( &ci->union_of_dead_ack ) ) {
            rank_item = OBJ_NEW(era_rank_counter_item_t);
            rank_item->rank = msg->ack_failed[r];
            rank_item->counter = 1;
            opal_list_append(&ci->union_of_dead_ack, &rank_item->super);
        }
    }

    for( rank_item = (era_rank_counter_item_t *)opal_list_get_first( &ci->gathered_info );
         rank_item != (era_rank_counter_item_t *)opal_list_get_end( &ci->gathered_info );
         rank_item = (era_rank_counter_item_t *)opal_list_get_next( &rank_item->super ) ) {
        if( rank_item->rank == msg->src_comm_rank ) {
            assert(rank_item->counter > 0);
            rank_item->counter--;
            OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) Received UP Message: found %d in list of people that contributed (wait for %d more)\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 rank_item->rank,
                                 rank_item->counter));
            break;
        }
    }
    if( rank_item == (era_rank_counter_item_t *)opal_list_get_end( &ci->gathered_info ) ) {
        rank_item = OBJ_NEW(era_rank_counter_item_t);
        rank_item->rank = msg->src_comm_rank;
        rank_item->counter = msg->nb_up_messages - 1;
        opal_list_append(&ci->gathered_info, &rank_item->super);
        OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Received UP Message: adding %d in list of people that contributed (wait for %d more)\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             rank_item->rank,
                             rank_item->counter));
    }

    era_check_status(ci);
}

static void msg_down(era_msg_t *msg)
{
    era_agreement_info_t *ci;
 
    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received DOWN Message: Agreement ID = (%d.%d).%d, sender: %d/%s, msg value: %08x.%d.%d..\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         msg->agreement_id.ERAID_FIELDS.contextid,
                         msg->agreement_id.ERAID_FIELDS.epoch,
                         msg->agreement_id.ERAID_FIELDS.agreementid,
                         msg->src_comm_rank,
                         ORTE_NAME_PRINT(&msg->src_proc_name),
                         *(int*)msg->agreement_value.bytes,
                         msg->agreement_value.ret,
                         msg->agreement_value.new_dead[0]));

    ci = era_lookup_agreeement_info( msg->agreement_id );
    if( NULL == ci ) {
        /** This can happen, if this DOWN is the result of a REQUEST, and
         *  we received another DOWN from another sent REQUEST, and we
         *  decided, so stored that agreement in the passed_agreements
         */
#if defined(OPAL_ENABLE_DEBUG)
        void *value;
        assert( opal_hash_table_get_value_uint64(&era_passed_agreements,
                                                 msg->agreement_id.ERAID_KEY,
                                                 &value) == OPAL_SUCCESS );
#endif
        return;
    }
    /** if I receive a down message on an agreement I know about, I already participated. */
    assert( NULL != ci->comm );

    era_decide(&msg->agreement_value, ci);
}

static void era_cb_fn(struct mca_btl_base_module_t* btl,
                      mca_btl_base_tag_t tag,
                      mca_btl_base_descriptor_t* descriptor,
                      void* cbdata)
{
    era_msg_t *msg;

    assert(MCA_BTL_TAG_FT_AGREE == tag);
    assert(1 == descriptor->des_dst_cnt);
    (void)cbdata;

    msg = (era_msg_t*)descriptor->des_dst->seg_addr.pval;

#if defined(PROGRESS_FAILURE_PROB)
#pragma message("Hard coded probability of failure inside the agreement")
    if( coll_ftbasic_era_debug_rank_may_fail &&
        (double)rand() / (double)RAND_MAX < PROGRESS_FAILURE_PROB ) {
        OPAL_OUTPUT_VERBOSE((0, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Killing myself just before receiving message [(%d.%d).%d, %d, %08x.%d.%d...] from %d/%s\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             msg->agreement_id.ERAID_FIELDS.contextid,
                             msg->agreement_id.ERAID_FIELDS.epoch,
                             msg->agreement_id.ERAID_FIELDS.agreementid,
                             msg->msg_type,
                             *(int*)msg->agreement_value.bytes,
                             msg->agreement_value.ret,
                             msg->agreement_value.new_dead[0],
                             msg->src_comm_rank,
                             ORTE_NAME_PRINT(&msg->src_proc_name)));
        raise(SIGKILL);
    }
#endif
    
    switch( msg->msg_type ) {
    case MSG_RESULT_REQUEST:
        result_request(msg);
        return;
    case MSG_UP:
        msg_up(msg);
        return;
    case MSG_DOWN:
        msg_down(msg);
        return;
    }
}

static void era_on_comm_rank_failure(ompi_communicator_t *comm, int rank, bool remote)
{
    void *value, *next_value;
    era_agreement_info_t *ci;
    void *node;
    uint64_t key64;
    int rc;
    era_identifier_t cid;

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) %d in communicator (%d.%d) died\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank,
                         comm->c_contextid,
                         comm->c_epoch));

    if( opal_hash_table_get_first_key_uint64(&era_ongoing_agreements,
                                             &key64,
                                             &value, &node) == OPAL_SUCCESS ) {
        do {
            cid.ERAID_KEY = key64;

            /** We need to get the next now, because era_mark_process_failed may remove ci from the hash table */
            rc = opal_hash_table_get_next_key_uint64(&era_ongoing_agreements,
                                                     &key64, &next_value, 
                                                     node, &node);

            if( cid.ERAID_FIELDS.contextid == comm->c_contextid &&
                cid.ERAID_FIELDS.epoch     == comm->c_epoch ) {
                ci = (era_agreement_info_t *)value;
                OPAL_OUTPUT_VERBOSE((6, ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ERA) Agreement ID (%d.%d).%d, rank %d died while doing the agreement\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                     ci->agreement_id.ERAID_FIELDS.contextid,
                                     ci->agreement_id.ERAID_FIELDS.epoch,
                                     ci->agreement_id.ERAID_FIELDS.agreementid,
                                     rank));
                era_mark_process_failed(ci, rank);
            }
            
            value = next_value;
        } while( rc == OPAL_SUCCESS );
    }
    
    if( NULL != ompi_stacked_rank_failure_callback_fct )
        (*ompi_stacked_rank_failure_callback_fct)(comm, rank, remote);
}

int mca_coll_ftbasic_agreement_era_init(void)
{
    if( era_inited ) {
        return OMPI_SUCCESS;
    }
    
    mca_bml.bml_register(MCA_BTL_TAG_FT_AGREE, era_cb_fn, NULL);

    OBJ_CONSTRUCT( &era_passed_agreements, opal_hash_table_t);
    opal_hash_table_init(&era_passed_agreements, 65537 /* Big Storage */);
    OBJ_CONSTRUCT( &era_ongoing_agreements, opal_hash_table_t);
    opal_hash_table_init(&era_ongoing_agreements, 16 /* We expect only a few */);
    
    ompi_stacked_rank_failure_callback_fct = ompi_rank_failure_cbfunc;
    ompi_rank_failure_cbfunc = era_on_comm_rank_failure;

    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Initialized\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    era_inited = 1;

    return OMPI_SUCCESS;
}

int mca_coll_ftbasic_agreement_era_finalize(void)
{
    void *node;
    void *value;
    uint64_t key64;
    era_passed_agreement_t    *old_agreement;
    era_agreement_info_t      *un_agreement;

    if( !era_inited ) {
        return OMPI_SUCCESS;
    }

    ompi_rank_failure_cbfunc = ompi_stacked_rank_failure_callback_fct;

    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Finalizing\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    if( opal_hash_table_get_first_key_uint64(&era_passed_agreements,
                                             &key64,
                                             &value, &node) == OPAL_SUCCESS ) {
        do {
            old_agreement = (era_passed_agreement_t *)value;
            OBJ_RELEASE(old_agreement);
        } while( opal_hash_table_get_next_key_uint64(&era_passed_agreements,
                                                     &key64, &value, 
                                                     node, &node) == OPAL_SUCCESS );
    }
    OBJ_DESTRUCT( &era_passed_agreements );

    if( opal_hash_table_get_first_key_uint64(&era_ongoing_agreements,
                                             &key64,
                                             &value, &node) == OPAL_SUCCESS ) {
        do {
            un_agreement = (era_agreement_info_t *)value;
            opal_output(0, "%s ftbasic:agreement (ERA) Error: Agreement ID (%d.%d).%d was started by some processor, but I never completed to it\n",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                        un_agreement->agreement_id.ERAID_FIELDS.contextid,
                        un_agreement->agreement_id.ERAID_FIELDS.epoch,
                        un_agreement->agreement_id.ERAID_FIELDS.agreementid);
            OBJ_RELEASE(un_agreement);
        } while( opal_hash_table_get_next_key_uint64(&era_ongoing_agreements,
                                                     &key64, &value, 
                                                     node, &node) == OPAL_SUCCESS );
    }
    OBJ_DESTRUCT( &era_ongoing_agreements );

    era_inited = 0;

    return OMPI_SUCCESS;
}

/*
 *	mca_coll_ftbasic_agreement_era_intra
 *
 *	Function:	- MPI_Comm_agree()
 *	Accepts:	- same as MPI_Comm_agree()
 *	Returns:	- MPI_SUCCESS or an MPI error code
 */

int mca_coll_ftbasic_agreement_era_intra(ompi_communicator_t* comm,
                                         ompi_group_t **group,
                                         int *flag,
                                         mca_coll_base_module_t *module)
{
    era_agreement_info_t *ci;
    era_identifier_t agreement_id;
    void *value;
    era_value_t agreement_value;
    era_passed_agreement_t *pa;
    mca_coll_ftbasic_agreement_t *ag_info;
    int r;

    ag_info = ( (mca_coll_ftbasic_module_t *)module )->agreement_info;
    assert( NULL != ag_info );

    /** Avoid cycling silently */
    if( ag_info->agreement_seq_num == UINT16_MAX ) {
        ag_info->agreement_seq_num = 1;
    } else {
        ag_info->agreement_seq_num++;
    }
    
    /* Let's find the id of the new agreement */
    agreement_id.ERAID_FIELDS.contextid   = comm->c_contextid;
    agreement_id.ERAID_FIELDS.epoch       = comm->c_epoch;
    agreement_id.ERAID_FIELDS.agreementid = (uint16_t)ag_info->agreement_seq_num;

    OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Entering Agreement ID = (%d.%d).%d\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         agreement_id.ERAID_FIELDS.contextid,
                         agreement_id.ERAID_FIELDS.epoch,
                         agreement_id.ERAID_FIELDS.agreementid));
    era_debug_print_group(1, *group, comm, "Before Agreement");

#if defined(PROGRESS_FAILURE_PROB)
#pragma message("Hard coded probability of failure inside the agreement")
    if( coll_ftbasic_era_debug_rank_may_fail &&
        (double)rand() / (double)RAND_MAX < PROGRESS_FAILURE_PROB ) {
        OPAL_OUTPUT_VERBOSE((0, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Killing myself just before entering the agreement (%d.%d).%d\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             agreement_id.ERAID_FIELDS.contextid,
                             agreement_id.ERAID_FIELDS.epoch,
                             agreement_id.ERAID_FIELDS.agreementid));
        raise(SIGKILL);
    }
#endif

    /* Let's create or find the current value */
    ci = era_lookup_agreeement_info(agreement_id);
    if( NULL == ci ) {
        ci = era_create_agreement_info(agreement_id);
    }

    assert( NULL == ci->comm );
    assert( NULL != group && NULL != *group );
    era_agreement_info_set_comm(ci, comm, *group);
    
    if( opal_hash_table_get_value_uint64(&era_passed_agreements, agreement_id.ERAID_KEY, &value) == OMPI_SUCCESS ) {
        OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) removing old agreement (%d.%d).%d from history, due to cycling of identifiers\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             agreement_id.ERAID_FIELDS.contextid,
                             agreement_id.ERAID_FIELDS.epoch,                         
                             agreement_id.ERAID_FIELDS.agreementid));
        pa = (era_passed_agreement_t*)value;
        opal_hash_table_remove_value_uint64(&era_passed_agreements, agreement_id.ERAID_KEY);
        OBJ_RELEASE(pa);
    }

    /* I participate */
    *(int*)agreement_value.bytes = *flag;
    agreement_value.operand = ompi_mpi_op_band.op.o_f_to_c_index;
    agreement_value.dt_count = 1;
    agreement_value.datatype = ompi_mpi_int.dt.id;
    for(r = 0; r < MAX_NEW_DEAD_SIZE; r++)
        agreement_value.new_dead[r] = -1;
    agreement_value.ret = 0;

    era_combine_agreement_values(ci, &agreement_value);

    /* I start the state machine */
    ci->status = GATHERING;

    /* And follow its logic */
    era_check_status(ci);

    /* Wait for the agreement to be resolved */
    while(1) {
        opal_progress();
        if( opal_hash_table_get_value_uint64(&era_passed_agreements,
                                             agreement_id.ERAID_KEY,
                                             &value) == OPAL_SUCCESS ) {
            agreement_value = ((era_passed_agreement_t*)value)->agreement_value;
            break;
        }
    } 
    
    *flag = *(int*)agreement_value.bytes;
    
    /* Update the group of failed processes */
    if(NULL != group) {
        OBJ_RELEASE(*group);
    }
    *group = comm->agreed_failed_ranks;
    OBJ_RETAIN(comm->agreed_failed_ranks);
    era_debug_print_group(1, *group, comm, "After Agreement");

    OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Leaving Agreement ID = (%d.%d).%d with %d.%d\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         agreement_id.ERAID_FIELDS.contextid,
                         agreement_id.ERAID_FIELDS.epoch,
                         agreement_id.ERAID_FIELDS.agreementid,
                         *flag,
                         agreement_value.ret));

    return agreement_value.ret;
}
