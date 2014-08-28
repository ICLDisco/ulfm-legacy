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
#include "ompi/mca/bml/base/base.h"
#include "orte/mca/errmgr/errmgr.h"

#include MCA_timer_IMPLEMENTATION_HEADER
#include "coll_ftbasic.h"
static opal_hash_table_t era_passed_agreements;
static opal_hash_table_t era_ongoing_agreements;
static opal_hash_table_t known_epoch_for_cid;
static int era_inited = 0;
static ompi_comm_rank_failure_callback_t *ompi_stacked_rank_failure_callback_fct = NULL;

typedef enum {
    MSG_UP = 1,
    MSG_DOWN,
    MSG_RESULT_REQUEST,
    MSG_UNDECIDED
} era_msg_type_t;

/**
 * This enum defines the status of the current process
 * when the consensus has not been decided yet
 */
typedef enum {
    NOT_CONTRIBUTED = 1,
    GATHERING,
    BROADCASTING
} era_proc_status_t;

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

typedef struct {
    int32_t  bits; /* Bitwise on 32 bits */
    int32_t  ret;  /* Return code */
    int      new_dead[MAX_NEW_DEAD_SIZE];    /**< -1 terminated bounded list of newly
                                              *      discovered dead ranks */
} era_value_t;

#define ERA_VALUE_SET_UNDEF(_e) do {            \
        (_e).bits = -1;                         \
        (_e).ret  = -1;                         \
    } while(0)
#define ERA_VALUE_IS_UNDEF(_e)    ( (_e).bits == -1 && (_e).ret == -1 )
#define ERA_VALUE_SET_NEUTRAL(_e) do {                              \
        (_e).bits = ~((int32_t)0);                                  \
        (_e).ret  = OMPI_SUCCESS;                                   \
        memset((_e).new_dead, -1, MAX_NEW_DEAD_SIZE*sizeof(int));   \
    } while(0)
#define ERA_VALUE_EQUAL(_a, _b)   ( (_a).bits == (_b).bits && (_a).ret == (_b).ret )

typedef struct {
    era_msg_type_t   msg_type;
    era_identifier_t agreement_id;
    era_value_t      agreement_value;
    union {
        int                 comm_based;
        orte_process_name_t proc_name_based;
    } src;
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

OBJ_CLASS_INSTANCE(era_rank_counter_item_t, opal_list_item_t, NULL, NULL);

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
    int                   ack_offset;        /**< Offset of the last acknowledged process when entering
                                              *   the agreement. Used to compute the acknowledged group,
                                              *   and compare with the descendents acknowledged processes */
    opal_list_t           gathered_info;     /**< The list of direct descendents that provided information (even partially) */
    opal_list_t           union_of_dead_ack; /**< The list of dead processes acknowledged by the descendents */
    int                   waiting_down_from; /**< If in BROADCAST state: who is supposed to send me the info */
} era_agreement_info_t;

static void  era_agreement_info_constructor (era_agreement_info_t *agreement_info)
{
    agreement_info->agreement_id.ERAID_KEY = 0;
    ERA_VALUE_SET_NEUTRAL(agreement_info->current_value);
    agreement_info->status = NOT_CONTRIBUTED;
    agreement_info->comm = NULL;
    agreement_info->waiting_down_from = -1;
    OBJ_CONSTRUCT(&agreement_info->gathered_info, opal_list_t);
    OBJ_CONSTRUCT(&agreement_info->union_of_dead_ack, opal_list_t);
    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Constructing Agreement Info %p\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (void*)agreement_info));
}

static void  era_agreement_info_destructor (era_agreement_info_t *agreement_info)
{
    opal_list_item_t *li;
    ompi_communicator_t *comm;
    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Destructing Agreement Info %p\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         (void*)agreement_info));
    while( NULL != (li = opal_list_remove_first(&agreement_info->gathered_info)) ) {
        OBJ_RELEASE(li);
    }
    OBJ_DESTRUCT(&agreement_info->gathered_info);
    while( NULL != (li = opal_list_remove_first(&agreement_info->union_of_dead_ack)) ) {
        OBJ_RELEASE(li);
    }
    OBJ_DESTRUCT(&agreement_info->union_of_dead_ack);
    if( NULL != agreement_info->comm ) {
        comm = agreement_info->comm;
        OBJ_RELEASE(comm);
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

static void era_agreement_info_set_comm(era_agreement_info_t *ci, ompi_communicator_t *comm)
{
    ompi_group_t *new_deads;
    int *nd_rank_array;
    int *co_rank_array;
    int r, p, nd_size;

    assert( comm->c_contextid == ci->agreement_id.ERAID_FIELDS.contextid );
    assert( comm->c_epoch     == ci->agreement_id.ERAID_FIELDS.epoch     );
    assert( ci->comm          == NULL                                    );
    ci->comm = comm;
    OBJ_RETAIN(comm);

    ci->ack_offset = comm->any_source_offset; /**< Keep a copy of the value when entering the agreement.
                                               *   It is important to do it now, when doing a split agreement with iAgree. */

    new_deads = OBJ_NEW(ompi_group_t);
    ompi_group_difference(ompi_group_all_failed_procs, comm->agreed_failed_ranks, &new_deads);
    nd_size = ompi_group_size(new_deads);
    if( nd_size > 0 ) {
        nd_rank_array = (int*)malloc(nd_size * sizeof(int));
        co_rank_array = (int*)malloc(nd_size * sizeof(int));
        for(r = 0 ; r < nd_size; r++)
            nd_rank_array[r] = r;
        ompi_group_translate_ranks(new_deads, nd_size, nd_rank_array, comm->c_local_group, co_rank_array);
        for(r = 0, p = 0; r < nd_size && p < MAX_NEW_DEAD_SIZE; r++) {
            if( co_rank_array[r] == MPI_UNDEFINED )
                continue; /**< This new dead process does not belong to the associated communicator */
            ci->current_value.new_dead[p++] = co_rank_array[r];
            OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) add rank %d as a new discovered dead process on agreement (%d.%d).%d\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 co_rank_array[r],
                                 ci->agreement_id.ERAID_FIELDS.contextid,
                                 ci->agreement_id.ERAID_FIELDS.epoch,                         
                                 ci->agreement_id.ERAID_FIELDS.agreementid));
        }
        free(nd_rank_array);
        free(co_rank_array);
    }
    OBJ_RELEASE(new_deads);
}

static uint32_t get_epoch_for_cid(uint32_t contextid)
{
    era_seq_num_record_t *rec;
    void *value;
    if( opal_hash_table_get_value_uint32(&known_epoch_for_cid, contextid, &value) == OMPI_SUCCESS ) {
        rec = (era_seq_num_record_t *)value;
        return rec->max_seq_num;
    }
    return 0;
}

static void update_epoch_for_cid(uint32_t contextid, uint32_t value)
{
    era_seq_num_record_t *rec;
    void *v;
    if( opal_hash_table_get_value_uint32(&known_epoch_for_cid, contextid, &v) == OMPI_SUCCESS ) {
        rec = (era_seq_num_record_t *)v;
        if( rec->max_seq_num < value )
            rec->max_seq_num = value;
    } else {
        rec = OBJ_NEW(era_seq_num_record_t);
        rec->max_seq_num = value;
        opal_hash_table_set_value_uint32(&known_epoch_for_cid, contextid, (void*)rec);
    }
}

int mca_coll_ftbasic_agreement_era_comm_init(ompi_communicator_t *comm, mca_coll_ftbasic_module_t *module)
{
    mca_coll_ftbasic_agreement_t *comm_ag_info;
    uint32_t known_epoch;

    comm_ag_info = OBJ_NEW(mca_coll_ftbasic_agreement_t);
    comm_ag_info->agreement_seq_num = 0;

    known_epoch = get_epoch_for_cid(comm->c_contextid);
    if( known_epoch >= comm->c_epoch ) {
        comm->c_epoch = known_epoch + 1;
    }
    update_epoch_for_cid(comm->c_contextid, comm->c_epoch);

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

static void send_comm_msg(ompi_communicator_t *comm,                          
                          int dst, 
                          era_identifier_t agreement_id,
                          era_msg_type_t type,
                          era_value_t *value,
                          int nb_up_msg,
                          int *ack_failed);

#define ERA_TAG_AGREEMENT MCA_COLL_BASE_TAG_AGREEMENT

static void era_combine_agreement_values(era_agreement_info_t *ni, era_value_t *value)
{
    int r, ir, to_insert, tmp;

    ni->current_value.bits &= value->bits;
    if( value->ret > ni->current_value.ret )
        ni->current_value.ret = value->ret;

    /* Merge the new_dead lists: keep the lowest ranks that belong to both,
     * knowing that -1 means end of list
     */
    for(r = 0, ir = 0; r < MAX_NEW_DEAD_SIZE && ir < MAX_NEW_DEAD_SIZE; r++) {
        if( value->new_dead[r] == -1 ) {
            break;
        }
        to_insert = value->new_dead[r];
        for(; ir < MAX_NEW_DEAD_SIZE; ir++) {
            if( ni->current_value.new_dead[ir] == -1 ) {
                ni->current_value.new_dead[ir] = to_insert;
                ir++;
                break;
            } else if( ni->current_value.new_dead[ir] > to_insert ) {
                tmp = ni->current_value.new_dead[ir];
                ni->current_value.new_dead[ir] = to_insert;
                to_insert = tmp;
            }
        }
    }
}

static void mark_as_undecided(era_agreement_info_t *ci, int rank)
{
    opal_output(0, "%s: %s (%s:%d) -- Need to implement function\n",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                __FUNCTION__,
                __FILE__, __LINE__);
    exit(0);
}

#define ERA_TOPOLOGY_BINARY_TREE

#if defined ERA_TOPOLOGY_BINARY_TREE
static int find_biggest_alive_lower_than_in_subtree(ompi_communicator_t *comm, int m, int sr)
{
    int c;
    if( sr >= comm->c_local_group->grp_proc_count )
        return -1;

    if( sr >= m )
        return -1; /**< Everything in that subtree is bigger than m */

    if( ompi_comm_is_proc_active(comm, sr, false) && sr < m )
        return sr;

    /* Look in right-hand tree */
    c = find_biggest_alive_lower_than_in_subtree(comm, m, sr*2 + 2);
    if( c >= 0 )
        return c; /**< Right hand tree is larger than left hand tree */

    c = find_biggest_alive_lower_than_in_subtree(comm, m, sr*2 + 1);
    if( c >= 0 )
        return c;

    return -1;
}

static int bt_parent(ompi_communicator_t *comm, int r) {
    int p;
    int st;

    if(r == 0) {
        return 0;
    }
    p = (r-1) / 2;
    if( !ompi_comm_is_proc_active(comm, p, false) ) {
        st = p;
        do {
            p = find_biggest_alive_lower_than_in_subtree(comm, r, st);
            if( st == 0 )
                break;
            st = (st-1)/2;
        } while(p == -1);
        if( p == -1 )
            p = r;
    }
    return p;
}

static int bt_children(ompi_communicator_t *comm, int r, int prev_child) {
    int next_child;
    if( prev_child == -1 )
        next_child = r+1;
    else
        next_child = prev_child + 1;

    /** Anybody with an ID > r is a potentation child */
    while(next_child < comm->c_local_group->grp_proc_count) {
        if( ompi_comm_is_proc_active(comm, next_child, false) && r == bt_parent(comm, next_child) )
            break;
        next_child++;
    }

    return next_child;
}

static int era_parent(era_agreement_info_t *ci)
{
    ompi_communicator_t *comm = ci->comm;

    return bt_parent(comm, comm->c_my_rank);
}

static int era_next_child(era_agreement_info_t *ci, int prev_child)
{
    ompi_communicator_t *comm = ci->comm;
    return bt_children(comm, comm->c_my_rank, prev_child);
}
#endif

#if defined ERA_TOPOLOGY_STAR
static int era_parent(era_agreement_info_t *ci)
{
    (void)ci;
    return 0;
}

static int era_next_child(era_agreement_info_t *ci, int prev_child)
{
    ompi_communicator_t *comm = ci->comm;
    if( comm->c_my_rank == 0 ) {
        if( prev_child == -1 ) return 1;
        return prev_child + 1;
    }
    return ompi_comm_size(comm);
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
    int r;

    da = OBJ_NEW(era_passed_agreement_t);
    memcpy(&da->agreement_value, decided_value, sizeof(era_value_t));

    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) decide %08x.%d.%d.. on agreement (%d.%d).%d\n",                         
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         decided_value->bits,
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
        for(r = 0; r < MAX_NEW_DEAD_SIZE && decided_value->new_dead[0] != -1; r++)
            /*nothing*/;
        new_deads_group = OBJ_NEW(ompi_group_t);
        new_agreed      = OBJ_NEW(ompi_group_t);
        ompi_group_incl(comm->c_local_group, r, decided_value->new_dead, &new_deads_group);
        ompi_group_union(comm->agreed_failed_ranks, new_deads_group, &new_agreed);
        OBJ_RELEASE(comm->agreed_failed_ranks);
        comm->agreed_failed_ranks = new_agreed;
        OBJ_RELEASE(new_deads_group);
    }

    r = -1;
    while( (r = era_next_child(ci, r)) < ompi_comm_size(comm) ) {
        send_comm_msg(comm, r, ci->agreement_id, MSG_DOWN, &da->agreement_value, 0, NULL);
    }

    OBJ_RELEASE(ci); /* This will take care of the content of ci too */
}

#if OPAL_ENABLE_DEBUG
static char *era_status_to_string(era_proc_status_t s) {
    switch(s) {
    case NOT_CONTRIBUTED:
        return "NOT_CONTRIBUTED";
    case GATHERING:
        return "GATHERING";
    case BROADCASTING:
        return "BROADCASTING";
    }
    return "UNDEFINED STATUS";
}
#endif /* OPAL_ENABLE_DEBUG */

static void era_compute_local_return_value(era_agreement_info_t *ci) {
    int r, nb_ack_before_agreement;
    era_rank_counter_item_t *rl;
    int found;
    ompi_group_t 
        *ack_descendent_union_group, 
        *ack_before_agreement_group = NULL,
        *ack_after_agreement_group,
        *tmp_sub_group;
    int          *adug_array, nb_adug, abag_array[3];

    /* Simplest case: some children has decided MPI_ERR_PROC_FAILED already 
     * OR I had children during the run, and they died and were not acknowledged before I entered
     * the agreement (see era_mark_process_failed). Then, the return value was set by
     * era_combine_agreement_values or era_mark_process_failed
     */
    if( ci->current_value.ret == MPI_ERR_PROC_FAILED )
        return;

    if( era_next_child(ci, -1) != ompi_comm_size(ci->comm) ) {
        /* I still have some children. Let's count them */
        r = -1;
        found = 0;
        while( r != ompi_comm_size(ci->comm) ) {
            r = era_next_child(ci, r);
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
                ci->current_value.ret = MPI_ERR_PROC_FAILED;
                return;
            }
            nb_adug++;
        }

        if( nb_adug > 0 /**< There is at least a failure acknowledged by the children. */ ) {
            /* Okay.. We may need to compare the group I acknowledged (when entering the agreement)
             * and the 'group' my children acknowledged... */

            if( ci->ack_offset == 0 ) {
                /* My children have some acknowledged, failures, but not me */
                ci->current_value.ret = MPI_ERR_PROC_FAILED;
                return;
            }

            ack_before_agreement_group = OBJ_NEW(ompi_group_t);
            tmp_sub_group = OBJ_NEW(ompi_group_t);
            abag_array[0] = 0;
            abag_array[1] = ci->ack_offset - 1;
            abag_array[2] = 1;
            ompi_group_range_incl(ompi_group_all_failed_procs, 1, &abag_array, &tmp_sub_group);
            ompi_group_intersection(tmp_sub_group,
                                    ci->comm->c_local_group,
                                    &ack_before_agreement_group);
            
            /** Did my children acknowledge the same number of ranks as me? */
            if( nb_adug != ack_before_agreement_group->grp_proc_count ) {
                ci->current_value.ret = MPI_ERR_PROC_FAILED;
                OBJ_RELEASE(ack_before_agreement_group);
                OBJ_RELEASE(tmp_sub_group);
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
            
            ompi_group_compare(ack_descendent_union_group, ack_before_agreement_group, &r);
            if( MPI_UNEQUAL == r ) {
                ci->current_value.ret = MPI_ERR_PROC_FAILED;
            }

            OBJ_RELEASE(ack_descendent_union_group);
            OBJ_RELEASE(tmp_sub_group);
            free(adug_array);
        }
    }

    /** At this point, I acknowledged the same failures as my children,
     *  and no failure prevented us to move forward. However, if I noticed
     *  a failure in the communicator that has not been acknowledged, I must
     *  still report an ERR_PROC_FAILED.
     */
    if( ompi_group_all_failed_procs->grp_proc_count > ci->ack_offset ) {
        /* New failures have been reported since I started the agreement */
        ack_after_agreement_group = OBJ_NEW(ompi_group_t);
        tmp_sub_group = OBJ_NEW(ompi_group_t);
        abag_array[0] = 0;
        abag_array[1] = ompi_group_all_failed_procs->grp_proc_count - 1; /**< >=0 since gpr_proc_count > ci->ack_offset >= 0 */
        abag_array[2] = 1;
        ompi_group_range_incl(ompi_group_all_failed_procs, 1, &abag_array, &tmp_sub_group);
        ompi_group_intersection(tmp_sub_group,
                                ci->comm->c_local_group,
                                &ack_after_agreement_group);
        OBJ_RELEASE(tmp_sub_group);

        if( NULL == ack_before_agreement_group ) {
            if( ci->ack_offset > 0 ) {
                ack_before_agreement_group = OBJ_NEW(ompi_group_t);
                tmp_sub_group = OBJ_NEW(ompi_group_t);
                abag_array[0] = 0;
                abag_array[1] = ci->ack_offset - 1;
                abag_array[2] = 1;
                ompi_group_range_incl(ompi_group_all_failed_procs, 1, &abag_array, &tmp_sub_group);
                ompi_group_intersection(tmp_sub_group,
                                        ci->comm->c_local_group,
                                        &ack_before_agreement_group);
                nb_ack_before_agreement = ack_before_agreement_group->grp_proc_count;
                OBJ_RELEASE(tmp_sub_group);
            } else {
                nb_ack_before_agreement = 0;
            }
        } else {
            nb_ack_before_agreement = 0;
        }

        if( ack_after_agreement_group->grp_proc_count != nb_ack_before_agreement ) {
            ci->current_value.ret = MPI_ERR_PROC_FAILED;
        }
        OBJ_RELEASE(ack_after_agreement_group);
    }

    if( NULL != ack_before_agreement_group )
        OBJ_RELEASE(ack_before_agreement_group);
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
}

static void era_mark_process_failed(era_agreement_info_t *ci, int rank)
{
    int r;
    if( ci->status > NOT_CONTRIBUTED ) {
        /* I may not have sent up yet (or I'm going to re-send up because of failures), 
         * and since I already contributed, this failure is not acknowledged yet
         * So, the return value should be MPI_ERR_PROC_FAILED.
         * Of course, if I have already contributed upward, the final return might still
         * be MPI_SUCCESS
         */
        ci->current_value.ret = MPI_ERR_PROC_FAILED;
    }
    if( ci->status == BROADCASTING ) {
        /* We are waiting from the parent on that agreement...
         * Is it the one that died? */
        if( rank == ci->waiting_down_from ) {
            /* OK, let's send my contribution again to the new parent and see if that's better */
            r = era_parent(ci);
            if( r == ci->comm->c_my_rank ) {
                /* Trouble: I'm the new root... Need to check that nobody decided before */
                opal_output(0, "%s:%d -- Need to implement this case\n", __FILE__, __LINE__);
            } else {
                send_up_msg(ci, r);
                ci->waiting_down_from = r;
            }
        }
    }
    if( ci->status == GATHERING ) {
        /* It could be a child, that's also important but taken care of by check_status */
        era_check_status(ci);
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
    ompi_group_t *acked_group = NULL, *tmp_sub_group;
    int *ack_rank_array, *co_rank_array;
    int  r, upsize;
    int  abag_array[3];
    int  nb_afa;

    assert( NULL != ci->comm );

    if( ci->ack_offset > 0 ) {
        acked_group = OBJ_NEW(ompi_group_t);
        tmp_sub_group = OBJ_NEW(ompi_group_t);
        abag_array[0] = 0;
        abag_array[1] = ci->ack_offset - 1;
        abag_array[2] = 1;
        ompi_group_range_incl(ompi_group_all_failed_procs, 1, &abag_array, &acked_group);
        ompi_group_intersection(tmp_sub_group,
                                ci->comm->c_local_group,
                                &acked_group);
        OBJ_RELEASE(tmp_sub_group);

        nb_afa = ompi_group_size(acked_group);
    } else {
        nb_afa = 0;
    }

    upsize = MAX_ACK_FAILED_SIZE * (nb_afa/MAX_ACK_FAILED_SIZE + 1);

    co_rank_array = (int*)malloc(upsize * sizeof(int));
    for(r = nb_afa; r < upsize; r++)
        co_rank_array[r] = -1;

    if( nb_afa > 0 ) {
        ack_rank_array = (int*)malloc(nb_afa * sizeof(int));
        for(r = 0 ; r < nb_afa; r++)
            ack_rank_array[r] = r;
        ompi_group_translate_ranks(acked_group, nb_afa, ack_rank_array, ci->comm->c_local_group, co_rank_array);
        free(ack_rank_array);
    }
    if( acked_group != NULL ) {
        OBJ_RELEASE(acked_group);
    }

    for(r = 0; r < upsize; r += MAX_ACK_FAILED_SIZE) {
        send_comm_msg(ci->comm, rank, ci->agreement_id, MSG_UP, &ci->current_value, 
                      upsize / MAX_ACK_FAILED_SIZE, &co_rank_array[r]);
    }
    free(co_rank_array);
}

static void send_comm_msg(ompi_communicator_t *comm,                          
                          int dst, 
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

    if( MSG_UP == type ) {
        OPAL_OUTPUT_VERBOSE((15, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) send message [(%d.%d).%d, %d, %08x.%d.%d.. %d UP msg, ADR: %d...] to %d\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             agreement_id.ERAID_FIELDS.contextid,
                             agreement_id.ERAID_FIELDS.epoch,
                             agreement_id.ERAID_FIELDS.agreementid,
                             type,
                             value->bits,
                             value->ret,
                             value->new_dead[0],
                             nb_up_msg,
                             ack_failed[0],
                             dst));
    } else {
        OPAL_OUTPUT_VERBOSE((15, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) send message [(%d.%d).%d, %d, %08x.%d.%d..] to %d\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             agreement_id.ERAID_FIELDS.contextid,
                             agreement_id.ERAID_FIELDS.epoch,
                             agreement_id.ERAID_FIELDS.agreementid,
                             type,
                             value->bits,
                             value->ret,
                             value->new_dead[0],
                             dst));
    }

    assert( agreement_id.ERAID_FIELDS.contextid == ompi_comm_get_cid(comm) );
    assert( agreement_id.ERAID_FIELDS.epoch == comm->c_epoch );

    peer = ompi_comm_peer_lookup(comm, dst);
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
    src->src.comm_based = ompi_comm_rank(comm);
    memcpy(&src->agreement_value, value, sizeof(era_value_t));
    if( MSG_UP == type ) {
        src->nb_up_messages = nb_up_msg;
        memcpy(&src->ack_failed, ack_failed, MAX_ACK_FAILED_SIZE * sizeof(int));
    }
    btl->btl_send(btl, btl_endpoint, des, MCA_BTL_TAG_FT_AGREE);
}

static void send_proc_name_msg(orte_process_name_t dst, 
                               era_identifier_t agreement_id,
                               era_msg_type_t type,
                               era_value_t *value)
{
    mca_btl_base_descriptor_t *des;
    era_msg_t *src;
    ompi_proc_t *peer;
    mca_bml_base_endpoint_t *proc_bml;
    mca_bml_base_btl_t *bml_btl;
    struct mca_btl_base_endpoint_t *btl_endpoint;
    mca_btl_base_module_t *btl;

    OPAL_OUTPUT_VERBOSE((15, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) send message [(%d.%d).%d, %d, %08x.%d.%d..] to %s\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         agreement_id.ERAID_FIELDS.contextid,
                         agreement_id.ERAID_FIELDS.epoch,
                         agreement_id.ERAID_FIELDS.agreementid,
                         type,
                         value->bits,
                         value->ret,
                         value->new_dead[0],
                         ORTE_NAME_PRINT(&dst)));

    peer = ompi_proc_find ( &dst );
    assert(NULL != peer);
    proc_bml = peer->proc_bml;
    assert(NULL != proc_bml);
    bml_btl = mca_bml_base_btl_array_get_index(&proc_bml->btl_eager, 0);
    assert(NULL != bml_btl);
    btl_endpoint = bml_btl->btl_endpoint;
    assert(NULL != btl_endpoint);
    btl = bml_btl->btl;
    assert(NULL != btl);

    assert( MSG_UP != type );
    des = btl->btl_alloc(btl, btl_endpoint, MCA_BTL_NO_ORDER, sizeof(era_msg_t) - sizeof(int) * MAX_ACK_FAILED_SIZE,
                         MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
    des->des_cbfunc = message_sent;
    des->des_cbdata = NULL;
    src = (era_msg_t*)des->des_src->seg_addr.pval;
    src->msg_type = type;
    src->agreement_id.ERAID_KEY = agreement_id.ERAID_KEY;
    src->src.proc_name_based = *ORTE_PROC_MY_NAME;
    memcpy(&src->agreement_value, value, sizeof(era_value_t));
    btl->btl_send(btl, btl_endpoint, des, MCA_BTL_TAG_FT_AGREE);
}

static void result_request(era_msg_t *msg)
{
    void *value;
    era_value_t old_agreement_value;
    if( opal_hash_table_get_value_uint64(&era_passed_agreements,
                                         msg->agreement_id.ERAID_KEY,
                                         &value) == OPAL_SUCCESS ) {
        old_agreement_value = ((era_passed_agreement_t*)value)->agreement_value;
        send_proc_name_msg(msg->src.proc_name_based, msg->agreement_id, MSG_DOWN, &old_agreement_value);
    } else {
        /** I should be a descendent of msg->src (since RESULT_REQUEST messages are sent to
         *  people below the caller.
         *  So, the caller is a candidate for the root, and will potentially take the decision
         *  I am waiting for, if it has not been taken already.
         *  Let's notify the caller that I haven't taken the decision
         */
        ERA_VALUE_SET_NEUTRAL(old_agreement_value);
        send_proc_name_msg(msg->src.proc_name_based, msg->agreement_id, MSG_UNDECIDED, &old_agreement_value);
    }
}

static void undecided(era_msg_t *msg)
{
    void *value;

    if( opal_hash_table_get_value_uint64(&era_passed_agreements,
                                         msg->agreement_id.ERAID_KEY,
                                         &value) == OPAL_SUCCESS ) {
        era_value_t old_agreement_value;
        /* This is an old agreement result. The answer, which is unneeded now,
         * should be compatible with the one stored
         */
        old_agreement_value = ((era_passed_agreement_t*)value)->agreement_value;
        assert( ERA_VALUE_EQUAL(old_agreement_value, msg->agreement_value) ||
                ERA_VALUE_IS_UNDEF(msg->agreement_value) );
    } else {
        /* If we don't know about the result of this agreement, 
         *   A) it should be about an ongoing agreement, so I haven't freed the communicator
         *   B) I should be waiting for an answer from this peer
         */
        era_agreement_info_t *ci;
        ompi_proc_t *peer;
        int rank;
        ompi_communicator_t *comm;

        ci = era_lookup_agreeement_info(msg->agreement_id);
        assert(NULL != ci);

        comm = ci->comm;
        assert( NULL != comm );

        /* We haven't decided on that guy yet */
        peer = ompi_proc_find ( &msg->src.proc_name_based );
        for(rank = 0; rank < ompi_comm_size(comm); rank++) {
            if( ompi_comm_peer_lookup(comm, rank) == peer ) {
                break;
            }
        }
        assert(rank < ompi_comm_size(comm));

        mark_as_undecided(ci, rank);
    }
}

static void msg_up(era_msg_t *msg)
{
    era_agreement_info_t *ci;
    era_rank_counter_item_t *rank_item;
    void *value;
    era_value_t *old_agreement_value;
    int r;
 
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received UP Message: Agreement ID = (%d.%d).%d, sender: %d, msg value: %08x.%d.%d.. %d up msg, ADR: %d...\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         msg->agreement_id.ERAID_FIELDS.contextid,
                         msg->agreement_id.ERAID_FIELDS.epoch,
                         msg->agreement_id.ERAID_FIELDS.agreementid,
                         msg->src.comm_based,
                         msg->agreement_value.bits,
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
        send_proc_name_msg(msg->src.proc_name_based, msg->agreement_id, MSG_DOWN, old_agreement_value);
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
         *  The children contributions have been taken into account already
         *  So, let's ignore the message(s), we will send the result when
         *  we receive the DOWN message.
         */
        OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Managing UP Message -- Already in BROADCASTING state: ignoring message\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        return;
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
        if( rank_item->rank == msg->src.comm_based ) {
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
        rank_item->rank = msg->src.comm_based;
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
 
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received DOWN Message: Agreement ID = (%d.%d).%d, sender: %d (might be grabled if DOWN is result of a REQUEST), msg value: %08x.%d.%d..\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         msg->agreement_id.ERAID_FIELDS.contextid,
                         msg->agreement_id.ERAID_FIELDS.epoch,
                         msg->agreement_id.ERAID_FIELDS.agreementid,
                         msg->src.comm_based,
                         msg->agreement_value.bits,
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
    switch( msg->msg_type ) {
    case MSG_RESULT_REQUEST:
        result_request(msg);
        return;
    case MSG_UNDECIDED:
        undecided(msg);
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
    void *value;
    era_agreement_info_t *ci;
    void *node;
    uint64_t key64;
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
        } while( opal_hash_table_get_next_key_uint64(&era_ongoing_agreements,
                                                         &key64, &value, 
                                                         node, &node) == OPAL_SUCCESS );
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
    OBJ_CONSTRUCT( &known_epoch_for_cid, opal_hash_table_t);
    opal_hash_table_init(&known_epoch_for_cid, 65537 /* This is a big one */);
    
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
    uint32_t key32;
    era_passed_agreement_t    *old_agreement;
    era_agreement_info_t      *un_agreement;
    era_seq_num_record_t      *seq_num_record;

    if( !era_inited ) {
        return OMPI_SUCCESS;
    }

    ompi_rank_failure_cbfunc = ompi_stacked_rank_failure_callback_fct;

    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Finalizing\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    if( opal_hash_table_get_first_key_uint32(&known_epoch_for_cid,
                                             &key32,
                                             &value, &node) == OPAL_SUCCESS ) {
        do {
            seq_num_record = (era_seq_num_record_t *)value;
            OBJ_RELEASE( seq_num_record );
        } while( opal_hash_table_get_next_key_uint32(&known_epoch_for_cid,
                                                     &key32, &value, 
                                                     node, &node) == OPAL_SUCCESS );
    }
    OBJ_DESTRUCT( &known_epoch_for_cid );

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

    /* Let's create or find the current value */
    ci = era_lookup_agreeement_info(agreement_id);
    if( NULL == ci ) {
        ci = era_create_agreement_info(agreement_id);
    }

    assert( NULL == ci->comm );
    era_agreement_info_set_comm(ci, comm);
    
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
    agreement_value.bits = *flag;
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
    
    *flag = agreement_value.bits;
    
    /* Update the group of failed processes if needed */
    if( group != NULL ) {
        ompi_group_intersection(ompi_group_all_failed_procs, comm->c_local_group, group);
    }

    return agreement_value.ret;
}
