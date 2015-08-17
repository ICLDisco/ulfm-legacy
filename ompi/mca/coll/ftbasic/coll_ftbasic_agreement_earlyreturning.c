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
#include "ompi/datatype/ompi_datatype_internal.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/btl/btl.h"
#include "ompi/mca/bml/bml.h"
#include "ompi/op/op.h"
#include "ompi/mca/bml/base/base.h"
#include "ompi/class/ompi_free_list.h"
#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/ess/ess.h"

#include MCA_timer_IMPLEMENTATION_HEADER
#include "coll_ftbasic.h"

extern int mca_coll_ftbasic_cur_era_topology;

static int era_inited = 0;
static opal_hash_table_t era_passed_agreements;
static opal_hash_table_t era_ongoing_agreements;
static opal_hash_table_t era_incomplete_messages;
static ompi_comm_rank_failure_callback_t *ompi_stacked_rank_failure_callback_fct = NULL;
static uint64_t msg_seqnum = 1;
static ompi_free_list_t era_iagree_requests = {{{0}}};

extern int coll_ftbasic_debug_rank_may_fail;

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
    BROADCASTING,
    COMPLETED
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

#if OPAL_ENABLE_DEBUG
#define PROGRESS_FAILURE_PROB 0.1
#endif

typedef struct {
    int32_t  ret;                            /**< Return code */
    uint16_t min_aid;                        /**< Used for garbage collection */
    uint16_t max_aid;                        /**< at era_agreement_value_set_gcrange, forall this->header.min_aid <= id <= this->header.max_aid,
                                              *     exists p in era_passed_agreements, such that
                                              *       (this->agreement_id.contextid == key(p).contextid &&
                                              *        this->agreement_id.epoch     == key(p).epoch &&
                                              *        key(p).agreementid           == id)
                                              *   at era_decide, min_passed_aid is the max over all ranks, max_passed_aid is
                                              *   min over all ranks. */
    int      operand;                        /**< operand applied on bytes. 
                                              *   One of OMPI_OP_BASE_FORTRAN_* values in mca/op/op.h */
    int      dt_count;                       /**< The number of datatypes in bytes */
    int      datatype;                       /**< Fortran index of predefined basic datatype in bytes */
    int      nb_new_dead;                    /**< Number of newly discovered dead */
} era_value_header_t;    
#define ERA_VALUE_BYTES_COUNT(_h) (ompi_datatype_basicDatatypes[(_h)->datatype]->super.size * (_h)->dt_count)

/* This is the non-linearized version of the era_value_t */
typedef struct {
    opal_object_t        super;
    era_value_header_t   header;
    uint8_t             *bytes;              /**< array of size datatype_size(header.datatype) * header.dt_count
                                              *   containing the value on which the agreement is done */
    int                 *new_dead_array;     /**< array of header.nb_new_dead integers with the newly discovered
                                              *   processes. */
} era_value_t;

static void  era_value_constructor (era_value_t *value)
{
    value->header.ret = 0;
    value->header.operand = -1;
    value->header.dt_count = -1;
    value->header.datatype = -1;
    value->header.nb_new_dead = -1;
    value->header.min_aid = (uint16_t)-1;
    value->header.max_aid = 0;
    value->bytes = NULL;
    value->new_dead_array = NULL;
}

static void  era_value_destructor (era_value_t *value)
{
    if( NULL != value->bytes )
        free(value->bytes);
    if( NULL != value->new_dead_array )
        free(value->new_dead_array);
}

OBJ_CLASS_INSTANCE(era_value_t, opal_object_t, era_value_constructor, era_value_destructor);

typedef struct {
    era_msg_type_t      msg_type;
    era_identifier_t    agreement_id;

    /** We give the source ID as both the rank in the communicator
     *  and the proc_name_t, because 90% of the time, the receiver
     *  will understand what the rank means and this is a faster
     *  operation than using the proc_name, but 10% of the time
     *  the receiver will not understand what the rank means
     *  and it needs to answer.
     */
    int                 src_comm_rank;
    orte_process_name_t src_proc_name;
    era_value_header_t  agreement_value_header;
    int                 nb_ack;         /**< Informs how many of these messages types 
                                         *   will be sent upward (in case ack_failed must
                                         *   be split in multiple messages. */
} era_msg_header_t;
#define ERA_MSG_SIZE(_h, _n) ( sizeof(era_msg_header_t) + ERA_VALUE_BYTES_COUNT(_h) + (_h)->nb_new_dead * sizeof(int) + (_n) * sizeof(int) )

typedef struct {
    unsigned int        bytes_received;
    uint8_t             bytes[1];
} era_incomplete_msg_t;

typedef struct {
    orte_process_name_t       src;  /**< src + msg_seqnum build a unique (up to rotation on msg_seqnum) */
    uint64_t           msg_seqnum;  /*   message identifier.*/
    unsigned int          msg_len;  /**< Length of the message */
    unsigned int      frag_offset;  /**< Offset (in bytes) of the fragment in the message */
    unsigned int         frag_len;  /**< Length (in bytes) of that fragment */
    uint8_t              bytes[1];  /**< Variable size member, of length frag_len */
} era_frag_t;

/**
 * Rank lists are used at variable places to keep track of who sent information.
 */
typedef struct {
    opal_list_item_t super;
    int32_t          rank;        /**< The rank of the descendent that provided information */
} era_rank_item_t;

OBJ_CLASS_INSTANCE(era_rank_item_t, opal_list_item_t, NULL, NULL /*print_destructor*/);

/**
 * topology-dependent tree structure
 */
typedef struct era_tree_s era_tree_t;

struct era_tree_s {
    int rank_in_comm;
    int parent;
    int next_sibling;
    int first_child;
};

/**
 * Communicator-specific Agreement persistent information
 */
#define AGS_TREE_DIRTY (1<<0)
#define AGS_AFR_DIRTY  (1<<1)
typedef struct era_comm_agreement_specific_s {
    opal_object_t parent;
    int          *agreed_failed_ranks;
    int           afr_size;
    era_tree_t   *tree;
    int           tree_size;
    int           ags_status;
} era_comm_agreement_specific_t;

static void  era_agreement_comm_specific_constructor(era_comm_agreement_specific_t *comm_specific)
{
    comm_specific->agreed_failed_ranks = NULL;
    comm_specific->afr_size            = 0;
    comm_specific->tree                = NULL;
    comm_specific->tree_size           = 0;
    comm_specific->ags_status          = AGS_TREE_DIRTY | AGS_AFR_DIRTY; /**< the AGS does not exist, so it needs to be created */
}

static void  era_agreement_comm_specific_destructor(era_comm_agreement_specific_t *comm_specific)
{
    if( NULL != comm_specific->agreed_failed_ranks ) {
        free(comm_specific->agreed_failed_ranks );
    }
    if( NULL != comm_specific->tree ) {
        free(comm_specific->tree);
    }
}

OBJ_CLASS_INSTANCE(era_comm_agreement_specific_t,
                   opal_object_t, 
                   era_agreement_comm_specific_constructor,
                   era_agreement_comm_specific_destructor);

#define AGS(comm)  ( (era_comm_agreement_specific_t*)(comm)->agreement_specific )

typedef struct era_iagree_request_s era_iagree_request_t;

/**
 * Main structure to remember the current status of an agreement that
 *  was started.
 */
typedef struct {
    opal_object_t         super;
    era_identifier_t      agreement_id;
    era_value_t          *current_value;
    era_proc_status_t     status;            /**< status of this process in that agreement. */
    uint16_t              min_passed_aid;    
    ompi_communicator_t  *comm;              /**< Communicator related to that agreement. Might be NULL
                                              *   if this process has not entered the agreement yet.*/
    era_iagree_request_t *req;               /**< Request, if this is called through an iagree */
    era_comm_agreement_specific_t *ags;      /**< Communicator-specific agreement information, at
                                              *   the time this agreement was started (might be different
                                              *   from comm->ags during the agreement) */
    int                   nb_acked;          /**< size of acked */
    int                  *acked;             /**< Last acknowledged processes when entering
                                              *   the agreement. Used to compare with the descendents 
                                              *   acknowledged processes */
    opal_list_t           gathered_info;     /**< The list of direct descendents that provided information (even partially) */
    opal_list_t           waiting_res_from;  /**< A list of ranks and status, from which we requested to "restart" the
                                              *   consensus (if the current process became root in the middle of one) */
    opal_list_t           early_requesters;  /**< Remember anybody who requests to receive the decision as
                                              *   the FD may be too slow to provide a symmetric view of the world,
                                              *   and passing over the passed agreements after the decision is taken
                                              *   is too slow */
    int                   waiting_down_from; /**< If in BROADCAST state: who is supposed to send me the info */
} era_agreement_info_t;

static void era_build_tree_structure(era_agreement_info_t *ci);
static int era_next_child(era_agreement_info_t *ci, int prev_child_in_comm);
static int era_parent(era_agreement_info_t *ci);

static void  era_agreement_info_constructor (era_agreement_info_t *agreement_info)
{
    agreement_info->agreement_id.ERAID_KEY = 0;
    agreement_info->status = NOT_CONTRIBUTED | OP_NOT_DEFINED;
    agreement_info->comm = NULL;
    agreement_info->waiting_down_from = -1;
    agreement_info->nb_acked = 0;
    agreement_info->current_value = NULL;
    agreement_info->acked = NULL;
    agreement_info->req = NULL;

    OBJ_CONSTRUCT(&agreement_info->gathered_info, opal_list_t);
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
    while( NULL != (li = opal_list_remove_first(&agreement_info->waiting_res_from)) ) {
        OBJ_RELEASE(li);
    }
    OBJ_DESTRUCT(&agreement_info->waiting_res_from);
    if( NULL != agreement_info->comm ) {
        OBJ_RELEASE(agreement_info->comm);
    }
    if( NULL != agreement_info->acked ) {
        free(agreement_info->acked);
        agreement_info->acked = NULL;
        agreement_info->nb_acked = 0;
    }
    if( NULL != agreement_info->current_value ) {
        OBJ_RELEASE( agreement_info->current_value );
        agreement_info->current_value = NULL;
    }

}

OBJ_CLASS_INSTANCE(era_agreement_info_t,
                   opal_object_t, 
                   era_agreement_info_constructor, 
                   era_agreement_info_destructor);

struct era_iagree_request_s {
    ompi_request_t        super;
    era_identifier_t      agreement_id;
    void                 *contrib;
    ompi_group_t        **group;
    era_agreement_info_t *ci;
};

OBJ_CLASS_INSTANCE(era_iagree_request_t,
                   ompi_request_t, 
                   NULL,
                   NULL);

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

static era_agreement_info_t *era_create_agreement_info(era_identifier_t agreement_id, era_value_header_t *header)
{
    era_agreement_info_t *ci;
    int r;
#if OPAL_ENABLE_DEBUG
    void *value;
    assert( opal_hash_table_get_value_uint64(&era_ongoing_agreements, 
                                             agreement_id.ERAID_KEY, 
                                             &value) != OPAL_SUCCESS );
#endif
    ci = OBJ_NEW(era_agreement_info_t);
    ci->agreement_id.ERAID_KEY = agreement_id.ERAID_KEY;
    ci->current_value = OBJ_NEW(era_value_t);
    memcpy(&ci->current_value->header, header, sizeof(era_value_header_t));

    assert( header->datatype >= 0 && header->datatype < OMPI_DATATYPE_MPI_MAX_PREDEFINED );
    /* ci->current_value->bytes will be filled up in combine_agreement_value, but let's allocate it here */
    ci->current_value->bytes = (uint8_t*)malloc( ERA_VALUE_BYTES_COUNT(&ci->current_value->header) );
    if( header->nb_new_dead > 0 ) {
        ci->current_value->new_dead_array = (int*)malloc(header->nb_new_dead * sizeof(int));
        for(r = 0; r < header->nb_new_dead; r++)
            ci->current_value->new_dead_array[r] = -1;
    }
    opal_hash_table_set_value_uint64(&era_ongoing_agreements,
                                     agreement_id.ERAID_KEY,
                                     ci);
    return ci;
}

#if OPAL_ENABLE_DEBUG
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
#define era_debug_print_group(g, c, i, h) do {} while(0)
#endif

static int era_comm_to_alive(era_agreement_info_t *ci, int r_in_comm);

static void era_update_return_value(era_agreement_info_t *ci, int nb_acked, int *acked) {
    ompi_group_t *ack_after_agreement_group, *tmp_sub_group;
    int r, abag_array[3];

    /* Simplest case: some children has decided MPI_ERR_PROC_FAILED already 
     * OR I had children during the run, and they died and were not acknowledged before I entered
     * the agreement (see era_mark_process_failed). Then, the return value was set by
     * era_combine_agreement_values or era_mark_process_failed
     */
    if( ci->current_value->header.ret == MPI_ERR_PROC_FAILED ) {
        OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) compute local return value for Agreement ID = (%d.%d).%d: already decided FAILED at line %s:%d\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ci->agreement_id.ERAID_FIELDS.contextid,
                             ci->agreement_id.ERAID_FIELDS.epoch,
                             ci->agreement_id.ERAID_FIELDS.agreementid,
                             __FILE__, __LINE__));
        return;
    }

    if( nb_acked >= 0 ) {

#if OPAL_ENABLE_DEBUG
        for(r = 1; r < nb_acked; r++)
            assert(acked[r-1] < acked[r]);
#endif /*OPAL_ENABLE_DEBUG*/

        /* I'm checking w.r.t. children or myself */
        if( ci->acked == NULL ) {
            /** I have not set my contribution yet, let's consider this as the base */
            if(nb_acked > 0) {
                ci->acked = (int*)malloc(nb_acked * sizeof(int));
                memcpy(ci->acked, acked, nb_acked * sizeof(int));
            }
            ci->nb_acked = nb_acked;
        } else {
            /** This contribution and mine must match exactly */
            if( nb_acked != ci->nb_acked ) {
                OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ERA) compute local return value for Agreement ID = (%d.%d).%d: decide FAILED because the acked arrays are of different size\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ci->agreement_id.ERAID_FIELDS.contextid,
                                     ci->agreement_id.ERAID_FIELDS.epoch,
                                     ci->agreement_id.ERAID_FIELDS.agreementid));
                ci->current_value->header.ret = MPI_ERR_PROC_FAILED;
                goto cleanup;
            }
            
            if( nb_acked == 0 ) {
                return;
            }
            for(r = 0; r < nb_acked; r++) {
                /** Both arrays should be globally ordered */
                if( acked[r] != ci->acked[r] ) {
                    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                                         "%s ftbasic:agreement (ERA) compute local return value for Agreement ID = (%d.%d).%d: decide FAILED because the acked arrays do not hold the same elements\n",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         ci->agreement_id.ERAID_FIELDS.contextid,
                                         ci->agreement_id.ERAID_FIELDS.epoch,
                                         ci->agreement_id.ERAID_FIELDS.agreementid));
                    ci->current_value->header.ret = MPI_ERR_PROC_FAILED;
                    goto cleanup;
                }
            }
        }

        return;
    }
        
    /** This is the final check: if I reach this point, I acknowledged the same failures
     *  as my children, and no failure prevented us to move forward. However, if I noticed
     *  a failure in the communicator that has not been acknowledged, I must still report
     *  an ERR_PROC_FAILED. */

    if( ompi_group_size(ompi_group_all_failed_procs) > ci->nb_acked ) {
        /* New failures have been reported since I started the agreement */
        ack_after_agreement_group = OBJ_NEW(ompi_group_t);
        tmp_sub_group = OBJ_NEW(ompi_group_t);
        abag_array[0] = 0;
        /** this is >=0 since ompi_group_size(ompi_group_all_failed_procs) > ci->nb_acked >= 0 */
        abag_array[1] = ompi_group_size(ompi_group_all_failed_procs) - 1;
        abag_array[2] = 1;
        ompi_group_range_incl(ompi_group_all_failed_procs, 1, &abag_array, &tmp_sub_group);
        ompi_group_intersection(tmp_sub_group,
                                ci->comm->c_local_group,
                                &ack_after_agreement_group);
        OBJ_RELEASE(tmp_sub_group);

        if( ompi_group_size(ack_after_agreement_group) != ci->nb_acked ) {
            OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) compute local return value for Agreement ID = (%d.%d).%d: decide FAILED because new failures happened: |acked| = %d, |afp on c_local_group| = %d\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ci->agreement_id.ERAID_FIELDS.contextid,
                                 ci->agreement_id.ERAID_FIELDS.epoch,
                                 ci->agreement_id.ERAID_FIELDS.agreementid,
                                 ci->nb_acked,
                                 ompi_group_size(ack_after_agreement_group)));
            ci->current_value->header.ret = MPI_ERR_PROC_FAILED;
            goto cleanup;
        } else {
            OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) compute local return value for Agreement ID = (%d.%d).%d: decide SUCCESS (1)\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ci->agreement_id.ERAID_FIELDS.contextid,
                                 ci->agreement_id.ERAID_FIELDS.epoch,
                                 ci->agreement_id.ERAID_FIELDS.agreementid));
        }
        OBJ_RELEASE(ack_after_agreement_group);
    } else {
        OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) compute local return value for Agreement ID = (%d.%d).%d: decide SUCCESS (2)\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ci->agreement_id.ERAID_FIELDS.contextid,
                             ci->agreement_id.ERAID_FIELDS.epoch,
                             ci->agreement_id.ERAID_FIELDS.agreementid));
    }

    assert(ci->current_value->header.ret == MPI_SUCCESS);
    return; /** < We don't clean the list of acked processes if the result is SUCCESS as we need to communicate it */

 cleanup:
    if( ci->nb_acked > 0 ) {
        assert(NULL != ci->acked);
        free(ci->acked);
        ci->acked = NULL;
        ci->nb_acked = 0;
    }
}

static int compare_ints(const void *a, const void *b)
{
    const int *ia = (const int *)a;
    const int *ib = (const int *)b;
    return (*ia > *ib) - (*ia < *ib);
}

static int compare_uint16_ts(const void *a, const void *b)
{
    const uint16_t *ia = (const uint16_t *)a;
    const uint16_t *ib = (const uint16_t *)b;
    return (*ia > *ib) - (*ia < *ib);
}

static void era_merge_new_dead_list(era_agreement_info_t *ci, int nb_src, int *src)
{
    int  s;
    int *dst, d, nb_dst;
    int *merge, nb_merge;

    dst = ci->current_value->new_dead_array;
    nb_dst = ci->current_value->header.nb_new_dead;

#if OPAL_ENABLE_DEBUG
        {
            for(d = 1; d < nb_dst; d++) 
                assert(dst[d-1] < dst[d]);
            for(s = 1; s < nb_src; s++) 
                assert(src[s-1] < src[s]);
        }
#endif /*OPAL_ENABLE_DEBUG*/
        
    if(nb_dst + nb_src == 0) return;
    merge = (int*)malloc((nb_dst + nb_src) * sizeof(int));
    nb_merge = 0;
    d = 0;
    s = 0;
    while(d < nb_dst && s < nb_src) {
        if( dst[d] == src[s] ) {
            merge[nb_merge++] = dst[d];
            d++;
            s++;
            continue;
        } else if( dst[d] < src[s] ) {
            merge[nb_merge++] = dst[d];
            d++;
            continue;
        } else {
            assert( dst[d] > src[s] );
            merge[nb_merge++] = src[s];
            s++;
            continue;
        }
    }
    while( d < nb_dst )
        merge[nb_merge++] = dst[d++];
    while( s < nb_src )
        merge[nb_merge++] = src[s++];

    if( nb_merge > nb_dst ) {
        ci->current_value->new_dead_array = (int*)realloc(ci->current_value->new_dead_array, nb_merge*sizeof(int));
        memcpy(ci->current_value->new_dead_array, merge, nb_merge * sizeof(int));
        ci->current_value->header.nb_new_dead = nb_merge;
    }
    free(merge);
}

static void era_agreement_value_set_gcrange(era_identifier_t eid, era_value_t *era_value)
{
    uint16_t *aid_list = NULL;
    uint64_t key64;
    int i;
    void *value, *node;
    size_t    aid_list_size = 0, aid_list_pos = 0;

    /* Deal with garbage collection management: find a contiguous range of
     *  agreements that have been decided. */
    if( opal_hash_table_get_first_key_uint64(&era_passed_agreements,
                                             &key64,
                                             &value, &node) == OPAL_SUCCESS ) {
        do {
            era_identifier_t pid;
            pid.ERAID_KEY = key64;

            if( pid.ERAID_FIELDS.contextid == eid.ERAID_FIELDS.contextid &&
                pid.ERAID_FIELDS.epoch     == eid.ERAID_FIELDS.epoch ) {
                if( aid_list_pos == aid_list_size ) {
                    aid_list_size = aid_list_size > 0 ? 2*aid_list_size : 1;
                    aid_list = (uint16_t*)realloc(aid_list, aid_list_size * sizeof(uint16_t));
                }
                aid_list[aid_list_pos++] = pid.ERAID_FIELDS.agreementid;
            }
        } while( opal_hash_table_get_next_key_uint64(&era_passed_agreements,
                                                     &key64, &value, 
                                                     node, &node) == OPAL_SUCCESS );
        if( aid_list_pos > 0 ) {
            if( aid_list_pos > 1 )
                qsort(aid_list, aid_list_pos, sizeof(uint16_t), compare_uint16_ts);
            era_value->header.min_aid = aid_list[0];
            for(i = 1; i < aid_list_pos; i++)
                if( aid_list[i] != aid_list[i-1] + 1 )
                    break;
            era_value->header.max_aid = aid_list[i-1];
            free(aid_list);
        }
    }
    OPAL_OUTPUT_VERBOSE((17, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) GC: agreement (%d.%d).%d: agreements %u to %u have been locally acknowledged\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         eid.ERAID_FIELDS.contextid,
                         eid.ERAID_FIELDS.epoch,                         
                         eid.ERAID_FIELDS.agreementid,
                         era_value->header.min_aid,
                         era_value->header.max_aid));

}

static void era_update_new_dead_list(era_agreement_info_t *ci)
{
    int *ra, s, r, t;
    era_comm_agreement_specific_t *ags;
    ompi_communicator_t *comm;

    comm = ci->comm;
    ags  = AGS(comm);
    assert(ags->afr_size >= 0); /**< I should be working on an ags still attached to the communicator */
    
    /** Worst case: all processes minus me are dead */
    ra = (int*)malloc( (ompi_group_size(comm->c_local_group) - 1) * sizeof(int) );
    t = 0;
    r = 0;
    for(s = 0; s < ompi_group_size(comm->c_local_group); s++) {
        if( t < ags->afr_size && ags->agreed_failed_ranks[t] == s ) {
            t++;
            continue;
        }
        if( !ompi_comm_is_proc_active(comm, s, false) ) {
            ra[r++] = s;
        }
    }

    OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) agreement (%d.%d).%d -- adding %d procs to the list of newly dead processes",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ci->agreement_id.ERAID_FIELDS.contextid,
                         ci->agreement_id.ERAID_FIELDS.epoch,
                         ci->agreement_id.ERAID_FIELDS.agreementid,
                         r));

#if OPAL_ENABLE_DEBUG
    {
        int _i, _j;
        for(_i = 0; _i < r; _i++)
            for(_j = _i+1; _j < r; _j++)
                assert(ra[_i] < ra[_j]);
        for(_i = 0; _i < ags->afr_size; _i++)
            for(_j = 0; _j < r; _j++)
                assert(ra[_j] != ags->agreed_failed_ranks[_i]);
    }
#endif

    era_merge_new_dead_list(ci, r, ra);

    free(ra);
}

static void era_ci_get_clean_ags_copy(era_agreement_info_t *ci)
{
    era_comm_agreement_specific_t *old, *new;

    if( AGS(ci->comm)->ags_status & (AGS_TREE_DIRTY | AGS_AFR_DIRTY) ) {
        old = AGS(ci->comm);

        if( AGS(ci->comm)->parent.obj_reference_count > 1 ) {
            /** Not only ci->comm points to this AGS at this time.
             *  This means that some other ci points to it, so it's unsafe to
             *  reuse it: create a copy for the other cis that point to it,
             *  and attach that new copy to the communicator, then update it.
             */
            new = OBJ_NEW(era_comm_agreement_specific_t);
            if( old->afr_size > 0 ) {
                /** old does not need to remember the agreed_failed ranks, avoid allocating memory */
                new->agreed_failed_ranks = old->agreed_failed_ranks;
                new->afr_size            = old->afr_size;

                old->agreed_failed_ranks = NULL;
                old->afr_size            = -1;
            }
            /* Do not rebuild the tree if not dirty */
            new->ags_status = old->ags_status;
            if( ! ( new->ags_status & AGS_TREE_DIRTY ) ) {
                new->tree_size = old->tree_size;
                new->tree = (era_tree_t*)malloc(new->tree_size * sizeof(era_tree_t));
                memcpy(new->tree, old->tree, new->tree_size * sizeof(era_tree_t));
            }
            ci->comm->agreement_specific = &new->parent;

            old->ags_status = 0; /**< The old is clean w.r.t. the tree, and it does not need the AFR */
            OBJ_RELEASE(old);
        }

    }

    ci->ags = AGS(ci->comm);
    OBJ_RETAIN(ci->ags);

    OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Agreement (%d.%d).%d: the tree structure is %s, the AFR is %s\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         ci->agreement_id.ERAID_FIELDS.contextid,
                         ci->agreement_id.ERAID_FIELDS.epoch,
                         ci->agreement_id.ERAID_FIELDS.agreementid,
                         ci->ags->ags_status & AGS_TREE_DIRTY ? "dirty" : "clean",
                         ci->ags->ags_status & AGS_AFR_DIRTY ? "dirty" : "clean"));

    if( ci->ags->ags_status & AGS_TREE_DIRTY ) {
        era_build_tree_structure(ci);
        ci->ags->ags_status &= ~AGS_TREE_DIRTY;
    }

    if( ci->ags->ags_status & AGS_AFR_DIRTY ) {
        era_update_new_dead_list(ci);
        ci->ags->ags_status &= ~AGS_AFR_DIRTY;
    }
}

static void era_agreement_info_set_comm(era_agreement_info_t *ci, ompi_communicator_t *comm, ompi_group_t *acked_group)
{
    ompi_group_t *tmp_grp1, *tmp_grp2;
    int *src_ra;
    int *dst_ra;
    int r, s, t, grp_size;

    assert( comm->c_contextid == ci->agreement_id.ERAID_FIELDS.contextid );
    assert( comm->c_epoch     == ci->agreement_id.ERAID_FIELDS.epoch     );
    assert( ci->comm          == NULL                                    );
    ci->comm = comm;
    OBJ_RETAIN(comm);

    OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Agreement (%d.%d).%d: assigning to communicator %d\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         ci->agreement_id.ERAID_FIELDS.contextid,
                         ci->agreement_id.ERAID_FIELDS.epoch,
                         ci->agreement_id.ERAID_FIELDS.agreementid,
                         comm->c_contextid));

    if( AGS(comm) == NULL ) {
        era_comm_agreement_specific_t *ags = OBJ_NEW(era_comm_agreement_specific_t);
        comm->agreement_specific = &ags->parent;
    }

    /* Update the return value based on local knowledge of 'acknowleged' ranks */
    grp_size = ompi_group_size(acked_group);
    if( grp_size > 0 ) {
        dst_ra = (int*)malloc(grp_size * sizeof(int));
        src_ra = (int*)malloc(grp_size * sizeof(int));
        for(r = 0; r < grp_size; r++)
            src_ra[r] = r;
        ompi_group_translate_ranks(acked_group, grp_size, src_ra, comm->c_local_group, dst_ra);
        free(src_ra);
        /** dst_ra must be sorted */
        qsort(dst_ra, grp_size, sizeof(int), compare_ints);
    } else {
        dst_ra = NULL;
    }
    era_update_return_value(ci, grp_size, dst_ra);
    if( grp_size > 0 )
        free(dst_ra);

    era_ci_get_clean_ags_copy(ci);
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
    ompi_op_t *op;
    ompi_datatype_t *dt;

    if( ni->status & OP_NOT_DEFINED ) {
        ni->current_value->header.operand = value->header.operand;
        ni->current_value->header.dt_count = value->header.dt_count;
        ni->current_value->header.datatype = value->header.datatype;
        memcpy(ni->current_value->bytes, value->bytes, ERA_VALUE_BYTES_COUNT(&value->header));
        ni->current_value->header.ret = value->header.ret;
        ni->current_value->header.min_aid = value->header.min_aid;
        ni->current_value->header.max_aid = value->header.max_aid;
        ni->status &= ~OP_NOT_DEFINED;
    } else {
        assert( ni->current_value->header.operand == value->header.operand );
        assert( ni->current_value->header.dt_count == value->header.dt_count );
        assert( ni->current_value->header.datatype == value->header.datatype );
        op = opal_pointer_array_get_item(ompi_op_f_to_c_table,
                                         ni->current_value->header.operand);
        assert(NULL != op);
        dt = opal_pointer_array_get_item(&ompi_datatype_f_to_c_table,
                                         ni->current_value->header.datatype);
        assert(NULL != dt);
        ompi_op_reduce( op, value->bytes, ni->current_value->bytes,
                        ni->current_value->header.dt_count, dt);

        if( value->header.ret > ni->current_value->header.ret )
            ni->current_value->header.ret = value->header.ret;

        if( value->header.min_aid > ni->current_value->header.min_aid )
            ni->current_value->header.min_aid = value->header.min_aid;
        if( value->header.max_aid < ni->current_value->header.max_aid )
            ni->current_value->header.max_aid = value->header.max_aid;
    }

    era_merge_new_dead_list(ni, value->header.nb_new_dead, value->new_dead_array);
}


#if OPAL_ENABLE_DEBUG
static int __tree_errors;

static int era_tree_check_node(era_tree_t *tree, int tree_size, int r, int display)
{
    int c, nb = 0, p, f, i;

    if(display) {
        fprintf(stderr, "TC %s -- %d/%d (%d) -- Parent: %d\n",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), r, tree_size, tree[r].rank_in_comm,
                tree[r].parent);
    }

    if( (p = tree[r].parent) != r ) {
        
        f = 0;
        for(c = tree[p].first_child;
            c != r && c!=tree_size;
            c = tree[c].next_sibling) {
            /** Nothing */
        }
        if( c != r ) {
            __tree_errors++;
            fprintf(stderr, "TC %s -- %d/%d(%d): my parent (%d) should have me in one of its children\n", 
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), r, tree_size, tree[r].rank_in_comm, tree[r].parent);
        }
    }

    if( tree[r].rank_in_comm <
        tree[ tree[r].parent ].rank_in_comm ) {
        __tree_errors++;
        fprintf(stderr, "TC %s -- %d/%d(%d): broken hiearchy as my parent is %d in the communicator\n",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), r, tree_size, tree[r].rank_in_comm, tree[tree[r].parent].rank_in_comm);
    }

    for(c = tree[r].first_child;
        c != tree_size;
        c = tree[c].next_sibling) {
        nb++;
        if( display ) {
            fprintf(stderr, "TC %s -- %d/%d(%d) -- Child %d: %d\n",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), r, tree_size, tree[r].rank_in_comm,
                    nb, c);
        }
        if( tree[c].parent != r) {
            __tree_errors++;
            fprintf(stderr, "TC %s -- %d/%d(%d): Each of my children should have me as their parent. %d has %d as parent.\n",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), r, tree_size, tree[r].rank_in_comm, c,  tree[c].parent);
        }
        if( nb > tree_size ) {
            __tree_errors++;
            fprintf(stderr, "TC %s -- %d/%d(%d): There is a cycle somewhere: I counted %d children, which is more than the tree size.\n",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), r, tree_size, tree[r].rank_in_comm, nb);
            break;
        }
    }

    if(display) {
        fprintf(stderr, "\n");
    }

    return nb;
}

static void era_tree_check(era_tree_t *tree, int tree_size, int display)
{
    /** Tree consistency check */
    int root = 0;
    int children = 0;
    int nodes = 0;
    int r;

    __tree_errors = 0;

    for(r = 0; r < tree_size; r++) {
        if( tree[r].rank_in_comm == -1 )
            continue;
        if( tree[r].parent == r ) {
            root++;
            if( tree[r].next_sibling != tree_size ) {
                fprintf(stderr, "TC -- %d/%d: root cannot have a sibling (%d)\n",
                        r, tree_size, tree[r].next_sibling);
                __tree_errors++;
            }
        }
        nodes++;
        children += era_tree_check_node(tree, tree_size, r, display);
    }
    if( root != 1 ) {
        fprintf(stderr, "TC -- There are %d roots in this tree. That's too many\n", root);
        __tree_errors++;
    }
    if(children + 1 != nodes) {
        fprintf(stderr, "TC -- There are %d nodes in this tree, and not %d\n", children+1, nodes);
        __tree_errors++;
    }

    assert(__tree_errors == 0);
}

static char *era_debug_tree(era_tree_t *tree, int tree_size);
#endif

static void era_tree_fn_star(era_tree_t *tree, int tree_size)
{
    int m;
    for(m = 0; m < tree_size; m++) { /**< O(nb_alive) comp. overhead */
        tree[m].parent        = 0;
        /** For tree_size == 1, 1 is also the terminating value, so this works */
        tree[m].first_child   = m == 0 ? 1 : tree_size;
        tree[m].next_sibling  = m == 0 ? tree_size : m + 1;
    }
}

static void era_tree_fn_string(era_tree_t *tree, int tree_size)
{
    int m;
    for(m = 0; m < tree_size; m++) { /**< O(nb_alive) comp. overhead */
        tree[m].parent        = m > 0 ? m-1 : 0;
        tree[m].first_child   = m + 1;
        tree[m].next_sibling  = tree_size;
    }
}

static void era_tree_fn_binary(era_tree_t *tree, int tree_size)
{
    int m;

    for(m = 0; m < tree_size; m++) { /**< O(nb_alive) comp. overhead */
        tree[m].parent        = m > 0 ? (m-1)/2 : 0;
        tree[m].first_child   = (2*m+1 < tree_size) ? 2*m+1 : tree_size;
        tree[m].next_sibling  = (m%2 != 0 && m+1 < tree_size ) ? m + 1 : tree_size;
    }
}

typedef void (*era_tree_fn_t)(era_tree_t *, int);
static era_tree_fn_t era_tree_fn = era_tree_fn_binary;

typedef struct {
    int rep;
    int size;
    era_tree_t *tree;
} hierarch_tree_info_t;

static void era_call_tree_fn(era_agreement_info_t *ci)
{
    hierarch_tree_info_t *reps, *rep_p;
    int                   rep, r, rc;
    opal_hash_table_t *rep_table;
    ompi_proc_t *proc;
    orte_vpid_t  daemon_id;
    era_tree_t  *subtree, *rep_tree;
    int          subtree_size, rep_tree_size;

    if( mca_coll_ftbasic_cur_era_topology > 0 ) {
        assert( sizeof(daemon_id) == sizeof(uint32_t) );

        rep_table = OBJ_NEW(opal_hash_table_t);
        opal_hash_table_init(rep_table, orte_process_info.num_daemons);

        reps = (hierarch_tree_info_t *)malloc( (orte_process_info.num_daemons) * sizeof(hierarch_tree_info_t) );

        rep = 0;
        for(r = 0; r < AGS(ci->comm)->tree_size; r++) {
            proc = ompi_group_get_proc_ptr(ci->comm->c_local_group, AGS(ci->comm)->tree[r].rank_in_comm);
            daemon_id = orte_ess.proc_get_daemon( &proc->proc_name );
            assert( ORTE_VPID_INVALID != daemon_id );
            if( opal_hash_table_get_value_uint32(rep_table, (uint32_t)daemon_id, (void**)&rep_p) != OPAL_SUCCESS ) {
                assert(rep <= orte_process_info.num_daemons);
                reps[rep].rep = r;
                reps[rep].size = 1;
                opal_hash_table_set_value_uint32(rep_table, (uint32_t)daemon_id, (void*)&reps[rep]);
                rep++;
            } else {
                rep_p->size++;
            }
        }

        subtree = (era_tree_t *)malloc( (ompi_comm_size(ci->comm) + orte_process_info.num_daemons) * sizeof(era_tree_t));
        subtree_size = 0;
        for(r = 0; r < rep; r++) {
            reps[r].tree = &subtree[subtree_size];
            subtree_size += reps[r].size;
            reps[r].size = 0;
        }
        rep_tree_size = 0;
        rep_tree = &subtree[subtree_size];

        for(r = 0; r < AGS(ci->comm)->tree_size; r++) {
            proc = ompi_group_get_proc_ptr(ci->comm->c_local_group, AGS(ci->comm)->tree[r].rank_in_comm);
            daemon_id = orte_ess.proc_get_daemon( &proc->proc_name );
            assert( ORTE_VPID_INVALID != daemon_id );
            rc = opal_hash_table_get_value_uint32(rep_table, (uint32_t)daemon_id, (void**)&rep_p);
            assert( rc == OPAL_SUCCESS );
            rep_p->tree[rep_p->size].rank_in_comm = r;

            if( rep_p->size == 0 ) {
                rep_tree[rep_tree_size].rank_in_comm = r;
                rep_tree_size++;
            }
            rep_p->size++;
        }

        for(r = 0; r < rep; r++) {
            era_tree_fn(reps[r].tree, reps[r].size);
#if OPAL_ENABLE_DEBUG
            era_tree_check(reps[r].tree, reps[r].size, 0);
            OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                                 "subtree[%d] = %s\n", r, era_debug_tree(reps[r].tree, reps[r].size)));
#endif
        }

        era_tree_fn(rep_tree, rep_tree_size);
#if OPAL_ENABLE_DEBUG
        era_tree_check(rep_tree, rep_tree_size, 0);
        OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                             "overtree = %s\n", era_debug_tree(rep_tree, rep_tree_size)));
#endif

        /* Merge all the subtrees in AGS(ci->comm)->tree */
        for(r = 0; r < rep; r++) {
            int s, rr;
            
            /* Translate from subtrees indexes to big tree indexes */
            for(s = 0; s < reps[r].size; s++) {
                rr = reps[r].tree[s].rank_in_comm;
                AGS(ci->comm)->tree[rr].parent = reps[r].tree[ reps[r].tree[s].parent ].rank_in_comm;

                AGS(ci->comm)->tree[rr].next_sibling = reps[r].tree[s].next_sibling == reps[r].size ? 
                    AGS(ci->comm)->tree_size :
                    reps[r].tree[ reps[r].tree[s].next_sibling ].rank_in_comm;
                AGS(ci->comm)->tree[rr].first_child  = reps[r].tree[s].first_child  == reps[r].size ? 
                    AGS(ci->comm)->tree_size :
                    reps[r].tree[ reps[r].tree[s].first_child  ].rank_in_comm;
            }
            
            /* Merge the subtrees according to the rep_tree */
            rr = rep_tree[r].rank_in_comm;
            AGS(ci->comm)->tree[rr].parent = rep_tree[ rep_tree[r].parent ].rank_in_comm;

            if( rep_tree[r].next_sibling != rep_tree_size ) {
                if( AGS(ci->comm)->tree[rr].next_sibling == AGS(ci->comm)->tree_size ) {
                    AGS(ci->comm)->tree[rr].next_sibling = rep_tree[rep_tree[r].next_sibling].rank_in_comm;
                } else {
                    s = AGS(ci->comm)->tree[rr].next_sibling;
                    do {
                        s = AGS(ci->comm)->tree[s].next_sibling;
                    } while( AGS(ci->comm)->tree[s].next_sibling != AGS(ci->comm)->tree_size );
                    AGS(ci->comm)->tree[s].next_sibling = rep_tree[rep_tree[r].next_sibling].rank_in_comm;
                }
            }

            if( rep_tree[r].first_child != rep_tree_size ) {
                if( AGS(ci->comm)->tree[rr].first_child == AGS(ci->comm)->tree_size ) {
                    AGS(ci->comm)->tree[rr].first_child = rep_tree[rep_tree[r].first_child].rank_in_comm;
                } else {
                    s = AGS(ci->comm)->tree[rr].first_child;
                    while( AGS(ci->comm)->tree[s].next_sibling != AGS(ci->comm)->tree_size ) {
                        s = AGS(ci->comm)->tree[s].next_sibling;
                    }
                    AGS(ci->comm)->tree[s].next_sibling = rep_tree[rep_tree[r].first_child].rank_in_comm;
                }
            }
        }

#if OPAL_ENABLE_DEBUG
        era_tree_check(AGS(ci->comm)->tree, AGS(ci->comm)->tree_size, 0);
        OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                             "finaltree = %s\n", era_debug_tree(AGS(ci->comm)->tree, AGS(ci->comm)->tree_size)));
#endif

        opal_hash_table_remove_all(rep_table);
        OBJ_RELEASE(rep_table);
        free(subtree);
    } else {
        era_tree_fn(AGS(ci->comm)->tree, AGS(ci->comm)->tree_size);
    }
}

#if OPAL_ENABLE_DEBUG
#define ERA_TREE_BUFFER_SIZE 4096
static char era_tree_buffer[ERA_TREE_BUFFER_SIZE];

static void era_debug_walk_tree(era_tree_t *tree, int tree_size, int node)
{
    snprintf(era_tree_buffer + strlen(era_tree_buffer), ERA_TREE_BUFFER_SIZE - strlen(era_tree_buffer),
             "(%d", tree[node].rank_in_comm);
    if( tree[node].first_child != tree_size ) {
        snprintf(era_tree_buffer + strlen(era_tree_buffer), ERA_TREE_BUFFER_SIZE - strlen(era_tree_buffer),
                 ", ");
        era_debug_walk_tree(tree, tree_size, tree[node].first_child);
    }
    snprintf(era_tree_buffer + strlen(era_tree_buffer), ERA_TREE_BUFFER_SIZE - strlen(era_tree_buffer),
             ")");

    if( tree[node].next_sibling != tree_size ) {
        snprintf(era_tree_buffer + strlen(era_tree_buffer), ERA_TREE_BUFFER_SIZE - strlen(era_tree_buffer),
                 ", ");
        era_debug_walk_tree(tree, tree_size, tree[node].next_sibling);
    }
}

static char *era_debug_tree(era_tree_t *tree, int tree_size)
{
    int i;
    era_tree_buffer[0]='\0';
    for(i = 0; i < tree_size; i++) {
        if( tree[i].parent == i ) {
            era_debug_walk_tree(tree, tree_size, i);
            break;
        }
    }
    return era_tree_buffer;
}
#endif

static void era_build_tree_structure(era_agreement_info_t *ci)
{
    int m, n;
    int offset, next_dead_idx;

    n = ompi_comm_size(ci->comm);
    AGS(ci->comm)->tree_size = n - AGS(ci->comm)->afr_size;
    /**< O(nb_alive) memory overhead during agreement */
    AGS(ci->comm)->tree = (era_tree_t*)realloc(AGS(ci->comm)->tree,
                                               AGS(ci->comm)->tree_size * sizeof(era_tree_t));
    offset = 0;
    next_dead_idx = 0;
    for(m = 0; m < AGS(ci->comm)->tree_size; m++) { /**< O(comm_size) comp. overhead */
        /** This assumes that agreed_failed_ranks is maintained sorted. */
        while( next_dead_idx < AGS(ci->comm)->afr_size && (m+offset) == AGS(ci->comm)->agreed_failed_ranks[next_dead_idx] ) {
            next_dead_idx++;
            offset++;
        }
        AGS(ci->comm)->tree[m].rank_in_comm = m + offset;
        /** Initialize each node as a separate root by itself */
        AGS(ci->comm)->tree[m].parent = m;
        AGS(ci->comm)->tree[m].first_child  = AGS(ci->comm)->tree_size;
        AGS(ci->comm)->tree[m].next_sibling = AGS(ci->comm)->tree_size;
    }

    era_call_tree_fn(ci);

    if( ompi_comm_rank(ci->comm) == 0 ) {
        OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Agreement (%d.%d).%d: re-built the tree structure with size %d: %s\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             ci->agreement_id.ERAID_FIELDS.contextid,
                             ci->agreement_id.ERAID_FIELDS.epoch,
                             ci->agreement_id.ERAID_FIELDS.agreementid,
                             AGS(ci->comm)->tree_size,
                             era_debug_tree(ci->ags->tree, ci->ags->tree_size)));
    }

#if OPAL_ENABLE_DEBUG
    era_tree_check(ci->ags->tree, ci->ags->tree_size, 0);
#endif
}

static int era_tree_rank_from_comm_rank(era_agreement_info_t *ci, int r_in_comm)
{
    int r_in_tree = r_in_comm >= ci->ags->tree_size ? ci->ags->tree_size-1 : r_in_comm;

    /** This search is at worst O(nb_dead) */
    while( ci->ags->tree[r_in_tree].rank_in_comm != r_in_comm ) {
        assert( ci->ags->tree[r_in_tree].rank_in_comm == -1 || ci->ags->tree[r_in_tree].rank_in_comm > r_in_comm );
        assert( r_in_tree > 0 );
        r_in_tree--;
    }
    return r_in_tree;
}

static void era_tree_remove_node(era_agreement_info_t *ci, int r_in_tree)
{
    /** All indices are in tree */
    int p, c, s, t;

    p = ci->ags->tree[r_in_tree].parent;
    if( p != r_in_tree ) {
        /** r_in_tree has a parent */

        /** First, for each children process, re-attach them to their new parent,
         *  reminding the first child in s, and the last child in c */
        s = c = ci->ags->tree[r_in_tree].first_child;
        if(s != ci->ags->tree_size ) {
            while(1) {
                ci->ags->tree[c].parent = p;
                if( ci->ags->tree[c].next_sibling == ci->ags->tree_size )
                    break;
                c = ci->ags->tree[c].next_sibling;
            }
        } /** If r_in_tree is a leaf, we don't need to do anything special */

        /** Then, unchain r_in_tree, inserting instead the chain s -> c if it exists */
        if( ci->ags->tree[p].first_child == r_in_tree ) {
            if( s == ci->ags->tree_size ) {
                ci->ags->tree[p].first_child = ci->ags->tree[r_in_tree].next_sibling;
            } else {
                ci->ags->tree[p].first_child = s;
                ci->ags->tree[c].next_sibling = ci->ags->tree[r_in_tree].next_sibling;
            }
        } else {
            for(t = ci->ags->tree[p].first_child;
                ci->ags->tree[t].next_sibling != r_in_tree;
                t = ci->ags->tree[t].next_sibling) {
                assert(t != ci->ags->tree_size); /** r_in_tree should still be chained to its parent */
            }
            if( s == ci->ags->tree_size ) {
                ci->ags->tree[t].next_sibling = ci->ags->tree[r_in_tree].next_sibling;
            } else {
                ci->ags->tree[t].next_sibling = s;
                ci->ags->tree[c].next_sibling = ci->ags->tree[r_in_tree].next_sibling;
            }
        }
    } else {
        int last_sib, new_root, prev_root, first_child, next_root;

        /* r_in_tree was root...*/
        assert( ci->ags->tree[r_in_tree].next_sibling == ci->ags->tree_size );
        assert( ci->ags->tree[r_in_tree].first_child != ci->ags->tree_size );

        /* find the children with smallest rank: only it can be root */
        new_root   = ci->ags->tree[r_in_tree].first_child;
        last_sib = -1;
        prev_root  = -1;
        for( p  = ci->ags->tree[r_in_tree].first_child;
             p != ci->ags->tree_size;
             p  = ci->ags->tree[p].next_sibling ) {
            if( new_root > p ) {
                new_root = p;
                prev_root = last_sib;
            }
            last_sib = p;
        }
        if( last_sib == new_root )
            last_sib = prev_root;

        next_root = ci->ags->tree[new_root].next_sibling;
        if( prev_root != -1 )
            ci->ags->tree[prev_root].next_sibling = next_root;
        ci->ags->tree[new_root].next_sibling = ci->ags->tree_size;
        ci->ags->tree[new_root].parent       = new_root;

        if( new_root != ci->ags->tree[r_in_tree].first_child )
            first_child = ci->ags->tree[r_in_tree].first_child;
        else if( next_root != ci->ags->tree_size ) {
            first_child = next_root;
        } else {
            first_child = ci->ags->tree[new_root].first_child;
        }

        if( first_child != ci->ags->tree[new_root].first_child) {
            if( last_sib != -1 ) {
                ci->ags->tree[last_sib].next_sibling = ci->ags->tree[new_root].first_child;
            }
            ci->ags->tree[new_root].first_child  = first_child;
            for(p = first_child; p != ci->ags->tree_size; p = ci->ags->tree[p].next_sibling) {
                ci->ags->tree[p].parent = new_root;
                if( p == last_sib)
                    break;
            }
        }
    }

#if OPAL_ENABLE_DEBUG
    {
        int prank = ci->ags->tree[r_in_tree].rank_in_comm;

        ci->ags->tree[r_in_tree].rank_in_comm = -1;
        ci->ags->tree[r_in_tree].parent = ci->ags->tree_size;
        ci->ags->tree[r_in_tree].first_child = ci->ags->tree_size;
        ci->ags->tree[r_in_tree].next_sibling = ci->ags->tree_size;
        era_tree_check(ci->ags->tree, ci->ags->tree_size, 0);

        OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Agreement (%d.%d).%d: removed node %d (at position %d) from the tree structure: %s\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             ci->agreement_id.ERAID_FIELDS.contextid,
                             ci->agreement_id.ERAID_FIELDS.epoch,
                             ci->agreement_id.ERAID_FIELDS.agreementid,
                             prank,
                             r_in_tree,
                             era_debug_tree(ci->ags->tree, ci->ags->tree_size)));
    }
#endif
}

static int era_parent(era_agreement_info_t *ci)
{
    int r_in_comm = ompi_comm_rank(ci->comm);
    int r_in_tree = era_tree_rank_from_comm_rank(ci, r_in_comm);
    int p_in_comm, p_in_tree, s_in_tree, c_in_tree;

    while(1) {
        p_in_tree = ci->ags->tree[r_in_tree].parent;
        p_in_comm = ci->ags->tree[p_in_tree].rank_in_comm;
        if( ompi_comm_is_proc_active(ci->comm, p_in_comm, false) )
            return p_in_comm;

        era_tree_remove_node(ci, p_in_tree);
        continue; /**< My new parent might be dead too: start again */
    }
}

static int era_next_child(era_agreement_info_t *ci, int prev_child_in_comm)
{
    ompi_communicator_t *comm = ci->comm;
    int prev_child_in_tree;
    int r_in_tree, c_in_tree, cc_in_tree, c_in_comm;
    int s_in_tree, s_in_comm, sc_in_tree;

    assert(NULL != comm);

    prev_child_in_tree = prev_child_in_comm == -1 ? -1 : era_tree_rank_from_comm_rank(ci, prev_child_in_comm);

    if(prev_child_in_tree == -1) {
        r_in_tree = era_tree_rank_from_comm_rank(ci, ompi_comm_rank(comm));
        while(1) {
            /* We search / fix the tree for the first alive child */
            c_in_tree = ci->ags->tree[r_in_tree].first_child;
            if( c_in_tree == ci->ags->tree_size )
                return ompi_comm_size(comm); /** there are none */
            c_in_comm = ci->ags->tree[c_in_tree].rank_in_comm;
            if( ompi_comm_is_proc_active(comm, c_in_comm, false) )
                return c_in_comm;
            era_tree_remove_node(ci, c_in_tree);
        }
    } else {
        r_in_tree = era_tree_rank_from_comm_rank(ci, prev_child_in_comm);
        while(1) {
            /* We search / fix the tree for the next alive sibling of r */
            s_in_tree = ci->ags->tree[r_in_tree].next_sibling;
            if( s_in_tree == ci->ags->tree_size )
                return ompi_comm_size(comm); /** No more */
            s_in_comm = ci->ags->tree[s_in_tree].rank_in_comm;
            if( ompi_comm_is_proc_active(comm, s_in_comm, false) )
                return s_in_comm;
            era_tree_remove_node(ci, s_in_tree);
        }
    }
}

static void era_collect_passed_agreements(era_identifier_t agreement_id, uint16_t min_aid, uint16_t max_aid)
{
    void *value;
    uint64_t key;
    int r;

    /* Garbage collect agreements that have been decided by all */
    OPAL_OUTPUT_VERBOSE((17, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Agreement (%d.%d).%d: GC: Agreements %u to %u have been acknowledged by all and can be collected\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         agreement_id.ERAID_FIELDS.contextid,
                         agreement_id.ERAID_FIELDS.epoch,                         
                         agreement_id.ERAID_FIELDS.agreementid,
                         min_aid,
                         max_aid));
    if( min_aid <= max_aid ) {
        era_identifier_t pid;
        pid.ERAID_FIELDS.contextid = agreement_id.ERAID_FIELDS.contextid;
        pid.ERAID_FIELDS.epoch     = agreement_id.ERAID_FIELDS.epoch;
        for(r = min_aid; r <= max_aid; r++) {
            pid.ERAID_FIELDS.agreementid = r;
            if( opal_hash_table_get_value_uint64(&era_passed_agreements, pid.ERAID_KEY, &value) == OMPI_SUCCESS ) {
                era_value_t *av = (era_value_t*)value;
                opal_hash_table_remove_value_uint64(&era_passed_agreements, pid.ERAID_KEY);
                OBJ_RELEASE(av);
                OPAL_OUTPUT_VERBOSE((17, ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ERA) Agreement (%d.%d).%d: GC: collect agreement (%d.%d).%d\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     agreement_id.ERAID_FIELDS.contextid,
                                     agreement_id.ERAID_FIELDS.epoch,
                                     agreement_id.ERAID_FIELDS.agreementid,
                                     pid.ERAID_FIELDS.contextid,
                                     pid.ERAID_FIELDS.epoch,
                                     pid.ERAID_FIELDS.agreementid));
            }
            /* It is possible that this agreement was freed already, if multiple are in flight */
        }
    }
}

static void era_decide(era_value_t *decided_value, era_agreement_info_t *ci)
{
    ompi_communicator_t *comm;
    int *new_agreed;
    era_rank_item_t *rl;
    int r, s, dead_size;
    void *value;

    OBJ_RETAIN(decided_value);

    OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) decide %08x.%d.%d.. on agreement (%d.%d).%d\n",                         
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         *(int*)decided_value->bytes,
                         decided_value->header.ret,
                         decided_value->header.nb_new_dead,
                         ci->agreement_id.ERAID_FIELDS.contextid,
                         ci->agreement_id.ERAID_FIELDS.epoch,                         
                         ci->agreement_id.ERAID_FIELDS.agreementid));

    /** We must leave ci in the era_ongoing_agreements, because either the
     *  iagree request or the blocking loop above need to find it for
     *  cleanup.*/

    assert( opal_hash_table_get_value_uint64(&era_passed_agreements, 
                                             ci->agreement_id.ERAID_KEY, &value) != OMPI_SUCCESS );
    ci->status = COMPLETED;
    opal_hash_table_set_value_uint64(&era_passed_agreements,
                                     ci->agreement_id.ERAID_KEY,
                                     decided_value);    

    comm = ci->comm;
    assert( NULL != comm );

    if( decided_value->header.nb_new_dead != 0 ) {
        OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) decide %08x.%d.%d on agreement (%d.%d).%d: adding up to %d processes to the list of agreed deaths\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             *(int*)decided_value->bytes,
                             decided_value->header.ret,
                             decided_value->header.nb_new_dead,
                             ci->agreement_id.ERAID_FIELDS.contextid,
                             ci->agreement_id.ERAID_FIELDS.epoch,                         
                             ci->agreement_id.ERAID_FIELDS.agreementid,
                             decided_value->header.nb_new_dead));
        
#if OPAL_ENABLE_DEBUG
        {
            int _i, _j;
            for(_i = 0; _i < decided_value->header.nb_new_dead; _i++) {
                assert(decided_value->new_dead_array[_i] >= 0 &&
                       decided_value->new_dead_array[_i] < ompi_comm_size(comm));
                for(_j = _i+1; _j < decided_value->header.nb_new_dead; _j++) {
                    assert(decided_value->new_dead_array[_i] < decided_value->new_dead_array[_j]);
                }
            }
        }
#endif /*OPAL_ENABLE_DEBUG*/

        dead_size = AGS(comm)->afr_size + decided_value->header.nb_new_dead;
        AGS(comm)->agreed_failed_ranks = (int*)realloc(AGS(comm)->agreed_failed_ranks, dead_size * sizeof(int));

        for(s = 0, r = 0; r < decided_value->header.nb_new_dead; r++) {
            while(s < AGS(comm)->afr_size && AGS(comm)->agreed_failed_ranks[s] < decided_value->new_dead_array[r] ) s++;
            if( s == AGS(comm)->afr_size ) {
                /** paste the remaining ints at the end of the array */
                memcpy(AGS(comm)->agreed_failed_ranks + AGS(comm)->afr_size,
                       decided_value->new_dead_array + r,
                       (decided_value->header.nb_new_dead - r) * sizeof(int));
                AGS(comm)->afr_size += decided_value->header.nb_new_dead - r;
                if(mca_coll_ftbasic_era_rebuild) AGS(comm)->ags_status |= AGS_TREE_DIRTY;
                break;
            } else if( AGS(comm)->agreed_failed_ranks[s] > decided_value->new_dead_array[r] ) {
                /** make some room for one int */
                memmove(AGS(comm)->agreed_failed_ranks + s + 1,
                        AGS(comm)->agreed_failed_ranks + s,
                        (AGS(comm)->afr_size - s) * sizeof(int));
                AGS(comm)->afr_size++;
                if(mca_coll_ftbasic_era_rebuild) AGS(comm)->ags_status |= AGS_TREE_DIRTY;
                /** and insert new_dead[r] */
                AGS(comm)->agreed_failed_ranks[s] = decided_value->new_dead_array[r];
            } else {
                /** It was already in, let's skip it */
            }
        }

#if OPAL_ENABLE_DEBUG
        {
            int _i, _j;
            for(_i = 0; _i < AGS(comm)->afr_size; _i++)
                for(_j = _i+1; _j < AGS(comm)->afr_size; _j++)
                    assert(AGS(comm)->agreed_failed_ranks[_i] < AGS(comm)->agreed_failed_ranks[_j]);
        }
#endif /*OPAL_ENABLE_DEBUG*/
    }

    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) decide %08x.%d.%d.. on agreement (%d.%d).%d: group of agreed deaths is of size %d\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         *(int*)decided_value->bytes,
                         decided_value->header.ret,
                         decided_value->header.nb_new_dead,
                         ci->agreement_id.ERAID_FIELDS.contextid,
                         ci->agreement_id.ERAID_FIELDS.epoch,                         
                         ci->agreement_id.ERAID_FIELDS.agreementid,
                         AGS(comm)->afr_size));

    r = -1;
    while( (r = era_next_child(ci, r)) < ompi_comm_size(comm) ) {

        /** Cleanup the early_requesters list, to avoid sending unecessary dupplicate messages */
        if( opal_list_get_size(&ci->early_requesters) > 0 ) {
            for(rl = (era_rank_item_t*)opal_list_get_first(&ci->early_requesters);
                rl != (era_rank_item_t*)opal_list_get_end(&ci->early_requesters);
                rl = (era_rank_item_t*)opal_list_get_next(&rl->super)) {
                if( rl->rank == r ) {
                    opal_list_remove_item(&ci->early_requesters, &rl->super);
                    break;
                }
            }
        }

        send_msg(comm, r, NULL, ci->agreement_id, MSG_DOWN, decided_value, 0, NULL);
    }

    /* In case we have some child we haven't detected yet */
    if( opal_list_get_size(&ci->early_requesters) > 0 ) {
        for(rl = (era_rank_item_t*)opal_list_get_first(&ci->early_requesters);
            rl != (era_rank_item_t*)opal_list_get_end(&ci->early_requesters);
            rl = (era_rank_item_t*)opal_list_get_next(&rl->super)) {
            send_msg(comm, rl->rank, NULL, ci->agreement_id, MSG_DOWN, decided_value, 0, NULL);
        }
    }
    
    era_collect_passed_agreements(ci->agreement_id, decided_value->header.min_aid, decided_value->header.max_aid);

    if( ci->req ) {
        ompi_request_complete(&ci->req->super, true);
    }
}

static void era_check_status(era_agreement_info_t *ci)
{
    int r;
    era_rank_item_t *rl;

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
        (void)era_parent(ci); /* Maybe I'm becoming the root, need to recheck my children. TODO: only rebuild the necessary part */
        r = -1;
        while( (r = era_next_child(ci, r)) < ompi_comm_size(ci->comm) ) {
            OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) check_status for Agreement ID = (%d.%d).%d, child %d is supposed to contribute\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                 ci->agreement_id.ERAID_FIELDS.contextid,
                                 ci->agreement_id.ERAID_FIELDS.epoch,
                                 ci->agreement_id.ERAID_FIELDS.agreementid,
                                 r));
            for( rl =  (era_rank_item_t*)opal_list_get_first(&ci->gathered_info);
                 rl != (era_rank_item_t*)opal_list_get_end(&ci->gathered_info);
                 rl =  (era_rank_item_t*)opal_list_get_next(&rl->super) ) {
                if( rl->rank == r ) {
                    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                                         "%s ftbasic:agreement (ERA) check_status for Agreement ID = (%d.%d).%d, child %d has sent its message\n",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                         ci->agreement_id.ERAID_FIELDS.contextid,
                                         ci->agreement_id.ERAID_FIELDS.epoch,
                                         ci->agreement_id.ERAID_FIELDS.agreementid,
                                         r));
                    break;
                }
            }
            if( rl == (era_rank_item_t*)opal_list_get_end(&ci->gathered_info) ) {
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
        era_update_return_value(ci, -1, NULL);

        if( ci->comm->c_my_rank == (r = era_parent(ci)) ) {
            OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) check_status for Agreement ID = (%d.%d).%d, all children of root have contributed\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                 ci->agreement_id.ERAID_FIELDS.contextid,
                                 ci->agreement_id.ERAID_FIELDS.epoch,
                                 ci->agreement_id.ERAID_FIELDS.agreementid));

            /* I'm root. I have to decide now. */
            era_decide(ci->current_value, ci);
        } else {
            OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) check_status for Agreement ID = (%d.%d).%d, all children of non-root have contributed\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                 ci->agreement_id.ERAID_FIELDS.contextid,
                                 ci->agreement_id.ERAID_FIELDS.epoch,
                                 ci->agreement_id.ERAID_FIELDS.agreementid));

            /* Let's forward up and wait for the DOWN messages */
            ci->waiting_down_from = r;
            send_msg(ci->comm, r, NULL, ci->agreement_id, MSG_UP, ci->current_value, 
                     ci->nb_acked, ci->acked);
            ci->status = BROADCASTING;
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
    era_rank_item_t *rc;
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
        rc = OBJ_NEW(era_rank_item_t);
        rc->rank = r;
        opal_list_append(&ci->waiting_res_from, &rc->super);
        send_msg(ci->comm, r, NULL, ci->agreement_id, MSG_RESULT_REQUEST, ci->current_value, 
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
    era_rank_item_t *rl;
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
        ci->current_value->header.ret = MPI_ERR_PROC_FAILED;
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
                send_msg(ci->comm, r, NULL, ci->agreement_id, MSG_UP, ci->current_value, 
                         ci->nb_acked, ci->acked);
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
        for(rl = (era_rank_item_t*)opal_list_get_first(&ci->waiting_res_from);
            rl != (era_rank_item_t*)opal_list_get_end(&ci->waiting_res_from);
            rl = (era_rank_item_t*)opal_list_get_next(&rl->super)) {
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
                    for(rl = (era_rank_item_t*)opal_list_get_first(&ci->waiting_res_from);
                        rl != (era_rank_item_t*)opal_list_get_end(&ci->waiting_res_from);
                        rl = (era_rank_item_t*)opal_list_get_next(&rl->super)) {
                        if( rl->rank == r ) {
                            break;
                        }
                    }

                    if( rl == (era_rank_item_t*)opal_list_get_end(&ci->waiting_res_from) ) {
                        OPAL_OUTPUT_VERBOSE((20,  ompi_ftmpi_output_handle,
                                             "%s ftbasic:agreement (ERA) Handling failure of process %d: Requesting contribution of process %d for Agreement (%d.%d).%d.\n",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                             rank, r,
                                             ci->agreement_id.ERAID_FIELDS.contextid,
                                             ci->agreement_id.ERAID_FIELDS.epoch,
                                             ci->agreement_id.ERAID_FIELDS.agreementid));
                        rl = OBJ_NEW(era_rank_item_t);
                        rl->rank = r;
                        opal_list_append(&ci->waiting_res_from, &rl->super);
                        send_msg(ci->comm, r, NULL, ci->agreement_id, MSG_RESULT_REQUEST, ci->current_value, 
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

static void fragment_sent(struct mca_btl_base_module_t* module,
                          struct mca_btl_base_endpoint_t* endpoint,
                          struct mca_btl_base_descriptor_t* descriptor,
                          int status)
{
    (void)module;
    (void)endpoint;
    (void)descriptor;
    (void)status;
}

static void send_msg(ompi_communicator_t *comm,
                     int dst,
                     orte_process_name_t *proc_name,
                     era_identifier_t agreement_id,
                     era_msg_type_t type,
                     era_value_t *value,
                     int          nb_ack_failed,
                     int         *ack_failed)
{
    mca_btl_base_descriptor_t *des;
    struct iovec iov[4]; /**< message header, flag bytes, newly_dead, acknowledged */
    long unsigned int niov = 0, b, i, copied, tocopy;
    era_msg_header_t msg_header;
    era_frag_t *frag;
    ompi_proc_t *peer;
    mca_bml_base_endpoint_t *proc_bml;
    mca_bml_base_btl_t *bml_btl;
    struct mca_btl_base_endpoint_t *btl_endpoint;
    mca_btl_base_module_t *btl;
    uint64_t my_seqnum;
    unsigned int to_send, sent;
    long unsigned int payload_size;

#if defined(PROGRESS_FAILURE_PROB)
#pragma message("Hard coded probability of failure inside the agreement")
    if( coll_ftbasic_debug_rank_may_fail &&
        (double)rand() / (double)RAND_MAX < PROGRESS_FAILURE_PROB ) {
        OPAL_OUTPUT_VERBOSE((0, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Killing myself just before sending message [(%d.%d).%d, %s, %08x.%d.%d..] to %d/%s\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             agreement_id.ERAID_FIELDS.contextid,
                             agreement_id.ERAID_FIELDS.epoch,
                             agreement_id.ERAID_FIELDS.agreementid,
                             era_msg_type_to_string(type),
                             *(int*)value->bytes,
                             value->header.ret,
                             value->header.nb_new_dead,
                             dst,
                             NULL != proc_name ? ORTE_NAME_PRINT(proc_name) : "(null)"));
        raise(SIGKILL);
    }
#endif

    if( MSG_UP == type ) {
        OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) send message [(%d.%d).%d, %s, %08x.%d.%d/%d] to %d/%s\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             agreement_id.ERAID_FIELDS.contextid,
                             agreement_id.ERAID_FIELDS.epoch,
                             agreement_id.ERAID_FIELDS.agreementid,
                             era_msg_type_to_string(type),
                             *(int*)value->bytes,
                             value->header.ret,
                             value->header.nb_new_dead,
                             nb_ack_failed,
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
                             value->header.ret,
                             value->header.nb_new_dead,
                             dst,
                             NULL != proc_name ? ORTE_NAME_PRINT(proc_name) : "(null)"));
        assert(nb_ack_failed == 0);
   }

#if OPAL_ENABLE_DEBUG
    if( type == MSG_DOWN ) {
        int _i;
        for(_i = 1; _i < value->header.nb_new_dead; _i++)
            assert(value->new_dead_array[_i-1] < value->new_dead_array[_i]);
    }
#endif

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

    to_send = ERA_MSG_SIZE(&value->header, nb_ack_failed);

    /* We prepare the header, that we store in msg */
    msg_header.msg_type = type;
    msg_header.agreement_id.ERAID_KEY = agreement_id.ERAID_KEY;
    memcpy(&msg_header.agreement_value_header, &value->header, sizeof(era_value_header_t));
    if( NULL != comm ) {
        msg_header.src_comm_rank = ompi_comm_rank(comm);
    } else {
        msg_header.src_comm_rank = -1;
    }
    msg_header.src_proc_name = *ORTE_PROC_MY_NAME;
    if( MSG_UP == type ) {
        msg_header.nb_ack = nb_ack_failed;
    } else {
        msg_header.nb_ack = 0;
    }

    iov[0].iov_base = (char*)&msg_header;
    iov[0].iov_len = sizeof(era_msg_header_t);
    niov = 1;
    
    if( ERA_VALUE_BYTES_COUNT(&value->header) > 0 ) {
        iov[niov].iov_base = value->bytes;
        iov[niov].iov_len = ERA_VALUE_BYTES_COUNT(&value->header);
        niov++;
    }
    if( value->header.nb_new_dead > 0 ) {
        iov[niov].iov_base = value->new_dead_array;
        iov[niov].iov_len  = value->header.nb_new_dead * sizeof(int);
        niov++;
    }
    if( MSG_UP == type && nb_ack_failed > 0 ) {
        iov[niov].iov_base = ack_failed;
        iov[niov].iov_len  = nb_ack_failed * sizeof(int);
        niov++;
    }

    OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) send message [(%d.%d).%d, %s, %08x.%d.%d/%d..] to %d/%s: send %d bytes through iov (nb = %d, lens = %d,%d,%d,%d)\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         agreement_id.ERAID_FIELDS.contextid,
                         agreement_id.ERAID_FIELDS.epoch,
                         agreement_id.ERAID_FIELDS.agreementid,
                         era_msg_type_to_string(type),
                         *(int*)value->bytes,
                         value->header.ret,
                         value->header.nb_new_dead,
                         msg_header.nb_ack,
                         dst,
                         NULL != proc_name ? ORTE_NAME_PRINT(proc_name) : "(null)",
                         to_send,
                         niov,
                         iov[0].iov_len,
                         iov[1].iov_len,
                         iov[2].iov_len,
                         iov[3].iov_len));

#if OPAL_ENABLE_DEBUG
            {
                char strbytes[256];
                long unsigned int w;
                strbytes[0] = '\0';

                i = 0;
                w = 0;
                b = 0;
                do {
                    if(b == iov[i].iov_len) {
                        i++;
                        w += snprintf(strbytes + strlen(strbytes), 256 - strlen(strbytes), "|");
                        if( i == niov )
                            break;
                        b = 0;
                    }
                    w += snprintf(strbytes + strlen(strbytes), 256 - strlen(strbytes), "%02x", ((uint8_t*)iov[i].iov_base)[b]);
                    b++;
                } while(w < 256);
                if( strlen(strbytes) >= 252 ) {
                    sprintf(strbytes + 252, "...");
                }

                OPAL_OUTPUT_VERBOSE((30, ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ERA) send message [(%d.%d).%d, %s, %08x.%d.%d/%d..] to %d/%s: %d bytes including header = %s\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                     agreement_id.ERAID_FIELDS.contextid,
                                     agreement_id.ERAID_FIELDS.epoch,
                                     agreement_id.ERAID_FIELDS.agreementid,
                                     era_msg_type_to_string(type),
                                     *(int*)value->bytes,
                                     value->header.ret,
                                     value->header.nb_new_dead,
                                     msg_header.nb_ack,
                                     dst,
                                     NULL != proc_name ? ORTE_NAME_PRINT(proc_name) : "(null)",
                                     to_send,
                                     strbytes));
            }
#endif

    sent    = 0;
    my_seqnum = msg_seqnum++;
    i = 0;
    b = 0;
    while( sent < to_send ) {
        /** Try to send everything in one go */
        des = btl->btl_alloc(btl, btl_endpoint, MCA_BTL_NO_ORDER, sizeof(era_frag_t) - 1 + to_send - sent,
                             MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
        payload_size = des->des_src->seg_len - sizeof(era_frag_t) + 1;
        assert( payload_size > 0 ); /** We can send at least a byte */

        des->des_cbfunc = fragment_sent;
        des->des_cbdata = NULL;
        frag = (era_frag_t*)des->des_src->seg_addr.pval;
        frag->src = *ORTE_PROC_MY_NAME;
        frag->msg_seqnum  = my_seqnum;
        frag->frag_offset = sent;
        frag->frag_len    = payload_size;
        frag->msg_len     = to_send;
        copied = 0;
        while( copied < payload_size ) {
            if( payload_size - copied <= iov[i].iov_len - b )
                tocopy = payload_size - copied;
            else
                tocopy = iov[i].iov_len - b;
            memcpy(frag->bytes + copied, ((uint8_t*)iov[i].iov_base) + b, tocopy);
            b += tocopy;
            if( b == iov[i].iov_len ) {
                assert(i+1 < niov || copied + tocopy == payload_size);
                i++;
                b = 0;
            }
            copied += tocopy;
        }
        btl->btl_send(btl, btl_endpoint, des, MCA_BTL_TAG_FT_AGREE);
        sent += payload_size;
    }
}

static void result_request(era_msg_header_t *msg_header)
{
    void *value;
    era_value_t *old_agreement_value;
    era_agreement_info_t *ci;
    int r;

    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received RESULT_REQUEST Message: Agreement ID = (%d.%d).%d, sender: %d/%s\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         msg_header->agreement_id.ERAID_FIELDS.contextid,
                         msg_header->agreement_id.ERAID_FIELDS.epoch,
                         msg_header->agreement_id.ERAID_FIELDS.agreementid,
                         msg_header->src_comm_rank,
                         ORTE_NAME_PRINT(&msg_header->src_proc_name)));

    if( opal_hash_table_get_value_uint64(&era_passed_agreements,
                                         msg_header->agreement_id.ERAID_KEY,
                                         &value) == OPAL_SUCCESS ) {
        old_agreement_value = (era_value_t*)value;
        send_msg(NULL, msg_header->src_comm_rank, &msg_header->src_proc_name, msg_header->agreement_id, MSG_DOWN, old_agreement_value, 0, NULL);
    } else {
        /** I should be a descendent of msg_header->src (since RESULT_REQUEST messages are sent to
         *  people below the caller.
         *  So, the caller is the current root (or it is dead now and a new root was selected)
         *  Two cases: */
        
        ci = era_lookup_agreeement_info(msg_header->agreement_id);
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

            ci->waiting_down_from = r;
            send_msg(ci->comm, r, NULL, ci->agreement_id, MSG_UP, ci->current_value, 
                     ci->nb_acked, ci->acked);
        } else {
            era_value_t success_value;
            OBJ_CONSTRUCT(&success_value, era_value_t);
            success_value.header.ret = MPI_SUCCESS;
            success_value.header.dt_count = 0;
            success_value.header.operand  = ompi_mpi_op_band.op.o_f_to_c_index;
            success_value.header.datatype = ompi_mpi_int.dt.id;
            success_value.header.nb_new_dead = 0;
            success_value.bytes = NULL;
            success_value.new_dead_array = NULL;

            /** Could be an old agreement that I collected already.
             *  If that is the case, the epoch requested should be <= the current epoch for
             *  that contextid (modulo rotation on the epoch numbering), and then the
             *  number of requested data must be 0, by convention on the last "flushing"
             *  agreement that was posted during the free.
             */
            if( msg_header->agreement_value_header.dt_count == 0 ) {
                uint16_t requested_cid = msg_header->agreement_id.ERAID_FIELDS.contextid;
                void *location = opal_pointer_array_get_item(&ompi_mpi_comm_epoch, requested_cid);
                uint32_t known_epoch = (uint32_t)((uintptr_t)location);
#if OPAL_ENABLE_DEBUG
                int64_t distance_betwheen_epochs = (int64_t)known_epoch - (int64_t)msg_header->agreement_id.ERAID_FIELDS.epoch;
                if( distance_betwheen_epochs > 0 )
                    assert(  distance_betwheen_epochs < (1LL<<31) );
                else
                    assert( -distance_betwheen_epochs < (1LL<<31) );
#endif
                /** Then, the answer is "success" */
                send_msg(NULL, msg_header->src_comm_rank, &msg_header->src_proc_name, msg_header->agreement_id, 
                         MSG_DOWN, &success_value, 0, NULL);

                OBJ_DESTRUCT(&success_value);
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
}

static void msg_up(era_msg_header_t *msg_header, uint8_t *bytes, int *new_dead, int *ack_failed)
{
    era_agreement_info_t *ci;
    era_rank_item_t *rank_item;
    void *value;
    era_value_t *av;
 
    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received UP Message: Agreement ID = (%d.%d).%d, sender: %d/%s, msg value: %08x.%d.%d/%d\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         msg_header->agreement_id.ERAID_FIELDS.contextid,
                         msg_header->agreement_id.ERAID_FIELDS.epoch,
                         msg_header->agreement_id.ERAID_FIELDS.agreementid,
                         msg_header->src_comm_rank,
                         ORTE_NAME_PRINT(&msg_header->src_proc_name),
                         *(int*)bytes,
                         msg_header->agreement_value_header.ret,
                         msg_header->agreement_value_header.nb_new_dead,
                         msg_header->nb_ack));

    /** It could be an UP message about a decided agreement:
     *  a child gives me its contribution, I broadcast and receive
     *  the decision, or decide myself, and then the child dies before
     *  it could transmit the decision to its own children. The children
     *  will contact me as their new parent, still in their BROADCAST phase,
     *  so what this UP message really means is "give me the decision." */
    if( opal_hash_table_get_value_uint64(&era_passed_agreements,
                                         msg_header->agreement_id.ERAID_KEY,
                                         &value) == OPAL_SUCCESS ) {
        av = (era_value_t*)value;
        send_msg(NULL, msg_header->src_comm_rank, &msg_header->src_proc_name, msg_header->agreement_id, MSG_DOWN, av, 
                 0, NULL);
        return;
    }

    ci = era_lookup_agreeement_info( msg_header->agreement_id );

    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Managing UP Message, agreement is %s\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ci == NULL ? "unknown" : "known"));

    if( NULL == ci ) {
        ci = era_create_agreement_info( msg_header->agreement_id, &msg_header->agreement_value_header );
        if( msg_header->agreement_value_header.nb_new_dead > 0 ) {
            /* ci->new_dead_array was allocated by create_agreement_info, let's copy it */
            memcpy(ci->current_value->new_dead_array, new_dead,
                   msg_header->agreement_value_header.nb_new_dead * sizeof(int));
        }
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
                             msg_header->src_comm_rank));

        /** We could receive multiple messages from msg_header->src_comm_rank, because the messages are split */
        for(rank_item = (era_rank_item_t*)opal_list_get_first(&ci->early_requesters);
            rank_item != (era_rank_item_t*)opal_list_get_end(&ci->early_requesters);
            rank_item = (era_rank_item_t*)opal_list_get_next(&rank_item->super)) {
            if( rank_item->rank == msg_header->src_comm_rank )
                return;
        }

        /** If not, add it */
        rank_item = OBJ_NEW(era_rank_item_t);
        rank_item->rank = msg_header->src_comm_rank;
        opal_list_append(&ci->early_requesters, &rank_item->super);
        return;
    }

    /** Did we receive enough contributions from that rank already?
     *  He could be re-sending his data, because of some failure that
     *  was discovered, and I requested it because I became root (because
     *  of another failure), but it discovered the first failure at the 
     *  same time, and started sending without me requesting.
     */
    for( rank_item = (era_rank_item_t *)opal_list_get_first( &ci->gathered_info );
         rank_item != (era_rank_item_t *)opal_list_get_end( &ci->gathered_info );
         rank_item = (era_rank_item_t *)opal_list_get_next( &rank_item->super ) ) {
        if( rank_item->rank == msg_header->src_comm_rank ) {
            /* We are not waiting from more messages, thank you */
            /* Maybe you're telling me again you want me to send? Let's check if I can't */
            era_check_status(ci);
            return;
        }
    }

    av = OBJ_NEW(era_value_t);
    memcpy(&av->header, &msg_header->agreement_value_header, sizeof(era_value_header_t));
    /* We don't allocate the arrays of bytes and new_dead ranks: we point in the message.
     * These pointers will need to be set to NULL *before* calling RELEASE.
     * We can do this, because combine_agreement_values does not keep a reference on av */
    av->bytes = bytes;
    if( av->header.nb_new_dead > 0 )
        av->new_dead_array = (int*) (bytes + ERA_VALUE_BYTES_COUNT(&av->header));
    else
        av->new_dead_array = NULL;

    /* ci holds the current agreement information structure */
    era_combine_agreement_values(ci, av);
    era_update_return_value(ci, msg_header->nb_ack, ack_failed);

    av->new_dead_array = NULL;
    av->bytes = NULL;
    OBJ_RELEASE(av);

    /* We already checked above that this process did not contribute yet */
    rank_item = OBJ_NEW(era_rank_item_t);
    rank_item->rank = msg_header->src_comm_rank;
    opal_list_append(&ci->gathered_info, &rank_item->super);
    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received UP Message: adding %d in list of people that contributed\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank_item->rank));

    era_check_status(ci);
}

static void msg_down(era_msg_header_t *msg_header, uint8_t *bytes, int *new_dead)
{
    era_agreement_info_t *ci;
    era_value_t *av;

    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received DOWN Message: Agreement ID = (%d.%d).%d, sender: %d/%s, msg value: %08x.%d.%d..\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         msg_header->agreement_id.ERAID_FIELDS.contextid,
                         msg_header->agreement_id.ERAID_FIELDS.epoch,
                         msg_header->agreement_id.ERAID_FIELDS.agreementid,
                         msg_header->src_comm_rank,
                         ORTE_NAME_PRINT(&msg_header->src_proc_name),
                         *(int*)bytes,
                         msg_header->agreement_value_header.ret,
                         msg_header->agreement_value_header.nb_new_dead));

    ci = era_lookup_agreeement_info( msg_header->agreement_id );
    if( NULL == ci ) {
        /** This can happen, if this DOWN is the result of a REQUEST, and
         *  we received another DOWN from another sent REQUEST, and we
         *  decided, so stored that agreement in the passed_agreements
         */
        return;
    }
    /** if I receive a down message on an agreement I know about, I already participated. */
    assert( NULL != ci->comm );

    av = OBJ_NEW(era_value_t);
    memcpy(&av->header, &msg_header->agreement_value_header, sizeof(era_value_header_t));
    /* We must allocate the arrays of bytes and new_dead ranks, because era_decide is going
     * to keep that era_value_t */
    av->bytes = (uint8_t *)malloc(ERA_VALUE_BYTES_COUNT(&av->header));
    memcpy(av->bytes, bytes, ERA_VALUE_BYTES_COUNT(&av->header));
    if( av->header.nb_new_dead > 0 ) {
        av->new_dead_array = (int*)malloc(av->header.nb_new_dead * sizeof(int));
        memcpy(av->new_dead_array, new_dead,
               av->header.nb_new_dead * sizeof(int));
    }
    era_decide(av, ci);

    OBJ_RELEASE(av);
}

static void era_cb_fn(struct mca_btl_base_module_t* btl,
                      mca_btl_base_tag_t tag,
                      mca_btl_base_descriptor_t* descriptor,
                      void* cbdata)
{
    era_incomplete_msg_t *incomplete_msg = NULL;
    era_msg_header_t *msg_header;
    era_frag_t *frag;
    uint64_t src_hash;
    void *value;
    opal_hash_table_t *msg_table;
    uint8_t *msg_bytes, *value_bytes;
    int *new_dead;
    int *ack_failed;

    assert(MCA_BTL_TAG_FT_AGREE == tag);
    assert(1 == descriptor->des_dst_cnt);
    (void)cbdata;

    frag = (era_frag_t*)descriptor->des_dst->seg_addr.pval;

    if( frag->msg_len == frag->frag_len ) {
        assert(frag->frag_offset == 0);
        msg_bytes = frag->bytes;
    } else {
        src_hash = orte_util_hash_name(&frag->src);
        if( opal_hash_table_get_value_uint64(&era_incomplete_messages, src_hash, &value) == OMPI_SUCCESS ) {
            msg_table = (opal_hash_table_t*)value;
        } else {
            msg_table = OBJ_NEW(opal_hash_table_t);
            opal_hash_table_init(msg_table, 3 /* This should be very small: few messages should fly in parallel */);
            opal_hash_table_set_value_uint64(&era_incomplete_messages, src_hash, (void*)msg_table);
        }

        if( opal_hash_table_get_value_uint64(msg_table, frag->msg_seqnum, &value) == OMPI_SUCCESS ) {
            incomplete_msg = (era_incomplete_msg_t*)value;
        } else {
            incomplete_msg = (era_incomplete_msg_t*)malloc(frag->msg_len + sizeof(unsigned int));
            incomplete_msg->bytes_received = 0;
            opal_hash_table_set_value_uint64(msg_table, frag->msg_seqnum, (void*)incomplete_msg);
        }

        memcpy( incomplete_msg->bytes + frag->frag_offset,
                frag->bytes,
                frag->frag_len );
        incomplete_msg->bytes_received += frag->frag_len;

        /** We receive the messages in order */
        if( incomplete_msg->bytes_received == frag->msg_len ) {
            msg_bytes = incomplete_msg->bytes;
            opal_hash_table_remove_value_uint64(msg_table, frag->msg_seqnum);
            /** We leave msg_table into the global table, as we will receive more messages */
        } else {
            /** This message is incomplete */
            return;
        }
    }

    msg_header = (era_msg_header_t *)msg_bytes;
    msg_bytes += sizeof(era_msg_header_t);

    if( ERA_VALUE_BYTES_COUNT(&msg_header->agreement_value_header) > 0 ) {
        value_bytes = msg_bytes;
        msg_bytes += ERA_VALUE_BYTES_COUNT(&msg_header->agreement_value_header);
    } else {
        value_bytes = NULL;
    }

    if( msg_header->agreement_value_header.nb_new_dead > 0 ) {
        new_dead = (int*)msg_bytes;
        msg_bytes += msg_header->agreement_value_header.nb_new_dead * sizeof(int);
    } else {
        new_dead = NULL;
    }

    if( msg_header->nb_ack > 0 ) {
        ack_failed = (int*)msg_bytes;
    } else {
        ack_failed = NULL;
    }
    
#if defined(PROGRESS_FAILURE_PROB)
#pragma message("Hard coded probability of failure inside the agreement")
    if( coll_ftbasic_debug_rank_may_fail &&
        (double)rand() / (double)RAND_MAX < PROGRESS_FAILURE_PROB ) {
        OPAL_OUTPUT_VERBOSE((0, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Killing myself just before receiving message [(%d.%d).%d, %d, %08x.%d.%d...] from %d/%s\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             msg_header->agreement_id.ERAID_FIELDS.contextid,
                             msg_header->agreement_id.ERAID_FIELDS.epoch,
                             msg_header->agreement_id.ERAID_FIELDS.agreementid,
                             msg_header->msg_type,
                             *(int*)value_bytes,
                             msg_header->agreement_value_header.ret,
                             msg_header->agreement_value_header.nb_new_dead,
                             msg_header->src_comm_rank,
                             ORTE_NAME_PRINT(&msg_header->src_proc_name)));
        raise(SIGKILL);
    }
#endif
    
    switch( msg_header->msg_type ) {
    case MSG_RESULT_REQUEST:
        result_request(msg_header);
        return;
    case MSG_UP:
        msg_up(msg_header, value_bytes, new_dead, ack_failed);
        return;
    case MSG_DOWN:
        msg_down(msg_header, value_bytes, new_dead);
        return;
    }

    if( NULL != incomplete_msg ) {
        free(incomplete_msg);
    }
}

static void era_on_comm_rank_failure(ompi_communicator_t *comm, int rank, bool remote)
{
    void *value, *next_value;
    era_agreement_info_t *ci;
    void *node;
    uint64_t key64, key64_2;
    int rc;
    era_identifier_t cid;
    ompi_proc_t *ompi_proc;
    opal_hash_table_t *msg_table;

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) %d in communicator (%d.%d) died\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank,
                         comm->c_contextid,
                         comm->c_epoch));

    if( AGS(comm) != NULL ) {
        AGS(comm)->ags_status |= AGS_AFR_DIRTY;
    }

    /** Discard incomplete messages, and remove the entry to store these messages */
    ompi_proc = ompi_group_peer_lookup(remote ? comm->c_remote_group : comm->c_local_group, rank);
    assert(NULL != ompi_proc);
    key64 = orte_util_hash_name( &ompi_proc->proc_name );
    if( opal_hash_table_get_value_uint64(&era_incomplete_messages, key64, &value) == OPAL_SUCCESS ) {
        msg_table = (opal_hash_table_t*)value;
        if( opal_hash_table_get_first_key_uint64(msg_table, &key64_2, &value, &node) == OPAL_SUCCESS ) {
            do {
                free( value );
            } while( opal_hash_table_get_next_key_uint64(msg_table,
                                                         &key64_2, &value, 
                                                         node, &node) == OPAL_SUCCESS );
        }
        opal_hash_table_remove_value_uint64(&era_incomplete_messages, key64);
    }

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

    switch( mca_coll_ftbasic_cur_era_topology < 0 ? -mca_coll_ftbasic_cur_era_topology : mca_coll_ftbasic_cur_era_topology ) {
    case 1:
        era_tree_fn = era_tree_fn_binary;
        break;
    case 2:
        era_tree_fn = era_tree_fn_string;
        break;
    case 3:
        era_tree_fn = era_tree_fn_star;
        break;
    default:
        era_tree_fn = era_tree_fn_binary;
    }
    
    mca_bml.bml_register(MCA_BTL_TAG_FT_AGREE, era_cb_fn, NULL);

    OBJ_CONSTRUCT( &era_iagree_requests, ompi_free_list_t);
    ompi_free_list_init_new( &era_iagree_requests,
                             sizeof(era_iagree_request_t),
                             opal_cache_line_size,
                             OBJ_CLASS(era_iagree_request_t),
                             0,opal_cache_line_size,
                             /* initial number of elements to allocate */ 0,
                             /* maximum number of elements */ INT_MAX,
                             /* increment */ 1,
                             NULL );
    
    OBJ_CONSTRUCT( &era_passed_agreements, opal_hash_table_t);
    /* The garbage collection system relies on iterating over all
     * passed agreements at the beginning of each new. It should be fast,
     * because there should be only a small number of passed agreements, since we
     * have garbage collection.
     * However, iterating over all the elements in the hash table is linear with the
     * number of buckets (see the implementation of opal_hash_table_get_next_key_uint64).
     * Thus, we need to keep a small number of buckets for era_passed_agreements to keep
     * good performance.
     */
    opal_hash_table_init(&era_passed_agreements, 32 /* We have GC, so small storage should be fine */);
    OBJ_CONSTRUCT( &era_ongoing_agreements, opal_hash_table_t);
    opal_hash_table_init(&era_ongoing_agreements, 16 /* We expect only a few */);
    
    OBJ_CONSTRUCT( &era_incomplete_messages, opal_hash_table_t);
    opal_hash_table_init(&era_incomplete_messages, 65536 /* Big Storage. Should be related to the universe size */);

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
    void                 *node;
    void                 *value;
    uint64_t              key64;
    era_value_t          *av;
    era_agreement_info_t *un_agreement;
    opal_hash_table_t    *msg_table;
    era_incomplete_msg_t *inc_msg;

    if( !era_inited ) {
        return OMPI_SUCCESS;
    }

    ompi_rank_failure_cbfunc = ompi_stacked_rank_failure_callback_fct;

    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Finalizing\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
    OPAL_OUTPUT_VERBOSE((7, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) GC: %d passed agreements remain in the passed agreements hash table\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         opal_hash_table_get_size(&era_passed_agreements)));
    if( opal_hash_table_get_first_key_uint64(&era_passed_agreements,
                                             &key64,
                                             &value, &node) == OPAL_SUCCESS ) {
        do {
            av = (era_value_t *)value;
#if OPAL_ENABLE_DEBUG
            era_identifier_t pid;
            pid.ERAID_KEY = key64;
            OPAL_OUTPUT_VERBOSE((7, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) GC: agreement (%d.%d).%d belongs to the passed agreements hash table\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 pid.ERAID_FIELDS.contextid,
                                 pid.ERAID_FIELDS.epoch,
                                 pid.ERAID_FIELDS.agreementid));
#endif
            OBJ_RELEASE(av);
        } while( opal_hash_table_get_next_key_uint64(&era_passed_agreements,
                                                     &key64, &value, 
                                                     node, &node) == OPAL_SUCCESS );
    }
    OBJ_DESTRUCT( &era_passed_agreements );
    OBJ_DESTRUCT( &era_iagree_requests );

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

    if( opal_hash_table_get_first_key_uint64(&era_incomplete_messages,
                                             &key64,
                                             &value, &node) == OPAL_SUCCESS ) {
        do {
            uint64_t key64_2;
            void *value_2, *node_2;
            msg_table = (opal_hash_table_t*)value;

            if( opal_hash_table_get_first_key_uint64(msg_table,
                                                     &key64_2,
                                                     &value_2, &node_2) == OPAL_SUCCESS ) {
                do {
                    inc_msg = (era_incomplete_msg_t *)value_2;
                    free(inc_msg);
                } while( opal_hash_table_get_next_key_uint64(msg_table, &key64_2,
                                                             &value_2, node_2, &node_2) == OPAL_SUCCESS );
            }

            OBJ_RELEASE(msg_table);
        }  while( opal_hash_table_get_next_key_uint64(&era_incomplete_messages,
                                                      &key64, &value, 
                                                      node, &node) == OPAL_SUCCESS );
    }
    OBJ_DESTRUCT( &era_incomplete_messages );

    era_inited = 0;

    return OMPI_SUCCESS;
}

static int mca_coll_ftbasic_agreement_era_prepare_agreement(ompi_communicator_t* comm,
                                                            ompi_group_t **group,
                                                            ompi_op_t *op,
                                                            ompi_datatype_t *dt,
                                                            int dt_count,
                                                            void *contrib,
                                                            mca_coll_base_module_t *module,
                                                            era_identifier_t *paid,
                                                            era_agreement_info_t **pci)
{
    era_agreement_info_t *ci;
    era_identifier_t agreement_id;
    void *value;
    era_value_t agreement_value;
    era_value_t *pa;
    mca_coll_ftbasic_agreement_t *ag_info;
    int i, ret;

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
    if( coll_ftbasic_debug_rank_may_fail &&
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

    OBJ_CONSTRUCT(&agreement_value, era_value_t);

    agreement_value.header.ret         = 0;
    agreement_value.header.operand     = op->o_f_to_c_index;
    agreement_value.header.dt_count    = dt_count;
    agreement_value.header.datatype    = dt->id;
    agreement_value.header.nb_new_dead = 0;

    /* Let's create or find the current value */
    ci = era_lookup_agreeement_info(agreement_id);
    if( NULL == ci ) {
        ci = era_create_agreement_info(agreement_id, &agreement_value.header);
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
        pa = (era_value_t*)value;
        opal_hash_table_remove_value_uint64(&era_passed_agreements, agreement_id.ERAID_KEY);
        OBJ_RELEASE(pa);
    }

    /* I participate */
    agreement_value.bytes = (uint8_t*)contrib;

    era_agreement_value_set_gcrange(agreement_id, &agreement_value);
    era_combine_agreement_values(ci, &agreement_value);
    
    agreement_value.bytes = NULL; /* We don't free &flag... */
    OBJ_DESTRUCT(&agreement_value);

    /* I start the state machine */
    ci->status = GATHERING;

    /* And follow its logic */
    era_check_status(ci);

    *paid = agreement_id;
    *pci = ci;
    return OMPI_SUCCESS;
}

static int mca_coll_ftbasic_agreement_era_complete_agreement(era_identifier_t agreement_id,
                                                             void *contrib,
                                                             ompi_group_t **group)
{
    era_value_t *av;
    int ret;
    int i;
    era_agreement_info_t *ci;
    ompi_communicator_t *comm;
    void *value;

    ci = era_lookup_agreeement_info(agreement_id);

    /** Now, it's time to remove that guy from the ongoing agreements */
    opal_hash_table_remove_value_uint64(&era_ongoing_agreements, agreement_id.ERAID_KEY);

    comm = ci->comm;

    OBJ_RELEASE(ci); /* This will take care of the content of ci too */

    ret = opal_hash_table_get_value_uint64(&era_passed_agreements,
                                           agreement_id.ERAID_KEY,
                                           &value);
    assert( OPAL_SUCCESS == ret);
    av = (era_value_t *)value;

    memcpy(contrib, av->bytes, ERA_VALUE_BYTES_COUNT(&av->header));
    ret = av->header.ret;

    /* We leave av in the era_passe_agreeements table, to answer future requests
     * from slow processes */

    /* Update the group of failed processes */
    for(i = 0; i < AGS(comm)->afr_size; i++) {
        ompi_proc_t *proc = ompi_group_get_proc_ptr(comm->c_local_group, AGS(comm)->agreed_failed_ranks[i]);
        ompi_errmgr_mark_failed_peer(proc, ORTE_PROC_STATE_TERMINATED);
    }
    /* User wants the group of new failures */
    if(NULL != group) {
        ompi_group_incl(comm->c_local_group, AGS(comm)->afr_size,
                        AGS(comm)->agreed_failed_ranks, group);
    }
    era_debug_print_group(1, *group, comm, "After Agreement");

    OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Leaving Agreement ID = (%d.%d).%d with ret = %d, 4 first bytes of flag = 0x%08x\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         agreement_id.ERAID_FIELDS.contextid,
                         agreement_id.ERAID_FIELDS.epoch,
                         agreement_id.ERAID_FIELDS.agreementid,
                         ret,
                         *(int*)contrib));

    return ret;
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
                                         ompi_op_t *op,
                                         ompi_datatype_t *dt,
                                         int dt_count,
                                         void *contrib,
                                         mca_coll_base_module_t *module)
{
    era_identifier_t agreement_id;
    era_agreement_info_t *ci;
    
    mca_coll_ftbasic_agreement_era_prepare_agreement(comm, group, op, dt, dt_count, contrib, module,
                                                     &agreement_id, &ci);
    
    /* Wait for the agreement to be resolved */
    while(ci->status != COMPLETED) {
        opal_progress();
    }

    return mca_coll_ftbasic_agreement_era_complete_agreement(agreement_id, contrib, group);
}

static int era_iagree_req_free(struct ompi_request_t** rptr)
{
    era_iagree_request_t *req = (era_iagree_request_t *)*rptr;
    req->ci->req = NULL;
    OMPI_FREE_LIST_RETURN( &era_iagree_requests,
                           (ompi_free_list_item_t*)(req));
    return OMPI_SUCCESS;
}

static int era_iagree_req_complete_cb(struct ompi_request_t* request)
{
    era_iagree_request_t *req = (era_iagree_request_t *)request;
    int rc;
    rc = mca_coll_ftbasic_agreement_era_complete_agreement(req->agreement_id, req->contrib, req->group);
    req->super.req_status.MPI_ERROR = rc;
    return rc;
}

int mca_coll_ftbasic_iagreement_era_intra(ompi_communicator_t* comm,
                                          ompi_group_t **group,
                                          ompi_op_t *op,
                                          ompi_datatype_t *dt,
                                          int dt_count,
                                          void *contrib,
                                          mca_coll_base_module_t *module,
                                          ompi_request_t **request)
{
    ompi_free_list_item_t* item;
    era_iagree_request_t *req;
    int rc;
    era_identifier_t agreement_id;
    era_agreement_info_t *ci;

    rc = OMPI_SUCCESS;
    OMPI_FREE_LIST_GET(&era_iagree_requests, item, rc);
    req = (era_iagree_request_t*)item;

    OMPI_REQUEST_INIT(&req->super, false);
    assert(MPI_UNDEFINED == req->super.req_f_to_c_index);
    
    mca_coll_ftbasic_agreement_era_prepare_agreement(comm, group, op, dt, dt_count, contrib, module,
                                                     &agreement_id, &ci);
    req->super.req_state = OMPI_REQUEST_ACTIVE;
    req->super.req_type = OMPI_REQUEST_IAGREE;
    req->super.req_status.MPI_SOURCE = MPI_UNDEFINED;
    req->super.req_status.MPI_TAG = MPI_UNDEFINED;
    req->super.req_mpi_object.comm = comm;
    req->super.req_complete_cb_data = NULL;

    req->super.req_free = era_iagree_req_free;
    req->super.req_cancel = NULL; /**< Don't know how to cancel an immediate agreement */
    req->super.req_complete_cb = era_iagree_req_complete_cb;

    req->agreement_id = agreement_id;
    req->contrib = contrib;
    req->group = group;
    req->ci = ci;

    ci->req = req;
    
    if( ci->status == COMPLETED ) {
        /**< must call this now, since it won't have been called in prepare
         *   as the request was not saved in ci at this time */
        ompi_request_complete(&req->super, true);
    }
    
    *request = &req->super;
    
    return OMPI_SUCCESS;
}

int mca_coll_ftbasic_agreement_era_free_comm(ompi_communicator_t* comm,
                                             mca_coll_base_module_t *module)
{
    ompi_group_t* acked; 
    era_identifier_t aid;
    int rc;

    OPAL_OUTPUT_VERBOSE((1, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Freeing Communicator (%d.%d).\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         comm->c_contextid,
                         comm->c_epoch));

    ompi_comm_failure_get_acked_internal( comm, &acked );

    do {
        rc = mca_coll_ftbasic_agreement_era_intra(comm, 
                                                  &acked,
                                                  &ompi_mpi_op_band.op,
                                                  &ompi_mpi_int.dt,
                                                  0,
                                                  NULL,
                                                  comm->c_coll.coll_agreement_module);
    } while(rc != MPI_SUCCESS);

    OBJ_RELEASE(acked);

    aid.ERAID_FIELDS.contextid = comm->c_contextid;
    aid.ERAID_FIELDS.epoch     = comm->c_epoch;

    /** We don't need to set aid.ERAID_FIELDS.agreementid to collect all of them */
    era_collect_passed_agreements(aid, 0, (uint16_t)-1);

    return OMPI_SUCCESS;
}
