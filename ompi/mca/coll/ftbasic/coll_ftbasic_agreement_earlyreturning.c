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

#include MCA_timer_IMPLEMENTATION_HEADER
#include "coll_ftbasic.h"

static opal_hash_table_t era_passed_agreements;
static opal_hash_table_t era_ongoing_agreements;
static opal_hash_table_t known_epoch_for_cid;
static int era_inited = 0;

typedef enum {
    MSG_UP = 1,
    MSG_DOWN,
    MSG_RESULT_REQUEST,
    MSG_RESULT
} era_msg_type_t;

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
            uint16_t consensusid;
            uint16_t contextid;    /**< Although context ids are 32 bit long, only the lower 16 bits are used */
            uint32_t epoch;
        } fields;
        uint64_t uint64;
    } u;
} era_identifier_t;

#define ERAID_KEY    u.uint64
#define ERAID_FIELDS u.fields

typedef struct {
    int32_t bits; /* Bitwise on 32 bits */
    int32_t ret;  /* Return code */
} era_value_t;
#define ERA_VALUE_SET_UNDEF(_e) do {            \
        (_e).bits = -1;                         \
        (_e).ret  = -1;                         \
    } while(0)
#define ERA_VALUE_IS_UNDEF(_e)    ( (_e).bits == -1 && (_e).ret == -1 )
#define ERA_VALUE_SET_NEUTRAL(_e) do {          \
        (_e).bits = ~((int32_t)0);              \
        (_e).ret  = OMPI_SUCCESS;               \
    } while(0)
#define ERA_VALUE_EQUAL(_a, _b)   ( (_a).bits == (_b).bits && (_a).ret == (_b).ret )

typedef struct {
    era_msg_type_t   msg_type;
    era_identifier_t consensus_id;
    era_value_t      consensus_value;
    union {
        int                 comm_based;
        orte_process_name_t proc_name_based;
    } src;
} era_msg_t;

typedef struct {
    opal_object_t   super;
    era_value_t     consensus_value;
} era_passed_agreement_t;

OBJ_CLASS_INSTANCE(era_passed_agreement_t, opal_object_t, NULL, NULL);

typedef struct {
    opal_list_item_t super;
    int32_t          rank;
} era_rank_list_item_t;

OBJ_CLASS_INSTANCE(era_rank_list_item_t, opal_list_item_t, NULL, NULL);

typedef struct {
    opal_object_t super;
    uint32_t      max_seq_num;
} era_seq_num_record_t;

OBJ_CLASS_INSTANCE(era_seq_num_record_t, opal_object_t, NULL, NULL);

typedef struct {
    opal_object_t        super;
    era_identifier_t     consensus_id;
    era_value_t          current_value;
    era_proc_status_t    status;
    ompi_communicator_t *comm;
    opal_list_t          gathered_info;
} era_consensus_info_t;

static void  era_consensus_info_constructor (era_consensus_info_t *consensus_info)
{
    consensus_info->consensus_id.ERAID_KEY = 0;
    ERA_VALUE_SET_NEUTRAL(consensus_info->current_value);
    consensus_info->status = NOT_CONTRIBUTED;
    consensus_info->comm = NULL;
    OBJ_CONSTRUCT(&consensus_info->gathered_info, opal_list_t);
}

static void  era_consensus_info_destructor (era_consensus_info_t *consensus_info)
{
    opal_list_item_t *li;
    ompi_communicator_t *comm;
    while( NULL != (li = opal_list_remove_first(&consensus_info->gathered_info)) ) {
        OBJ_RELEASE(li);
    }
    OBJ_DESTRUCT(&consensus_info->gathered_info);
    if( NULL != consensus_info->comm ) {
        comm = consensus_info->comm;
        OBJ_RELEASE(comm);
    }
}

OBJ_CLASS_INSTANCE(era_consensus_info_t,
                   opal_object_t, 
                   era_consensus_info_constructor, 
                   era_consensus_info_destructor);

static era_consensus_info_t *era_lookup_agreeement_info(era_identifier_t consensus_id)
{
    void *value;

    if( opal_hash_table_get_value_uint64(&era_ongoing_agreements,
                                         consensus_id.ERAID_KEY,
                                         &value) == OPAL_SUCCESS ) {
        return (era_consensus_info_t *)value;
    } else {
        return NULL;
    }
}

static era_consensus_info_t *era_create_agreement_info(era_identifier_t consensus_id)
{
    era_consensus_info_t *ci;
#if OPAL_ENABLE_DEBUG
    void *value;
    assert( opal_hash_table_get_value_uint64(&era_ongoing_agreements, 
                                             consensus_id.ERAID_KEY, 
                                             &value) != OPAL_SUCCESS );
#endif
    ci = OBJ_NEW(era_consensus_info_t);
    ci->consensus_id.ERAID_KEY = consensus_id.ERAID_KEY;
    opal_hash_table_set_value_uint64(&era_ongoing_agreements,
                                     consensus_id.ERAID_KEY,
                                     ci);
    return ci;
}

static void era_agreement_info_set_comm(era_consensus_info_t *ci, ompi_communicator_t *comm)
{
    assert( comm->c_contextid == ci->consensus_id.ERAID_FIELDS.contextid );
    assert( comm->c_epoch     == ci->consensus_id.ERAID_FIELDS.epoch     );
    assert( ci->comm          == NULL                                    );
    ci->comm = comm;
    OBJ_RETAIN(comm);
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

static void send_comm_msg(ompi_communicator_t *comm,                          
                          int dst, 
                          era_identifier_t consensus_id,
                          era_msg_type_t type,
                          era_value_t value);

#define ERA_TAG_AGREEMENT MCA_COLL_BASE_TAG_AGREEMENT

static void era_combine_agreement_values(era_consensus_info_t *ni, era_value_t value)
{
    ni->current_value.bits &= value.bits;
    if( value.ret > ni->current_value.ret )
        ni->current_value.ret = value.ret;
}

static void mark_as_undecided(era_consensus_info_t *ci, int rank)
{
    opal_output(0, "%s: %s (%s:%d) -- Need to implement function\n",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                __FUNCTION__,
                __FILE__, __LINE__);
}

#define ERA_TOPOLOGY_BINARY_TREE

#if defined ERA_TOPOLOGY_BINARY_TREE
static int era_parent(era_consensus_info_t *ci)
{
    ompi_communicator_t *comm = ci->comm;
    if( comm->c_my_rank == 0 )
        return 0;
    return (comm->c_my_rank - 1) / 2;
}

static int era_next_child(era_consensus_info_t *ci, int prev_child)
{
    ompi_communicator_t *comm = ci->comm;
    int l, r;
    l = (comm->c_my_rank + 1) * 2 - 1;
    r = l + 1;
    if( prev_child == -1 ) {
        if( l < ompi_comm_size(comm) )
            return l;
        return ompi_comm_size(comm);
    } else if(prev_child == l) {
        if( r < ompi_comm_size(comm) )
            return r;
        return ompi_comm_size(comm);
    } else {
        return ompi_comm_size(comm);
    }
}
#endif

#if defined ERA_TOPOLOGY_STAR
static int era_parent(era_consensus_info_t *ci)
{
    (void)ci;
    return 0;
}

static int era_next_child(era_consensus_info_t *ci, int prev_child)
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
static int era_parent(era_consensus_info_t *ci)
{
    ompi_communicator_t *comm = ci->comm;
    if( comm->c_my_rank > 0 )
        return comm->c_my_rank - 1;
    return 0;
}

static int era_next_child(era_consensus_info_t *ci, int prev_child)
{
    ompi_communicator_t *comm = ci->comm;
    if( prev_child == -1 )
        return comm->c_my_rank + 1;
    return ompi_comm_size(comm);
}
#endif

static void era_decide(era_value_t decided_value, era_consensus_info_t *ci)
{
    era_passed_agreement_t *da;
    ompi_communicator_t *comm;
    int r;

    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) decide %08x.%d on consensus (%d.%d).%d\n",                         
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         decided_value.bits,
                         decided_value.ret,
                         ci->consensus_id.ERAID_FIELDS.contextid,
                         ci->consensus_id.ERAID_FIELDS.epoch,                         
                         ci->consensus_id.ERAID_FIELDS.consensusid));

    opal_hash_table_remove_value_uint64(&era_ongoing_agreements, ci->consensus_id.ERAID_KEY);

    comm = ci->comm;

    da = OBJ_NEW(era_passed_agreement_t);
    da->consensus_value = decided_value;
    opal_hash_table_set_value_uint64(&era_passed_agreements,
                                     ci->consensus_id.ERAID_KEY,
                                     da);

    r = -1;
    while( (r = era_next_child(ci, r)) < ompi_comm_size(comm) ) {
        send_comm_msg(comm, r, ci->consensus_id, MSG_DOWN, decided_value);
    }

    OBJ_RELEASE(ci); /* This will take care of the content of ni too */
}

static void era_check_status(era_consensus_info_t *ci)
{
    int r;
    era_rank_list_item_t *rl;
    int found;

    if( ci->status == NOT_CONTRIBUTED ) {
        OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) check_status for Consensus ID = (%d.%d).%d, status = NOT_CONTRIBUTED\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             ci->consensus_id.ERAID_FIELDS.contextid,
                             ci->consensus_id.ERAID_FIELDS.epoch,
                             ci->consensus_id.ERAID_FIELDS.consensusid));
        /* Well, I haven't contributed to this consensus yet, and you'll not make a decision without me */
        return;
    }

    if( ci->status == GATHERING ) {
        /* I contributed myself, and I may just have received a contribution from a child */
        /* Let's see if it's time to pass up */
        r = -1;
        while( (r = era_next_child(ci, r)) < ompi_comm_size(ci->comm) ) {
            found = 0;
            for( rl =  (era_rank_list_item_t*)opal_list_get_first(&ci->gathered_info);
                 rl != (era_rank_list_item_t*)opal_list_get_end(&ci->gathered_info);
                 rl =  (era_rank_list_item_t*)opal_list_get_next(&rl->super) ) {
                if( rl->rank == r ) {
                    found = 1;
                    break;
                }
            }
            if( !found ) {
                OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ERA) check_status for Consensus ID = (%d.%d).%d, status = GATHERING, but some children have not contributed\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                     ci->consensus_id.ERAID_FIELDS.contextid,
                                     ci->consensus_id.ERAID_FIELDS.epoch,
                                     ci->consensus_id.ERAID_FIELDS.consensusid));
                /* We are still waiting for a message from at least a child. Let's wait */
                return;
            }
        }
        /* Left that loop? We're good to go up */
        if( ci->comm->c_my_rank == (r = era_parent(ci)) ) {
            OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) check_status for Consensus ID = (%d.%d).%d, status = GATHERING, and all children of root have contributed\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                 ci->consensus_id.ERAID_FIELDS.contextid,
                                 ci->consensus_id.ERAID_FIELDS.epoch,
                                 ci->consensus_id.ERAID_FIELDS.consensusid));

            /* I'm root. I have to decide now. */
            era_decide(ci->current_value, ci);
        } else {
            OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) check_status for Consensus ID = (%d.%d).%d, status = GATHERING, and all children of non-root have contributed\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                 ci->consensus_id.ERAID_FIELDS.contextid,
                                 ci->consensus_id.ERAID_FIELDS.epoch,
                                 ci->consensus_id.ERAID_FIELDS.consensusid));
            /* Let's forward up and wait for the DOWN messages */
            send_comm_msg(ci->comm, r, ci->consensus_id, MSG_UP, ci->current_value);
            ci->status = BROADCASTING;
        }
        return;
    }

    opal_output(0, "%s: %s (%s:%d) -- Need to implement function for that case (status = %d)\n",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                __FUNCTION__,
                __FILE__, __LINE__,
                ci->status);
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

static void send_comm_msg(ompi_communicator_t *comm,                          
                          int dst, 
                          era_identifier_t consensus_id,
                          era_msg_type_t type,
                          era_value_t value)
{
    mca_btl_base_descriptor_t *des;
    era_msg_t *src;
    ompi_proc_t *peer;
    mca_bml_base_endpoint_t *proc_bml;
    mca_bml_base_btl_t *bml_btl;
    struct mca_btl_base_endpoint_t *btl_endpoint;
    mca_btl_base_module_t *btl;

    OPAL_OUTPUT_VERBOSE((15, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) send message [(%d.%d).%d, %d, %08x.%d] to %d\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         consensus_id.ERAID_FIELDS.contextid,
                         consensus_id.ERAID_FIELDS.epoch,
                         consensus_id.ERAID_FIELDS.consensusid,
                         type,
                         value.bits,
                         value.ret,
                         dst));

    assert( consensus_id.ERAID_FIELDS.contextid == ompi_comm_get_cid(comm) );
    assert( consensus_id.ERAID_FIELDS.epoch == comm->c_epoch );

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

    des = btl->btl_alloc(btl, btl_endpoint, MCA_BTL_NO_ORDER, sizeof(era_msg_t),
                         MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
    des->des_cbfunc = message_sent;
    des->des_cbdata = NULL;
    src = (era_msg_t*)des->des_src->seg_addr.pval;
    src->msg_type = type;
    src->consensus_id.ERAID_KEY = consensus_id.ERAID_KEY;
    src->src.comm_based = ompi_comm_rank(comm);
    src->consensus_value = value;
    btl->btl_send(btl, btl_endpoint, des, MCA_BTL_TAG_FT_AGREE);
}

static void send_proc_name_msg(orte_process_name_t dst, 
                               era_identifier_t consensus_id,
                               era_msg_type_t type,
                               era_value_t value)
{
    mca_btl_base_descriptor_t *des;
    era_msg_t *src;
    ompi_proc_t *peer;
    mca_bml_base_endpoint_t *proc_bml;
    mca_bml_base_btl_t *bml_btl;
    struct mca_btl_base_endpoint_t *btl_endpoint;
    mca_btl_base_module_t *btl;

    OPAL_OUTPUT_VERBOSE((15, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) send message [(%d.%d).%d, %d, %08x.%d] to %s\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         consensus_id.ERAID_FIELDS.contextid,
                         consensus_id.ERAID_FIELDS.epoch,
                         consensus_id.ERAID_FIELDS.consensusid,
                         type,
                         value.bits,
                         value.ret,
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

    des = btl->btl_alloc(btl, btl_endpoint, MCA_BTL_NO_ORDER, sizeof(era_msg_t),
                         MCA_BTL_DES_FLAGS_PRIORITY | MCA_BTL_DES_FLAGS_BTL_OWNERSHIP);
    des->des_cbfunc = message_sent;
    des->des_cbdata = NULL;
    src = (era_msg_t*)des->des_src->seg_addr.pval;
    src->msg_type = type;
    src->consensus_id.ERAID_KEY = consensus_id.ERAID_KEY;
    src->src.proc_name_based = *ORTE_PROC_MY_NAME;
    src->consensus_value = value;
    btl->btl_send(btl, btl_endpoint, des, MCA_BTL_TAG_FT_AGREE);
}

static void result_request(era_msg_t *msg)
{
    void *value;
    era_value_t old_agreement_value;
    if( opal_hash_table_get_value_uint64(&era_passed_agreements,
                                         msg->consensus_id.ERAID_KEY,
                                         &value) == OPAL_SUCCESS ) {
        old_agreement_value = ((era_passed_agreement_t*)value)->consensus_value;
        send_proc_name_msg(msg->src.proc_name_based, msg->consensus_id, MSG_RESULT, old_agreement_value);
    } else {
        /* Is it an ongoing consensus? */
        era_consensus_info_t *ci;

        ci = era_lookup_agreeement_info(msg->consensus_id);

        if( NULL != ci ) {            
        } else {
            /* It is a competely unknown consensus... */
        }
    }
}

static void result(era_msg_t *msg)
{
    void *value;

    if( opal_hash_table_get_value_uint64(&era_passed_agreements,
                                         msg->consensus_id.ERAID_KEY,
                                         &value) == OPAL_SUCCESS ) {
        era_value_t old_agreement_value;
        /* This is an old agreement result. The answer, which is unneeded now,
         * should be compatible with the one stored
         */
        old_agreement_value = ((era_passed_agreement_t*)value)->consensus_value;
        assert( ERA_VALUE_EQUAL(old_agreement_value, msg->consensus_value) ||
                ERA_VALUE_IS_UNDEF(msg->consensus_value) );
    } else {
        /* If we don't know about the result of this agreement, 
         *   A) it should be about an ongoing agreement, so I haven't freed the communicator
         *   B) I should be waiting for an answer from this peer
         */
        era_consensus_info_t *ci;

        ci = era_lookup_agreeement_info(msg->consensus_id);
        if( NULL == ci ) {
#if OPAL_ENABLE_DEBUG
            era_passed_agreement_t *pa;
            void *value;

            /* We received a result message about a consensus that is not ongoing...*/
            /* Then it's because we have decided since about it */
            assert( opal_hash_table_get_value_uint64(&era_passed_agreements, 
                                                     msg->consensus_id.ERAID_KEY, 
                                                     &value) == OMPI_SUCCESS );
            /* AND we decided the same thing... */
            pa = (era_passed_agreement_t*)value;
            assert( ERA_VALUE_EQUAL(pa->consensus_value, msg->consensus_value) );
#endif 
            /* We can ignore this message: we decided already */
        } else {
            ompi_proc_t *peer;
            int rank;
            ompi_communicator_t *comm;

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

            if( !ERA_VALUE_IS_UNDEF(msg->consensus_value) ) {
                era_decide(msg->consensus_value, ci);
            } else {
                mark_as_undecided(ci, rank);
            }
        }
    }
}

static void msg_up(era_msg_t *msg)
{
    era_consensus_info_t *ci;
    era_rank_list_item_t *rank_item;
 
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received UP Message: Consensus ID = (%d.%d).%d, sender: %d, msg value: %08x.%d\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         msg->consensus_id.ERAID_FIELDS.contextid,
                         msg->consensus_id.ERAID_FIELDS.epoch,
                         msg->consensus_id.ERAID_FIELDS.consensusid,
                         msg->src.comm_based,
                         msg->consensus_value.bits,
                         msg->consensus_value.ret));

    ci = era_lookup_agreeement_info( msg->consensus_id );
    if( NULL == ci ) {
        ci = era_create_agreement_info( msg->consensus_id );
        /* We will attach the communicator when we contribute to it */
    }

    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received UP Message for %s agreement\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ci == NULL ? "unknown" : "known"));

    /* ni holds the current consensus information structure */
    era_combine_agreement_values(ci, msg->consensus_value);
    rank_item = OBJ_NEW(era_rank_list_item_t);
    rank_item->rank = msg->src.comm_based;
    opal_list_append(&ci->gathered_info, &rank_item->super);
    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received UP Message: adding %d in list of people that contributed\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank_item->rank));

    era_check_status(ci);
}

static void msg_down(era_msg_t *msg)
{
    era_consensus_info_t *ci;
 
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received DOWN Message: Consensus ID = (%d.%d).%d, sender: %d, msg value: %08x.%d\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         msg->consensus_id.ERAID_FIELDS.contextid,
                         msg->consensus_id.ERAID_FIELDS.epoch,
                         msg->consensus_id.ERAID_FIELDS.consensusid,
                         msg->src.comm_based,
                         msg->consensus_value.bits,
                         msg->consensus_value.ret));

    ci = era_lookup_agreeement_info( msg->consensus_id );
    assert( NULL != ci /** if I receive a down message, I'm still doing the agreement */ );
    assert( NULL != ci->comm );

    era_decide(msg->consensus_value, ci);
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
    case MSG_RESULT:
        result(msg);
        return;
    case MSG_UP:
        msg_up(msg);
        return;
    case MSG_DOWN:
        msg_down(msg);
        return;
    }
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
    era_consensus_info_t      *un_agreement;
    era_seq_num_record_t      *seq_num_record;

    if( !era_inited ) {
        return OMPI_SUCCESS;
    }

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
            un_agreement = (era_consensus_info_t *)value;
            opal_output(0, "%s ftbasic:agreement (ERA) Error: Consensus ID (%d.%d).%d was started by some processor, but I never completed to it\n",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                        un_agreement->consensus_id.ERAID_FIELDS.contextid,
                        un_agreement->consensus_id.ERAID_FIELDS.epoch,
                        un_agreement->consensus_id.ERAID_FIELDS.consensusid);
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
    era_consensus_info_t *ci;
    era_identifier_t consensus_id;
    void *value;
    era_value_t agreement_value;
    era_passed_agreement_t *pa;
    mca_coll_ftbasic_agreement_t *ftbasic_info;

    ftbasic_info = ( (mca_coll_ftbasic_module_t *)module )->agreement_info;
    assert( NULL != ftbasic_info );

    /** Avoid cycling silently */
    if( ftbasic_info->agreement_seq_num == UINT16_MAX ) {
        ftbasic_info->agreement_seq_num = 1;
    } else {
        ftbasic_info->agreement_seq_num++;
    }
    
    /* Let's find the id of the new consensus */
    consensus_id.ERAID_FIELDS.contextid   = comm->c_contextid;
    consensus_id.ERAID_FIELDS.epoch       = comm->c_epoch;
    consensus_id.ERAID_FIELDS.consensusid = (uint16_t)ftbasic_info->agreement_seq_num;

    /* Let's create or find the current value */
    ci = era_lookup_agreeement_info(consensus_id);
    if( NULL == ci ) {
        ci = era_create_agreement_info(consensus_id);
    }

    assert( NULL == ci->comm );
    era_agreement_info_set_comm(ci, comm);
    
    if( opal_hash_table_get_value_uint64(&era_passed_agreements, consensus_id.ERAID_KEY, &value) == OMPI_SUCCESS ) {
        OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) removing old consensus (%d.%d).%d from history, due to cycling of identifiers\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             consensus_id.ERAID_FIELDS.contextid,
                             consensus_id.ERAID_FIELDS.epoch,                         
                             consensus_id.ERAID_FIELDS.consensusid));
        pa = (era_passed_agreement_t*)value;
        opal_hash_table_remove_value_uint64(&era_passed_agreements, consensus_id.ERAID_KEY);
        OBJ_RELEASE(pa);
    }

    /* I participate */
    agreement_value.bits = *flag;
    agreement_value.ret  = OMPI_SUCCESS;
    era_combine_agreement_values(ci, agreement_value);

    /* I start the state machine */
    ci->status = GATHERING;

    /* And follow its logic */
    era_check_status(ci);

    /* Wait for the consensus to be resolved */
    while(1) {
        opal_progress();
        if( opal_hash_table_get_value_uint64(&era_passed_agreements,
                                             consensus_id.ERAID_KEY,
                                             &value) == OPAL_SUCCESS ) {
            agreement_value = ((era_passed_agreement_t*)value)->consensus_value;
            break;
        }
    } 
    
    *flag = agreement_value.bits;
    
    return agreement_value.ret;
}
