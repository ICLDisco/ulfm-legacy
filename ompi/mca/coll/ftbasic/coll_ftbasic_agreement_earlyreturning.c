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
static opal_hash_table_t era_future_agreements;
static opal_hash_table_t era_living_communicators;
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
        struct {
            uint32_t contextid;
            int16_t  consensusid;
            int16_t  epoch;
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
    opal_object_t     super;
    era_identifier_t  consensus_id;
    era_value_t       current_value;
    era_proc_status_t status;
    opal_list_t       gathered_info;
} era_consensus_info_t;

static void  era_consensus_info_constructor (era_consensus_info_t *consensus_info)
{
    consensus_info->status = NOT_CONTRIBUTED;
    consensus_info->consensus_id.ERAID_KEY = 0;
    ERA_VALUE_SET_NEUTRAL(consensus_info->current_value);
    OBJ_CONSTRUCT(&consensus_info->gathered_info, opal_list_t);
}

static void  era_consensus_info_destructor (era_consensus_info_t *consensus_info)
{
    opal_list_item_t *li;
    while( NULL != (li = opal_list_remove_first(&consensus_info->gathered_info)) ) {
        OBJ_RELEASE(li);
    }
    OBJ_DESTRUCT(&consensus_info->gathered_info);
}

OBJ_CLASS_INSTANCE(era_consensus_info_t, opal_object_t, 
                   era_consensus_info_constructor, 
                   era_consensus_info_destructor);

typedef struct {
    mca_coll_ftbasic_agreement_t  super;
    uint32_t                      c_contextid;       /**< Copy of the contextid of the corresponding communicator,
                                                      *   to remove from hash table when needed */
    opal_hash_table_t             ongoing_consensus; /**< Hash table of ongoing consensus on this communicator */
} era_comm_agreement_t;

static void era_comm_agreement_constructor (era_comm_agreement_t *comm_ag_info)
{
    OBJ_CONSTRUCT(&comm_ag_info->ongoing_consensus, opal_hash_table_t);
    opal_hash_table_init(&comm_ag_info->ongoing_consensus, 4 /* Should be related to the average number of ongoing consensus for a single comm */ );
}

static void era_comm_agreement_destructor (era_comm_agreement_t *comm_ag_info)
{
    era_consensus_info_t *value;
    void *node;
    uint64_t key;
    if( opal_hash_table_get_first_key_uint64(&comm_ag_info->ongoing_consensus,
                                             &key,
                                             (void**)&value, &node) == OPAL_SUCCESS ) {
        do {
            /* All consensus should be done at this point, or this is an erroneous MPI program */
            opal_output(0, "%s ftbasic:agreement (ERA) Error: Consensus ID (%d.%d).%d is still ongoing when the corresponding communicator is released\n",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                        value->consensus_id.ERAID_FIELDS.contextid,
                        value->consensus_id.ERAID_FIELDS.epoch,
                        value->consensus_id.ERAID_FIELDS.consensusid);
            OBJ_RELEASE( value );
        } while( opal_hash_table_get_next_key_uint64(&comm_ag_info->ongoing_consensus,
                                                     &key, (void**)&value, 
                                                     node, &node) == OPAL_SUCCESS );
        }
    OBJ_DESTRUCT(&comm_ag_info->ongoing_consensus);
}

OBJ_CLASS_INSTANCE(era_comm_agreement_t, mca_coll_ftbasic_agreement_t, 
                   era_comm_agreement_constructor,
                   era_comm_agreement_destructor);

static era_comm_agreement_t *era_comm_agreement_for_comm(ompi_communicator_t *comm)
{
    void *value;
    mca_coll_ftbasic_module_t *module;

    int rc = opal_hash_table_get_value_uint32(&era_living_communicators,
                                              comm->c_contextid,
                                              &value);
    assert(rc == OMPI_SUCCESS);
    module = (mca_coll_ftbasic_module_t *)value;

    return (era_comm_agreement_t *)module->agreement_info;
}

int mca_coll_ftbasic_agreement_era_comm_init(ompi_communicator_t *comm, mca_coll_ftbasic_module_t *module)
{
    era_comm_agreement_t *comm_ag_info;

    comm_ag_info = OBJ_NEW(era_comm_agreement_t);
    comm_ag_info->c_contextid = comm->c_contextid;
    module->agreement_info = (mca_coll_ftbasic_agreement_t *)comm_ag_info;

    opal_hash_table_set_value_uint32(&era_living_communicators,
                                     comm->c_contextid,
                                     module);

    return OMPI_SUCCESS;
}

int mca_coll_ftbasic_agreement_era_comm_finalize(mca_coll_ftbasic_module_t *module)
{
    era_comm_agreement_t *comm_ag_info;
    comm_ag_info = (era_comm_agreement_t *)module->agreement_info;
    opal_hash_table_remove_value_uint64(&era_living_communicators,
                                        comm_ag_info->c_contextid);
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

static void mark_as_undecided(era_consensus_info_t *ni, int rank)
{
    opal_output(0, "%s: %s (%s:%d) -- Need to implement function\n",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                __FUNCTION__,
                __FILE__, __LINE__);
}

#define ERA_TOPOLOGY_BINARY_TREE

#if defined ERA_TOPOLOGY_BINARY_TREE
static int era_parent(era_consensus_info_t *ni, ompi_communicator_t *comm)
{
    if( comm->c_my_rank == 0 )
        return 0;
    return (comm->c_my_rank - 1) / 2;
}

static int era_next_child(era_consensus_info_t *ni, ompi_communicator_t *comm, int prev_child)
{
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
static int era_parent(era_consensus_info_t *ni, ompi_communicator_t *comm)
{
    return 0;
}

static int era_next_child(era_consensus_info_t *ni, ompi_communicator_t *comm, int prev_child)
{
    if( comm->c_my_rank == 0 ) {
        if( prev_child == -1 ) return 1;
        return prev_child + 1;
    }
    return ompi_comm_size(comm);
}
#endif

#if defined ERA_TOPOLOGY_STRING
static int era_parent(era_consensus_info_t *ni, ompi_communicator_t *comm)
{
    if( comm->c_my_rank > 0 )
        return comm->c_my_rank - 1;
    return 0;
}

static int era_next_child(era_consensus_info_t *ni, ompi_communicator_t *comm, int prev_child)
{
    if( prev_child == -1 )
        return comm->c_my_rank + 1;
    return ompi_comm_size(comm);
}
#endif

static void era_decide(ompi_communicator_t *comm, era_identifier_t consensus_id, era_value_t decided_value,
                       era_consensus_info_t *ni,
                       era_comm_agreement_t *comm_ag_info)
{
    era_passed_agreement_t *da;
    int r;

    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) decide %08x.%d on consensus (%d.%d).%d\n",                         
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         decided_value.bits,
                         decided_value.ret,
                         consensus_id.ERAID_FIELDS.contextid,
                         consensus_id.ERAID_FIELDS.epoch,
                         consensus_id.ERAID_FIELDS.consensusid));

    assert( NULL != comm_ag_info ); /* we can't decide on a communicator without the decision structures */
    opal_hash_table_remove_value_uint64(&comm_ag_info->ongoing_consensus,
                                        consensus_id.ERAID_KEY);

    OBJ_RELEASE(ni); /* This will take care of the content of ni too */

    da = OBJ_NEW(era_passed_agreement_t);
    da->consensus_value = decided_value;
    opal_hash_table_set_value_uint64(&era_passed_agreements,
                                     consensus_id.ERAID_KEY,
                                     da);

    r = -1;
    while( (r = era_next_child(ni, comm, r)) < ompi_comm_size(comm) ) {
        send_comm_msg(comm, r, consensus_id, MSG_DOWN, decided_value);
    }
}

static void era_check_status(era_consensus_info_t *ni, ompi_communicator_t *comm, era_comm_agreement_t *comm_ag_info)
{
    int r;
    era_rank_list_item_t *rl;
    int found;

    if( ni->status == NOT_CONTRIBUTED ) {
        OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) check_status for Consensus ID = (%d.%d).%d, status = NOT_CONTRIBUTED\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                             ni->consensus_id.ERAID_FIELDS.contextid,
                             ni->consensus_id.ERAID_FIELDS.epoch,
                             ni->consensus_id.ERAID_FIELDS.consensusid));
        /* Well, I haven't contributed to this consensus yet, and you'll not make a decision without me */
        return;
    }

    if( ni->status == GATHERING ) {
        /* I contributed myself, and I may just have received a contribution from a child */
        /* Let's see if it's time to pass up */
        r = -1;
        while( (r = era_next_child(ni, comm, r)) < ompi_comm_size(comm) ) {
            found = 0;
            for( rl =  (era_rank_list_item_t*)opal_list_get_first(&ni->gathered_info);
                 rl != (era_rank_list_item_t*)opal_list_get_end(&ni->gathered_info);
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
                                     ni->consensus_id.ERAID_FIELDS.contextid,
                                     ni->consensus_id.ERAID_FIELDS.epoch,
                                     ni->consensus_id.ERAID_FIELDS.consensusid));
                /* We are still waiting for a message from at least a child. Let's wait */
                return;
            }
        }
        /* Left that loop? We're good to go up */
        if( comm->c_my_rank == (r = era_parent(ni, comm)) ) {
            OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) check_status for Consensus ID = (%d.%d).%d, status = GATHERING, and all children of root have contributed\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                 ni->consensus_id.ERAID_FIELDS.contextid,
                                 ni->consensus_id.ERAID_FIELDS.epoch,
                                 ni->consensus_id.ERAID_FIELDS.consensusid));

            /* I'm root. I have to decide now. */
            assert( NULL != comm_ag_info ); /* Better find the ag_info, if ni exists */
            era_decide(comm, ni->consensus_id, ni->current_value, ni, comm_ag_info);
        } else {
            OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ERA) check_status for Consensus ID = (%d.%d).%d, status = GATHERING, and all children of non-root have contributed\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                 ni->consensus_id.ERAID_FIELDS.contextid,
                                 ni->consensus_id.ERAID_FIELDS.epoch,
                                 ni->consensus_id.ERAID_FIELDS.consensusid));
            /* Let's forward up and wait for the DOWN messages */
            send_comm_msg(comm, r, ni->consensus_id, MSG_UP, ni->current_value);
            ni->status = BROADCASTING;
        }
        return;
    }

    opal_output(0, "%s: %s (%s:%d) -- Need to implement function for that case (status = %d)\n",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                __FUNCTION__,
                __FILE__, __LINE__,
                ni->status);
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
    } else {
        /* Before talking too early, let's see if we are still working on that agreement */
        /* Here, I should iterate on all my communcicators,
         * OR on all ongoing agreements, and check if it
         * corresponds to msg->consensus_id */
        /* TODO !!! */
        ERA_VALUE_SET_UNDEF(old_agreement_value);
    }
    send_proc_name_msg(msg->src.proc_name_based, msg->consensus_id, MSG_RESULT, old_agreement_value);
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
        ompi_communicator_t *comm;
        era_consensus_info_t *ni;
        era_comm_agreement_t *comm_ag_info;
        int rc;
        ompi_proc_t *peer;
        int rank;

        comm = ompi_comm_lookup( msg->consensus_id.ERAID_FIELDS.contextid );
        assert(NULL != comm);
        assert(comm->epoch == msg->consensus_id.ERAID_FIELDS.epoch);

        comm_ag_info = era_comm_agreement_for_comm(comm);
        assert( NULL != comm_ag_info ); /* same reasoning as above */
        rc = opal_hash_table_get_value_uint64(&comm_ag_info->ongoing_consensus,
                                              msg->consensus_id.ERAID_KEY,
                                              &value);

        assert( rc == OPAL_SUCCESS ); /* again */
        ni = (era_consensus_info_t*)value;

        peer = ompi_proc_find ( &msg->src.proc_name_based );
        for(rank = 0; rank < ompi_comm_size(comm); rank++) {
            if( ompi_comm_peer_lookup(comm, rank) == peer ) {
                break;
            }
        }
        assert(rank < ompi_comm_size(comm));

        if( !ERA_VALUE_IS_UNDEF(msg->consensus_value) ) {
            era_decide(comm, msg->consensus_id, msg->consensus_value, ni, comm_ag_info);
        } else {
            mark_as_undecided(ni, rank);
        }
    }
}

static era_consensus_info_t *era_lookup_info_for_comm(ompi_communicator_t *comm, era_identifier_t consensus_id, era_comm_agreement_t *comm_ag_info)
{
    void *value;
    era_consensus_info_t *ni;

    if( opal_hash_table_get_value_uint64(&comm_ag_info->ongoing_consensus,
                                         consensus_id.ERAID_KEY,
                                         &value) == OPAL_SUCCESS ) {
        /* already started */
        ni = (era_consensus_info_t*)value;
    } else {
        /* Not started... Or maybe it was started before I created the communicator ? */
        if( opal_hash_table_get_value_uint64(&era_future_agreements,
                                             consensus_id.ERAID_KEY,
                                             &value) == OPAL_SUCCESS ) {
            /* Moving it away from the pending list */
            ni = (era_consensus_info_t*)value;
            opal_hash_table_remove_value_uint64(&era_future_agreements, consensus_id.ERAID_KEY);
        } else {
            /* Nope: it's a new one that we can attach to the communicator */
            ni = OBJ_NEW(era_consensus_info_t);
            ni->consensus_id.ERAID_KEY = consensus_id.ERAID_KEY;
        }
        /* And moving it into the communicator */
        opal_hash_table_set_value_uint64(&comm_ag_info->ongoing_consensus,
                                         consensus_id.ERAID_KEY,
                                         ni);
    }
    return ni;
}

static void msg_up(era_msg_t *msg)
{
    era_consensus_info_t *ni;
    ompi_communicator_t *comm;
    void *value;
    era_rank_list_item_t *rank_item;
    era_comm_agreement_t *comm_ag_info = NULL;

    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received UP Message: Consensus ID = (%d.%d).%d, sender: %d, msg value: %08x.%d\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         msg->consensus_id.ERAID_FIELDS.contextid,
                         msg->consensus_id.ERAID_FIELDS.epoch,
                         msg->consensus_id.ERAID_FIELDS.consensusid,
                         msg->src.comm_based,
                         msg->consensus_value.bits,
                         msg->consensus_value.ret));

    comm = ompi_comm_lookup( msg->consensus_id.ERAID_FIELDS.contextid );

    if( NULL == comm ) {
        OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Received UP Message: unkown communicator, storing information for later\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        /* Bad luck: somebody created the communicator faster than me and decided
         *  to start a consensus right away... */
        if( opal_hash_table_get_value_uint64(&era_future_agreements,
                                             msg->consensus_id.ERAID_KEY,
                                             &value) == OPAL_SUCCESS ) {
            /* Somebody else talked to us already about it */
            ni = (era_consensus_info_t*)value;
        } else {
            /* it's a new guy */
            ni = OBJ_NEW(era_consensus_info_t);
            ni->consensus_id.ERAID_KEY = msg->consensus_id.ERAID_KEY;
            opal_hash_table_set_value_uint64(&era_future_agreements,
                                             msg->consensus_id.ERAID_KEY,
                                             ni);
        }
    } else {
        OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ERA) Received UP Message: known communicator\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        /* OK, I have a communicator for that consensus... Let's try to find it */
        comm_ag_info = era_comm_agreement_for_comm(comm);
        assert( NULL != comm_ag_info );
        ni = era_lookup_info_for_comm(comm, msg->consensus_id, comm_ag_info);
    }

    /* ni holds the current consensus information structure */
    era_combine_agreement_values(ni, msg->consensus_value);
    rank_item = OBJ_NEW(era_rank_list_item_t);
    rank_item->rank = msg->src.comm_based;
    opal_list_append(&ni->gathered_info, &rank_item->super);
    OPAL_OUTPUT_VERBOSE((20, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received UP Message: adding %d in list of people that contributed\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         rank_item->rank));

    if( NULL != comm ) {
        assert( NULL != comm_ag_info );
        era_check_status(ni, comm, comm_ag_info);
    }
}

static void msg_down(era_msg_t *msg)
{
    era_consensus_info_t *ni;
    ompi_communicator_t *comm;
    void *value;
    era_comm_agreement_t *comm_ag_info;
    int rc;
 
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Received DOWN Message: Consensus ID = (%d.%d).%d, sender: %d, msg value: %08x.%d\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         msg->consensus_id.ERAID_FIELDS.contextid,
                         msg->consensus_id.ERAID_FIELDS.epoch,
                         msg->consensus_id.ERAID_FIELDS.consensusid,
                         msg->src.comm_based,
                         msg->consensus_value.bits,
                         msg->consensus_value.ret));

    comm = ompi_comm_lookup( msg->consensus_id.ERAID_FIELDS.contextid );
    assert(NULL != comm); /* We can not go down without my participation */

    comm_ag_info = era_comm_agreement_for_comm(comm);
    assert( NULL != comm_ag_info ); /* same reasoning as above */
    rc = opal_hash_table_get_value_uint64(&comm_ag_info->ongoing_consensus,
                                          msg->consensus_id.ERAID_KEY,
                                          &value);
    assert( rc == OPAL_SUCCESS ); /* again */

    ni = (era_consensus_info_t*)value;
    assert( NULL != ni );

    era_decide(comm, msg->consensus_id, msg->consensus_value, ni, comm_ag_info);
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
    
    opal_output(0, "Calling ERA init\n");
    
    OBJ_CONSTRUCT( &era_passed_agreements, opal_hash_table_t);
    opal_hash_table_init(&era_passed_agreements, 16 /* Why not? */);
    OBJ_CONSTRUCT( &era_future_agreements, opal_hash_table_t);
    opal_hash_table_init(&era_future_agreements, 4 /* We expect only a few */);
    mca_bml.bml_register(MCA_BTL_TAG_FT_AGREE, era_cb_fn, NULL);
    OBJ_CONSTRUCT( &era_living_communicators, opal_hash_table_t);
    opal_hash_table_init(&era_living_communicators, 16 /* Why not? */);

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
    uint64_t key;
    era_passed_agreement_t    *old_agreement;
    era_consensus_info_t      *un_agreement;
    mca_coll_ftbasic_module_t *comm_module;
    era_comm_agreement_t      *comm_ag_info;

    if( !era_inited ) {
        return OMPI_SUCCESS;
    }

    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ERA) Finalizing\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

    if( opal_hash_table_get_first_key_uint64(&era_passed_agreements,
                                             &key,
                                             &value, &node) == OPAL_SUCCESS ) {
        do {
            old_agreement = (era_passed_agreement_t *)value;
            OBJ_RELEASE(old_agreement);
        } while( opal_hash_table_get_next_key_uint64(&era_passed_agreements,
                                                     &key, &value, 
                                                     node, &node) == OPAL_SUCCESS );
    }
    OBJ_DESTRUCT( &era_passed_agreements );

    if( opal_hash_table_get_first_key_uint64(&era_living_communicators,
                                             &key,
                                             &value, &node) == OPAL_SUCCESS ) {
        do {
            comm_module  = (mca_coll_ftbasic_module_t *)value;
            comm_ag_info = (era_comm_agreement_t*)comm_module->agreement_info;
            OBJ_RELEASE( comm_ag_info );
        } while( opal_hash_table_get_next_key_uint64(&era_living_communicators,
                                                     &key, &value, 
                                                     node, &node) == OPAL_SUCCESS );
    }
    OBJ_DESTRUCT( &era_living_communicators );

    if( opal_hash_table_get_first_key_uint64(&era_future_agreements,
                                             &key,
                                             &value, &node) == OPAL_SUCCESS ) {
        do {
            un_agreement = (era_consensus_info_t *)value;
            opal_output(0, "%s ftbasic:agreement (ERA) Error: Consensus ID (%d.%d).%d was started by some processor, but I never participated to it\n",
                        ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                        un_agreement->consensus_id.ERAID_FIELDS.contextid,
                        un_agreement->consensus_id.ERAID_FIELDS.epoch,
                        un_agreement->consensus_id.ERAID_FIELDS.consensusid);
            OBJ_RELEASE(un_agreement);
        } while( opal_hash_table_get_next_key_uint64(&era_future_agreements,
                                                     &key, &value, 
                                                     node, &node) == OPAL_SUCCESS );
    }
    OBJ_DESTRUCT( &era_future_agreements );

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
    era_consensus_info_t *ni;
    era_identifier_t consensus_id;
    era_comm_agreement_t *comm_ag_info;
    void *value;
    era_value_t agreement_value;

    comm_ag_info = (era_comm_agreement_t*)((mca_coll_ftbasic_module_t *)module)->agreement_info;
    assert(NULL != comm_ag_info);

    comm_ag_info->super.agreement_seq_num++;

    /* Let's find the id of the new consensus */
    consensus_id.ERAID_FIELDS.consensusid = (int16_t)comm_ag_info->super.agreement_seq_num;
    consensus_id.ERAID_FIELDS.contextid   = comm->c_contextid;
    consensus_id.ERAID_FIELDS.epoch       = comm->epoch;

    /* Let's create or find the current value */
    ni = era_lookup_info_for_comm(comm, consensus_id, comm_ag_info);

    /* I participate */
    agreement_value.bits = *flag;
    agreement_value.ret  = OMPI_SUCCESS;
    era_combine_agreement_values(ni, agreement_value);

    /* I start the state machine */
    ni->status = GATHERING;

    /* And follow its logic */
    era_check_status(ni, comm, comm_ag_info);

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
