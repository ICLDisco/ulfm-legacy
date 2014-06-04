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
    OBJ_CONSTRUCT(&consensus_info->gathered_info, opal_hash_table_t);
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
    opal_object_t     super;
    int16_t           next_consensusid;  /**< Next Consensus ID (for this communicator) */
    opal_hash_table_t ongoing_consensus; /**< Hash table of ongoing consensus on this communicator */
} era_comm_ft_data_t;

static void era_comm_ft_data_constructor (era_comm_ft_data_t *ft_data)
{
    ft_data->next_consensusid  = 0;
    OBJ_CONSTRUCT(&ft_data->ongoing_consensus, opal_hash_table_t);
}

static void era_comm_ft_data_destructor (era_comm_ft_data_t *ft_data)
{
    era_consensus_info_t *value;
    void *node;
    uint64_t key;
    if( opal_hash_table_get_first_key_uint64(&ft_data->ongoing_consensus,
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
        } while( opal_hash_table_get_next_key_uint64(&ft_data->ongoing_consensus,
                                                     &key, (void**)&value, 
                                                     node, &node) == OPAL_SUCCESS );
        }
    OBJ_DESTRUCT(&ft_data->ongoing_consensus);
}

OBJ_CLASS_INSTANCE(era_comm_ft_data_t, opal_object_t, 
                   era_comm_ft_data_constructor,
                   era_comm_ft_data_destructor);

static opal_hash_table_t era_passed_agreements;
static opal_hash_table_t era_future_agreements;

#define ERA_TAG_AGREEMENT MCA_COLL_BASE_TAG_AGREEMENT

static void era_combine_agreement_values(era_consensus_info_t *ni, era_value_t value)
{
    opal_output(0, "%s: %s (%s:%d) -- Need to implement function\n",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                __FUNCTION__,
                __FILE__, __LINE__);
}

static void mark_as_undecided(era_consensus_info_t *ni, int rank)
{
    opal_output(0, "%s: %s (%s:%d) -- Need to implement function\n",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                __FUNCTION__,
                __FILE__, __LINE__);
}

static void era_check_status(era_consensus_info_t *ni, ompi_communicator_t *comm)
{
    opal_output(0, "%s: %s (%s:%d) -- Need to implement function\n",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                __FUNCTION__,
                __FILE__, __LINE__);
}

static int era_next_child(era_consensus_info_t *ni, ompi_communicator_t *comm, int prev_child)
{
    opal_output(0, "%s: %s (%s:%d) -- Need to implement function\n",
                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                __FUNCTION__,
                __FILE__, __LINE__);
    return ompi_comm_size(comm);
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

static void era_decide(ompi_communicator_t *comm, era_identifier_t consensus_id, era_value_t decided_value, era_consensus_info_t *ni)
{
    era_comm_ft_data_t *ft_data;
    era_passed_agreement_t *da;

    ft_data = (era_comm_ft_data_t*)comm->ft_data;
    assert( NULL != ft_data ); /* we can't decide on a communicator without the decision structures */

    opal_hash_table_remove_value_uint64(&ft_data->ongoing_consensus,
                                        consensus_id.ERAID_KEY);

    OBJ_RELEASE(ni); /* This will take care of the content of ni too */

    da = OBJ_NEW(era_passed_agreement_t);
    da->consensus_value = decided_value;
    opal_hash_table_set_value_uint64(&era_passed_agreements,
                                     consensus_id.ERAID_KEY,
                                     da);
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
        era_comm_ft_data_t *ft_data;
        int rc;
        ompi_proc_t *peer;
        int rank;

        comm = ompi_comm_lookup( msg->consensus_id.ERAID_FIELDS.contextid );
        assert(NULL != comm);
        assert(comm->epoch == msg->consensus_id.ERAID_FIELDS.epoch);

        ft_data = (era_comm_ft_data_t*)comm->ft_data;
        assert( NULL != ft_data ); /* same reasoning as above */
        rc = opal_hash_table_get_value_uint64(&ft_data->ongoing_consensus,
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
            era_decide(comm, msg->consensus_id, msg->consensus_value, ni);
        } else {
            mark_as_undecided(ni, rank);
        }
    }
}

static era_comm_ft_data_t *era_lookup_create_ft_data_for_comm(ompi_communicator_t *comm)
{
    era_comm_ft_data_t *ft_data = (era_comm_ft_data_t*)comm->ft_data;
    if( NULL == ft_data ) {
        ft_data = OBJ_NEW(era_comm_ft_data_t);
        comm->ft_data = (void*)ft_data;
    }
    return ft_data;
}

static era_consensus_info_t *era_lookup_info_for_comm(ompi_communicator_t *comm, era_identifier_t consensus_id)
{
    era_comm_ft_data_t *ft_data;
    void *value;
    era_consensus_info_t *ni;

    ft_data = era_lookup_create_ft_data_for_comm(comm);

    if( opal_hash_table_get_value_uint64(&ft_data->ongoing_consensus,
                                         consensus_id.ERAID_KEY,
                                         &value) == OPAL_SUCCESS ) {
        /* already started */
        ni = (era_consensus_info_t*)value;
    } else {
        /* Not started... Or maybe before I created the communicator ? */
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
        opal_hash_table_set_value_uint64(&ft_data->ongoing_consensus,
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
        /* OK, I have a communicator for that consensus... Let's try to find it */
        ni = era_lookup_info_for_comm(comm, msg->consensus_id);
    }

    /* ni holds the current consensus information structure */
    era_combine_agreement_values(ni, msg->consensus_value);
    rank_item = OBJ_NEW(era_rank_list_item_t);
    rank_item->rank = msg->src.comm_based;
    opal_list_append(&ni->gathered_info, &rank_item->super);

    if( NULL != comm ) {
        era_check_status(ni, comm);
    }
}

static void msg_down(era_msg_t *msg)
{
    era_consensus_info_t *ni;
    ompi_communicator_t *comm;
    void *value;
    era_comm_ft_data_t *ft_data;
    int rc;
    int r;
 
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

    ft_data = (era_comm_ft_data_t*)comm->ft_data;
    assert( NULL != ft_data ); /* same reasoning as above */
    rc = opal_hash_table_get_value_uint64(&ft_data->ongoing_consensus,
                                          msg->consensus_id.ERAID_KEY,
                                          &value);

    assert( rc == OPAL_SUCCESS ); /* again */
    ni = (era_consensus_info_t*)value;

    era_decide(comm, msg->consensus_id, msg->consensus_value, ni);
    r = -1;
    while( (r = era_next_child(ni, comm, r)) < ompi_comm_size(comm) ) {
        send_comm_msg(comm, r, ni->consensus_id, MSG_DOWN, msg->consensus_value);
    }
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

/*
 * mca_coll_ftbasic_agreement_era_init
 *
 * Function:    - MPI_Comm_agree()
 * Sets up the generic callback to answer to asynchronous consensus requests
 *   that may happen when this process has not started or is already done
 *   with the corresponding consensus
 */
int
mca_coll_ftbasic_agreement_era_init(mca_coll_ftbasic_module_t *module)
{
    (void)module;
    OBJ_CONSTRUCT( &era_passed_agreements, opal_hash_table_t);
    opal_hash_table_init(&era_passed_agreements, 16 /* Why not? */);
    OBJ_CONSTRUCT( &era_future_agreements, opal_hash_table_t);
    opal_hash_table_init(&era_future_agreements, 4 /* We expect only a few */);
    mca_bml.bml_register(MCA_BTL_TAG_FT_AGREE, era_cb_fn, NULL);

    return OMPI_SUCCESS;
}

/*
 * mca_coll_ftbasic_agreement_era_fini
 *
 * Function:    - MPI_Comm_agree()
 * Clears up memory of previous consensus
 */
int mca_coll_ftbasic_agreement_era_fini(mca_coll_ftbasic_module_t *module)
{
    void *node;
    void *value;
    uint64_t key;
    era_passed_agreement_t *old_agreement;
    era_consensus_info_t   *un_agreement;

    (void)module;
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
    era_comm_ft_data_t *ft_data;
    void *value;
    era_value_t agreement_value;

    ft_data = era_lookup_create_ft_data_for_comm(comm);
    /* That's a new consensus */
    ft_data->next_consensusid++;

    /* Let's find the id of the new consensus */
    consensus_id.ERAID_FIELDS.consensusid = ft_data->next_consensusid;
    consensus_id.ERAID_FIELDS.contextid   = comm->c_contextid;
    consensus_id.ERAID_FIELDS.epoch       = comm->epoch;

    /* Let's create or find the current value */
    ni = era_lookup_info_for_comm(comm, consensus_id);

    /* I participate */
    agreement_value.bits = *flag;
    agreement_value.ret  = OMPI_SUCCESS;
    era_combine_agreement_values(ni, agreement_value);

    /* I start the state machine */
    ni->status = GATHERING;

    /* And follow its logic */
    era_check_status(ni, comm);

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
