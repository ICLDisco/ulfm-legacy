/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2014-2015 The University of Tennessee and The University
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

int coll_ftbasic_debug_rank_may_fail = 0;

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
 * Agreement Object Support
 *************************************/
static void mca_coll_ftbasic_agreement_construct(mca_coll_ftbasic_agreement_t *v_info)
{
    v_info->agreement_seq_num = 0;
}

static void mca_coll_ftbasic_agreement_destruct(mca_coll_ftbasic_agreement_t *v_info)
{
    opal_list_item_t* item = NULL;

#ifdef IAGREE
    if( NULL != v_info->cur_request ) {
        OBJ_RELEASE(v_info->cur_request);
        v_info->cur_request = NULL;
    }
#endif
}

OBJ_CLASS_INSTANCE(mca_coll_ftbasic_agreement_t,
                   opal_object_t,
                   mca_coll_ftbasic_agreement_construct,
                   mca_coll_ftbasic_agreement_destruct);


/*************************************
 * Initalize and Finalize Operations
 *************************************/
int mca_coll_ftbasic_agreement_init(ompi_communicator_t *comm, mca_coll_ftbasic_module_t *module)
{
    int ret;

    switch( mca_coll_ftbasic_cur_agreement_method ) {
    case COLL_FTBASIC_EARLY_RETURNING:
        mca_coll_ftbasic_agreement_era_comm_init(comm, module);
        break;
    default:
        break;
    }

    return OMPI_SUCCESS;
}

int mca_coll_ftbasic_agreement_finalize(mca_coll_ftbasic_module_t *module)
{
    int ret;

    switch( mca_coll_ftbasic_cur_agreement_method ) {
    case COLL_FTBASIC_EARLY_RETURNING:
        mca_coll_ftbasic_agreement_era_comm_finalize(module);
        break;
    default:
        break;
    }

    return OMPI_SUCCESS;
}
