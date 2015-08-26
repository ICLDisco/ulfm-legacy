/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 *
 * These symbols are in a file by themselves to provide nice linker
 * semantics.  Since linkers generally pull in symbols by object
 * files, keeping these symbols as the only symbols in this file
 * prevents utility programs such as "ompi_info" from having to import
 * entire components just to query their version and parameters.
 */

#include "ompi_config.h"
#include "coll_ftbasic.h"
#include "coll_ftbasic_agreement.h"

#include "mpi.h"

#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"

#include "ompi/mca/coll/coll.h"
#include "coll_ftbasic.h"

/*
 * Public string showing the coll ompi_ftbasic component version number
 */
const char *mca_coll_ftbasic_component_version_string =
    "Open MPI ftbasic collective MCA component version " OMPI_VERSION;

/*
 * Global variables
 */
int mca_coll_ftbasic_priority  = 0;
mca_coll_ftbasic_agreement_method_t mca_coll_ftbasic_cur_agreement_method = COLL_FTBASIC_EARLY_RETURNING;
int mca_coll_ftbasic_cur_era_topology = 1;
int mca_coll_ftbasic_era_rebuild = 1;

/*
 * Local function
 */
static int ftbasic_register(void);
static int ftbasic_close(void);
/*
 * Instantiate the public struct with all of our public information
 * and pointers to our public functions in it
 */

const mca_coll_base_component_2_0_0_t mca_coll_ftbasic_component = {

    /* First, the mca_component_t struct containing meta information
     * about the component itself */

    {
     MCA_COLL_BASE_VERSION_2_0_0,

     /* Component name and version */
     "ftbasic",
     OMPI_MAJOR_VERSION,
     OMPI_MINOR_VERSION,
     OMPI_RELEASE_VERSION,

     /* Component open and close functions */
     NULL,
     ftbasic_close,
     NULL,
     ftbasic_register
    },
    {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },

    /* Initialization / querying functions */

    mca_coll_ftbasic_init_query,
    mca_coll_ftbasic_comm_query
};

static int
ftbasic_close(void)
{
    if( mca_coll_ftbasic_cur_agreement_method ==  COLL_FTBASIC_EARLY_RETURNING ) {
        return mca_coll_ftbasic_agreement_era_finalize();
    }
    return OMPI_SUCCESS;
}

static int
ftbasic_register(void)
{
    int value;

    /* Use a low priority, but allow other components to be lower */
    mca_base_param_reg_int(&mca_coll_ftbasic_component.collm_version,
                           "priority",
                           "Priority of the ftbasic coll component",
                           false, false, mca_coll_ftbasic_priority,
                           &mca_coll_ftbasic_priority);

    mca_base_param_reg_int(&mca_coll_ftbasic_component.collm_version,
                           "method",
                           "Agreement method"
                           " 0 = Early Returning Consensus,"
                           " 1 = Early Consensus Termination (default))",
                           false, false,
                           mca_coll_ftbasic_cur_agreement_method,
                           &value);
    switch(value) {
    case 0:
        mca_coll_ftbasic_cur_agreement_method = COLL_FTBASIC_EARLY_TERMINATION;
        opal_output_verbose(6, ompi_ftmpi_output_handle,
                            "%s ftbasic:register) Agreement Algorithm - Early Terminating Consensus Algorithm",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) );
        break;
    default:  /* Includes the valid case 1 */
        mca_coll_ftbasic_cur_agreement_method = COLL_FTBASIC_EARLY_RETURNING;
        opal_output_verbose(6, ompi_ftmpi_output_handle,
                            "%s ftbasic:register) Agreement Algorithm - Early Returning Consensus Algorithm",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME) );
        break;
    }

    mca_base_param_reg_int(&mca_coll_ftbasic_component.collm_version,
                           "era_topology",
                           "Early Returned Agreement Topology ("
                           "positive number -> network topology aware (default); negative number -> flat topology;"
                           "1 -> binary tree (default);"
                           "2 -> star tree;"
                           "3 -> string tree)",
                           false, false,
                           mca_coll_ftbasic_cur_era_topology,
                           &mca_coll_ftbasic_cur_era_topology);

    mca_base_param_reg_int(&mca_coll_ftbasic_component.collm_version,
                           "era_rebuild",
                           "ERA rebuild the tree after a post-failure agreement (default 0)",
                           false, false,
                           mca_coll_ftbasic_era_rebuild,
                           &mca_coll_ftbasic_era_rebuild);


    return OMPI_SUCCESS;
}


static void
mca_coll_ftbasic_module_construct(mca_coll_ftbasic_module_t *module)
{
    module->is_intercomm = false;

    module->mccb_reqs = NULL;
    module->mccb_num_reqs = 0;

    module->mccb_statuses = NULL;
    module->mccb_num_statuses = 0;

    module->mccb_coll_tag_agreement = MCA_COLL_BASE_TAG_AGREEMENT;

    /* This object is managed by the agreement operation selected */
    module->agreement_info = NULL;
}

static void
mca_coll_ftbasic_module_destruct(mca_coll_ftbasic_module_t *module)
{

    /* Finalize the agreement function */
    if( ompi_ftmpi_enabled && !module->is_intercomm ) {
        mca_coll_ftbasic_agreement_finalize(module);
    }

    module->is_intercomm = false;

    /* This object is managed by the agreement operation selected */
    module->agreement_info = NULL;

    if (NULL != module->mccb_reqs) {
        free(module->mccb_reqs);
        module->mccb_reqs = NULL;
    }

    if( NULL != module->mccb_statuses ) {
        free(module->mccb_statuses);
        module->mccb_statuses = NULL;
    }
}


OBJ_CLASS_INSTANCE(mca_coll_ftbasic_module_t,
                   mca_coll_base_module_t,
                   mca_coll_ftbasic_module_construct,
                   mca_coll_ftbasic_module_destruct);
