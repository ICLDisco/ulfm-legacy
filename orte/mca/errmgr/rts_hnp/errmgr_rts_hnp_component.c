/*
 * Copyright (c) 2010      Cisco Systems, Inc. All rights reserved.
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 *
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "orte_config.h"
#include "opal/util/output.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/errmgr/base/base.h"
#include "orte/mca/errmgr/base/errmgr_private.h"
#include "errmgr_rts_hnp.h"

/*
 * Public string for version number
 */
const char *orte_errmgr_rts_hnp_component_version_string = 
    "ORTE ERRMGR rts_hnp MCA component version " ORTE_VERSION;

/*
 * Local functionality
 */
static int rts_hnp_open(void);
static int rts_hnp_close(void);
static int rts_hnp_component_query(mca_base_module_t **module, int *priority);

/*
 * Instantiate the public struct with all of our public information
 * and pointer to our public functions in it
 */
orte_errmgr_base_component_t mca_errmgr_rts_hnp_component = {
    /* Handle the general mca_component_t struct containing 
     *  meta information about the component rts_hnp
     */
    {
        ORTE_ERRMGR_BASE_VERSION_3_0_0,
        /* Component name and version */
        "rts_hnp",
        ORTE_MAJOR_VERSION,
        ORTE_MINOR_VERSION,
        ORTE_RELEASE_VERSION,
        
        /* Component open and close functions */
        rts_hnp_open,
        rts_hnp_close,
        rts_hnp_component_query
    },
    {
        /* The component is checkpoint ready */
        MCA_BASE_METADATA_PARAM_CHECKPOINT
    },
};

int errmgr_rts_hnp_proc_fail_xcast_delay = 1;

static int my_priority;

static int rts_hnp_open(void) 
{
    mca_base_component_t *c = &mca_errmgr_rts_hnp_component.base_version;
    int val;

    mca_base_param_reg_int(c, "priority",
                           "Priority of the rts_hnp errmgr component",
                           false, false, 1000,
                           &my_priority);

    mca_base_param_reg_int(c,
                           "verbose",
                           "Verbose level for the ErrMgr rts_hnp component",
                           false, false,
                           mca_errmgr_rts_hnp_component.verbose, 
                           &mca_errmgr_rts_hnp_component.verbose);
    /* If there is a custom verbose level for this component than use it
     * otherwise take our parents level and output channel
     */
    if ( 0 != mca_errmgr_rts_hnp_component.verbose) {
        mca_errmgr_rts_hnp_component.output_handle = opal_output_open(NULL);
        opal_output_set_verbosity(mca_errmgr_rts_hnp_component.output_handle,
                                  mca_errmgr_rts_hnp_component.verbose);
    } else {
        mca_errmgr_rts_hnp_component.output_handle = orte_errmgr_base.output;
    }

    mca_base_param_reg_int(c, "proc_fail_xcast_delay",
                           "Number of seconds to wait after the first failure before broadcasting out the list of failed procs"
                           " [Default: 1 sec]",
                           false, false,
                           1, &val);
    errmgr_rts_hnp_proc_fail_xcast_delay = val;
    if( errmgr_rts_hnp_proc_fail_xcast_delay < 0 ) {
        errmgr_rts_hnp_proc_fail_xcast_delay = 0;
    }

    opal_output_verbose(10, mca_errmgr_rts_hnp_component.output_handle,
                        "errmgr:rts_hnp: open()");
    opal_output_verbose(20, mca_errmgr_rts_hnp_component.output_handle,
                        "errmgr:rts_hnp: open: verbose       = %d", 
                        mca_errmgr_rts_hnp_component.verbose);
    opal_output_verbose(20, mca_errmgr_rts_hnp_component.output_handle,
                        "errmgr:rts_hnp: open: priority      = %d", 
                        my_priority);
    opal_output_verbose(20, mca_errmgr_rts_hnp_component.output_handle,
                        "errmgr:rts_hnp: open: xcast_delay   = %d", 
                        errmgr_rts_hnp_proc_fail_xcast_delay);

    return ORTE_SUCCESS;
}

static int rts_hnp_close(void)
{
    opal_output_verbose(10, mca_errmgr_rts_hnp_component.output_handle,
                        "errmgr:rts_hnp: close()");

    return ORTE_SUCCESS;
}

static int rts_hnp_component_query(mca_base_module_t **module, int *priority)
{
    if( ORTE_PROC_IS_HNP ) {
        /* we are the rts HNP component */
        *priority = my_priority;
        *module = (mca_base_module_t *)&orte_errmgr_rts_hnp_module;
        return ORTE_SUCCESS;
    }

    *module = NULL;
    *priority = -1;
    return ORTE_ERROR;
}
