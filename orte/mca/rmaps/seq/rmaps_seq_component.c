/*
 * Copyright (c) 2004-2008 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "orte_config.h"
#include "orte/constants.h"

#include "opal/mca/base/base.h"
#include "opal/mca/base/mca_base_param.h"

#include "orte/mca/rmaps/rmaps.h"
#include "rmaps_seq.h"

/*
 * Local functions
 */

static int orte_rmaps_seq_open(void);
static int orte_rmaps_seq_close(void);
static int orte_rmaps_seq_query(mca_base_module_t **module, int *priority);

static int my_priority;

orte_rmaps_base_component_t mca_rmaps_seq_component = {
      {
        ORTE_RMAPS_BASE_VERSION_2_0_0,

        "seq", /* MCA component name */
        ORTE_MAJOR_VERSION,  /* MCA component major version */
        ORTE_MINOR_VERSION,  /* MCA component minor version */
        ORTE_RELEASE_VERSION,  /* MCA component release version */
        orte_rmaps_seq_open,  /* component open  */
        orte_rmaps_seq_close, /* component close */
        orte_rmaps_seq_query  /* component query */
      },
      {
          /* The component is checkpoint ready */
          MCA_BASE_METADATA_PARAM_CHECKPOINT
      }
};


/**
  * component open/close/init function
  */
static int orte_rmaps_seq_open(void)
{
    mca_base_component_t *c = &mca_rmaps_seq_component.base_version;

    mca_base_param_reg_int(c, "priority",
                           "Priority of the seq rmaps component",
                           false, false, 60,
                           &my_priority);
    return ORTE_SUCCESS;
}


static int orte_rmaps_seq_query(mca_base_module_t **module, int *priority)
{
    *priority = my_priority;
    *module = (mca_base_module_t *)&orte_rmaps_seq_module;
    return ORTE_SUCCESS;
}

/**
 *  Close all subsystems.
 */

static int orte_rmaps_seq_close(void)
{
    return ORTE_SUCCESS;
}


