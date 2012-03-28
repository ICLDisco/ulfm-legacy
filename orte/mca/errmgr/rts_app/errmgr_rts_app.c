/*
 * Copyright (c) 2009-2011 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved. 
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 *    
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "orte_config.h"

#include <sys/types.h>
#ifdef HAVE_UNISTD_H
#include <unistd.h>
#endif  /* HAVE_UNISTD_H */
#ifdef HAVE_STRING_H
#include <string.h>
#endif

#include "opal/util/output.h"
#include "opal/dss/dss.h"

#include "orte/util/error_strings.h"
#include "orte/util/name_fns.h"
#include "orte/runtime/orte_globals.h"
#include "orte/mca/rml/rml.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/odls/odls_types.h"

#include "orte/mca/errmgr/base/base.h"
#include "orte/mca/errmgr/base/errmgr_private.h"
#include "errmgr_rts_app.h"

/*
 * Module functions: Global
 */
static int init(void);
static int finalize(void);

static int update_state(orte_jobid_t job,
                        orte_job_state_t jobstate,
                        orte_process_name_t *proc_name,
                        orte_proc_state_t state,
                        pid_t pid,
                        orte_exit_code_t exit_code);

static int abort_peers(orte_process_name_t *procs,
                       orte_std_cntr_t num_procs);

static int errmgr_app_process_fault(orte_process_name_t *proc, orte_proc_state_t state);

/******************
 * HNP module
 ******************/
orte_errmgr_base_module_t orte_errmgr_rts_app_module = {
    init,
    finalize,
    orte_errmgr_base_log,
    orte_errmgr_base_abort,
    abort_peers,
    update_state,
    NULL,
    NULL,
    NULL,
    orte_errmgr_base_register_migration_warning,
    NULL
};

/************************
 * API Definitions
 ************************/
static int init(void)
{
    return ORTE_SUCCESS;
}

static int finalize(void)
{
    return ORTE_SUCCESS;
}

static int update_state(orte_jobid_t job,
                        orte_job_state_t jobstate,
                        orte_process_name_t *proc,
                        orte_proc_state_t state,
                        pid_t pid,
                        orte_exit_code_t exit_code)
{
    int ret;
    orte_ns_cmp_bitmask_t mask;

    OPAL_OUTPUT_VERBOSE((1, orte_errmgr_base.output,
                         "%s errmgr:rts_app: job %s reported state %s"
                         " for proc %s state %s exit_code %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(job),
                         orte_job_state_to_str(jobstate),
                         (NULL == proc) ? "NULL" : ORTE_NAME_PRINT(proc),
                         orte_proc_state_to_str(state), exit_code));
    
    /*
     * if orte is trying to shutdown, just let it
     */
    if (orte_finalizing) {
        return ORTE_SUCCESS;
    }

    if (ORTE_PROC_STATE_COMM_FAILED == state) {
        mask = ORTE_NS_CMP_ALL;
        /* if it is our own connection, ignore it */
        if (OPAL_EQUAL == orte_util_compare_name_fields(mask, ORTE_PROC_MY_NAME, proc)) {
            return ORTE_SUCCESS;
        }

        /* delete the route */
        orte_routed.delete_route(proc);

        /* see is this was a lifeline */
        if (ORTE_SUCCESS != orte_routed.route_lost(proc)) {
            return ORTE_ERR_UNRECOVERABLE;
        }
    }

    if( ORTE_SUCCESS != (ret = errmgr_app_process_fault(proc, state)) ) {
        ORTE_ERROR_LOG(ret);
        return ret;
    }

    return ORTE_SUCCESS;
}

static int abort_peers(orte_process_name_t *procs, orte_std_cntr_t num_procs)
{
    int ret, exit_status = ORTE_SUCCESS;
    opal_buffer_t buffer;
    orte_std_cntr_t i;
    orte_daemon_cmd_flag_t command = ORTE_DAEMON_ABORT_PROCS_CALLED;

    OPAL_OUTPUT_VERBOSE((1, orte_errmgr_base.output,
                         "%s errmgr:rts_app: Aborting %2d peers",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         num_procs));

    /*
     * Pack up the list of processes and send them to the HNP
     */
    OBJ_CONSTRUCT(&buffer, opal_buffer_t);

    if (ORTE_SUCCESS != (ret = opal_dss.pack(&buffer, &command, 1, ORTE_DAEMON_CMD))) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    /* pack number of processes */
    if (ORTE_SUCCESS != (ret = opal_dss.pack(&buffer, &(num_procs), 1, ORTE_STD_CNTR))) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    /* Pack the list of names */
    for( i = 0; i < num_procs; ++i ) {
        OPAL_OUTPUT_VERBOSE((1, orte_errmgr_base.output,
                             "%s errmgr:rts_app:     Abort %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(&procs[i]) ));


        if (ORTE_SUCCESS != (ret = opal_dss.pack(&buffer, &(procs[i]), 1, ORTE_NAME))) {
            ORTE_ERROR_LOG(ret);
            exit_status = ret;
            goto cleanup;
        }
    }

    /* Send to HNP for termination */
    if (0 > (ret = orte_rml.send_buffer(ORTE_PROC_MY_HNP, &buffer, ORTE_RML_TAG_DAEMON, 0))) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

 cleanup:
    OBJ_DESTRUCT(&buffer);

    return exit_status;
}


/******************
 * Local functions
 ******************/
static int errmgr_app_process_fault(orte_process_name_t *proc, orte_proc_state_t state)
{
    int ret, exit_status = ORTE_SUCCESS;

    /*
     * Notify the MPI layer
     */
    if( NULL != orte_errmgr_base_app_callback ) {
        if( ORTE_SUCCESS != (ret = orte_errmgr_base_app_callback(*proc, state)) ) {
            ORTE_ERROR_LOG(ret);
            exit_status = ret;
            goto cleanup;
        }
    }

 cleanup:
    return exit_status;
}
