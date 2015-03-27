/*
 * Copyright (c) 2009-2011 The Trustees of Indiana University.
 *                         All rights reserved.
 * Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved. 
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2011      Oracle and/or all its affiliates.  All rights reserved. 
 * Copyright (c) 2011      Los Alamos National Security, LLC.
 *                         All rights reserved.
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
#ifdef HAVE_SYS_WAIT_H
#include <sys/wait.h>
#endif

#include "opal/util/output.h"
#include "opal/dss/dss.h"

#include "orte/mca/rml/rml.h"
#include "orte/mca/odls/odls.h"
#include "orte/mca/odls/base/base.h"
#include "orte/mca/odls/base/odls_private.h"
#include "orte/mca/plm/base/plm_private.h"
#include "orte/mca/plm/plm.h"
#include "orte/mca/rmaps/rmaps_types.h"
#include "orte/mca/sensor/sensor.h"
#include "orte/mca/routed/routed.h"
#include "orte/mca/notifier/notifier.h"
#include "orte/mca/grpcomm/grpcomm.h"
#include "orte/mca/ess/ess.h"

#include "orte/util/error_strings.h"
#include "orte/util/name_fns.h"
#include "orte/util/proc_info.h"
#include "orte/util/show_help.h"
#include "orte/util/nidmap.h"

#include "orte/runtime/orte_globals.h"
#include "orte/runtime/orte_locks.h"
#include "orte/runtime/orte_quit.h"
#include "orte/runtime/data_type_support/orte_dt_support.h"

#include "orte/mca/errmgr/errmgr.h"
#include "orte/mca/errmgr/base/base.h"
#include "orte/mca/errmgr/base/errmgr_private.h"

#include "errmgr_rts_hnp.h"

static int init(void);
static int finalize(void);

static int predicted_fault(opal_list_t *proc_list,
                           opal_list_t *node_list,
                           opal_list_t *suggested_map);

static int update_state(orte_jobid_t job,
                        orte_job_state_t jobstate,
                        orte_process_name_t *proc,
                        orte_proc_state_t state,
                        pid_t pid,
                        orte_exit_code_t exit_code);

static int suggest_map_targets(orte_proc_t *proc,
                               orte_node_t *oldnode,
                               opal_list_t *node_list);

static int ft_event(int state);


/******************
 * rts_hnp module
 ******************/
orte_errmgr_base_module_t orte_errmgr_rts_hnp_module = {
    init,
    finalize,
    orte_errmgr_base_log,
    orte_errmgr_base_abort,
    orte_errmgr_base_abort_peers,
    update_state,
    predicted_fault,
    suggest_map_targets,
    ft_event,
    orte_errmgr_base_register_migration_warning,
    NULL
};


/*
 * Local functions
 */
static void rts_hnp_abort(orte_jobid_t job, orte_exit_code_t exit_code);
static void failed_start(orte_job_t *jdata);
static void update_local_procs_in_job(orte_job_t *jdata, orte_job_state_t jobstate,
                                      orte_proc_state_t state, orte_exit_code_t exit_code);
static void check_job_complete(orte_job_t *jdata);
static void killprocs(orte_jobid_t job, orte_vpid_t vpid);
static void update_proc(orte_job_t *jdata,
                        orte_process_name_t *proc,
                        orte_proc_state_t state,
                        pid_t pid,
                        orte_exit_code_t exit_code);

/************************************
 * Run-Through Stabilization Specific
 ************************************/
void rts_hnp_record_dead_daemon(orte_job_t *jdat,
                                orte_vpid_t vpid,
                                orte_proc_state_t state,
                                orte_exit_code_t exit_code);

static int rts_hnp_stable_global_update_state(orte_jobid_t job,
                                              orte_job_state_t jobstate,
                                              orte_process_name_t *proc_name,
                                              orte_proc_state_t state,
                                              pid_t pid,
                                              orte_exit_code_t exit_code);
static int rts_hnp_stable_process_fault(orte_job_t *jdata,
                                        orte_process_name_t *proc_name,
                                        orte_proc_state_t state);
static int rts_hnp_stable_process_fault_app(orte_job_t *jdata,
                                            orte_process_name_t *proc,
                                            orte_proc_state_t state);
static int rts_hnp_stable_process_fault_daemon(orte_job_t *jdata,
                                               orte_process_name_t *proc,
                                               orte_proc_state_t state);

static bool rts_hnp_ignore_current_update = false;

/* Two pointers to pointer_arrays for the xcast of process failure notifications */
static opal_pointer_array_t *proc_failures_pending_xcast = NULL;
static opal_pointer_array_t *proc_failures_current_xcast = NULL;
static bool stable_xcast_timer_active = false;
static opal_event_t *stable_xcast_timer_event = NULL;

static void rts_hnp_stable_xcast_fn(int fd, short event, void *cbdata);


/**********************
 * From RTS_HNP
 **********************/
static int init(void)
{
    opal_output_verbose(10, mca_errmgr_rts_hnp_component.output_handle,
                        "errmgr:rts_hnp: init()");

    if( NULL == proc_failures_pending_xcast ) {
        proc_failures_pending_xcast = OBJ_NEW(opal_pointer_array_t);
        opal_pointer_array_init(proc_failures_pending_xcast, 16, INT32_MAX, 8);
    }

    if( NULL == proc_failures_current_xcast ) {
        proc_failures_current_xcast = OBJ_NEW(opal_pointer_array_t);
        opal_pointer_array_init(proc_failures_current_xcast, 16, INT32_MAX, 8);
    }

    if( NULL == stable_xcast_timer_event ) {
        stable_xcast_timer_event = opal_event_evtimer_new(opal_event_base, rts_hnp_stable_xcast_fn, NULL);
    }

    return ORTE_SUCCESS;
}

static int finalize(void)
{
    orte_std_cntr_t i;
    void *item = NULL;

    opal_output_verbose(10, mca_errmgr_rts_hnp_component.output_handle,
                        "errmgr:hnp(stable): finalize()");

    if( NULL != stable_xcast_timer_event ) {
        if( stable_xcast_timer_active ) {
            opal_event_evtimer_del(stable_xcast_timer_event);
        }
        free(stable_xcast_timer_event);
    }

    if( NULL != proc_failures_pending_xcast ) {
        for( i = 0; i < proc_failures_pending_xcast->size; ++i) {
            item = opal_pointer_array_get_item(proc_failures_pending_xcast, i);
            if( NULL != item ) {
                OBJ_RELEASE(item);
            }
        }
        opal_pointer_array_remove_all(proc_failures_pending_xcast);
        OBJ_RELEASE(proc_failures_pending_xcast);
        proc_failures_pending_xcast = NULL;
    }

    if( NULL != proc_failures_current_xcast ) {
        for( i = 0; i < proc_failures_current_xcast->size; ++i) {
            item = opal_pointer_array_get_item(proc_failures_current_xcast, i);
            if( NULL != item ) {
                OBJ_RELEASE(item);
            }
        }
        opal_pointer_array_remove_all(proc_failures_current_xcast);
        OBJ_RELEASE(proc_failures_current_xcast);
        proc_failures_current_xcast = NULL;
    }

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
    orte_job_t *jdata = NULL;
    orte_proc_t *pptr = NULL;
    orte_exit_code_t sts;

    OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                         "%s errmgr:rts_hnp: job %s reported state %s"
                         " for proc %s state %s pid %d exit_code %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(job),
                         orte_job_state_to_str(jobstate),
                         (NULL == proc) ? "NULL" : ORTE_NAME_PRINT(proc),
                         orte_proc_state_to_str(state), pid, exit_code));
    
    /*
     * if orte is trying to shutdown, just let it
     */
    if (orte_finalizing) {
        return ORTE_SUCCESS;
    }

    /*
     * Attempt to stabilize, instead of abort
     */
    rts_hnp_ignore_current_update = false;
    if( ORTE_SUCCESS != (ret = rts_hnp_stable_global_update_state(job,
                                                                  jobstate,
                                                                  proc,
                                                                  state,
                                                                  pid,
                                                                  exit_code)) ) {
        return ret;
    }
    if( rts_hnp_ignore_current_update ) {
        return ORTE_SUCCESS;
    }

    /*
     * If stabilization is not possible, then handle normally
     */
    if (NULL == proc) {
        /* this is an update for an entire local job */
        if (ORTE_JOBID_INVALID == job) {
            /* whatever happened, we don't know what job
             * it happened to
             */
            if (ORTE_JOB_STATE_NEVER_LAUNCHED == jobstate) {
                orte_never_launched = true;
            }
            orte_show_help("help-orte-errmgr.txt", "errmgr:unknown-job-error",
                           true, orte_job_state_to_str(jobstate));
            rts_hnp_abort(job, exit_code);
            return ORTE_SUCCESS;
        }

        /* get the job object */
        if (NULL == (jdata = orte_get_job_data_object(job))) {
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            return ORTE_ERR_NOT_FOUND;
        }
        /* update the state */
        jdata->state = jobstate;

        OPAL_OUTPUT_VERBOSE((1, mca_errmgr_rts_hnp_component.output_handle,
                             "%s errmgr:rts_hnp: job %s reported state %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_JOBID_PRINT(jdata->jobid),
                             orte_job_state_to_str(jobstate)));

        switch (jobstate) {
        case ORTE_JOB_STATE_TERMINATED:
            /* support batch-operated jobs */
            update_local_procs_in_job(jdata, jobstate, ORTE_PROC_STATE_TERMINATED, 0);
            jdata->num_terminated = jdata->num_procs;
            check_job_complete(jdata);
            break;

        case ORTE_JOB_STATE_ABORTED:
            /* support batch-operated jobs */
            update_local_procs_in_job(jdata, jobstate, ORTE_PROC_STATE_ABORTED, exit_code);
            /* order all local procs for this job to be killed */
            killprocs(jdata->jobid, ORTE_VPID_WILDCARD);
            jdata->num_terminated = jdata->num_procs;
            check_job_complete(jdata);
            break;

        case ORTE_JOB_STATE_FAILED_TO_START:
            failed_start(jdata);
            check_job_complete(jdata);  /* set the local proc states */
            /* the job object for this job will have been NULL'd
             * in the array if the job was solely local. If it isn't
             * NULL, then we need to tell everyone else to die
             */
            if (NULL != (jdata = orte_get_job_data_object(job))) {
                sts = exit_code;
                if (ORTE_PROC_MY_NAME->jobid == job && !orte_abnormal_term_ordered) {
                    /* set the flag indicating that a daemon failed so we use the proper
                     * methods for attempting to shutdown the rest of the system
                     */
                    orte_abnormal_term_ordered = true;
                    if (WIFSIGNALED(exit_code)) { /* died on signal */
#ifdef WCOREDUMP
                        if (WCOREDUMP(exit_code)) {
                            orte_show_help("help-plm-base.txt", "daemon-died-signal-core", true,
                                           WTERMSIG(exit_code));
                            sts = WTERMSIG(exit_code);
                        } else {
                            orte_show_help("help-plm-base.txt", "daemon-died-signal", true,
                                           WTERMSIG(exit_code));
                            sts = WTERMSIG(exit_code);
                        }
#else
                        orte_show_help("help-plm-base.txt", "daemon-died-signal", true,
                                       WTERMSIG(exit_code));
                        sts = WTERMSIG(exit_code);
#endif /* WCOREDUMP */
                    } else {
                        orte_show_help("help-plm-base.txt", "daemon-died-no-signal", true,
                                       WEXITSTATUS(exit_code));
                        sts = WEXITSTATUS(exit_code);
                    }
                }
                rts_hnp_abort(jdata->jobid, sts);
            }
            break;

        case ORTE_JOB_STATE_SILENT_ABORT:
            failed_start(jdata);
            check_job_complete(jdata);  /* set the local proc states */
            /* the job object for this job will have been NULL'd
             * in the array if the job was solely local. If it isn't
             * NULL, then we need to tell everyone else to die
             */
            if (NULL != (jdata = orte_get_job_data_object(job))) {
                if (ORTE_PROC_MY_NAME->jobid == job && !orte_abnormal_term_ordered) {
                    /* set the flag indicating that a daemon failed so we use the proper
                     * methods for attempting to shutdown the rest of the system
                     */
                    orte_abnormal_term_ordered = true;
                }
                rts_hnp_abort(jdata->jobid, exit_code);
            }
            break;

        case ORTE_JOB_STATE_RUNNING:
            /* update all procs in job */
            update_local_procs_in_job(jdata, jobstate, ORTE_PROC_STATE_RUNNING, 0);
            /* record that we reported */
            jdata->num_daemons_reported++;
            /* report if requested */
            if (orte_report_launch_progress) {
                if (0 == jdata->num_daemons_reported % 100 || jdata->num_daemons_reported == orte_process_info.num_procs) {
                    opal_output(orte_clean_output, "Reported: %d (out of %d) daemons - %d (out of %d) procs",
                                (int)jdata->num_daemons_reported, (int)orte_process_info.num_procs,
                                (int)jdata->num_launched, (int)jdata->num_procs);
                }
            }
            break;
        case ORTE_JOB_STATE_NEVER_LAUNCHED:
            orte_never_launched = true;
            jdata->num_terminated = jdata->num_procs;
            check_job_complete(jdata);  /* set the local proc states */
            /* the job object for this job will have been NULL'd
             * in the array if the job was solely local. If it isn't
             * NULL, then we need to tell everyone else to die
             */
            if (NULL != (jdata = orte_get_job_data_object(job))) {
                rts_hnp_abort(jdata->jobid, exit_code);
            }
            break;
        case ORTE_JOB_STATE_SENSOR_BOUND_EXCEEDED:
            /* update all procs in job */
            update_local_procs_in_job(jdata, jobstate,
                                      ORTE_PROC_STATE_SENSOR_BOUND_EXCEEDED,
                                      exit_code);
            /* order all local procs for this job to be killed */
            killprocs(jdata->jobid, ORTE_VPID_WILDCARD);
            check_job_complete(jdata);  /* set the local proc states */
            /* the job object for this job will have been NULL'd
             * in the array if the job was solely local. If it isn't
             * NULL, then we need to tell everyone else to die
             */
            if (NULL != (jdata = orte_get_job_data_object(job))) {
                rts_hnp_abort(jdata->jobid, exit_code);
            }
            break;
        case ORTE_JOB_STATE_COMM_FAILED:
            /* order all local procs for this job to be killed */
            killprocs(jdata->jobid, ORTE_VPID_WILDCARD);
            check_job_complete(jdata);  /* set the local proc states */
            /* the job object for this job will have been NULL'd
             * in the array if the job was solely local. If it isn't
             * NULL, then we need to tell everyone else to die
             */
            if (NULL != (jdata = orte_get_job_data_object(job))) {
                rts_hnp_abort(jdata->jobid, exit_code);
            }
            break;
        case ORTE_JOB_STATE_HEARTBEAT_FAILED:
            /* order all local procs for this job to be killed */
            killprocs(jdata->jobid, ORTE_VPID_WILDCARD);
            check_job_complete(jdata);  /* set the local proc states */
            /* the job object for this job will have been NULL'd
             * in the array if the job was solely local. If it isn't
             * NULL, then we need to tell everyone else to die
             */
            if (NULL != (jdata = orte_get_job_data_object(job))) {
                rts_hnp_abort(jdata->jobid, exit_code);
            }
            break;

        default:
            break;
        }
        return ORTE_SUCCESS;
    }
    
    /* get the job object */
    if (NULL == (jdata = orte_get_job_data_object(proc->jobid))) {
        /* if the orteds are terminating, check job complete */
        if (orte_orteds_term_ordered) {
            opal_output(0, "TERM ORDERED - CHECKING COMPLETE");
            check_job_complete(NULL);
            return ORTE_SUCCESS;
        } else {
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            return ORTE_ERR_NOT_FOUND;
        }
    }

    /* update is for a specific proc */
    switch (state) {
    case ORTE_PROC_STATE_ABORTED:
    case ORTE_PROC_STATE_ABORTED_BY_SIG:
    case ORTE_PROC_STATE_TERM_WO_SYNC:
        update_proc(jdata, proc, state, pid, exit_code);
        /* kill all local procs */
        killprocs(proc->jobid, ORTE_VPID_WILDCARD);
        check_job_complete(jdata);  /* need to set the job state */
        /* the job object for this job will have been NULL'd
         * in the array if the job was solely local. If it isn't
         * NULL, then we need to tell everyone else to die
         */
        if (NULL != (jdata = orte_get_job_data_object(proc->jobid))) {
            rts_hnp_abort(jdata->jobid, exit_code);
        }
        break;

    case ORTE_PROC_STATE_FAILED_TO_START:
    case ORTE_PROC_STATE_CALLED_ABORT:
        update_proc(jdata, proc, state, pid, exit_code);
        check_job_complete(jdata);
        /* the job object for this job will have been NULL'd
         * in the array if the job was solely local. If it isn't
         * NULL, then we need to tell everyone else to die
         */
        if (NULL != (jdata = orte_get_job_data_object(proc->jobid))) {
            rts_hnp_abort(jdata->jobid, exit_code);
        }
        break;

    case ORTE_PROC_STATE_DEREGISTERED:
    case ORTE_PROC_STATE_REGISTERED:
    case ORTE_PROC_STATE_RUNNING:
        update_proc(jdata, proc, state, pid, exit_code);
        break;

    case ORTE_PROC_STATE_LAUNCHED:
        /* record the pid for this child */
        update_proc(jdata, proc, state, pid, exit_code);
        break;

    case ORTE_PROC_STATE_TERMINATED:
    case ORTE_PROC_STATE_TERM_NON_ZERO:
    case ORTE_PROC_STATE_KILLED_BY_CMD:
        update_proc(jdata, proc, state, pid, exit_code);
        check_job_complete(jdata);
        break;

    case ORTE_PROC_STATE_SENSOR_BOUND_EXCEEDED:
        /* kill all jobs */
        update_proc(jdata, proc, state, pid, exit_code);
        /* kill all local procs */
        killprocs(proc->jobid, ORTE_VPID_WILDCARD);
        check_job_complete(jdata);  /* need to set the job state */
        /* the job object for this job will have been NULL'd
         * in the array if the job was solely local. If it isn't
         * NULL, then we need to tell everyone else to die
         */
        if (NULL != (jdata = orte_get_job_data_object(proc->jobid))) {
            rts_hnp_abort(jdata->jobid, exit_code);
        }
        break;

    case ORTE_PROC_STATE_COMM_FAILED:
        /* is this to a daemon? */
        if (ORTE_PROC_MY_NAME->jobid != proc->jobid) {

            /* If there is only one daemon (HNP) then check for completion here */
            if(jdata->num_daemons_reported <= 1 ) {
                check_job_complete(jdata);  /* need to set the job state */
            }

            /* otherwise - ignore it */
            break;
        }
        /* if this is my own connection, ignore it */
        if (ORTE_PROC_MY_NAME->vpid == proc->vpid) {
            OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                                 "%s My own connection - ignoring it",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            break;
        }
        /* if we have ordered orteds to terminate, record it */
        if (orte_orteds_term_ordered) {
            OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                                 "%s Daemons terminating - recording daemon %s as gone",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(proc)));
            /* remove from dependent routes, if it is one */
            orte_routed.route_lost(proc);
            /* update daemon job */
            if (NULL != (pptr = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, proc->vpid))) {
                if (pptr->state < ORTE_PROC_STATE_TERMINATED) {
                    pptr->state = state;
                    jdata->num_terminated++;
                }
            }
            /* check if complete */
            check_job_complete(jdata);
            break;
        }
        /* if abort is in progress, see if this one failed to tell
         * us it had terminated
         */
        if (orte_abnormal_term_ordered) {
            OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                                 "%s Abort in progress - recording daemon %s as gone",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ORTE_NAME_PRINT(proc)));
            /* remove from dependent routes, if it is one */
            orte_routed.route_lost(proc);
            /* update daemon job */
            if (NULL != (pptr = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, proc->vpid))) {
                if (pptr->state < ORTE_PROC_STATE_TERMINATED) {
                    pptr->state = state;
                    jdata->num_terminated++;
                }
            }
            /* check if complete */
            check_job_complete(jdata);
        }

        /* ********************** RTS START ****************** */
        /* Need to purge the old information */
        /* delete the route */
        orte_routed.delete_route(proc);
        /* purge the oob */
        orte_rml.purge(proc);

        if (NULL == (pptr = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, proc->vpid))) {
            ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
            orte_show_help("help-orte-errmgr-hnp.txt", "errmgr-hnp:daemon-died", true,
                           ORTE_VPID_PRINT(proc->vpid), "Unknown");
        } else {
            orte_show_help("help-orte-errmgr-hnp.txt", "errmgr-hnp:daemon-died", true,
                           ORTE_VPID_PRINT(proc->vpid),
                           (NULL == pptr->node) ? "Unknown" : 
                           ((NULL == pptr->node->name) ? "Unknown" : pptr->node->name));
        }
        /* remove this proc from the daemon job */
        rts_hnp_record_dead_daemon(jdata, proc->vpid, state, exit_code);
        /* kill all local procs */
        killprocs(ORTE_JOBID_WILDCARD, ORTE_VPID_WILDCARD);
        /* kill all jobs */
        rts_hnp_abort(ORTE_JOBID_WILDCARD, exit_code);
        /* check if all is complete so we can terminate */
        check_job_complete(jdata);
        /* ********************** RTS END ****************** */

        break;

    case ORTE_PROC_STATE_HEARTBEAT_FAILED:
        /* heartbeats are only for daemons */
        if (NULL != (pptr = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, proc->vpid))) {
            if (pptr->state < ORTE_PROC_STATE_TERMINATED) {
                pptr->state = state;
                jdata->num_terminated++;
            }
        }
        /* remove from dependent routes, if it is one */
        orte_routed.route_lost(proc);
        /* kill all local procs */
        killprocs(ORTE_JOBID_WILDCARD, ORTE_VPID_WILDCARD);
        /* kill all jobs */
        rts_hnp_abort(ORTE_JOBID_WILDCARD, exit_code);
        return ORTE_ERR_UNRECOVERABLE;

    default:
        break;
    }

    return ORTE_SUCCESS;
}

static int predicted_fault(opal_list_t *proc_list,
                           opal_list_t *node_list,
                           opal_list_t *suggested_map)
{
    return ORTE_ERR_NOT_IMPLEMENTED;
}

static int suggest_map_targets(orte_proc_t *proc,
                               orte_node_t *oldnode,
                               opal_list_t *node_list)
{
    return ORTE_ERR_NOT_IMPLEMENTED;
}

static int ft_event(int state)
{
    return ORTE_SUCCESS;
}

/*****************
 * Local Functions
 *****************/
static void rts_hnp_abort(orte_jobid_t job, orte_exit_code_t exit_code)
{
    int rc;

    /* if we are already in progress, then ignore this call */
    if (opal_atomic_trylock(&orte_abort_inprogress_lock)) { /* returns 1 if already locked */
        OPAL_OUTPUT_VERBOSE((1, mca_errmgr_rts_hnp_component.output_handle,
                             "%s errmgr:rts_hnp: abort in progress, ignoring abort on job %s with status %d",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_JOBID_PRINT(job), exit_code));
        return;
    }

    OPAL_OUTPUT_VERBOSE((1, mca_errmgr_rts_hnp_component.output_handle,
                         "%s errmgr:rts_hnp: abort called on job %s with status %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(job), exit_code));
    
    /* set control params to indicate we are terminating */
    orte_job_term_ordered = true;
    orte_abnormal_term_ordered = true;
    orte_enable_recovery = false;

    /* set the exit status, just in case whomever called us failed
     * to do so - it can only be done once, so we are protected
     * from overwriting it
     */
    ORTE_UPDATE_EXIT_STATUS(exit_code);    

    /* tell the plm to terminate the orteds - they will automatically
     * kill their local procs
     */
    if (ORTE_SUCCESS != (rc = orte_plm.terminate_orteds())) {
        ORTE_ERROR_LOG(rc);
    }
}

static void failed_start(orte_job_t *jdata)
{
    opal_list_item_t *item, *next;
    orte_odls_job_t *jobdat;
    orte_odls_child_t *child;
    orte_proc_t *proc;

    /* lookup the local jobdat for this job */
    jobdat = NULL;
    for (item = opal_list_get_first(&orte_local_jobdata);
         item != opal_list_get_end(&orte_local_jobdata);
         item = opal_list_get_next(item)) {
        jobdat = (orte_odls_job_t*)item;

        /* is this the specified job? */
        if (jobdat->jobid == jdata->jobid) {
            break;
        }
    }
    if (NULL == jobdat) {
        /* race condition - may not have been formed yet */
        return;
    }
    jobdat->state = ORTE_JOB_STATE_FAILED_TO_START;

    OPAL_THREAD_LOCK(&orte_odls_globals.mutex);

    for (item = opal_list_get_first(&orte_local_children);
         item != opal_list_get_end(&orte_local_children);
         item = next) {
        next = opal_list_get_next(item);
        child = (orte_odls_child_t*)item;
        if (child->name->jobid == jobdat->jobid) {
            if (ORTE_PROC_STATE_LAUNCHED > child->state ||
                ORTE_PROC_STATE_UNTERMINATED < child->state) {
                /* get the master proc object */
                proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, child->name->vpid);
                proc->state = child->state;
                proc->exit_code = child->exit_code;
                /* update the counter so we can terminate */
                jdata->num_terminated++;
                /* remove the child from our list */
                opal_list_remove_item(&orte_local_children, &child->super);
                OBJ_RELEASE(child);
                jobdat->num_local_procs--;
            }
        }
    }

    opal_condition_signal(&orte_odls_globals.cond);
    OPAL_THREAD_UNLOCK(&orte_odls_globals.mutex);

    OPAL_OUTPUT_VERBOSE((1, mca_errmgr_rts_hnp_component.output_handle,
                         "%s errmgr:rts_hnp: job %s reported incomplete start",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(jdata->jobid)));
}

static void update_local_procs_in_job(orte_job_t *jdata, orte_job_state_t jobstate,
                                      orte_proc_state_t state, orte_exit_code_t exit_code)
{
    opal_list_item_t *item, *next;
    orte_odls_job_t *jobdat;
    orte_odls_child_t *child;
    orte_proc_t *proc;

    /* lookup the local jobdat for this job */
    jobdat = NULL;
    for (item = opal_list_get_first(&orte_local_jobdata);
         item != opal_list_get_end(&orte_local_jobdata);
         item = opal_list_get_next(item)) {
        jobdat = (orte_odls_job_t*)item;

        /* is this the specified job? */
        if (jobdat->jobid == jdata->jobid) {
            break;
        }
    }
    if (NULL == jobdat) {
        /* race condition - may not have been formed yet */
        return;
    }
    jobdat->state = jobstate;
    jdata->state = jobstate;

    OPAL_THREAD_LOCK(&orte_odls_globals.mutex);

    for (item = opal_list_get_first(&orte_local_children);
         item != opal_list_get_end(&orte_local_children);
         item = next) {
        next = opal_list_get_next(item);
        child = (orte_odls_child_t*)item;
        if (jdata->jobid == child->name->jobid) {
            child->state = state;
            proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, child->name->vpid);
            proc->state = state;
            if (proc->exit_code < exit_code) {
                proc->exit_code = exit_code;
            }
            if (ORTE_PROC_STATE_UNTERMINATED < state) {
                opal_list_remove_item(&orte_local_children, &child->super);
                OBJ_RELEASE(child);
                jdata->num_terminated++;
                jobdat->num_local_procs--;
            } else if (ORTE_PROC_STATE_RUNNING) {
                jdata->num_launched++;
            } else if (ORTE_PROC_STATE_REGISTERED == state) {
                jdata->num_reported++;
                if (jdata->dyn_spawn_active &&
                    jdata->num_reported == jdata->num_procs) {
                    OPAL_RELEASE_THREAD(&jdata->dyn_spawn_lock,
                                        &jdata->dyn_spawn_cond,
                                        &jdata->dyn_spawn_active);
                }
            } else if (ORTE_PROC_STATE_DEREGISTERED == state) {
                ; /* JJH TODO */
            }
        }
    }

    opal_condition_signal(&orte_odls_globals.cond);
    OPAL_THREAD_UNLOCK(&orte_odls_globals.mutex);

}

static void update_proc(orte_job_t *jdata,
                        orte_process_name_t *proc,
                        orte_proc_state_t state,
                        pid_t pid,
                        orte_exit_code_t exit_code)
{
    opal_list_item_t *item, *next;
    orte_odls_child_t *child;
    orte_proc_t *proct = NULL, *tmp_proc = NULL;
    orte_node_t *node = NULL;
    orte_odls_job_t *jobdat, *jdat;
    int i, j;

    jobdat = NULL;
    for (item  = opal_list_get_first(&orte_local_jobdata);
         item != opal_list_get_end(&orte_local_jobdata);
         item  = opal_list_get_next(item)) {
        jdat = (orte_odls_job_t*)item;
        if (jdat->jobid == jdata->jobid) {
            jobdat = jdat;
            break;
        }
    }
    if (NULL == jobdat) {
        OPAL_OUTPUT((mca_errmgr_rts_hnp_component.output_handle,
                    "%s failed to update the state of proc %s(pid %d) to %s(exit code %d) in jobid %s",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                    ORTE_NAME_PRINT(proc), (int)pid,
                    orte_job_state_to_str(state), (int)exit_code,
                    ORTE_JOBID_PRINT(jdata->jobid)));
        ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
    }

    OPAL_THREAD_LOCK(&orte_odls_globals.mutex);

    /***   UPDATE LOCAL CHILD   ***/
    for (item = opal_list_get_first(&orte_local_children);
         item != opal_list_get_end(&orte_local_children);
         item = next) {
        next = opal_list_get_next(item);
        child = (orte_odls_child_t*)item;
        if (child->name->jobid == proc->jobid) {
            if (child->name->vpid == proc->vpid) {
                /*
                 * If the child has been deregistered/finished then this is not
                 * an error worth reporting. Just do accounting.
                 */
                proct = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, child->name->vpid);
                node = proct->node;

                if( !child->fini_recvd ) {
                    child->state = state;
                    if (0 < pid) {
                        child->pid = pid;
                    }
                    child->exit_code = exit_code;
                    proct->state = state;
                    if (0 < pid) {
                        proct->pid = pid;
                    }
                    proct->exit_code = exit_code;
                }
                if (ORTE_PROC_STATE_UNTERMINATED < state) {
                    opal_list_remove_item(&orte_local_children, &child->super);
                    OBJ_RELEASE(child);
                    if (NULL != jobdat) {
                        jobdat->num_local_procs--;
                    }
                    jdata->num_terminated++;

                    /*
                     * Release resources held by this process on the node
                     */
                    for (j = 0; j < node->procs->size; j++) {
                        if (NULL == (tmp_proc = (orte_proc_t*)opal_pointer_array_get_item(node->procs, j))) {
                            continue;
                        }
                        /* Only act on the node with this process */
                        if( OPAL_EQUAL == orte_util_compare_name_fields(ORTE_NS_CMP_ALL,
                                                                        &(tmp_proc->name), &(proct->name)) ) {
                            node->slots_inuse--;
                            node->num_procs--;
                            OPAL_OUTPUT_VERBOSE((4, mca_errmgr_rts_hnp_component.output_handle,
                                                 "%s releasing proc %s from node %s - [%3d / %3d local]",
                                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                                 ORTE_NAME_PRINT(&proct->name), node->name,
                                                 node->slots_inuse, node->num_procs));
                            /* set the entry in the node array to NULL */
                            opal_pointer_array_set_item(node->procs, j, NULL);
                            /* release the proc once for the map entry */
                            OBJ_RELEASE(tmp_proc);
                        }
                    }
                } else if (ORTE_PROC_STATE_RUNNING == state) {
                    jdata->num_launched++;
                    if (jdata->num_launched == jdata->num_procs) {
                        jdata->state = ORTE_JOB_STATE_RUNNING;
                    }
                } else if (ORTE_PROC_STATE_REGISTERED == state) {
                    jdata->num_reported++;
                    if (jdata->dyn_spawn_active &&
                        jdata->num_reported == jdata->num_procs) {
                        OPAL_RELEASE_THREAD(&jdata->dyn_spawn_lock,
                                            &jdata->dyn_spawn_cond,
                                            &jdata->dyn_spawn_active);
                    }
                } else if (ORTE_PROC_STATE_DEREGISTERED == state) {
                    ; /* JJH TODO */
                }
                return;
            }
        }
    }

    opal_condition_signal(&orte_odls_globals.cond);
    OPAL_THREAD_UNLOCK(&orte_odls_globals.mutex);

    /***   UPDATE REMOTE CHILD   ***/
    for (i=0; i < jdata->procs->size; i++) {
        if (NULL == (proct = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, i))) {
            continue;
        }
        if (proct->name.jobid != proc->jobid ||
            proct->name.vpid != proc->vpid) {
            continue;
        }
        proct->state = state;
        if (0 < pid) {
            proct->pid = pid;
        }
        proct->exit_code = exit_code;
        if (ORTE_PROC_STATE_UNTERMINATED < state) {
            /*
             * Release resources held by this process on the node
             */
            node = proct->node;

            for (j = 0; j < node->procs->size; j++) {
                if (NULL == (tmp_proc = (orte_proc_t*)opal_pointer_array_get_item(node->procs, j))) {
                    continue;
                }
                /* Only act on the node with this process */
                if( OPAL_EQUAL == orte_util_compare_name_fields(ORTE_NS_CMP_ALL,
                                                                &(tmp_proc->name), &(proct->name)) ) {
                    node->slots_inuse--;
                    node->num_procs--;
                    OPAL_OUTPUT_VERBOSE((4, mca_errmgr_rts_hnp_component.output_handle,
                                         "%s releasing proc %s from node %s - [%3d / %3d remote]",
                                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                         ORTE_NAME_PRINT(&proct->name), node->name,
                                         node->slots_inuse, node->num_procs));
                    /* set the entry in the node array to NULL */
                    opal_pointer_array_set_item(node->procs, j, NULL);
                    /* release the proc once for the map entry */
                    OBJ_RELEASE(tmp_proc);
                    break;
                }
            }
        }
        if (ORTE_PROC_STATE_REGISTERED == state) {
            jdata->num_reported++;
            if (jdata->dyn_spawn_active &&
                jdata->num_reported == jdata->num_procs) {
                OPAL_RELEASE_THREAD(&jdata->dyn_spawn_lock,
                                    &jdata->dyn_spawn_cond,
                                    &jdata->dyn_spawn_active);
            }
        } else if (ORTE_PROC_STATE_DEREGISTERED == state) {
            ; /* JJH TODO */
        } else if (ORTE_PROC_STATE_UNTERMINATED < state) {
            /* update the counter so we can terminate */
            jdata->num_terminated++;
        } else if (ORTE_PROC_STATE_RUNNING == state) {
            jdata->num_launched++;
            if (jdata->num_launched == jdata->num_procs) {
                jdata->state = ORTE_JOB_STATE_RUNNING;
            }
        }
        return;
    }
}

static void check_job_complete(orte_job_t *jdata)
{
    orte_proc_t *proc;
    int i;
    orte_std_cntr_t j;
    orte_job_t *job;
    orte_node_t *node;
    orte_job_map_t *map;
    orte_std_cntr_t index;
    bool one_still_alive;
    orte_vpid_t non_zero=0, lowest=0;
    char *msg;

    if (NULL == jdata) {
        /* just check to see if the daemons are complete */
        OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                             "%s errmgr:rts_hnp:check_job_complete - received NULL job, checking daemons",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        goto CHECK_DAEMONS;
    }

    for (i=0; i < jdata->procs->size && !jdata->abort; i++) {
        if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, i))) {
            /* the proc array may no longer be left justified, so
             * we need to check everything
             */
            continue;
        }

        if (0 != proc->exit_code) {
            non_zero++;
            if (0 == lowest) {
                lowest = proc->exit_code;
            }
        }

        switch (proc->state) {
        case ORTE_PROC_STATE_KILLED_BY_CMD:
            OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                                 "%s errmgr:rts_hnp:check_job_completed proc %s killed by cmd",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&proc->name)));
            /* we ordered this proc to die, so it isn't an abnormal termination
             * and we don't flag it as such - just check the remaining jobs to
             * see if anyone is still alive
             */
            if (jdata->num_terminated >= jdata->num_procs) {
                /* this job has terminated - now we need to check to see if ALL
                 * the other jobs have also completed and wakeup if that is true
                 */
                if (!jdata->abort) {
                    jdata->state = ORTE_JOB_STATE_KILLED_BY_CMD;
                }
            }
            goto CHECK_ALIVE;
            break;
        case ORTE_PROC_STATE_ABORTED:
            OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                                 "%s errmgr:rts_hnp:check_job_completed proc %s aborted",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&proc->name)));
            if (!jdata->abort) {
                jdata->state = ORTE_JOB_STATE_ABORTED;
                /* point to the lowest rank to cause the problem */
                jdata->aborted_proc = proc;
                /* retain the object so it doesn't get free'd */
                OBJ_RETAIN(proc);
                jdata->abort = true;
                ORTE_UPDATE_EXIT_STATUS(proc->exit_code);
            }
            break;
        case ORTE_PROC_STATE_FAILED_TO_START:
            OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                                 "%s errmgr_rts_hnp:check_job_completed proc %s failed to start",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&proc->name)));
            if (!jdata->abort) {
                jdata->state = ORTE_JOB_STATE_FAILED_TO_START;
                /* point to the lowest rank to cause the problem */
                jdata->aborted_proc = proc;
                /* retain the object so it doesn't get free'd */
                OBJ_RETAIN(proc);
                jdata->abort = true;
                ORTE_UPDATE_EXIT_STATUS(proc->exit_code);
            }
            break;
        case ORTE_PROC_STATE_ABORTED_BY_SIG:
            OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                                 "%s errmgr:rts_hnp:check_job_completed proc %s aborted by signal",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&proc->name)));
            if (!jdata->abort) {
                jdata->state = ORTE_JOB_STATE_ABORTED_BY_SIG;
                /* point to the lowest rank to cause the problem */
                jdata->aborted_proc = proc;
                /* retain the object so it doesn't get free'd */
                OBJ_RETAIN(proc);
                jdata->abort = true;
                ORTE_UPDATE_EXIT_STATUS(proc->exit_code);
            }
            break;
        case ORTE_PROC_STATE_TERM_WO_SYNC:
            OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                                 "%s errmgr:rts_hnp:check_job_completed proc %s terminated without sync",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&proc->name)));
            if (!jdata->abort) {
                jdata->state = ORTE_JOB_STATE_ABORTED_WO_SYNC;
                /* point to the lowest rank to cause the problem */
                jdata->aborted_proc = proc;
                /* retain the object so it doesn't get free'd */
                OBJ_RETAIN(proc);
                jdata->abort = true;
                ORTE_UPDATE_EXIT_STATUS(proc->exit_code);
                /* now treat a special case - if the proc exit'd without a required
                 * sync, it may have done so with a zero exit code. We want to ensure
                 * that the user realizes there was an error, so in this -one- case,
                 * we overwrite the process' exit code with the default error code
                 */
                ORTE_UPDATE_EXIT_STATUS(ORTE_ERROR_DEFAULT_EXIT_CODE);
            }
            break;
        case ORTE_PROC_STATE_COMM_FAILED:
            if (!jdata->abort) {
                jdata->state = ORTE_JOB_STATE_COMM_FAILED;
                /* point to the lowest rank to cause the problem */
                jdata->aborted_proc = proc;
                /* retain the object so it doesn't get free'd */
                OBJ_RETAIN(proc);
                jdata->abort = true;
                ORTE_UPDATE_EXIT_STATUS(proc->exit_code);
            }
            break;
        case ORTE_PROC_STATE_SENSOR_BOUND_EXCEEDED:
            if (!jdata->abort) {
                jdata->state = ORTE_JOB_STATE_SENSOR_BOUND_EXCEEDED;
                /* point to the lowest rank to cause the problem */
                jdata->aborted_proc = proc;
                /* retain the object so it doesn't get free'd */
                OBJ_RETAIN(proc);
                jdata->abort = true;
                ORTE_UPDATE_EXIT_STATUS(proc->exit_code);
            }
            break;
        case ORTE_PROC_STATE_CALLED_ABORT:
            if (!jdata->abort) {
                jdata->state = ORTE_JOB_STATE_CALLED_ABORT;
                /* point to the first proc to cause the problem */
                jdata->aborted_proc = proc;
                /* retain the object so it doesn't get free'd */
                OBJ_RETAIN(proc);
                jdata->abort = true;
                ORTE_UPDATE_EXIT_STATUS(proc->exit_code);
            }
            break;
        case ORTE_PROC_STATE_HEARTBEAT_FAILED:
            if (!jdata->abort) {
                jdata->state = ORTE_JOB_STATE_HEARTBEAT_FAILED;
                /* point to the lowest rank to cause the problem */
                jdata->aborted_proc = proc;
                /* retain the object so it doesn't get free'd */
                OBJ_RETAIN(proc);
                jdata->abort = true;
                ORTE_UPDATE_EXIT_STATUS(proc->exit_code);
            }
            break;
        case ORTE_PROC_STATE_TERM_NON_ZERO:
            ORTE_UPDATE_EXIT_STATUS(proc->exit_code);
            if (orte_abort_non_zero_exit) {
                if (!jdata->abort) {
                    jdata->state = ORTE_JOB_STATE_NON_ZERO_TERM;
                    /* point to the lowest rank to cause the problem */
                    jdata->aborted_proc = proc;
                    /* retain the object so it doesn't get free'd */
                    OBJ_RETAIN(proc);
                    jdata->abort = true;
                }
            }
            break;

        default:
            if (ORTE_PROC_STATE_UNTERMINATED < proc->state &&
                jdata->controls & ORTE_JOB_CONTROL_CONTINUOUS_OP) {
                OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                                     "%s errmgr:rts_hnp:check_job_completed proc %s terminated and continuous",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(&proc->name)));
                if (!jdata->abort) {
                    proc->state = ORTE_PROC_STATE_ABORTED;
                    jdata->state = ORTE_JOB_STATE_ABORTED;
                    /* point to the lowest rank to cause the problem */
                    jdata->aborted_proc = proc;
                    /* retain the object so it doesn't get free'd */
                    OBJ_RETAIN(proc);
                    jdata->abort = true;
                    ORTE_UPDATE_EXIT_STATUS(proc->exit_code);
                }
            }
            break;
        }
    }
    
    if (jdata->abort) {
        /* the job aborted - turn off any sensors on this job */
        orte_sensor.stop(jdata->jobid);
    }

    if (ORTE_JOB_STATE_UNTERMINATED > jdata->state &&
        jdata->num_terminated >= jdata->num_procs) {
        /* this job has terminated */
        jdata->state = ORTE_JOB_STATE_TERMINATED;

        /* turn off any sensor monitors on this job */
        orte_sensor.stop(jdata->jobid);

        if (0 < non_zero) {
            if (!orte_report_child_jobs_separately || 1 == ORTE_LOCAL_JOBID(jdata->jobid)) {
                /* update the exit code */
                ORTE_UPDATE_EXIT_STATUS(lowest);
            }

            /* warn user */
            opal_output(orte_clean_output,
                        "-------------------------------------------------------\n"
                        "While %s job %s terminated normally, %s %s. Further examination may be required.\n"
                        "-------------------------------------------------------",
                        (1 == ORTE_LOCAL_JOBID(jdata->jobid)) ? "the primary" : "child",
                        (1 == ORTE_LOCAL_JOBID(jdata->jobid)) ? "" : ORTE_LOCAL_JOBID_PRINT(jdata->jobid),
                        ORTE_VPID_PRINT(non_zero),
                        (1 == non_zero) ? "process returned\na non-zero exit code." : "processes returned\nnon-zero exit codes.");
        }
        OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                             "%s errmgr:rts_hnp:check_job_completed declared job %s normally terminated - checking all jobs",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_JOBID_PRINT(jdata->jobid)));
    }

    /* if this job is a continuously operating one, then don't do
     * anything further - just return here
     */
    if (NULL != jdata &&
        (ORTE_JOB_CONTROL_CONTINUOUS_OP & jdata->controls ||
         ORTE_JOB_CONTROL_RECOVERABLE & jdata->controls)) {
        goto CHECK_ALIVE;
    }

    /* if the job that is being checked is the HNP, then we are
     * trying to terminate the orteds. In that situation, we
     * do -not- check all jobs - we simply notify the RTS_HNP
     * that the orteds are complete. Also check special case
     * if jdata is NULL - we want
     * to definitely declare the job done if the orteds
     * have completed, no matter what else may be happening.
     * This can happen if a ctrl-c hits in the "wrong" place
     * while launching
     */
CHECK_DAEMONS:
    if (jdata == NULL || jdata->jobid == ORTE_PROC_MY_NAME->jobid) {
        if (0 == orte_routed.num_routes()) {
            /* orteds are done! */
            OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                                 "%s orteds complete - exiting",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
            if (NULL == jdata) {
                jdata = orte_get_job_data_object(ORTE_PROC_MY_NAME->jobid);
            }
            jdata->state = ORTE_JOB_STATE_TERMINATED;
            orte_quit();
            return;
        }
        return;
    }

    /* Release the resources used by this job. Since some errmgrs may want
     * to continue using resources allocated to the job as part of their
     * fault recovery procedure, we only do this once the job is "complete".
     * Note that an aborted/killed job -is- flagged as complete and will
     * therefore have its resources released. We need to do this after
     * we call the errmgr so that any attempt to restart the job will
     * avoid doing so in the exact same place as the current job
     */
    if (NULL != jdata->map  && jdata->state == ORTE_JOB_STATE_TERMINATED) {
        map = jdata->map;
        for (index = 0; index < map->nodes->size; index++) {
            if (NULL == (node = (orte_node_t*)opal_pointer_array_get_item(map->nodes, index))) {
                continue;
            }
            OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                                 "%s releasing procs from node %s",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 node->name));
            for (i = 0; i < node->procs->size; i++) {
                if (NULL == (proc = (orte_proc_t*)opal_pointer_array_get_item(node->procs, i))) {
                    continue;
                }
                if (proc->name.jobid != jdata->jobid) {
                    /* skip procs from another job */
                    continue;
                }
                node->slots_inuse--;
                node->num_procs--;
                OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                                     "%s releasing proc %s from node %s",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                     ORTE_NAME_PRINT(&proc->name), node->name));
                /* set the entry in the node array to NULL */
                opal_pointer_array_set_item(node->procs, i, NULL);
                /* release the proc once for the map entry */
                OBJ_RELEASE(proc);
            }
        }
        OBJ_RELEASE(map);
        jdata->map = NULL;
    }

CHECK_ALIVE:
    /* now check to see if all jobs are done - release this jdata
     * object when we find it
     */
    one_still_alive = false;
    for (j=1; j < orte_job_data->size; j++) {
        if (NULL == (job = (orte_job_t*)opal_pointer_array_get_item(orte_job_data, j))) {
            /* since we are releasing jdata objects as we
             * go, we can no longer assume that the job_data
             * array is left justified
             */
            continue;
        }
        /* if this is the job we are checking AND it normally terminated,
         * then go ahead and release it. We cannot release it if it
         * abnormally terminated as mpirun needs the info so it can
         * report appropriately to the user
         *
         * NOTE: do not release the primary job (j=1) so we
         * can pretty-print completion message
         */
        if (NULL != jdata && job->jobid == jdata->jobid &&
            (jdata->state == ORTE_JOB_STATE_TERMINATED ||
             jdata->state == ORTE_JOB_STATE_KILLED_BY_CMD)) {
            /* release this object, ensuring that the
             * pointer array internal accounting
             * is maintained!
             */
            if (1 < j) {
                opal_pointer_array_set_item(orte_job_data, j, NULL);  /* ensure the array has a NULL */
                OBJ_RELEASE(jdata);
            }
            continue;
        }
        /* if the job is flagged to not be monitored, skip it */
        if (ORTE_JOB_CONTROL_DO_NOT_MONITOR & job->controls) {
            continue;
        }
        /* when checking for job termination, we must be sure to NOT check
         * our own job as it - rather obviously - has NOT terminated!
         */
        if (job->num_terminated < job->num_procs) {
            /* we have at least one job that is not done yet - we cannot
             * just return, though, as we need to ensure we cleanout the
             * job data for the job that just completed
             */
            OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                                 "%s errmgr:rts_hnp:check_job_completed job %s is not terminated (%d:%d)",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_JOBID_PRINT(job->jobid),
                                 job->num_terminated, job->num_procs));
            one_still_alive = true;
        }
        else {
            OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                                 "%s errmgr:rts_hnp:check_job_completed job %s is terminated (%d vs %d [%s])",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_JOBID_PRINT(job->jobid),
                                 job->num_terminated, job->num_procs,
                                 (NULL == jdata) ? "UNKNOWN" : orte_job_state_to_str(jdata->state) ));
        }
    }
    /* if a job is still alive, we just return */
    if (one_still_alive) {
        OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                             "%s errmgr:rts_hnp:check_job_completed at least one job is not terminated",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        return;
    }
    /* if we get here, then all jobs are done, so terminate */
    OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                         "%s errmgr:rts_hnp:check_job_completed all jobs terminated",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
    /* set the exit status to 0 - this will only happen if it
     * wasn't already set by an error condition
     */
    ORTE_UPDATE_EXIT_STATUS(0);
    /* provide a notifier message if that framework is active - ignored otherwise */
    if (NULL != (job = (orte_job_t*)opal_pointer_array_get_item(orte_job_data, 1))) {
        if (NULL == job->name) {
            job->name = strdup(orte_process_info.nodename);
        }
        if (NULL == job->instance) {
            asprintf(&job->instance, "%d", orte_process_info.pid);
        }
        if (0 == orte_exit_status) {
            asprintf(&msg, "Job %s:%s complete", job->name, job->instance);
            orte_notifier.log(ORTE_NOTIFIER_INFO, 0, msg);
        } else {
            asprintf(&msg, "Job %s:%s terminated abnormally", job->name, job->instance);
            orte_notifier.log(ORTE_NOTIFIER_ALERT, orte_exit_status, msg);
        }
        free(msg);
        /* this job object will be release during finalize */
    }

    orte_jobs_complete();
    /* if I am the only daemon alive, then I can exit now */
    if (0 == orte_routed.num_routes()) {
        OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                             "%s orteds complete - exiting",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        orte_quit();
    }
}

static void killprocs(orte_jobid_t job, orte_vpid_t vpid)
{
    opal_pointer_array_t cmd;
    orte_proc_t proc;
    int rc;

    /* stop local sensors for this job */
    if (ORTE_VPID_WILDCARD == vpid) {
        orte_sensor.stop(job);
    }

    if (ORTE_JOBID_WILDCARD == job 
        && ORTE_VPID_WILDCARD == vpid) {
        
        if (ORTE_SUCCESS != (rc = orte_odls.kill_local_procs(NULL))) {
            ORTE_ERROR_LOG(rc);
        }
        return;
    }

    OBJ_CONSTRUCT(&cmd, opal_pointer_array_t);
    OBJ_CONSTRUCT(&proc, orte_proc_t);
    proc.name.jobid = job;
    proc.name.vpid = vpid;
    ORTE_EPOCH_SET(proc.name.epoch,orte_ess.proc_get_epoch(&(proc.name)));
    opal_pointer_array_add(&cmd, &proc);
    if (ORTE_SUCCESS != (rc = orte_odls.kill_local_procs(&cmd))) {
        ORTE_ERROR_LOG(rc);
    }
    OBJ_DESTRUCT(&cmd);
    OBJ_DESTRUCT(&proc);
}

/* *************************** RTS START **************************** */
void rts_hnp_record_dead_daemon(orte_job_t *jdat,
                                orte_vpid_t vpid,
                                orte_proc_state_t state,
                                orte_exit_code_t exit_code)
{
#if 1
    orte_proc_t *pptr = NULL;
    orte_node_t *node;

    /*
     * Access daemon information
     */
    if( NULL == (pptr = (orte_proc_t*)opal_pointer_array_get_item(jdat->procs, vpid)) ) {
        /* JJH ? Error? */
        return;
    }

    /*
     * If the process is already marked as down, just skip
     */
    if( ORTE_PROC_STATE_TERMINATED <= pptr->state ) {
        return;
    }

    /* need to record that this one died */
    pptr->state = state;
    pptr->exit_code = exit_code;
    ORTE_UPDATE_EXIT_STATUS(exit_code);

    /* remove it from the job array */
    opal_pointer_array_set_item(jdat->procs, vpid, NULL);
    orte_process_info.num_procs--;

    /* mark the node as down so it won't be used in mapping
     * procs to be relaunched
     */
    node = pptr->node;
    node->state = ORTE_NODE_STATE_DOWN;
    node->daemon = NULL;
    OBJ_RELEASE(pptr);  /* maintain accounting */

    /*jdat->num_procs--;*/
    jdat->num_terminated++;

#else
    orte_job_t *jdt;
    orte_proc_t *pdat;

    if (NULL != (pdat = (orte_proc_t*)opal_pointer_array_get_item(jdat->procs, vpid)) &&
        ORTE_PROC_STATE_TERMINATED != pdat->state) {
        /* mark all procs on this node as having terminated */
        for (i=0; i < node->procs->size; i++) {
            if (NULL == (pdat = (orte_proc_t*)opal_pointer_array_get_item(node->procs, i))) {
                continue;
            }
            /* get the job data object for this process */
            if (NULL == (jdt = orte_get_job_data_object(pdat->name.jobid))) {
                /* It is possible that the process job finishes before the daemons.
                 * In that case the process state is set to normal termination, and
                 * the job data has already been cleared. So no need to throw an
                 * error.
                 */
                if( ORTE_PROC_STATE_TERMINATED != pdat->state ) {
                    opal_output(0,
                                "%s Error: Failed to find job_data for proc %s (%s) on node %s",
                                ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                ORTE_NAME_PRINT(&pdat->name),
                                orte_proc_state_to_str(pdat->state),
                                node->name );
                    /* major problem */
                    ORTE_ERROR_LOG(ORTE_ERR_NOT_FOUND);
                }
                continue;
            }
            pdat->state = ORTE_PROC_STATE_ABORTED;
            jdt->num_terminated++;
        }
    }
#endif
}

static int rts_hnp_stable_global_update_state(orte_jobid_t job,
                                              orte_job_state_t jobstate,
                                              orte_process_name_t *proc_name,
                                              orte_proc_state_t state,
                                              pid_t pid,
                                              orte_exit_code_t exit_code)
{
    int ret = ORTE_SUCCESS;
    orte_job_t *jdata = NULL;
    static int num_deregistered = 0;

    /*
     * if orte is trying to shutdown, just let it
     */
    if (orte_finalizing ||
        orte_job_term_ordered ||
        orte_orteds_term_ordered ||
        ORTE_PROC_STATE_TERMINATED == state ) {
        return ORTE_SUCCESS;
    }

    /*
     * Skip self updates
     */
    if( NULL != proc_name &&
        OPAL_EQUAL == orte_util_compare_name_fields(ORTE_NS_CMP_ALL, ORTE_PROC_MY_NAME, proc_name) ) {
        OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                             "%s errmgr:hnp(stable): Update reported on self (%s), state %s. Skip...",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_NAME_PRINT(proc_name),
                             orte_proc_state_to_str(state) ));
        return ORTE_SUCCESS;
    }

    /*
     * Get the job data object for this process
     */
    if( NULL != proc_name ) { /* Get job from proc's jobid */
        jdata = orte_get_job_data_object(proc_name->jobid);
    } else { /* Get from the general job */
        jdata = orte_get_job_data_object(job);
    }
    if( NULL == jdata ) {
        OPAL_OUTPUT_VERBOSE((1, mca_errmgr_rts_hnp_component.output_handle,
                             "%s errmgr:hnp(stable): Error: Cannot find job %s for Process %s",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             ORTE_JOBID_PRINT(job),
                             (NULL == proc_name) ? "NULL" : ORTE_NAME_PRINT(proc_name) ));
        ret = ORTE_ERROR;
        ORTE_ERROR_LOG(ret);
        return ret;
    }

    /*
     * If this is a tool, ignore
     */
    if( jdata->num_apps == 0 &&
        OPAL_EQUAL != orte_util_compare_name_fields(ORTE_NS_CMP_JOBID, ORTE_PROC_MY_NAME, proc_name) ) {
        OPAL_OUTPUT_VERBOSE((1, mca_errmgr_rts_hnp_component.output_handle,
                             "%s errmgr:hnp(stable): An external tool disconnected. Ignore...",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));
        return ORTE_SUCCESS;
    }

    /*
     * If we had a deregisteration, count it as the job shutting down
     */
    if( ORTE_PROC_STATE_DEREGISTERED == state ) {
        ++num_deregistered;
        OPAL_OUTPUT_VERBOSE((1, mca_errmgr_rts_hnp_component.output_handle,
                             "%s errmgr:rts_hnp: %3d procs have deregistered - proc %s (%s)",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             num_deregistered,
                             (NULL == proc_name) ? "NULL" : ORTE_NAME_PRINT(proc_name),
                             orte_proc_state_to_str(state) ));
    }
    if( num_deregistered > 0 ) {
        return ORTE_SUCCESS;
    }

    OPAL_OUTPUT_VERBOSE((5, mca_errmgr_rts_hnp_component.output_handle,
                         "%s errmgr:rts_hnp: Check Stable for job %s (%s), proc %s (%s) exit_code %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_JOBID_PRINT(job),
                         orte_job_state_to_str(jobstate),
                         (NULL == proc_name) ? "NULL" : ORTE_NAME_PRINT(proc_name),
                         orte_proc_state_to_str(state), exit_code));

    /* ORTE_PROC_STATE_TERM_WO_SYNC == exit(-1) */

    if( ORTE_PROC_STATE_ABORTED        == state ||
        ORTE_PROC_STATE_ABORTED_BY_SIG == state ||
        ORTE_PROC_STATE_KILLED_BY_CMD  == state ||
        ORTE_PROC_STATE_COMM_FAILED    == state ||
        ORTE_PROC_STATE_TERM_WO_SYNC   == state ) {
        if( ORTE_SUCCESS != (ret = rts_hnp_stable_process_fault(jdata, proc_name, state)) ) {
            ORTE_ERROR_LOG(ret);
            return ret;
        }
    }

    return ORTE_SUCCESS;
}

static int rts_hnp_stable_process_fault(orte_job_t *jdata,
                                        orte_process_name_t *proc_name,
                                        orte_proc_state_t state)
{
    int exit_status = ORTE_SUCCESS;
    orte_odls_job_t *jobdat = NULL;
    opal_list_item_t *item = NULL;

    jdata->controls |= ORTE_JOB_CONTROL_RECOVERABLE;

    if( proc_name->jobid == ORTE_PROC_MY_NAME->jobid ) {
        exit_status = rts_hnp_stable_process_fault_daemon(jdata, proc_name, state);
    } else {
        update_proc(jdata, proc_name, state, 0, 0);
        exit_status = rts_hnp_stable_process_fault_app(jdata, proc_name, state);

        jobdat = NULL;
        for (item  = opal_list_get_first(&orte_local_jobdata);
             item != opal_list_get_end(&orte_local_jobdata);
             item  = opal_list_get_next(item)) {
            jobdat = (orte_odls_job_t*)item;
            if (jobdat->jobid == jdata->jobid) {
                jobdat->num_participating = -1;
            }
        }
    }

    return exit_status;
}

static int rts_hnp_stable_process_fault_app(orte_job_t *jdata,
                                            orte_process_name_t *proc,
                                            orte_proc_state_t state)
{
    int exit_status = ORTE_SUCCESS;
    orte_proc_t *xcast_proc = NULL;
    struct timeval soon;
    bool rts_hnp_check_job = false;

    OPAL_OUTPUT_VERBOSE((3, mca_errmgr_rts_hnp_component.output_handle,
                         "errmgr:hnp(stable):process_fault_app() "
                         "------- Application fault reported! proc %s (0x%x - %s) ",
                         ORTE_NAME_PRINT(proc),
                         state,
                         orte_proc_state_to_str(state) ));

    rts_hnp_ignore_current_update = true;

    /*
     * If that was the last process in the job, then we need to check for job
     * termination after processing the failure. This will also force us to
     * check if all jobs have completed.
     */
    if( jdata->num_launched <= jdata->num_terminated ) {
        OPAL_OUTPUT_VERBOSE((3, mca_errmgr_rts_hnp_component.output_handle,
                             "errmgr:hnp(stable):process_fault_app() "
                             "------- Terminate Job, No processes left [L %2d, R %2d, T %2d, D %2d] (0x%x)",
                             jdata->num_launched,
                             jdata->num_reported,
                             jdata->num_terminated,
                             jdata->num_daemons_reported,
                             jdata->state));
        /* This will trigger a check_job() at the end of this function.
         * So as to determine if all of the jobs are finished.
         */
        rts_hnp_check_job = true;
    }

    /*
     * If no processes are reported in then should not xcast.
     * This is often the case when launching non-OMPI applications.
     * JJH: However the failure could have occurred during launch of an OMPI
     *      application before it has reported in. In this case we should
     *      keep a list of non-xcast'ed process failures that get sent out
     *      with the first successful xcast.
     */
    if( 0 >= jdata->num_reported ) {
        OPAL_OUTPUT_VERBOSE((3, mca_errmgr_rts_hnp_component.output_handle,
                             "errmgr:hnp(stable):process_fault_app() "
                             "------- Skip xcast, no processes reported in. (L %2d, R %2d, T %2d, D %2d] (0x%x)",
                             jdata->num_launched,
                             jdata->num_reported,
                             jdata->num_terminated,
                             jdata->num_daemons_reported,
                             jdata->state));

        exit_status = ORTE_SUCCESS;
        goto cleanup;
    }

    xcast_proc = OBJ_NEW(orte_proc_t);
    xcast_proc->name.jobid = proc->jobid;
    xcast_proc->name.vpid  = proc->vpid;
    xcast_proc->state      = state;
    opal_pointer_array_add(proc_failures_pending_xcast, (void*)xcast_proc);

    /* Setup the timer, if it is not already going */
    if( !stable_xcast_timer_active ) {
        stable_xcast_timer_active = true;

        opal_event_evtimer_set(opal_event_base, stable_xcast_timer_event, rts_hnp_stable_xcast_fn, NULL);
        if( errmgr_rts_hnp_proc_fail_xcast_delay > 0 ) {
            soon.tv_sec  = errmgr_rts_hnp_proc_fail_xcast_delay;
            soon.tv_usec = 0;
        } else {
            soon.tv_sec  = 0;
            soon.tv_usec = 1;
        }
        opal_event_evtimer_add(stable_xcast_timer_event, &soon);
    }

 cleanup:
    if( rts_hnp_check_job ) {
        check_job_complete(jdata);
    }

    return exit_status;
}

static void rts_hnp_stable_xcast_fn(int fd, short event, void *cbdata)
{
    int ret, exit_status = ORTE_SUCCESS;
    opal_pointer_array_t *swap = NULL;
    orte_proc_t *xcast_proc = NULL;
    opal_buffer_t buffer;
    orte_std_cntr_t i;
    void *item = NULL;
    int num_procs;
    orte_job_t *jdata;
    int n;

    /*
     * Swap the xcast list pointers
     */
    swap = proc_failures_pending_xcast;
    proc_failures_pending_xcast = proc_failures_current_xcast;
    proc_failures_current_xcast = swap;

    /*
     * Xcast a message to all of the local processes telling them about
     * the loss of the process.
     */
    OBJ_CONSTRUCT(&buffer, opal_buffer_t);

    /*
     * Count the number of procs to send out
     */
    num_procs = 0;
    for( i = 0; i < proc_failures_current_xcast->size; ++i) {
        if( NULL == (xcast_proc = (orte_proc_t*)opal_pointer_array_get_item(proc_failures_current_xcast, i))) {
            continue;
        }
        ++num_procs;
    }

    if (ORTE_SUCCESS != (ret = opal_dss.pack(&buffer, &(num_procs), 1, OPAL_INT))) {
        ORTE_ERROR_LOG(ret);
        exit_status = ret;
        goto cleanup;
    }

    /*
     * Pack up all procs
     */
    for( i = 0; i < proc_failures_current_xcast->size; ++i) {
        if( NULL == (xcast_proc = (orte_proc_t*)opal_pointer_array_get_item(proc_failures_current_xcast, i))) {
            continue;
        }

        if (ORTE_SUCCESS != (ret = opal_dss.pack(&buffer, &(xcast_proc->name), 1, ORTE_NAME))) {
            ORTE_ERROR_LOG(ret);
            exit_status = ret;
            goto cleanup;
        }

        if (ORTE_SUCCESS != (ret = opal_dss.pack(&buffer, &(xcast_proc->state), 1, ORTE_PROC_STATE))) {
            ORTE_ERROR_LOG(ret);
            exit_status = ret;
            goto cleanup;
        }
    }

    /*
     * xcast them out
     * Need to xcast to all jobids since they may all need to know
     * For example, in comm_spawn case there are two different jobids
     */
    OPAL_OUTPUT_VERBOSE((4, mca_errmgr_rts_hnp_component.output_handle,
                         "errmgr:hnp(stable):rts_hnp_stable_xcast_fn() "
                         "------- Notifying all jobs that %3d processes have failed",
                         num_procs));

    for( n = 0; n < orte_job_data->size; ++n ) {
        if( NULL == (jdata = (orte_job_t*)opal_pointer_array_get_item(orte_job_data, n)) ) {
            continue;
        }
        /* We can skip the daemons */
        if( jdata->jobid == ORTE_PROC_MY_NAME->jobid ) {
            continue;
        }
        /* Skip jobs that are empty */
        if( jdata->num_launched <= jdata->num_terminated ) {
            OPAL_OUTPUT_VERBOSE((4, mca_errmgr_rts_hnp_component.output_handle,
                                 "errmgr:hnp(stable):rts_hnp_stable_xcast_fn() "
                                 "------- Skip   Job %s No procs left.",
                                 ORTE_JOBID_PRINT(jdata->jobid) ));
            continue;
        }
        OPAL_OUTPUT_VERBOSE((4, mca_errmgr_rts_hnp_component.output_handle,
                             "errmgr:hnp(stable):rts_hnp_stable_xcast_fn() "
                             "------- Notify Job %s [L %2d, R %2d, T %2d, D %2d] (procs %2d / %2d)",
                             ORTE_JOBID_PRINT(jdata->jobid),
                             jdata->num_launched,
                             jdata->num_reported,
                             jdata->num_terminated,
                             jdata->num_daemons_reported,
                             jdata->num_procs,
                             (jdata->num_procs - jdata->num_terminated) ));
        if( ORTE_SUCCESS != (ret = orte_grpcomm.xcast(jdata->jobid, &buffer, ORTE_RML_TAG_ERRMGR)) ) {
            ORTE_ERROR_LOG(ret);
            exit_status = ret;
            goto cleanup;
        }
    }

 cleanup:
    OBJ_DESTRUCT(&buffer);

    /* Clear the current buffer */
    for( i = 0; i < proc_failures_current_xcast->size; ++i) {
        item = opal_pointer_array_get_item(proc_failures_current_xcast, i);
        if( NULL != item ) {
            OBJ_RELEASE(item);
            opal_pointer_array_set_item(proc_failures_current_xcast, i, NULL);
        }
    }
    opal_pointer_array_remove_all(proc_failures_current_xcast);

    stable_xcast_timer_active = false;

    return;
}

static int rts_hnp_stable_process_fault_daemon(orte_job_t *jdata,
                                               orte_process_name_t *proc,
                                               orte_proc_state_t state)
{
    orte_proc_t *loc_proc = NULL, *child_proc = NULL;
    orte_std_cntr_t i_proc;

    OPAL_OUTPUT_VERBOSE((15, mca_errmgr_rts_hnp_component.output_handle,
                         "%s errmgr:hnp(stable): process_fault_daemon() "
                         "------- Daemon fault reported! proc %s (0x%x)",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                         ORTE_NAME_PRINT(proc),
                         state));

    /*
     * Find the local proc.
     * Do not update the state here, we need to do that last
     */
    if( NULL == (loc_proc = (orte_proc_t*)opal_pointer_array_get_item(jdata->procs, proc->vpid)) ) {
        /* JJH ? Error? */
        return ORTE_SUCCESS;
    }

    /*
     * Remove the route to this process
     */
    orte_routed.delete_route(proc);

    /*
     * If the aborted daemon had active processes on its node, then we should
     * make sure to signal that all the children are gone.
     */
    if( loc_proc->node->num_procs > 0 ) {
        OPAL_OUTPUT_VERBOSE((10, mca_errmgr_rts_hnp_component.output_handle,
                             "%s errmgr:base: stabalize_runtime() "
                             "------- Daemon lost with the following processes",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

        for(i_proc = 0; i_proc < opal_pointer_array_get_size(loc_proc->node->procs); ++i_proc) {
            child_proc = (orte_proc_t*)opal_pointer_array_get_item(loc_proc->node->procs, i_proc);
            if( NULL == child_proc ) {
                continue;
            }

            OPAL_OUTPUT_VERBOSE((10, mca_errmgr_rts_hnp_component.output_handle,
                                 "%s errmgr:base: stabalize_runtime() "
                                 "\t %s [0x%x]",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                                 ORTE_NAME_PRINT(&child_proc->name),
                                 child_proc->state));

            if( child_proc->last_errmgr_state < child_proc->state ) {
                child_proc->last_errmgr_state = child_proc->state;
                orte_errmgr.update_state(child_proc->name.jobid, ORTE_JOB_STATE_COMM_FAILED, 
                                         &(child_proc->name), ORTE_PROC_STATE_COMM_FAILED,
                                         0, 1); 
            }
        }
    } else {
        /* This daemon had no children, so just mask the failure */
        rts_hnp_ignore_current_update = true;
    }

    /*
     * Record the dead daemon
     */
    rts_hnp_record_dead_daemon(jdata, proc->vpid, state, 0);

    return ORTE_SUCCESS;
}

/* *************************** RTS END **************************** */
