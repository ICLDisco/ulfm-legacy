/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2006 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "ompi_config.h"
#include "coll_ftbasic.h"
#include "coll_ftbasic_agreement.h"

#include <stdio.h>
#include "orte/util/name_fns.h"
#include "orte/mca/errmgr/errmgr.h"

#include "mpi.h"
#include "opal/mca/base/mca_base_param.h"
#include "opal/util/bit_ops.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/base.h"
#include "coll_ftbasic.h"

/*
 * Initial query function that is invoked during MPI_INIT, allowing
 * this component to disqualify itself if it doesn't support the
 * required level of thread support.
 */
int
mca_coll_ftbasic_init_query(bool enable_progress_threads,
                          bool enable_mpi_threads)
{
    /* Nothing to do */

    return OMPI_SUCCESS;
}


/*
 * Invoked when there's a new communicator that has been created.
 * Look at the communicator and decide which set of functions and
 * priority we want to return.
 */
mca_coll_base_module_t *
mca_coll_ftbasic_comm_query(struct ompi_communicator_t *comm, 
                          int *priority)
{
    int size;
    mca_coll_ftbasic_module_t *ftbasic_module;

    ftbasic_module = OBJ_NEW(mca_coll_ftbasic_module_t);
    if (NULL == ftbasic_module) return NULL;

    *priority = mca_coll_ftbasic_priority;

    /* Allocate the data that hangs off the communicator */

    if (OMPI_COMM_IS_INTER(comm)) {
        size = ompi_comm_remote_size(comm);
    } else {
        size = ompi_comm_size(comm);
    }
    ftbasic_module->mccb_num_reqs = size * 2;
    ftbasic_module->mccb_reqs = (ompi_request_t**) 
        malloc(sizeof(ompi_request_t *) * ftbasic_module->mccb_num_reqs);

    ftbasic_module->mccb_num_statuses = size * 2; /* x2 for alltoall */
    ftbasic_module->mccb_statuses = (ompi_status_public_t*)
        malloc(sizeof(ompi_status_public_t) * ftbasic_module->mccb_num_statuses);

    /* Choose whether to use [intra|inter], and [linear|log]-based
     * algorithms. */
    ftbasic_module->super.coll_module_enable = mca_coll_ftbasic_module_enable;
    ftbasic_module->super.ft_event = mca_coll_ftbasic_ft_event;

    if (OMPI_COMM_IS_INTER(comm)) {
        ftbasic_module->super.coll_allgather  = mca_coll_ftbasic_allgather_inter;
        ftbasic_module->super.coll_allgatherv = mca_coll_ftbasic_allgatherv_inter;
        ftbasic_module->super.coll_allreduce  = mca_coll_ftbasic_allreduce_inter;
        ftbasic_module->super.coll_alltoall   = mca_coll_ftbasic_alltoall_inter;
        ftbasic_module->super.coll_alltoallv  = mca_coll_ftbasic_alltoallv_inter;
        ftbasic_module->super.coll_alltoallw  = mca_coll_ftbasic_alltoallw_inter;
        ftbasic_module->super.coll_barrier    = mca_coll_ftbasic_barrier_inter_lin;
        ftbasic_module->super.coll_bcast      = mca_coll_ftbasic_bcast_lin_inter;
        ftbasic_module->super.coll_exscan     = NULL;
        ftbasic_module->super.coll_gather     = mca_coll_ftbasic_gather_inter;
        ftbasic_module->super.coll_gatherv    = mca_coll_ftbasic_gatherv_inter;
        ftbasic_module->super.coll_reduce     = mca_coll_ftbasic_reduce_lin_inter;
        ftbasic_module->super.coll_reduce_scatter = mca_coll_ftbasic_reduce_scatter_inter;
        ftbasic_module->super.coll_scan       = NULL;
        ftbasic_module->super.coll_scatter    = mca_coll_ftbasic_scatter_inter;
        ftbasic_module->super.coll_scatterv   = mca_coll_ftbasic_scatterv_inter;
    } else if (ompi_comm_size(comm) <= mca_coll_ftbasic_crossover) {
        ftbasic_module->super.coll_allgather  = mca_coll_ftbasic_allgather_intra;
        ftbasic_module->super.coll_allgatherv = mca_coll_ftbasic_allgatherv_intra;
        ftbasic_module->super.coll_allreduce  = mca_coll_ftbasic_allreduce_intra;
        ftbasic_module->super.coll_alltoall   = mca_coll_ftbasic_alltoall_intra;
        ftbasic_module->super.coll_alltoallv  = mca_coll_ftbasic_alltoallv_intra;
        ftbasic_module->super.coll_alltoallw  = mca_coll_ftbasic_alltoallw_intra;
        ftbasic_module->super.coll_barrier    = mca_coll_ftbasic_barrier_intra_lin;
        ftbasic_module->super.coll_bcast      = mca_coll_ftbasic_bcast_lin_intra;
        ftbasic_module->super.coll_exscan     = mca_coll_ftbasic_exscan_intra;
        ftbasic_module->super.coll_gather     = mca_coll_ftbasic_gather_intra;
        ftbasic_module->super.coll_gatherv    = mca_coll_ftbasic_gatherv_intra;
        ftbasic_module->super.coll_reduce     = mca_coll_ftbasic_reduce_lin_intra;
        ftbasic_module->super.coll_reduce_scatter = mca_coll_ftbasic_reduce_scatter_intra;
        ftbasic_module->super.coll_scan       = mca_coll_ftbasic_scan_intra;
        ftbasic_module->super.coll_scatter    = mca_coll_ftbasic_scatter_intra;
        ftbasic_module->super.coll_scatterv   = mca_coll_ftbasic_scatterv_intra;
    } else {
        ftbasic_module->super.coll_allgather  = mca_coll_ftbasic_allgather_intra;
        ftbasic_module->super.coll_allgatherv = mca_coll_ftbasic_allgatherv_intra;
        ftbasic_module->super.coll_allreduce  = mca_coll_ftbasic_allreduce_intra;
        ftbasic_module->super.coll_alltoall   = mca_coll_ftbasic_alltoall_intra;
        ftbasic_module->super.coll_alltoallv  = mca_coll_ftbasic_alltoallv_intra;
        ftbasic_module->super.coll_alltoallw  = mca_coll_ftbasic_alltoallw_intra;
        ftbasic_module->super.coll_barrier    = mca_coll_ftbasic_barrier_intra_log;
        ftbasic_module->super.coll_bcast      = mca_coll_ftbasic_bcast_log_intra;
        ftbasic_module->super.coll_exscan     = mca_coll_ftbasic_exscan_intra;
        ftbasic_module->super.coll_gather     = mca_coll_ftbasic_gather_intra;
        ftbasic_module->super.coll_gatherv    = mca_coll_ftbasic_gatherv_intra;
        ftbasic_module->super.coll_reduce     = mca_coll_ftbasic_reduce_log_intra;
        ftbasic_module->super.coll_reduce_scatter = mca_coll_ftbasic_reduce_scatter_intra;
        ftbasic_module->super.coll_scan       = mca_coll_ftbasic_scan_intra;
        ftbasic_module->super.coll_scatter    = mca_coll_ftbasic_scatter_intra;
        ftbasic_module->super.coll_scatterv   = mca_coll_ftbasic_scatterv_intra;
    }

    /*
     * Agreement operation setup
     */
    /* Init the agreement function */
    mca_coll_ftbasic_agreement_init(ftbasic_module);

    /* Choose the correct operations */
    switch( mca_coll_ftbasic_cur_agreement_method ) {
    case COLL_FTBASIC_ALLREDUCE:
        ftbasic_module->super.coll_agreement  = mca_coll_ftbasic_agreement_allreduce;
        ftbasic_module->super.coll_iagreement = mca_coll_ftbasic_iagreement_allreduce;
        break;
    case COLL_FTBASIC_TWO_PHASE:
        ftbasic_module->super.coll_agreement  = mca_coll_ftbasic_agreement_two_phase;
        ftbasic_module->super.coll_iagreement = mca_coll_ftbasic_iagreement_two_phase;
        break;
    case COLL_FTBASIC_LOG_TWO_PHASE:
        ftbasic_module->super.coll_agreement  = mca_coll_ftbasic_agreement_log_two_phase;
        ftbasic_module->super.coll_iagreement = mca_coll_ftbasic_iagreement_log_two_phase;
    default:
        break;
    }

    return &(ftbasic_module->super);
}


/*
 * Init module on the communicator
 */
int
mca_coll_ftbasic_module_enable(mca_coll_base_module_t *module,
                             struct ompi_communicator_t *comm)
{
    /* All done */
    return OMPI_SUCCESS;
}


int mca_coll_ftbasic_ft_event(int state)
{

    /* Nothing to do for checkpoint */

    return OMPI_SUCCESS;
}
