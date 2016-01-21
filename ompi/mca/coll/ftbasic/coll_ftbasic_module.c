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

#if OPAL_ENABLE_FT_MPI
static int
mca_coll_ftbasic_agreement(struct ompi_communicator_t* comm,
                           struct ompi_group_t **group,
                           ompi_op_t *op,
                           ompi_datatype_t *dt,
                           int dt_count,
                           void *contrib,
                           struct mca_coll_base_module_2_0_0_t *module)
{
    return comm->c_coll.coll_allreduce(MPI_IN_PLACE, contrib, dt_count, dt, op,
                                       comm, module);
}

static int
mca_coll_ftbasic_iagreement(struct ompi_communicator_t* comm,
                            struct ompi_group_t *group,
                            ompi_op_t *op,
                            ompi_datatype_t *dt,
                            int dt_count,
                            void *contrib,
                            struct mca_coll_base_module_2_0_0_t *module,
                            ompi_request_t **request)
{
    *request = MPI_REQUEST_NULL;

    return OMPI_ERR_NOT_SUPPORTED;
}

#endif /* OPAL_ENABLE_FT_MPI */

/*
 * Initial query function that is invoked during MPI_INIT, allowing
 * this component to disqualify itself if it doesn't support the
 * required level of thread support.
 */
int
mca_coll_ftbasic_init_query(bool enable_progress_threads,
                          bool enable_mpi_threads)
{
    if( mca_coll_ftbasic_cur_agreement_method == COLL_FTBASIC_EARLY_RETURNING ) {
        return mca_coll_ftbasic_agreement_era_init();
    }

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

    ftbasic_module->is_intercomm = OMPI_COMM_IS_INTER(comm);

    /*
     * Allocate the data that hangs off the communicator
     * Intercommunicators not currently supported
     */
    if( ompi_ftmpi_enabled && !OMPI_COMM_IS_INTER(comm) ) {
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
    } else {
        ftbasic_module->mccb_num_reqs = 0;
        ftbasic_module->mccb_reqs = NULL;
        ftbasic_module->mccb_num_statuses = 0;
        ftbasic_module->mccb_statuses = NULL;
    }

    /*
     * Choose whether to use [intra|inter], and [linear|log]-based
     * algorithms.
     */
    ftbasic_module->super.coll_module_enable = mca_coll_ftbasic_module_enable;
    ftbasic_module->super.ft_event = mca_coll_ftbasic_ft_event;

    /* This component does not provide any base collectives,
     * just the FT collectives.
     */
    ftbasic_module->super.coll_allgather      = NULL;
    ftbasic_module->super.coll_allgatherv     = NULL;
    ftbasic_module->super.coll_allreduce      = NULL;
    ftbasic_module->super.coll_alltoall       = NULL;
    ftbasic_module->super.coll_alltoallv      = NULL;
    ftbasic_module->super.coll_alltoallw      = NULL;
    ftbasic_module->super.coll_barrier        = NULL;
    ftbasic_module->super.coll_bcast          = NULL;
    ftbasic_module->super.coll_exscan         = NULL;
    ftbasic_module->super.coll_gather         = NULL;
    ftbasic_module->super.coll_gatherv        = NULL;
    ftbasic_module->super.coll_reduce         = NULL;
    ftbasic_module->super.coll_reduce_scatter = NULL;
    ftbasic_module->super.coll_scan           = NULL;
    ftbasic_module->super.coll_scatter        = NULL;
    ftbasic_module->super.coll_scatterv       = NULL;

    /* agreement is a reduction with a bitwise OR */
    ftbasic_module->super.coll_agreement  = mca_coll_ftbasic_agreement;
    ftbasic_module->super.coll_iagreement = mca_coll_ftbasic_iagreement;

    /*
     * Agreement operation setup
     * Intercommunicators not currently supported
     */
    if( ompi_ftmpi_enabled ) {
        /* Init the agreement function */
        mca_coll_ftbasic_agreement_init(comm, ftbasic_module);

        /* Choose the correct operations */
        switch( mca_coll_ftbasic_cur_agreement_method ) {
        case COLL_FTBASIC_EARLY_TERMINATION:
            if( !OMPI_COMM_IS_INTER(comm) ) {
                ftbasic_module->super.coll_agreement  = mca_coll_ftbasic_agreement_eta_intra;
            }
            break;
        default: /* Manages the COLL_FTBASIC_EARLY_RETURNING as default case too */
            if( OMPI_COMM_IS_INTER(comm) ) {
                ftbasic_module->super.coll_agreement  = mca_coll_ftbasic_agreement_era_inter;
            } else {
                ftbasic_module->super.coll_agreement  = mca_coll_ftbasic_agreement_era_intra;
                ftbasic_module->super.coll_iagreement = mca_coll_ftbasic_iagreement_era_intra;
            }
            break;
        }
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
