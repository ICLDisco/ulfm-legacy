/*
 * Copyright (c) 2013      The University of Tennessee and The University
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

#include MCA_timer_IMPLEMENTATION_HEADER
#include "coll_ftbasic.h"


/**
 * This Agreement implements the protocol proposed in
 *   Early Consensus in Message-passing Systems Enriched with a Perfect Failure Detector
 *     and its Application in the Theta Model.
 *   by Francois Bonnet, Michel Raynal
 *   in 2010 European Dependable Computing Conference
 */

#define STATUS_WAITING  0
#define STATUS_CRASHED  1
#define STATUS_RECEIVED 2
typedef struct {
    int est;
    int status;
    int know;
} ftbasic_eta_proc_agreement_t;

typedef struct {
    int round;
    int est;
    int know;
} ftbasic_eta_agreement_msg_t;

#define FTBASIC_ETA_TAG_AGREEMENT MCA_COLL_BASE_TAG_AGREEMENT

static void ftbasic_eta_received_message(ftbasic_eta_agreement_msg_t  *out, 
                                         int *nbrecv, int *nbknow, 
                                         ftbasic_eta_agreement_msg_t  *in, 
                                         ftbasic_eta_proc_agreement_t *ag)
{
    (*nbrecv)++;

    if( in->est == 0 ) {
        out->est = 0;
    }

    if( (1 == in->know) && (0 == ag->know) ) {
        ag->know = 1;
        (*nbknow)++;
    }
}

/*
 *	agreement_eta_intra
 *
 *	Function:	- MPI_Comm_agree()
 *	Accepts:	- same as MPI_Comm_agree()
 *	Returns:	- MPI_SUCCESS or an MPI error code
 */

int
mca_coll_ftbasic_agreement_eta_intra(ompi_communicator_t* comm,
                                     ompi_group_t **group,
                                     int *flag,
                                     mca_coll_base_module_t *module)
{
    ftbasic_eta_agreement_msg_t out, *in;
    ftbasic_eta_proc_agreement_t *ag;
    ompi_request_t **reqs;
    MPI_Status *statuses;
    int me, i, np, nbrecv, rc, ret = MPI_SUCCESS, nbknow = 0, nbcrashed = 0, round_complete;

    np = ompi_comm_size(comm);
    me = ompi_comm_rank(comm);

    ag = (ftbasic_eta_proc_agreement_t*)calloc( np, sizeof(ftbasic_eta_proc_agreement_t) );
    /* This should go in the module query, and a module member should be used here */
    reqs = (ompi_request_t **)malloc( 2 * np * sizeof(ompi_request_t *) );
    for(i = 0; i < (2*np); reqs[i++] = MPI_REQUEST_NULL);
    statuses = (MPI_Status*)malloc( 2 * np * sizeof(MPI_Status) );
    in = (ftbasic_eta_agreement_msg_t*)calloc( np, sizeof(ftbasic_eta_agreement_msg_t) );

    out.est = *flag;
    out.know = 0;
    out.round = 1;

    while(out.round <= (np + 1)) {

        *flag = out.est;
        nbrecv = 0;

        /**
         * Post all the requests, first the receives and then the sends.
         */
        for(i = 0; i < np; i++) {
            assert(MPI_REQUEST_NULL == reqs[np+i]);
            if( me != i && (ag[i].status != STATUS_CRASHED) ) {
                ag[i].status = STATUS_WAITING;
                MCA_PML_CALL(irecv(&in[i], 3, MPI_INT, 
                                   i, FTBASIC_ETA_TAG_AGREEMENT, comm, 
                                   &reqs[np+i]));
            } else {
                reqs[np+i] = MPI_REQUEST_NULL;
            }
        }
        for(i = 0; i < np; i++) {
            assert(MPI_REQUEST_NULL == reqs[i]);
            if( me != i && (ag[i].status != STATUS_CRASHED) ) {
                MCA_PML_CALL(isend(&out, 3, MPI_INT, 
                                   i, FTBASIC_ETA_TAG_AGREEMENT, 
                                   MCA_PML_BASE_SEND_STANDARD, comm, 
                                   &reqs[i]));
            } else {
                reqs[i] = MPI_REQUEST_NULL;
            }
        }

        do {
            rc = ompi_request_wait_all(2*np, reqs, statuses);
            round_complete = 1;

            if( MPI_SUCCESS == rc ) {
                /* The waitall returned success: all posted requests completed */
                for(i = 0; i < np; i++) {
                    assert(MPI_REQUEST_NULL == reqs[i]);
                    assert(MPI_REQUEST_NULL == reqs[i+np]);
                    /* Ignore every processes that is either dead or already
                       sent the expected message and myself */
                    if( (ag[i].status == STATUS_CRASHED) || (me == i) )
                        continue;
                    ftbasic_eta_received_message(&out, &nbrecv, &nbknow, &in[i], &ag[i]);
                    ag[i].status = STATUS_RECEIVED;
                }
                break;  /* Done with this loop */
            }
            if( rc != MPI_ERR_IN_STATUS ) {
                ret = rc;
                goto clean_and_exit;
            }

            for(i = 0; i < np; i++) {
                /* Ignore all processes not on a waiting state and myself */
                if( (ag[i].status != STATUS_WAITING) || (me == i) ) {
                    assert(MPI_REQUEST_NULL == reqs[i]);
                    assert(MPI_REQUEST_NULL == reqs[i+np]);
                    continue;
                }
                if( (MPI_SUCCESS == statuses[i].MPI_ERROR) &&
                    (MPI_SUCCESS == statuses[np+i].MPI_ERROR) ) {

                    assert(MPI_REQUEST_NULL == reqs[i]);
                    assert(MPI_REQUEST_NULL == reqs[i+np]);
                    ftbasic_eta_received_message(&out, &nbrecv, &nbknow, &in[i], &ag[i]);
                    ag[i].status = STATUS_RECEIVED;
                } else {

                    if( (MPI_ERR_PROC_FAILED == statuses[i].MPI_ERROR) ||
                        (MPI_ERR_PROC_FAILED == statuses[np+i].MPI_ERROR) ) {

                        /* Failure detected */
                        ag[i].status = STATUS_CRASHED;
                        nbcrashed++;
                        OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                                             "%s ftbasic:agreement (ETA) communication with rank %d failed. Mark it as dead!",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i));

                        /* Release the requests, they can't be subsequently completed */
                        if(MPI_REQUEST_NULL != reqs[i])
                            MPI_Request_free(&reqs[i]);
                        if(MPI_REQUEST_NULL != reqs[i])
                            MPI_Request_free(&reqs[i+np]);
                    } else if( (MPI_ERR_PENDING == statuses[i].MPI_ERROR) ||
                               (MPI_ERR_PENDING == statuses[np+i].MPI_ERROR)) {

                        /* The pending request will be waited on at the next iteration. */
                        round_complete = 0;
                    } else {
                        if( (MPI_SUCCESS != statuses[i].MPI_ERROR) &&
                            (MPI_ERR_PROC_FAILED != statuses[i].MPI_ERROR) &&
                            (MPI_ERR_PENDING != statuses[i].MPI_ERROR) ) {
                            ret = statuses[i].MPI_ERROR;
                        } else {
                            ret = statuses[i+np].MPI_ERROR;
                        }
                        goto clean_and_exit;
                    }
                }
            }
        } while( 1 != round_complete );

        if( (nbknow + nbcrashed >= np - 1) && (out.know == 1) ) {
            break;
        }

        out.know = (nbknow > 0) || (nbrecv >= np - out.round + 1);
        out.round++;
    }

 clean_and_exit:
    for (i = 0; i < 2*np; ++i)
        if(MPI_REQUEST_NULL != reqs[i])
            ompi_request_free(&reqs[i]);
    free(reqs);
    free(statuses);
    free(in);
    /* Let's build the group of failed processes */
    if( NULL != group ) {
        int pos, *failed = (int*)ag;
        
        for( pos = i = 0; i < np; i++ ) {
            if( STATUS_CRASHED == ag[i].status ) {
                failed[pos++] = i;
            }
        }
        ompi_group_incl(comm->c_remote_group, pos, failed, group);
        free(ag);
    }

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ETA) return with flag %d and dead group with %d processes",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), *flag,
                         (NULL == group) ? 0 : (*group)->grp_proc_count));
    return ret;
}
