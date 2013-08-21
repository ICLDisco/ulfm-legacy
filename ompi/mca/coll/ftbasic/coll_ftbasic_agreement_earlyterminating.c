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
    ftbasic_eta_proc_agreement_t *ag;
    ompi_request_t **reqs;
    MPI_Status *statuses;
    ftbasic_eta_agreement_msg_t out;
    ftbasic_eta_agreement_msg_t *in;
    int  i, j, np, nbrecv, rc, ret, nbknow, nbcrashed, round_complete;

    np = ompi_comm_size(comm);
    i = ompi_comm_rank(comm);

    ag = (ftbasic_eta_proc_agreement_t*)calloc( np, sizeof(ftbasic_eta_proc_agreement_t) );
    /* This should go in the module query, and a module member should be used here */
    reqs = (ompi_request_t **)malloc( 2 * np * sizeof(ompi_request_t *) );
    for(j = 0; j < (2*np); reqs[j++] = MPI_REQUEST_NULL);
    statuses = (MPI_Status*)malloc( 2 * np * sizeof(MPI_Status) );
    in = (ftbasic_eta_agreement_msg_t*)calloc( np, sizeof(ftbasic_eta_agreement_msg_t) );

    out.est = *flag;
    out.know = 0;
    out.round = 1;
    nbknow = 0;
    nbcrashed = 0;

    ret = MPI_SUCCESS;

    while(out.round <= np + 1) {

        *flag = out.est;

        nbrecv = 0;

        /**
         * Post all the requests, first the receives and then the sends.
         */
        for(j = 0; j < np; j++) {
            assert(MPI_REQUEST_NULL == reqs[np+j]);
            if( i != j && (ag[j].status != STATUS_CRASHED) ) {
                ag[j].status = STATUS_WAITING;
                MCA_PML_CALL(irecv(&in[j], 3, MPI_INT, 
                                   j, FTBASIC_ETA_TAG_AGREEMENT, comm, 
                                   &reqs[np+j]));
            } else {
                reqs[np+j] = MPI_REQUEST_NULL;
            }
        }
        for(j = 0; j < np; j++) {
            assert(MPI_REQUEST_NULL == reqs[j]);
            if( i != j && (ag[j].status != STATUS_CRASHED) ) {
                MCA_PML_CALL(isend(&out, 3, MPI_INT, 
                                   j, FTBASIC_ETA_TAG_AGREEMENT, 
                                   MCA_PML_BASE_SEND_STANDARD, comm, 
                                   &reqs[j]));
            }else {
                reqs[j] = MPI_REQUEST_NULL;
            }
        }

        do {
            rc = ompi_request_wait_all(2*np, reqs, statuses);
            round_complete = 1;

            if( MPI_SUCCESS != rc ) {
                if( rc != MPI_ERR_IN_STATUS ) {
                    ret = rc;
                    goto clean_and_exit;
                }

                for(j = 0; j < np; j++) {
                    /* Ignore dead processes and myself */
                    if( (ag[j].status == STATUS_CRASHED) ||
                        (ag[j].status == STATUS_RECEIVED) ||
                        (i == j) )
                        continue;
                    
                    if( (MPI_SUCCESS == statuses[j].MPI_ERROR) &&
                        (MPI_SUCCESS == statuses[np+j].MPI_ERROR) ) {

                        assert(MPI_REQUEST_NULL == reqs[j]);
                        assert(MPI_REQUEST_NULL == reqs[j+np]);
                        ftbasic_eta_received_message(&out, &nbrecv, &nbknow, &in[j], &ag[j]);
                        ag[j].status = STATUS_RECEIVED;
                    } else {

                        if( (MPI_ERR_PROC_FAILED == statuses[j].MPI_ERROR) ||
                            (MPI_ERR_PROC_FAILED == statuses[np+j].MPI_ERROR) ) {

                            /* Failure detected */
                            ag[j].status = STATUS_CRASHED;
                            nbcrashed++;

                            /* Release the request, it can't be subsequently completed */
                            MPI_Request_free(&reqs[j]);
                            MPI_Request_free(&reqs[j+np]);
                        } else if( (MPI_ERR_PENDING == statuses[j].MPI_ERROR) ||
                                   (MPI_ERR_PENDING == statuses[np+j].MPI_ERROR)) {
                            /* Need to wait these messages to end current round */
                            {
                                int k;
                                for(k = 0; k < 2*np; k++) {
                                    if( MPI_ERR_PENDING != statuses[k].MPI_ERROR &&
                                        MPI_SUCCESS != statuses[k].MPI_ERROR &&
                                        MPI_ERR_PROC_FAILED != statuses[k].MPI_ERROR ) {
                                        rc = statuses[k].MPI_ERROR;
#if defined(OPAL_ENABLE_DEBUG)
                                        opal_output(ompi_ftmpi_output_handle,
                                                    "%s ftbasic:agreement) (ETA) point to point communication with rank %d failed with error code %d",
                                                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                                                    k, statuses[k].MPI_ERROR);
#endif
                                        goto clean_and_exit;
                                    }
                                }
                            }

                            round_complete = 0;
                        } else {
                            if( (MPI_SUCCESS != statuses[j].MPI_ERROR) &&
                                (MPI_ERR_PROC_FAILED != statuses[j].MPI_ERROR) &&
                                (MPI_ERR_PENDING != statuses[j].MPI_ERROR) ) {
                                ret = statuses[j].MPI_ERROR;
                            } else {
                                ret = statuses[j+np].MPI_ERROR;
                            }
                            goto clean_and_exit;
                        }
                    }
                }
            } else {
                /* The waitall returned success: all posted requests completed */
                for(j = 0; j < np; j++) {
                    assert(MPI_REQUEST_NULL == reqs[j]);
                    assert(MPI_REQUEST_NULL == reqs[j+np]);
                    /* Ignore dead processes and myself */
                    if( (ag[j].status == STATUS_CRASHED) || (i == j) )
                        continue;
                    assert( ag[j].status == STATUS_WAITING );
                    ftbasic_eta_received_message(&out, &nbrecv, &nbknow, &in[j], &ag[j]);
                    ag[j].status = STATUS_RECEIVED;
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
    free(ag);
    free(reqs);
    free(statuses);
    free(in);

    return ret;
}
