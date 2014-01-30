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

#define STATUS_NO_INFO               0
#define STATUS_CRASHED           (1<<0)
#define STATUS_TOLD_ME_HE_KNOWS  (1<<1)
#define STATUS_KNOWS_I_KNOW      (1<<2)
typedef struct {
    int est;
    int status;
} ftbasic_eta_proc_agreement_t;

typedef struct {
    int round;
    int est;
    int knows;
} ftbasic_eta_agreement_msg_t;

#define FTBASIC_ETA_TAG_AGREEMENT MCA_COLL_BASE_TAG_AGREEMENT

static void ftbasic_eta_received_message(ftbasic_eta_agreement_msg_t  *out, 
                                         ftbasic_eta_agreement_msg_t  *in, 
                                         ftbasic_eta_proc_agreement_t *ag)
{
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ETA) Received Message (round = %d, est = %d, knows = %d) in Aggregate (est = %d, status = %d), out = (round = %d, est = %d, knows = %d)\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         in->round, in->est, in->knows,
                         ag->est, ag->status,
                         out->round, out->est, out->knows));

    /* Implements the logical and of answers */
    if( in->est == 0 ) {
        out->est = 0;
    }

    ag->status |= (in->knows * STATUS_TOLD_ME_HE_KNOWS);

    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ETA) Changed State with Aggregate (status = %d), out = (est = %d)\n",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), 
                         ag->status,
                         out->est));
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

    static int UTAG = MCA_COLL_BASE_TAG_MAX_POST_AGREEMENT;
    if( UTAG - 1 < MCA_COLL_BASE_TAG_MAX ) {
        UTAG = MCA_COLL_BASE_TAG_MAX_POST_AGREEMENT;
    } else {
        UTAG = UTAG - 1;
    }

    np = ompi_comm_size(comm);
    me = ompi_comm_rank(comm);
    ag = (ftbasic_eta_proc_agreement_t*)calloc( np, sizeof(ftbasic_eta_proc_agreement_t) );
    /* This should go in the module query, and a module member should be used here */
    reqs = (ompi_request_t **)malloc( 2 * np * sizeof(ompi_request_t *) );
    for(i = 0; i < (2*np); reqs[i++] = MPI_REQUEST_NULL);
    statuses = (MPI_Status*)malloc( 2 * np * sizeof(MPI_Status) );
    in = (ftbasic_eta_agreement_msg_t*)calloc( np, sizeof(ftbasic_eta_agreement_msg_t) );

    out.est = *flag;
    out.knows = 0;
    out.round = 1;

    while(out.round <= (np + 1)) {

        *flag = out.est;

        /**
         * Post all the requests, first the receives and then the sends.
         */
        for(i = 0; i < np; i++) {
            assert(MPI_REQUEST_NULL == reqs[np+i]);
            if( me != i && (!(ag[i].status & STATUS_CRASHED)) && ( !(ag[i].status & STATUS_TOLD_ME_HE_KNOWS) ) ) {
                /* Need to know more about this guy */
                MCA_PML_CALL(irecv(&in[i], 3, MPI_INT, 
                                   i, UTAG, comm, 
                                   &reqs[np+i]));
            } else {
                reqs[np+i] = MPI_REQUEST_NULL;
            }
        }
        for(i = 0; i < np; i++) {
            assert(MPI_REQUEST_NULL == reqs[i]);
            if( me != i && (!(ag[i].status & STATUS_CRASHED)) && (!(ag[i].status & STATUS_KNOWS_I_KNOW)) ) {
                /* Need to communicate with this guy */
                MCA_PML_CALL(isend(&out, 3, MPI_INT, 
                                   i, UTAG, 
                                   MCA_PML_BASE_SEND_STANDARD, comm, 
                                   &reqs[i]));
                ag[i].status |= out.knows * STATUS_KNOWS_I_KNOW;
            } else {
                reqs[i] = MPI_REQUEST_NULL;
            }
        }

        OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ETA) Starting Round %d\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), out.round));
        do {
            OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ETA) Entering waitall\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME)));

            rc = ompi_request_wait_all(2*np, reqs, statuses);
            round_complete = 1;

            nbrecv = 0;

            /* Short loop if the whole operation succeeded -- no failure during this round */
            if( MPI_SUCCESS == rc ) {
                /* The waitall returned success: all posted requests completed */
                for(i = 0; i < np; i++) {
                    assert(MPI_REQUEST_NULL == reqs[i]);
                    assert(MPI_REQUEST_NULL == reqs[i+np]);
                    /* Ignore every processes that is either dead or already
                       sent the expected message and myself */
                    if( (ag[i].status & STATUS_CRASHED) || (me == i) || (ag[i].status & STATUS_TOLD_ME_HE_KNOWS) )
                        continue;
                    ftbasic_eta_received_message(&out, &in[i], &ag[i]);
                    nbrecv++;
                }
                break;  /* Done with the wait_all loop */
            }
            if( rc != MPI_ERR_IN_STATUS ) {
                ret = rc;
                goto clean_and_exit;
            }

            /* Long loop if somebody failed */
            for(i = 0; i < np; i++) {
                /* Ignore all processes from which I don't expect information */
                if( (ag[i].status & STATUS_CRASHED) || (me == i) || (ag[i].status & STATUS_TOLD_ME_HE_KNOWS) ) {
                    assert(MPI_REQUEST_NULL == reqs[i]);
                    assert(MPI_REQUEST_NULL == reqs[i+np]);
                    continue;
                }
                if( (MPI_SUCCESS == statuses[i].MPI_ERROR) &&
                    (MPI_SUCCESS == statuses[np+i].MPI_ERROR) ) {

                    assert(MPI_REQUEST_NULL == reqs[i]);
                    assert(MPI_REQUEST_NULL == reqs[i+np]);
                    ftbasic_eta_received_message(&out, &in[i], &ag[i]);
                    nbrecv++;
                } else {

                    if( (MPI_ERR_PROC_FAILED == statuses[i].MPI_ERROR) ||
                        (MPI_ERR_PROC_FAILED == statuses[np+i].MPI_ERROR) ) {

                        /* Failure detected */
                        ag[i].status |= STATUS_CRASHED;
                        OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                                             "%s ftbasic:agreement (ETA) communication with rank %d failed. Mark it as dead!",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i));

                        /* Release the requests, they can't be subsequently completed */
                        if(MPI_REQUEST_NULL != reqs[i])
                            ompi_request_free(&reqs[i]);
                        if(MPI_REQUEST_NULL != reqs[i+np])
                            ompi_request_free(&reqs[i+np]);
                    } else if( (MPI_ERR_PENDING == statuses[i].MPI_ERROR) ||
                               (MPI_ERR_PENDING == statuses[np+i].MPI_ERROR)) {

                        /* The pending request(s) will be waited on at the next iteration. */
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

        nbcrashed = 0;
        for(i = 0; i < np; i++)
            if( ag[i].status & STATUS_CRASHED )
                nbcrashed++;
        nbknow = 0;
        for(i = 0; i < np; i++)
            if( (!(ag[i].status & STATUS_CRASHED)) && (ag[i].status & STATUS_TOLD_ME_HE_KNOWS) )
                nbknow++;

        OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ETA) end of Round %d: nbcrashed = %d, nbknow = %d, nbrecv = %d. out.knows = %d\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             out.round, nbcrashed, nbknow, nbrecv, out.knows));
        
        if( (nbknow + nbcrashed >= np - 1) && (out.knows == 1) ) {
            break;
        }

        out.knows = (nbknow > 0) || (nbrecv >= np - out.round + 1);
        out.round++;
    }

 clean_and_exit:
    OPAL_OUTPUT_VERBOSE((50, ompi_ftmpi_output_handle,
                "%s ftbasis:agreement (ETA) decided in %d rounds ", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), out.round));
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
