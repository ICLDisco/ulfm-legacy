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

/** Those are the possible status of the process following the algorithm */
#define STATUS_NO_INFO               0
#define STATUS_CRASHED           (1<<0)
#define STATUS_TOLD_ME_HE_KNOWS  (1<<1)
#define STATUS_KNOWS_I_KNOW      (1<<2)
/** Those are used solely to track requests completion */
#define STATUS_SEND_COMPLETE     (1<<3)
#define STATUS_RECV_COMPLETE     (1<<4)

typedef struct {
    int est;
    int knows;
} ftbasic_eta_agreement_msg_t;

#define FTBASIC_ETA_TAG_AGREEMENT MCA_COLL_BASE_TAG_AGREEMENT

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
    int *proc_status; /**< char would be enough, but we use the same area to build the group of dead processes at the end */
    ompi_request_t **reqs;
    MPI_Status *statuses;
    int me, i, ri, nr, np, nbrecv, rc, ret = MPI_SUCCESS, nbknow = 0, nbcrashed = 0, round;

    np = ompi_comm_size(comm);
    me = ompi_comm_rank(comm);
    proc_status = (int *)calloc( np, sizeof(int) );
    /* This should go in the module query, and a module member should be used here */
    reqs = (ompi_request_t **)calloc( 2 * np, sizeof(ompi_request_t *) ); /** < Need to calloc or set to MPI_REQUEST_NULL to ensure cleanup in all cases. */
    statuses = (MPI_Status*)malloc( 2 * np * sizeof(MPI_Status) );
    in = (ftbasic_eta_agreement_msg_t*)calloc( np, sizeof(ftbasic_eta_agreement_msg_t) );

    out.est = *flag;
    out.knows = 0;
    round = 1;

#define NEED_TO_RECV(_i) (me != _i && (!(proc_status[_i] & STATUS_CRASHED)) && (!(proc_status[_i] & STATUS_TOLD_ME_HE_KNOWS)))
#define NEED_TO_SEND(_i) (me != _i && (!(proc_status[_i] & STATUS_CRASHED)) && (!(proc_status[_i] & STATUS_KNOWS_I_KNOW)))

    while(round <= (np + 1)) {
        OPAL_OUTPUT_VERBOSE((50, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ETA) Starting Round %d\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), round));

        *flag = out.est;

        /**
         * Post all the requests, first the receives and then the sends.
         */
        nr = 0;
        for(i = 0; i < np; i++) {
            if( NEED_TO_RECV(i) ) {
                /* Need to know more about this guy */
                MCA_PML_CALL(irecv(&in[i], 2, MPI_INT, 
                                   i, FTBASIC_ETA_TAG_AGREEMENT, comm, 
                                   &reqs[nr++]));
                proc_status[i] &= ~STATUS_RECV_COMPLETE;
                OPAL_OUTPUT_VERBOSE((100, ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ETA) Request for recv of rank %d is at %d(%p)\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i, nr-1, (void*)reqs[nr-1]));
            } else {
                proc_status[i] |= STATUS_RECV_COMPLETE;
            }
            if( NEED_TO_SEND(i) ) {
                /* Need to communicate with this guy */
                MCA_PML_CALL(isend(&out, 2, MPI_INT, 
                                   i, FTBASIC_ETA_TAG_AGREEMENT, 
                                   MCA_PML_BASE_SEND_STANDARD, comm, 
                                   &reqs[nr++]));
                proc_status[i] &= ~STATUS_SEND_COMPLETE;
                OPAL_OUTPUT_VERBOSE((100, ompi_ftmpi_output_handle,
                                     "%s ftbasic:agreement (ETA) Request for send of rank %d is at %d(%p)\n",
                                     ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i, nr-1, (void*)reqs[nr-1]));
            } else {
                proc_status[i] |= STATUS_SEND_COMPLETE;
            }
        }

        do {
            OPAL_OUTPUT_VERBOSE((100, ompi_ftmpi_output_handle,
                                 "%s ftbasic:agreement (ETA) Entering waitall(%d)\n",
                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), nr));

            rc = ompi_request_wait_all(nr, reqs, statuses);
            
            /**< If we need to re-wait on some requests, we're going to pack them at index nr */
            nr = 0;

            nbrecv = 0;

            if( rc != MPI_ERR_IN_STATUS && rc != MPI_SUCCESS ) {
                ret = rc;
                goto clean_and_exit;
            }

            /* Long loop if somebody failed */
            ri = 0;
            for(i = 0; i < np; i++) {

                if( !(proc_status[i] & STATUS_RECV_COMPLETE) ) {
                    if( (rc == MPI_SUCCESS) || (MPI_SUCCESS == statuses[ri].MPI_ERROR) ) {
                        assert(MPI_REQUEST_NULL == reqs[ri]);

                        /* Implements the logical and of answers */
                        if( in[i].est == 0 ) {
                            out.est = 0;
                        }
                        proc_status[i] |= ( (in[i].knows * STATUS_TOLD_ME_HE_KNOWS) | STATUS_RECV_COMPLETE);
                        nbrecv++;

                        OPAL_OUTPUT_VERBOSE((100, ompi_ftmpi_output_handle,
                                             "%s ftbasic:agreement (ETA) Request %d(%p) for recv of rank %d is completed.\n",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ri, (void*)reqs[ri], i));
                    } else {
                        if( (MPI_ERR_PROC_FAILED == statuses[ri].MPI_ERROR) ) {
                            /* Failure detected */
                            proc_status[i] |= (STATUS_CRASHED | STATUS_RECV_COMPLETE);
                            OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                                                 "%s ftbasic:agreement (ETA) recv with rank %d failed on request at index %d(%p). Mark it as dead!",
                                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i, ri, (void*)reqs[ri]));

                            /* Release the request, it can't be subsequently completed */
                            if(MPI_REQUEST_NULL != reqs[ri])
                                ompi_request_free(&reqs[ri]);
                        } else if( (MPI_ERR_PENDING == statuses[ri].MPI_ERROR) ) {
                            /* The pending request(s) will be waited on at the next iteration. */
                            assert( ri >= nr );
                            assert( MPI_REQUEST_NULL != reqs[ri] );
                            assert( ri == nr || reqs[nr] == MPI_REQUEST_NULL );
                            OPAL_OUTPUT_VERBOSE((100, ompi_ftmpi_output_handle,
                                                 "%s ftbasic:agreement (ETA) Request %d(%p) for recv of rank %d remains pending. Renaming it as Request %d\n",
                                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ri, (void*)reqs[ri], i, nr));
                            reqs[nr] = reqs[ri];
                            if( ri != nr ) 
                                reqs[ri] = MPI_REQUEST_NULL;
                            nr++;
                        } else {
                            ret = statuses[ri].MPI_ERROR;
                            goto clean_and_exit;
                        }
                    }
                    ri++;
                }

                if( !(proc_status[i] & STATUS_SEND_COMPLETE) ) {
                    if( (rc == MPI_SUCCESS) || (MPI_SUCCESS == statuses[ri].MPI_ERROR) ) {
                        assert(MPI_REQUEST_NULL == reqs[ri]);
                        proc_status[i] |= ((out.knows * STATUS_KNOWS_I_KNOW) | STATUS_SEND_COMPLETE);

                        OPAL_OUTPUT_VERBOSE((100, ompi_ftmpi_output_handle,
                                             "%s ftbasic:agreement (ETA) Request %d(%p) for send of rank %d is completed.\n",
                                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ri, (void*)reqs[ri], i));
                    } else {
                        if( (MPI_ERR_PROC_FAILED == statuses[ri].MPI_ERROR) ) {
                            /* Failure detected */
                            proc_status[i] |= (STATUS_CRASHED | STATUS_SEND_COMPLETE);

                            OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                                                 "%s ftbasic:agreement (ETA) send with rank %d failed on Request %d(%p). Mark it as dead!",
                                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), i, ri, (void*)reqs[ri]));

                            /* Release the request, it can't be subsequently completed */
                            if(MPI_REQUEST_NULL != reqs[ri])
                                ompi_request_free(&reqs[ri]);
                        } else if( (MPI_ERR_PENDING == statuses[ri].MPI_ERROR) ) {
                            /* The pending request(s) will be waited on at the next iteration. */
                            assert( ri >= nr );
                            assert( MPI_REQUEST_NULL != reqs[ri] );
                            assert( ri == nr || reqs[nr] == MPI_REQUEST_NULL );
                            OPAL_OUTPUT_VERBOSE((100, ompi_ftmpi_output_handle,
                                                 "%s ftbasic:agreement (ETA) Request %d(%p) for send of rank %d remains pending. Renaming it as Request %d\n",
                                                 ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ri, (void*)reqs[ri], i, nr));
                            reqs[nr] = reqs[ri];
                            if( ri != nr ) 
                                reqs[ri] = MPI_REQUEST_NULL;
                            nr++;
                        } else {
                            ret = statuses[ri].MPI_ERROR;
                            goto clean_and_exit;
                        }
                    }
                    ri++;
                }

            }

        } while( 0 != nr );

#undef NEED_TO_SEND
#undef NEED_TO_RECV

        nbcrashed = 0;
        for(i = 0; i < np; i++)
            if( proc_status[i] & STATUS_CRASHED )
                nbcrashed++;
        nbknow = 0;
        for(i = 0; i < np; i++)
            if( (!(proc_status[i] & STATUS_CRASHED)) && (proc_status[i] & STATUS_TOLD_ME_HE_KNOWS) )
                nbknow++;

        OPAL_OUTPUT_VERBOSE((50, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (ETA) end of Round %d: nbcrashed = %d, nbknow = %d, nbrecv = %d. out.knows = %d\n",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME),
                             round, nbcrashed, nbknow, nbrecv, out.knows));
        
        if( (nbknow + nbcrashed >= np - 1) && (out.knows == 1) ) {
            break;
        }

        out.knows = (nbknow > 0) || (nbrecv >= np - round + 1);
        round++;
    }

 clean_and_exit:
    OPAL_OUTPUT_VERBOSE((10, ompi_ftmpi_output_handle,
                "%s ftbasis:agreement (ETA) decided in %d rounds ", ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), round));
    for (ri = 0; ri < 2*np; ++ri)
        if( NULL != reqs[ri] && MPI_REQUEST_NULL != reqs[ri])
            ompi_request_free( &reqs[ri] );
    free(reqs);
    free(statuses);
    free(in);
    /* Let's build the group of failed processes */
    if( NULL != group ) {
        int pos;
        /* We overwrite proc_status because it is not used anymore */
        int *failed = proc_status;
        
        for( pos = i = 0; i < np; i++ ) {
            if( STATUS_CRASHED & proc_status[i] ) {
                failed[pos++] = i;
            }
        }
        ompi_group_incl(comm->c_remote_group, pos, failed, group);
        free(proc_status);
    }
    free(ag);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (ETA) return with flag %d and dead group with %d processes",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), *flag,
                         (NULL == group) ? 0 : (*group)->grp_proc_count));
    return ret;
}

