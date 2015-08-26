/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2015      The University of Tennessee and The University
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
#include <stdio.h>

#include "ompi/mpi/c/bindings.h"
#include "ompi/runtime/params.h"
#include "ompi/communicator/communicator.h"
#include "ompi/proc/proc.h"

#if OPAL_HAVE_WEAK_SYMBOLS && OMPI_PROFILING_DEFINES
#pragma weak MPIX_Comm_agree = PMPIX_Comm_agree
#endif

#if OMPI_PROFILING_DEFINES
#include "ompi/mpiext/ftmpi/c/profile/defines.h"
#endif

#include "ompi/mpiext/ftmpi/mpiext_ftmpi_c.h"

static const char FUNC_NAME[] = "MPIX_Comm_agree";


int MPIX_Comm_agree(MPI_Comm comm, int *flag)
{
    int rc = MPI_SUCCESS;
    ompi_group_t* acked; 

    /* Argument checking */
    if (MPI_PARAM_CHECK) {
        OMPI_ERR_INIT_FINALIZE(FUNC_NAME);
        if (ompi_comm_invalid(comm)) {
            return OMPI_ERRHANDLER_INVOKE(MPI_COMM_WORLD, MPI_ERR_COMM, FUNC_NAME);
        }
        OMPI_ERRHANDLER_CHECK(rc, comm, rc, FUNC_NAME);
    }

    ompi_comm_failure_get_acked_internal( comm, &acked );
    
    rc = comm->c_coll.coll_agreement( (ompi_communicator_t*)comm,
                                      &acked, /* Acked failures are ignored */
                                      &ompi_mpi_op_band.op,
                                      &ompi_mpi_int.dt,
                                      1,
                                      flag,
                                      comm->c_coll.coll_agreement_module);
    OBJ_RELEASE( acked );
    OMPI_ERRHANDLER_RETURN(rc, comm, rc, FUNC_NAME);
}

int OMPI_Comm_agree(MPI_Comm comm, int *flag)
{
      return MPIX_Comm_agree(comm, flag);
}

