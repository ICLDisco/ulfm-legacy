/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */
#include "ompi_config.h"
#include <stdio.h>


#include "ompi/mpi/f77/bindings.h"
#include "ompi/mpi/f77/constants.h"
#include "ompi/group/group.h"
#include "ompi/communicator/communicator.h"

#include "ompi/mpiext/ftmpi/f77/ftmpi_f77_support.h"

F77_STAMP_FN(OMPI_Comm_iagreement_f,
             ompi_comm_iagreement,
             OMPI_COMM_IAGREEMENT,
             (MPI_Fint *comm, ompi_fortran_logical_t *flag, MPI_Fint *request, MPI_Fint *ierr),
             (comm, flag, request, ierr))

#if OMPI_PROFILING_DEFINES && ! OPAL_HAVE_WEAK_SYMBOLS
#include "ompi/mpiext/ftmpi/f77/profile/defines.h"
#endif

#include "ompi/mpiext/ftmpi/mpiext_ftmpi_c.h"

static void OMPI_Comm_iagreement_f(MPI_Fint *comm, ompi_fortran_logical_t *flag, MPI_Fint *request, MPI_Fint *ierr)
{
    MPI_Comm c_comm = MPI_Comm_f2c(*comm);
    MPI_Request c_req;
    OMPI_LOGICAL_NAME_DECL(flag)

    *ierr = OMPI_INT_2_FINT(OMPI_Comm_iagreement(c_comm,
                                                 OMPI_LOGICAL_SINGLE_NAME_CONVERT(flag),
                                                 &c_req));

    if (MPI_SUCCESS == OMPI_FINT_2_INT(*ierr)) {
        *request = MPI_Request_c2f(c_req);
    }
}
