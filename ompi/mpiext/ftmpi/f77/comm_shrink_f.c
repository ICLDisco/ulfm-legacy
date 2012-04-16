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

F77_STAMP_FN(OMPI_Comm_shrink_f,
             ompi_comm_shrink,
             OMPI_COMM_SHRINK,
             (MPI_Fint *comm, MPI_Fint *newcomm, MPI_Fint *ierr),
             (comm, newcomm, ierr))

#if OMPI_PROFILING_DEFINES && ! OPAL_HAVE_WEAK_SYMBOLS
#include "ompi/mpiext/ftmpi/f77/profile/defines.h"
#endif

#include "ompi/mpiext/ftmpi/mpiext_ftmpi_c.h"

static void OMPI_Comm_shrink_f(MPI_Fint *comm, MPI_Fint *newcomm, MPI_Fint *ierr)
{
    MPI_Comm c_newcomm;
    MPI_Comm c_comm = MPI_Comm_f2c(*comm);

    *ierr = OMPI_INT_2_FINT(OMPI_Comm_shrink(c_comm,
                                             &c_newcomm));
    if (MPI_SUCCESS == OMPI_FINT_2_INT(*ierr)) {
        *newcomm = MPI_Comm_c2f(c_newcomm);
    }
}