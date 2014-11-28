/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2010-2014 The Trustees of the University of Tennessee.
 *                         All rights reserved.
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

F77_STAMP_FN(MPIX_Comm_agree_f,
             mpix_comm_agree,
             MPIX_COMM_AGREE,
             (MPI_Fint *comm, ompi_fortran_logical_t *flag, MPI_Fint *ierr),
             (comm, flag, ierr))
F77_STAMP_FN(MPIX_Comm_agree_f,
             ompi_comm_agree,
             OMPI_COMM_AGREE,
             (MPI_Fint *comm, ompi_fortran_logical_t *flag, MPI_Fint *ierr),
             (comm, flag, ierr))

#if OMPI_PROFILING_DEFINES && ! OPAL_HAVE_WEAK_SYMBOLS
#include "ompi/mpiext/ftmpi/f77/profile/defines.h"
#endif

#include "ompi/mpiext/ftmpi/mpiext_ftmpi_c.h"

static void MPIX_Comm_agree_f(MPI_Fint *comm, ompi_fortran_logical_t *flag, MPI_Fint *ierr)
{
    MPI_Comm c_comm = MPI_Comm_f2c(*comm);
    OMPI_LOGICAL_NAME_DECL(flag)

    *ierr = OMPI_INT_2_FINT(MPIX_Comm_agree(c_comm,
                                            OMPI_LOGICAL_SINGLE_NAME_CONVERT(flag)));
}
