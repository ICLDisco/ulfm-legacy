/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 *
 */
#include <stdbool.h>

/********************************
 * Communicators
 ********************************/
OMPI_DECLSPEC int OMPI_Comm_revoke(MPI_Comm comm);

OMPI_DECLSPEC int OMPI_Comm_shrink(MPI_Comm comm, MPI_Comm *newcomm);

OMPI_DECLSPEC int OMPI_Comm_failure_ack(MPI_Comm comm);
OMPI_DECLSPEC int OMPI_Comm_failure_get_acked(MPI_Comm comm, MPI_Group *failedgrp);

OMPI_DECLSPEC int OMPI_Comm_agree(MPI_Comm comm, int *flag);
OMPI_DECLSPEC int OMPI_Comm_iagree(MPI_Comm comm, int *flag, MPI_Request *request);

#if 0
/********************************
 * Windows
 ********************************/
OMPI_DECLSPEC int OMPI_Win_revoke(MPI_Win win);
OMPI_DECLSPEC int OMPI_Win_get_failed(MPI_Win win, MPI_Group *failedgrp);
#endif

#if 0
/********************************
 * I/O
 ********************************/
OMPI_DECLSPEC int OMPI_File_revoke(MPI_File fh);
#endif

#if 0
/********************************
 * Error Hanlders
 ********************************/
OMPI_DECLSPEC int OMPI_Errhandler_compare(MPI_Errhandler errhandler1,
                                          MPI_Errhandler errhandler2,
                                          int *result);
#endif

#if 0
/********************************
 * Kill
 ********************************/
OMPI_DECLSPEC int OMPI_Kill(MPI_Comm comm, int rank, MPI_Info info);
#endif
