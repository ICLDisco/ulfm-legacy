/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2010-2015 The Trustees of the University of Tennessee.
 *                         All rights reserved.
 * $COPYRIGHT$
 *
 * Additional copyrights may follow
 *
 * $HEADER$
 *
 */
#include <stdbool.h>

/********************************
 * Error codes and classes
 ********************************/
#include <mpi.h>
#define MPIX_ERR_PROC_FAILED          MPI_ERR_PROC_FAILED
#define MPIX_ERR_PROC_FAILED_PENDING  MPI_ERR_PROC_FAILED_PENDING
#define MPIX_ERR_REVOKED              MPI_ERR_REVOKED
#define MPIX_FT                       MPI_FT

/********************************
 * Communicators
 ********************************/
OMPI_DECLSPEC int MPIX_Comm_revoke(MPI_Comm comm);
OMPI_DECLSPEC int MPIX_Comm_shrink(MPI_Comm comm, MPI_Comm *newcomm);
OMPI_DECLSPEC int MPIX_Comm_failure_ack(MPI_Comm comm);
OMPI_DECLSPEC int MPIX_Comm_failure_get_acked(MPI_Comm comm, MPI_Group *failedgrp);

OMPI_DECLSPEC int MPIX_Comm_agree(MPI_Comm comm, int *flag);
OMPI_DECLSPEC int MPIX_Comm_iagree(MPI_Comm comm, int *flag, MPI_Request *request);

/**
 * Deprecated functions to be soon removed.
 */
OMPI_DECLSPEC int OMPI_Comm_revoke(MPI_Comm comm) __mpi_interface_deprecated__("OMPI_Comm_revoke is deprecated, use MPIX_Comm_revoke");

OMPI_DECLSPEC int OMPI_Comm_shrink(MPI_Comm comm, MPI_Comm *newcomm) __mpi_interface_deprecated__("OMPI_Comm_shrink is deprecated, use MPIX_Comm_shrink");

OMPI_DECLSPEC int OMPI_Comm_failure_ack(MPI_Comm comm) __mpi_interface_deprecated__("OMPI_Comm_failure_ack is deprecated, use MPIX_Comm_failure_ack");
OMPI_DECLSPEC int OMPI_Comm_failure_get_acked(MPI_Comm comm, MPI_Group *failedgrp) __mpi_interface_deprecated__("OMPI_Comm_failure_get_acked is deprecated, use MPIX_Comm_failure_get_acked");

OMPI_DECLSPEC int OMPI_Comm_agree(MPI_Comm comm, int *flag) __mpi_interface_deprecated__("OMPI_Comm_agree is deprecated, use MPIX_Comm_agree");
OMPI_DECLSPEC int OMPI_Comm_iagree(MPI_Comm comm, int *flag, MPI_Request *request) __mpi_interface_deprecated__("OMPI_Comm_iagree is deprecated, use MPIX_Comm_iagree");

#if 0
/********************************
 * Windows
 ********************************/
OMPI_DECLSPEC int MPIX_Win_revoke(MPI_Win win);
OMPI_DECLSPEC int MPIX_Win_get_failed(MPI_Win win, MPI_Group *failedgrp);
#endif

#if 0
/********************************
 * I/O
 ********************************/
OMPI_DECLSPEC int MPIX_File_revoke(MPI_File fh);
#endif

