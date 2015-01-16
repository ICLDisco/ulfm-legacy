/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 *
 */
#ifndef OMPI_MPIEXT_FTMPI_F77_PROFILE_DEFINES_H
#define OMPI_MPIEXT_FTMPI_F77_PROFILE_DEFINES_H
/*
 * This file is included in the top directory only if 
 * profiling is required. Once profiling is required,
 * this file will replace all MPI_* symbols with 
 * PMPI_* symbols
 */

#define MPIX_Comm_revoke PMPIX_Comm_revoke

#define MPIX_Comm_shrink PMPIX_Comm_shrink

#define MPIX_Comm_failure_ack PMPIX_Comm_failure_ack
#define MPIX_Comm_failure_get_acked PMPIX_Comm_failure_get_acked

#define MPIX_Comm_agree PMPIX_Comm_agree
#define MPIX_Comm_iagree PMPIX_Comm_iagree

/* Deprecated functions */
#define OMPI_Comm_revoke POMPI_Comm_revoke

#define OMPI_Comm_shrink POMPI_Comm_shrink

#define OMPI_Comm_failure_ack POMPI_Comm_failure_ack
#define OMPI_Comm_failure_get_acked POMPI_Comm_failure_get_acked

#define OMPI_Comm_agree POMPI_Comm_agree
#define OMPI_Comm_iagree POMPI_Comm_iagree

#endif /* OMPI_MPIEXT_FTMPI_F77_PROFILE_DEFINES_H */
