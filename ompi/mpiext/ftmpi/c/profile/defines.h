/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 *
 */
#ifndef OMPI_MPIEXT_FTMPI_C_PROFILE_DEFINES_H
#define OMPI_MPIEXT_FTMPI_C_PROFILE_DEFINES_H
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

#endif /* OMPI_MPIEXT_FTMPI_C_PROFILE_DEFINES_H */
