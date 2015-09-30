! -*- fortran -*-
! Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
! Copyright (c) 2010-2015 The Trustees of the University of Tennessee.
!                         All rights reserved.
! $COPYRIGHT$
! 
! Additional copyrights may follow
! 
! $HEADER$
!

include 'mpif.h'
!
! Error codes
!
       integer MPIX_ERR_PROC_FAILED
       integer MPIX_ERR_REVOKED

       parameter (MPIX_ERR_PROC_FAILED = MPI_ERR_PROC_FAILED)
       parameter (MPIX_ERR_REVOKED = MPI_ERR_REVOKED)

