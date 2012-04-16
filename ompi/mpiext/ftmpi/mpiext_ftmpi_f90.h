! -*- fortran -*-
! Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
! $COPYRIGHT$
! 
! Additional copyrights may follow
! 
! $HEADER$
!

! Include the parameters for this extension
! Included from config/ompi_ext.m4 into mpif90-ext.f90
! include '../mpiext/ftmpi/mpiext_ftmpi_f77.h'

!
! Communicators
!
interface OMPI_Comm_revoke
    subroutine ompi_comm_revoke(comm, ierr)
      integer, intent(IN) :: comm
      integer, intent(OUT) :: ierr
    end subroutine ompi_comm_revoke
end interface OMPI_Comm_revoke

interface OMPI_Comm_shrink
    subroutine ompi_comm_shrink(comm, newcomm, ierr)
      integer, intent(IN) :: comm
      integer, intent(OUT) :: newcomm, ierr
    end subroutine ompi_comm_shrink
end interface OMPI_Comm_shrink

interface OMPI_Comm_failure_ack
    subroutine ompi_comm_failure_ack(comm, ierr)
      integer, intent(IN) :: comm
      integer, intent(OUT) :: ierr
    end subroutine ompi_comm_failure_ack
end interface OMPI_Comm_failure_ack

interface OMPI_Comm_failure_get_acked
    subroutine ompi_comm_failure_get_acked(comm, failedgrp, ierr)
        integer, intent(IN) :: comm
        logical, intent(OUT) :: failedgrp, ierr
    end subroutine ompi_comm_failure_get_acked
end interface OMPI_Comm_failure_get_acked

interface OMPI_Comm_agreement
    subroutine ompi_comm_agreement(comm, ierr)
      integer, intent(IN) :: comm
      integer, intent(OUT) :: ierr
    end subroutine ompi_comm_agreement
end interface OMPI_Comm_agreement

interface OMPI_Comm_iagreement
    subroutine ompi_comm_iagreement(comm, request, ierr)
      integer, intent(IN) :: comm
      integer, intent(OUT) :: request, ierr
    end subroutine ompi_comm_iagreement
end interface OMPI_Comm_iagreement

!
! Validation: Windows
! Todo
!


!
! Validation: File Handles
! Todo
!


!
