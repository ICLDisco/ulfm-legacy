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

! Include the parameters for this extension
! Included from config/ompi_ext.m4 into mpif90-ext.f90
! include '../mpiext/ftmpi/mpiext_ftmpi_f77.h'

!
! Communicators
!
interface MPIX_Comm_revoke
    subroutine mpix_comm_revoke(comm, ierr)
      integer, intent(IN) :: comm
      integer, intent(OUT) :: ierr
    end subroutine mpix_comm_revoke
end interface MPIX_Comm_revoke

interface MPIX_Comm_shrink
    subroutine mpix_comm_shrink(comm, newcomm, ierr)
      integer, intent(IN) :: comm
      integer, intent(OUT) :: newcomm, ierr
    end subroutine mpix_comm_shrink
end interface MPIX_Comm_shrink

interface MPIX_Comm_failure_ack
    subroutine mpix_comm_failure_ack(comm, ierr)
      integer, intent(IN) :: comm
      integer, intent(OUT) :: ierr
    end subroutine mpix_comm_failure_ack
end interface MPIX_Comm_failure_ack

interface MPIX_Comm_failure_get_acked
    subroutine mpix_comm_failure_get_acked(comm, failedgrp, ierr)
        integer, intent(IN) :: comm
        logical, intent(OUT) :: failedgrp, ierr
    end subroutine mpix_comm_failure_get_acked
end interface MPIX_Comm_failure_get_acked

interface MPIX_Comm_agree
    subroutine mpix_comm_agree(comm, ierr)
      integer, intent(IN) :: comm
      integer, intent(OUT) :: ierr
    end subroutine mpix_comm_agree
end interface MPIX_Comm_agree

interface MPIX_Comm_iagree
    subroutine mpix_comm_iagree(comm, request, ierr)
      integer, intent(IN) :: comm
      integer, intent(OUT) :: request, ierr
    end subroutine mpix_comm_iagree
end interface MPIX_Comm_iagree

! And the deprecated functions

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

interface OMPI_Comm_agree
    subroutine ompi_comm_agree(comm, ierr)
      integer, intent(IN) :: comm
      integer, intent(OUT) :: ierr
    end subroutine ompi_comm_agree
end interface OMPI_Comm_agree

interface OMPI_Comm_iagree
    subroutine ompi_comm_iagree(comm, request, ierr)
      integer, intent(IN) :: comm
      integer, intent(OUT) :: request, ierr
    end subroutine ompi_comm_iagree
end interface OMPI_Comm_iagree

!
! Validation: Windows
! Todo
!


!
! Validation: File Handles
! Todo
!


!
