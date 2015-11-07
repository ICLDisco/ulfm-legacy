# -*- makefile -*-
#
# Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
#                         University Research and Technology
#                         Corporation.  All rights reserved.
# Copyright (c) 2004-2005 The University of Tennessee and The University
#                         of Tennessee Research Foundation.  All rights
#                         reserved.
# Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
#                         University of Stuttgart.  All rights reserved.
# Copyright (c) 2004-2005 The Regents of the University of California.
#                         All rights reserved.
# Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
#
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

# This makefile.am does not stand on its own - it is included from ompi/Makefile.am

headers += \
        communicator/communicator.h

libmpi_la_SOURCES += \
        communicator/comm_init.c \
	communicator/comm.c \
	communicator/comm_cid.c

if WANT_FT_MPI
libmpi_la_SOURCES += \
	communicator/comm_ft.c communicator/comm_ft_reliable_bcast.c communicator/comm_ft_propagate.c communicator/comm_ft_revoke.c
endif # WANT_FT_MPI
