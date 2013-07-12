# -*- makefile -*-
#
# Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
#                         University Research and Technology
#                         Corporation.  All rights reserved.
# Copyright (c) 2004-2012 The University of Tennessee and The University
#                         of Tennessee Research Foundation.  All rights
#                         reserved.
# Copyright (c) 2004-2009 High Performance Computing Center Stuttgart, 
#                         University of Stuttgart.  All rights reserved.
# Copyright (c) 2004-2005 The Regents of the University of California.
#                         All rights reserved.
# Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

EXTRA_DIST = CMakeLists.txt

AM_LDFLAGS = -lpthread

noinst_PROGRAMS = revoke revokeshrink revokeshrinkkill revokeshrinkkillrecover

revoke_SOURCES = revoke.c
revoke_LDADD = \
	$(top_builddir)/src/libmpi.la \
	$(top_builddir)/src/libompi.la 
revoke_DEPENDENCIES = $(revoke_LDADD)

revokeshrink_SOURCES = revokeshrink.c
revokeshrink_LDADD = \
	$(top_builddir)/src/libmpi.la \
	$(top_builddir)/src/libompi.la 
revokeshrink_DEPENDENCIES = $(revokeshrink_LDADD)

revokeshrinkkill_SOURCES = revokeshrinkkill.c
revokeshrinkkill_LDADD = \
	$(top_builddir)/src/libmpi.la \
	$(top_builddir)/src/libompi.la 
revokeshrinkkill_DEPENDENCIES = $(revokeshrinkkill_LDADD)

revokeshrinkkillrecover_SOURCES = revokeshrinkkillrecover.c
revokeshrinkkillrecover_LDADD = \
	$(top_builddir)/src/libmpi.la \
	$(top_builddir)/src/libompi.la 
revokeshrinkkillrecover_DEPENDENCIES = $(revokeshrinkkillrecover_LDADD)
