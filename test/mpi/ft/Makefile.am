# -*- makefile -*-
#
# Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
#                         University Research and Technology
#                         Corporation.  All rights reserved.
# Copyright (c) 2004-2005 The University of Tennessee and The University
#                         of Tennessee Research Foundation.  All rights
#                         reserved.
# Copyright (c) 2004-2009 High Performance Computing Center Stuttgart, 
#                         University of Stuttgart.  All rights reserved.
# Copyright (c) 2004-2005 The Regents of the University of California.
#                         All rights reserved.
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

EXTRA_DIST = CMakeLists.txt

AM_LDFLAGS = -lpthread

noinst_PROGRAMS = inv invshrink invshrinkkill invshrinkkillrecover

inv_SOURCES = inv.c
inv_LDADD = \
	$(top_builddir)/src/libmpi.la \
	$(top_builddir)/src/libompi.la 
inv_DEPENDENCIES = $(inv_LDADD)

invshrink_SOURCES = invshrink.c
invshrink_LDADD = \
	$(top_builddir)/src/libmpi.la \
	$(top_builddir)/src/libompi.la 
invshrink_DEPENDENCIES = $(invshrink_LDADD)

invshrinkkill_SOURCES = invshrinkkill.c
invshrinkkill_LDADD = \
	$(top_builddir)/src/libmpi.la \
	$(top_builddir)/src/libompi.la 
invshrinkkill_DEPENDENCIES = $(invshrinkkill_LDADD)

invshrinkkillrecover_SOURCES = invshrinkkillrecover.c
invshrinkkillrecover_LDADD = \
	$(top_builddir)/src/libmpi.la \
	$(top_builddir)/src/libompi.la 
invshrinkkillrecover_DEPENDENCIES = $(invshrinkkillrecover_LDADD)
