# -*- makefile -*-
#
# Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
#                         University Research and Technology
#                         Corporation.  All rights reserved.
# Copyright (c) 2004-2014 The University of Tennessee and The University
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

noinst_PROGRAMS = err_returns revoke revshrink revshrinkkill revshrinkkillrecover

err_returns_SOURCES = err_returns.c
err_returns_LDADD = \
	$(top_builddir)/ompi/libmpi.la
err_returns_DEPENDENCIES = $(err_returns_LDADD)

revoke_SOURCES = revoke.c
revoke_LDADD = \
	$(top_builddir)/ompi/libmpi.la
revoke_DEPENDENCIES = $(revoke_LDADD)

revshrink_SOURCES = revshrink.c
revshrink_LDADD = \
	$(top_builddir)/ompi/libmpi.la
revshrink_DEPENDENCIES = $(revshrink_LDADD)

revshrinkkill_SOURCES = revshrinkkill.c
revshrinkkill_LDADD = \
	$(top_builddir)/ompi/libmpi.la
revshrinkkill_DEPENDENCIES = $(revshrinkkill_LDADD)

revshrinkkillrecover_SOURCES = revshrinkkillrecover.c
revshrinkkillrecover_LDADD = \
	$(top_builddir)/ompi/libmpi.la
revshrinkkillrecover_DEPENDENCIES = $(revshrinkkillrecover_LDADD)
