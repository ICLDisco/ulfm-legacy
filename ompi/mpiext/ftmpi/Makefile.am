#
# Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

#
# C Bindings
#
SUBDIRS = c/profile
SUBDIRS += f77/profile

headers = \
	mpiext_ftmpi_c.h

sources = \
	c/comm_revoke.c \
	c/comm_shrink.c \
	c/comm_failure_ack.c \
	c/comm_failure_get_acked.c \
	c/comm_agree.c \
	c/comm_iagree.c

#
# F77 bindings
#
f77_sources =
if OMPI_WANT_F77_BINDINGS

headers += \
	mpiext_ftmpi_f77.h \
	f77/ftmpi_f77_support.h

f77_sources += \
	f77/comm_revoke_f.c \
	f77/comm_shrink_f.c \
	f77/comm_failure_ack_f.c \
	f77/comm_failure_get_acked_f.c \
	f77/comm_agree_f.c \
	f77/comm_iagree_f.c
endif

#
# F90 bindings
#
if OMPI_WANT_F90_BINDINGS
headers += \
	mpiext_ftmpi_f90.h
endif

extcomponentdir = $(pkglibdir)

AM_CPPFLAGS = -DOMPI_PROFILING_DEFINES=0 -DOMPI_PROFILE_LAYER=0 -DOMPI_COMPILING_F77_WRAPPERS=1

# For C Bindings
lib = libext_mpiext_ftmpi.la
lib_sources = $(sources)

libext_mpiext_ftmpi_la_SOURCES = $(lib_sources)
libext_mpiext_ftmpi_la_LDFLAGS = -module -avoid-version

# For F77 Bindings
if OMPI_WANT_F77_BINDINGS
f77_lib = libext_mpiext_ftmpi_f77.la
f77_lib_sources = $(f77_sources)

libext_mpiext_ftmpi_f77_la_SOURCES = $(f77_lib_sources)
libext_mpiext_ftmpi_f77_la_LIBADD  = $(lib_sources)
libext_mpiext_ftmpi_f77_la_LDFLAGS = -module -avoid-version
if WANT_PMPI_F77_BINDINGS_LAYER
  libext_mpiext_ftmpi_f77_la_LIBADD += f77/profile/libext_pmpiext_ftmpi_f77.la
endif
endif

noinst_LTLIBRARIES = $(lib)
if OMPI_WANT_F77_BINDINGS
noinst_LTLIBRARIES += $(f77_lib)
endif

ompidir = $(includedir)/openmpi/ompi/mpiext/ftmpi
ompi_HEADERS = \
        $(headers)
