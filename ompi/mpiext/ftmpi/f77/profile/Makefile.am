#
# Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

#
# OMPI_PROFILING_DEFINES flag s enabled when we want our MPI_* symbols
# to be replaced by PMPI_*. In other words, this flag decides 
# whether "profile/defines.h" is included or not. "profile/defines.h"
# replaces all MPI_* symbols with PMPI_* symbols. In this directory
# we definately need it to be 1.
#
AM_CPPFLAGS = -DOMPI_PROFILING_DEFINES=1 -DOMPI_PROFILE_LAYER=1 -DOMPI_COMPILING_F77_WRAPPERS=1

noinst_LTLIBRARIES =
if OMPI_WANT_F77_BINDINGS
if WANT_PMPI_BINDINGS_LAYER
noinst_LTLIBRARIES += libext_pmpiext_f77_ftmpi.la
endif
endif

headers = 
nodist_libext_pmpiext_f77_ftmpi_la_SOURCES = 
if OMPI_WANT_F77_BINDINGS
headers += defines.h

nodist_libext_pmpiext_f77_ftmpi_la_SOURCES += \
	pcomm_revoke_f.c \
	pcomm_shrink_f.c \
	pcomm_failure_ack_f.c \
	pcomm_failure_get_acked_f.c \
	pcomm_agree_f.c \
	pcomm_iagree_f.c
endif

#
# Sym link in the sources from the real MPI directory
#
$(nodist_libext_pmpiext_f77_ftmpi_la_SOURCES):
	if test ! -r $@ ; then \
		pname=`echo $@ | cut -b '2-'` ; \
		$(LN_S) $(top_srcdir)/ompi/mpiext/ftmpi/f77/$$pname $@ ; \
	fi

# Conditionally install the header files
if OMPI_WANT_F77_BINDINGS
if WANT_INSTALL_HEADERS
ompidir = $(includedir)/openmpi/ompi/mpiext/ftmpi
ompi_HEADERS = $(headers)
endif

# These files were created by targets above

MAINTAINERCLEANFILES = $(nodist_libext_pmpiext_f77_ftmpi_la_SOURCES)
endif
