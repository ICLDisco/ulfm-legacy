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
AM_CPPFLAGS = -DOMPI_PROFILING_DEFINES=1

noinst_LTLIBRARIES =
if WANT_PMPI_BINDINGS_LAYER
noinst_LTLIBRARIES += libext_pmpiext_ftmpi_c.la
endif

headers = defines.h

nodist_libext_pmpiext_ftmpi_c_la_SOURCES = \
	pcomm_revoke.c \
	pcomm_shrink.c \
	pcomm_failure_ack.c \
	pcomm_failure_get_acked.c \
	pcomm_agree.c \
	pcomm_iagree.c

#
# Sym link in the sources from the real MPI directory
#
$(nodist_libext_pmpiext_ftmpi_c_la_SOURCES):
	if test ! -r $@ ; then \
		pname=`echo $@ | cut -b '2-'` ; \
		$(LN_S) $(top_srcdir)/ompi/mpiext/ftmpi/c/$$pname $@ ; \
	fi

# Conditionally install the header files

if WANT_INSTALL_HEADERS
ompidir = $(includedir)/openmpi/ompi/mpiext/ftmpi
ompi_HEADERS = $(headers)
endif

# These files were created by targets above

MAINTAINERCLEANFILES = $(nodist_libext_pmpiext_ftmpi_c_la_SOURCES)
