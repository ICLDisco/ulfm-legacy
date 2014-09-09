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
# Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

EXTRA_DIST = .windows

sources = \
        coll_ftbasic.h \
        coll_ftbasic_component.c \
        coll_ftbasic_module.c \
        coll_ftbasic_agreement.c \
        coll_ftbasic_agreement.h \
        coll_ftbasic_agreement_2phase.c \
		coll_ftbasic_agreement_log2phase.c \
		coll_ftbasic_agreement_earlyterminating.c \
		coll_ftbasic_agreement_earlyreturning.c \
        coll_ftbasic_agreement_allreduce.c

# Make the output library in this directory, and name it either
# mca_<type>_<name>.la (for DSO builds) or libmca_<type>_<name>.la
# (for static builds).

if MCA_BUILD_ompi_coll_ftbasic_DSO
component_noinst =
component_install = mca_coll_ftbasic.la
else
component_noinst = libmca_coll_ftbasic.la
component_install =
endif

mcacomponentdir = $(pkglibdir)
mcacomponent_LTLIBRARIES = $(component_install)
mca_coll_ftbasic_la_SOURCES = $(sources)
mca_coll_ftbasic_la_LDFLAGS = -module -avoid-version

noinst_LTLIBRARIES = $(component_noinst)
libmca_coll_ftbasic_la_SOURCES =$(sources)
libmca_coll_ftbasic_la_LDFLAGS = -module -avoid-version
