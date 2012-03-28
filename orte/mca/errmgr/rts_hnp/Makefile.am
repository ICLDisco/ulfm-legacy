#
# Copyright (c) 2010      Cisco Systems, Inc.  All rights reserved. 
# Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
#
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

EXTRA_DIST = .windows

sources = \
        errmgr_rts_hnp.h \
        errmgr_rts_hnp_component.c \
        errmgr_rts_hnp.c

# Make the output library in this directory, and name it either
# mca_<type>_<name>.la (for DSO builds) or libmca_<type>_<name>.la
# (for static builds).

if MCA_BUILD_orte_errmgr_rts_hnp_DSO
component_noinst =
component_install = mca_errmgr_rts_hnp.la
else
component_noinst = libmca_errmgr_rts_hnp.la
component_install =
endif

mcacomponentdir = $(pkglibdir)
mcacomponent_LTLIBRARIES = $(component_install)
mca_errmgr_rts_hnp_la_SOURCES = $(sources)
mca_errmgr_rts_hnp_la_LDFLAGS = -module -avoid-version

noinst_LTLIBRARIES = $(component_noinst)
libmca_errmgr_rts_hnp_la_SOURCES =$(sources)
libmca_errmgr_rts_hnp_la_LDFLAGS = -module -avoid-version
