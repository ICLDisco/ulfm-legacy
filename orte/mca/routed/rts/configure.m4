# -*- shell-script -*-
#
# Copyright (c) 2011      Los Alamos National Security, LLC.
#                         All rights reserved.
# Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#
# MCA_routed_rts_CONFIG([action-if-found], [action-if-not-found])
# -----------------------------------------------------------
AC_DEFUN([MCA_orte_routed_rts_CONFIG], [
    AC_CONFIG_FILES([orte/mca/routed/rts/Makefile])

    AS_IF([test "$orte_without_full_support" = 0],
          [$1],
          [$2])

    # If we don't want FT, don't compile this component
    AS_IF([test "$opal_want_ft_mpi" = "1"],
        [$1],
        [$2])
])
