# -*- shell-script -*-
#
# Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

# MCA_coll_ftbasic_CONFIG([action-if-found], [action-if-not-found])
# -----------------------------------------------------------
AC_DEFUN([MCA_ompi_coll_ftbasic_CONFIG],[
    AC_CONFIG_FILES([ompi/mca/coll/ftbasic/Makefile])

    # If we don't want FT, don't compile this component
    AS_IF([test "$opal_want_ft_mpi" = "1"],
        [$1],
        [$2])
])dnl
