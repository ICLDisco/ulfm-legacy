# Copyright (c) 2012-2014 The University of Tennessee and The University
*                         of Tennessee Research Foundation.  All rights
*                         reserved. 
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

SET(ERR_RETURNS_FILES err_returns.c)
SET(REVSHRINK_FILES revshrink.c)
SET(REVSHRINKKILL_FILES revshrinkill.c)
SET(REVSHRINKKILLRECOVER_FILES revshrinkkillrecover.c)

ADD_EXECUTABLE (err_returns ${ERR_RETURNS_FILES})
ADD_EXECUTABLE (revshrink ${REVSHRINK_FILES})
ADD_EXECUTABLE (revshrinkkill ${REVSHRINKKILL_FILES})
ADD_EXECUTABLE (revshrinkkillrecover ${REVSHRINKKILLRECOVER_FILES})

TARGET_LINK_LIBRARIES (revshrink revshrinkkill revshrinkkillrecover libmpi)
