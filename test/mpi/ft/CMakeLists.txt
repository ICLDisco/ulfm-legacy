# Copyright (c) 2012      The University of Tennessee and The University
*                         of Tennessee Research Foundation.  All rights
*                         reserved. 
# $COPYRIGHT$
# 
# Additional copyrights may follow
# 
# $HEADER$
#

SET(INVSHRINK_FILES
  invshrink.c
)

SET(INVSHRINKKILL_FILES
  invshrinkill.c
)

SET(INVSHRINKKILLRECOVER_FILES
  invshrinkkillrecover.c
)

ADD_EXECUTABLE (invshrink ${INVSHRINK_FILES})
ADD_EXECUTABLE (invshrinkkill ${INVSHRINKKILL_FILES})
ADD_EXECUTABLE (invshrinkkillrecover ${INVSHRINKKILLRECOVER_FILES})

TARGET_LINK_LIBRARIES (invshrink invshrinkkill invshrinkkillrecover libmpi)
