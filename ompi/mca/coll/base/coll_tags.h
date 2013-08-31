/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2005 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 *
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#ifndef MCA_COLL_BASE_TAGS_H
#define MCA_COLL_BASE_TAGS_H

/*
 * Tags that can be used for MPI point-to-point functions when
 * implementing collectives via point-to-point.
 */

#define MCA_COLL_BASE_TAG_ALLGATHER -10
#define MCA_COLL_BASE_TAG_ALLGATHERV -11
#define MCA_COLL_BASE_TAG_ALLREDUCE -12
#define MCA_COLL_BASE_TAG_ALLTOALL -13
#define MCA_COLL_BASE_TAG_ALLTOALLV -14
#define MCA_COLL_BASE_TAG_ALLTOALLW -15
#define MCA_COLL_BASE_TAG_BARRIER -16
#define MCA_COLL_BASE_TAG_BCAST -17
#define MCA_COLL_BASE_TAG_EXSCAN -18
#define MCA_COLL_BASE_TAG_GATHER -19
#define MCA_COLL_BASE_TAG_GATHERV -20
#define MCA_COLL_BASE_TAG_REDUCE -21
#define MCA_COLL_BASE_TAG_REDUCE_SCATTER -22
#define MCA_COLL_BASE_TAG_SCAN -23
#define MCA_COLL_BASE_TAG_SCATTER -24
#define MCA_COLL_BASE_TAG_SCATTERV -25

#if OPAL_ENABLE_FT_MPI
#define MCA_COLL_BASE_TAG_MAX_PRE_AGREEMENT -25
#define MCA_COLL_BASE_TAG_AGREEMENT -26
#define MCA_COLL_BASE_TAG_AGREEMENT_CATCH_UP -27
#define MCA_COLL_BASE_TAG_AGREEMENT_CATCH_UP_REQ -28
#define MCA_COLL_BASE_TAG_AGREEMENT_UR_ELECTED -29
#define MCA_COLL_BASE_TAG_SHRINK -30
#define MCA_COLL_BASE_TAG_MAX_POST_AGREEMENT -31

#define MCA_COLL_BASE_TAG_MIN -10
#define MCA_COLL_BASE_TAG_MAX -31
#endif /* OPAL_ENABLE_FT_MPI */

#endif /* MCA_COLL_BASE_TAGS_H */
