/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2007 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2015 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2008      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#ifndef MCA_COLL_FTBASIC_EXPORT_H
#define MCA_COLL_FTBASIC_EXPORT_H

#include "ompi_config.h"

#include "mpi.h"
#include "opal/mca/mca.h"

#include "opal/class/opal_bitmap.h"
#include "ompi/class/ompi_free_list.h"

#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/request/request.h"
#include "ompi/group/group.h"
#include "ompi/communicator/communicator.h"
#include "ompi/runtime/params.h"

#include "orte/runtime/orte_globals.h"

BEGIN_C_DECLS

/*
 * TAGS for agreement collectives
 */
#define MCA_COLL_FTBASIC_TAG_AGREEMENT(module)      (module->mccb_coll_tag_agreement)
#define MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP     (MCA_COLL_BASE_TAG_AGREEMENT_CATCH_UP)
#define MCA_COLL_FTBASIC_TAG_AGREEMENT_CATCH_UP_REQ (MCA_COLL_BASE_TAG_AGREEMENT_CATCH_UP_REQ)
#define MCA_COLL_FTBASIC_TAG_AGREEMENT_UR_ELECTED   (MCA_COLL_BASE_TAG_AGREEMENT_UR_ELECTED)
/* WBLAND 7/9/12 - This was causing hangs becuase leader was not incrementing
 * while paritipants were.
 #define MCA_COLL_FTBASIC_TAG_STEP  (MCA_COLL_BASE_TAG_AGREEMENT_UR_ELECTED - MCA_COLL_BASE_TAG_AGREEMENT)*/
#define MCA_COLL_FTBASIC_TAG_STEP 0


/* Globally exported variables */

OMPI_MODULE_DECLSPEC extern const mca_coll_base_component_2_0_0_t
mca_coll_ftbasic_component;
extern int mca_coll_ftbasic_priority;
extern int mca_coll_ftbasic_crossover;

#if defined(OPAL_ENABLE_DEBUG)
OMPI_DECLSPEC extern int coll_ftbasic_era_debug_rank_may_fail;
#endif

enum mca_coll_ftbasic_agreement_method_t {
    COLL_FTBASIC_EARLY_TERMINATION = 0,
    COLL_FTBASIC_EARLY_RETURNING   = 1
};
typedef enum mca_coll_ftbasic_agreement_method_t mca_coll_ftbasic_agreement_method_t;

extern mca_coll_ftbasic_agreement_method_t mca_coll_ftbasic_cur_agreement_method;
extern int mca_coll_ftbasic_era_rebuild;

struct mca_coll_ftbasic_request_t;

/*
 * Base agreement structure
 * Individual agreement algorithms will extend this struct as needed
 */
struct mca_coll_ftbasic_agreement_t {
    /* This is a general object */
    opal_object_t super;

    /* Agreement Sequence Number */
    int agreement_seq_num;

    /* Current non-blocking Agreement Request */
#ifdef IAGREE
    struct mca_coll_ftbasic_request_t *cur_request;
#endif
};
typedef struct mca_coll_ftbasic_agreement_t mca_coll_ftbasic_agreement_t;
OBJ_CLASS_DECLARATION(mca_coll_ftbasic_agreement_t);

struct mca_coll_ftbasic_module_t {
    mca_coll_base_module_t super;

    /* Communicator Type */
    bool is_intercomm;

    /* Array of requests */
    ompi_request_t **mccb_reqs;
    int mccb_num_reqs;

    /* Array of statuses */
    ompi_status_public_t *mccb_statuses;
    int mccb_num_statuses;

    /* Tag start number
     * This allows for us to change context within a communicator without
     * flushing the PML stack.
     */
    int mccb_coll_tag_agreement;

    /* Pointer to the agreement structure */
    mca_coll_ftbasic_agreement_t *agreement_info;
};
typedef struct mca_coll_ftbasic_module_t mca_coll_ftbasic_module_t;
OBJ_CLASS_DECLARATION(mca_coll_ftbasic_module_t);

/*
 * API functions
 */
int mca_coll_ftbasic_init_query(bool enable_progress_threads,
                                bool enable_mpi_threads);
mca_coll_base_module_t
*mca_coll_ftbasic_comm_query(struct ompi_communicator_t *comm,
                             int *priority);

int mca_coll_ftbasic_module_enable(mca_coll_base_module_t *module,
                                   struct ompi_communicator_t *comm);

int mca_coll_ftbasic_ft_event(int status);

/*
 * Agreement protocols
 */

/* Early termination algorithm */
int
mca_coll_ftbasic_agreement_eta_intra(ompi_communicator_t* comm,
                                     ompi_group_t **group,
                                     ompi_op_t *op,
                                     ompi_datatype_t *dt,
                                     int dt_count,
                                     void *contrib,
                                     mca_coll_base_module_t *module);
/* Early returning algorithm */
int
mca_coll_ftbasic_agreement_era_intra(ompi_communicator_t* comm,
                                     ompi_group_t **group,
                                     ompi_op_t *op,
                                     ompi_datatype_t *dt,
                                     int dt_count,
                                     void *contrib,
                                     mca_coll_base_module_t *module);
int mca_coll_ftbasic_iagreement_era_intra(ompi_communicator_t* comm,
                                          ompi_group_t *group,
                                          ompi_op_t *op,
                                          ompi_datatype_t *dt,
                                          int dt_count,
                                          void *contrib,
                                          mca_coll_base_module_t *module,
                                          ompi_request_t **request);
int
mca_coll_ftbasic_agreement_era_inter(ompi_communicator_t* comm,
                                     ompi_group_t **group,
                                     ompi_op_t *op,
                                     ompi_datatype_t *dt,
                                     int dt_count,
                                     void *contrib,
                                     mca_coll_base_module_t *module);

/*
 * Utility functions
 */
static inline void mca_coll_ftbasic_free_reqs(ompi_request_t ** reqs,
                                              int count)
{
    int i;
    for (i = 0; i < count; ++i) {
        if( OMPI_REQUEST_INVALID != reqs[i]->req_state ) {
            ompi_request_free(&reqs[i]);
        }
    }
}

END_C_DECLS

#endif /* MCA_COLL_FTBASIC_EXPORT_H */
