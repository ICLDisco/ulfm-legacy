/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * Copyright (c) 2015      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */
#ifndef MCA_COLL_FTBASIC_AGREEMENT_EXPORT_H
#define MCA_COLL_FTBASIC_AGREEMENT_EXPORT_H

#include "coll_ftbasic.h"

BEGIN_C_DECLS

/*
 * Base supporting functions
 */

int mca_coll_ftbasic_agreement_init(ompi_communicator_t *comm, mca_coll_ftbasic_module_t *module);
int mca_coll_ftbasic_agreement_finalize(mca_coll_ftbasic_module_t *module);

/*
 * Early Returning Specific
 */
int mca_coll_ftbasic_agreement_era_comm_init(ompi_communicator_t *comm, mca_coll_ftbasic_module_t *module);
int mca_coll_ftbasic_agreement_era_comm_finalize(mca_coll_ftbasic_module_t *module);
int mca_coll_ftbasic_agreement_era_init(void);
int mca_coll_ftbasic_agreement_era_finalize(void);

END_C_DECLS

#endif /* MCA_COLL_FTBASIC_AGREEMENT_EXPORT_H */
