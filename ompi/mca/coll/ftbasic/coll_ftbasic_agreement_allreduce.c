/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 *
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "ompi_config.h"
#include "coll_ftbasic.h"
#include "coll_ftbasic_agreement.h"

#include <unistd.h>
#include "orte/util/name_fns.h"
#include "orte/mca/plm/plm_types.h"
#include "orte/mca/errmgr/errmgr.h"

#include "mpi.h"
#include "ompi/constants.h"

#include "opal/util/bit_ops.h"
#include "opal/mca/event/event.h"

#include "ompi/mca/pml/pml.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/mca/coll/base/coll_tags.h"
#include "ompi/proc/proc.h"
#include "ompi/mpiext/ftmpi/mpiext_ftmpi_c.h"

#include "ompi/op/op.h"

#include MCA_timer_IMPLEMENTATION_HEADER
#include "coll_ftbasic.h"

/*
 * Globals
 */
static int root = 0;

static int internal_agreement_allreduce(ompi_communicator_t* comm,
                                        ompi_group_t **group,
                                        int *flag,
                                        opal_bitmap_t *local_bitmap,
                                        mca_coll_ftbasic_module_t *ftbasic_module);
static int internal_iagreement_allreduce(ompi_communicator_t* comm,
                                         ompi_group_t **group,
                                         int *flag,
                                         opal_bitmap_t *local_bitmap,
                                         mca_coll_ftbasic_module_t *ftbasic_module,
                                         struct mca_coll_ftbasic_request_t *collreq);

static ompi_op_t *ftbasic_agreement_allreduce_op = NULL;
static void ftbasic_agreement_allreduce_op_fn(void * invec, void *inoutvec, int *len, struct ompi_datatype_t **datatype);

/*************************************
 * Algorithm specific agreement structure
 *************************************/
struct mca_coll_ftbasic_agreement_allreduce_t {
    /* Base object */
    mca_coll_ftbasic_agreement_t super;
};
typedef struct mca_coll_ftbasic_agreement_allreduce_t mca_coll_ftbasic_agreement_allreduce_t;
OBJ_CLASS_DECLARATION(mca_coll_ftbasic_agreement_allreduce_t);

static void mca_coll_ftbasic_agreement_allreduce_construct(mca_coll_ftbasic_agreement_allreduce_t *v_info)
{
    /* Nothing needed */
}

static void mca_coll_ftbasic_agreement_allreduce_destruct(mca_coll_ftbasic_agreement_allreduce_t *v_info)
{
    /* Nothing needed */
}
OBJ_CLASS_INSTANCE(mca_coll_ftbasic_agreement_allreduce_t,
                   mca_coll_ftbasic_agreement_t,
                   mca_coll_ftbasic_agreement_allreduce_construct,
                   mca_coll_ftbasic_agreement_allreduce_destruct);

/*************************************
 * Initialization and Finalization
 *************************************/
int mca_coll_ftbasic_agreement_allreduce_init(mca_coll_ftbasic_module_t *module)
{
    mca_coll_ftbasic_agreement_allreduce_t *loc_info;

    loc_info = OBJ_NEW(mca_coll_ftbasic_agreement_allreduce_t);
    module->agreement_info = (mca_coll_ftbasic_agreement_t*)loc_info;

    /************************************************
     * Create a special Op
     * - MPI_LOR on the first N-1 bits
     * - MPI LAND on the last bit
     ************************************************/
    if( NULL == ftbasic_agreement_allreduce_op ) {
        ftbasic_agreement_allreduce_op = ompi_op_create_user(true,
                               (ompi_op_fortran_handler_fn_t *) ftbasic_agreement_allreduce_op_fn);
    }

    return OMPI_SUCCESS;
}

int mca_coll_ftbasic_agreement_allreduce_finalize(mca_coll_ftbasic_module_t *module)
{
    mca_coll_ftbasic_agreement_allreduce_t *loc_info = (mca_coll_ftbasic_agreement_allreduce_t*)(module->agreement_info);

    if( NULL != module->agreement_info ) {
        OBJ_RELEASE(loc_info);
        module->agreement_info = NULL;
    }

    if( NULL != ftbasic_agreement_allreduce_op ) {
        OBJ_RELEASE( ftbasic_agreement_allreduce_op );
        ftbasic_agreement_allreduce_op = NULL;
    }

    return OMPI_SUCCESS;
}


/*************************************
 * High level methods
 *************************************/
int mca_coll_ftbasic_agreement_allreduce(ompi_communicator_t* comm,
                                         ompi_group_t **group,
                                         int *flag,
                                         mca_coll_base_module_t *module)
{
    int ret, exit_status = OMPI_SUCCESS;
    mca_coll_ftbasic_module_t *ftbasic_module = (mca_coll_ftbasic_module_t*) module;
    opal_bitmap_t *local_bitmap = NULL;

    local_bitmap = OBJ_NEW(opal_bitmap_t);

    /*
     * Common setup before the agreement operation
     */
    ret = mca_coll_ftbasic_agreement_base_setup_blocking(comm, group, flag,
                                                         local_bitmap, ftbasic_module);
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto cleanup;
    }
    if( 1 == ompi_comm_num_active_local(comm) ) {
        goto cleanup;
    }

    /*
     * Core agreement operation
     */
    do {
        ret = internal_agreement_allreduce(comm, group, flag,
                                           local_bitmap, ftbasic_module);
        /* Success, continue */
        if( OMPI_SUCCESS == ret ) { break; }
        /* FAIL_STOP if peer failure, try again */
        else if( MPI_ERR_PROC_FAILED == ret ) { continue; }
        /* Some other error, just fail */
        else { break; }
    } while(1);

    /*
     * The protocols should be reliable, so a failure at this point should
     * not happen. If it does report and bail.
     */
    if( OMPI_SUCCESS != ret ) {
        opal_output(0, "%s ftbasic: agreement) (base  ) Error: Reliable consensus protocol failed (should never happen) ret = %d",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret);
        exit_status = ret;
        goto cleanup;
    }

    /*
     * Common cleanup after the agreement operation
     */
    ret = mca_coll_ftbasic_agreement_base_finish_blocking(comm, group, flag,
                                                          local_bitmap, ftbasic_module);
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto cleanup;
    }

 cleanup:
    if( NULL != local_bitmap ) {
        OBJ_RELEASE(local_bitmap);
        local_bitmap = NULL;
    }

    return exit_status;
}

int mca_coll_ftbasic_iagreement_allreduce(ompi_communicator_t* comm,
                                          ompi_group_t **group,
                                          int *flag,
                                          mca_coll_base_module_t *module,
                                          ompi_request_t **request)
{
    int ret, exit_status = OMPI_SUCCESS;
    mca_coll_ftbasic_module_t *ftbasic_module = (mca_coll_ftbasic_module_t*) module;
    opal_bitmap_t *local_bitmap = NULL;

    local_bitmap = OBJ_NEW(opal_bitmap_t);

    /*
     * Common setup before the agreement operation
     */
    ret = mca_coll_ftbasic_agreement_base_setup_nonblocking(comm, group, flag,
                                                            local_bitmap, ftbasic_module, request);
    if( OMPI_SUCCESS != ret ) {
        exit_status = ret;
        goto cleanup;
    }
    if( 1 == ompi_comm_num_active_local(comm) ) {
        goto cleanup;
    }

    /*
     * Core agreement operation
     */
    do {
        ret = internal_iagreement_allreduce(comm, group, flag,
                                            local_bitmap, ftbasic_module,
                                            ftbasic_module->agreement_info->cur_request);
        /* Success, continue */
        if( OMPI_SUCCESS == ret ) { break; }
        /* FAIL_STOP if peer failure, try again */
        else if( MPI_ERR_PROC_FAILED == ret ) { continue; }
        /* Some other error, just fail */
        else { break; }
    } while(1);

    /*
     * The protocols should be reliable, so a failure at this point should
     * not happen. If it does report and bail.
     */
    if( OMPI_SUCCESS != ret ) {
        opal_output(0, "%s ftbasic: agreement) (base  ) Error: Reliable consensus protocol failed (should never happen) ret = %d",
                    ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret);
        exit_status = ret;
        goto cleanup;
    }

 cleanup:
    /* Ok to delete this here, since we cache the value on the request object */
    if( NULL != local_bitmap ) {
        OBJ_RELEASE(local_bitmap);
        local_bitmap = NULL;
    }

    return exit_status;
}


/*************************************
 * Allreduce algorithm
 * Note: Not meant to recover from failures, just here for benchmarking
 *************************************/
static int internal_agreement_allreduce(ompi_communicator_t* comm,
                                        ompi_group_t **group,
                                        int *flag,
                                        opal_bitmap_t *local_bitmap,
                                        mca_coll_ftbasic_module_t *ftbasic_module)
{
    int ret, exit_status = OMPI_SUCCESS;
    int rank;
    int size;
    int packet_size = 0;
    opal_bitmap_t *tmp_bitmap = NULL;
#if OPAL_ENABLE_DEBUG
    char *tmp_bitstr = NULL;
#endif

    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_AR_SETUP);

    rank = ompi_comm_rank(comm);
    size = ompi_comm_size(comm);

    packet_size = local_bitmap->array_size;


    /************************************************
     * Choose a root (lowest, non-failed process)
     ************************************************/
    root = -1;
    COLL_FTBASIC_FIND_LOWEST_ALIVE_BASE(comm, size, root);

    OPAL_OUTPUT_VERBOSE((5, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement) (allreduce) Using Root = %d",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), root));

    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_AR_SETUP);
    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_AR_GATHER);


    /************************************************
     * Reduce/Gather to the root
     ************************************************/
#if OPAL_ENABLE_DEBUG
    tmp_bitstr = opal_bitmap_get_string(local_bitmap);
    OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                         "%s ftbasic:agreement (allreduce) Reduce/Gather from all peers (Seed [%s])",
                         ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), tmp_bitstr ));
    if( NULL != tmp_bitstr ) {
        free(tmp_bitstr);
        tmp_bitstr = NULL;
    }
#endif

    tmp_bitmap = OBJ_NEW(opal_bitmap_t);
    opal_bitmap_init(tmp_bitmap, size + FTBASIC_AGREEMENT_EXTRA_BITS);
    ret = comm->c_coll.coll_reduce((local_bitmap->bitmap),
                                   (tmp_bitmap->bitmap),
                                   packet_size, MPI_UNSIGNED_CHAR,
                                   ftbasic_agreement_allreduce_op,
                                   root, comm, &(ftbasic_module->super));
    opal_bitmap_copy(local_bitmap, tmp_bitmap);
    if( OMPI_SUCCESS != ret ) {
        opal_output_verbose(1, ompi_ftmpi_output_handle,
                            "%s ftbasic:agreement (allreduce) Error: Reduce/Gather Failed (ret = %3d) - Abort",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret);
        exit_status = ret;
        goto cleanup;
    }


    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_AR_GATHER);
    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_AR_DECIDE);


    /************************************************
     * Generate the result
     ************************************************/
    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_AR_DECIDE);
    AGREEMENT_START_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_AR_BCAST);


    /************************************************
     * Bcast the result
     ************************************************/
#if OPAL_ENABLE_DEBUG
    if( rank == root ) {
        if( NULL != tmp_bitstr ) {
            free(tmp_bitstr);
            tmp_bitstr = NULL;
        }
        tmp_bitstr = opal_bitmap_get_string(local_bitmap);
        OPAL_OUTPUT_VERBOSE((3, ompi_ftmpi_output_handle,
                             "%s ftbasic:agreement (allreduce) Bcast Bitmap [%s] to all peers",
                             ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), tmp_bitstr ));
    }
#endif

    ret = comm->c_coll.coll_bcast( (local_bitmap->bitmap), packet_size, MPI_UNSIGNED_CHAR,
                                   root, comm, &(ftbasic_module->super));
    if( OMPI_SUCCESS != ret ) {
        opal_output_verbose(1, ompi_ftmpi_output_handle,
                            "%s ftbasic:agreement (allreduce) Error: Bcast Failed (ret = %3d) - Abort",
                            ORTE_NAME_PRINT(ORTE_PROC_MY_NAME), ret);
        exit_status = ret;
        goto cleanup;
    }

    AGREEMENT_END_TIMER(COLL_FTBASIC_AGREEMENT_TIMER_AR_BCAST);

 cleanup:
#if OPAL_ENABLE_DEBUG
    if( NULL != tmp_bitstr ) {
        free(tmp_bitstr);
        tmp_bitstr = NULL;
    }
#endif

    if( NULL != tmp_bitmap ) {
        OBJ_RELEASE(tmp_bitmap);
        tmp_bitmap = NULL;
    }

    return exit_status;
}

static int internal_iagreement_allreduce(ompi_communicator_t* comm,
                                         ompi_group_t **group,
                                         int *flag,
                                         opal_bitmap_t *local_bitmap,
                                         mca_coll_ftbasic_module_t *ftbasic_module,
                                         struct mca_coll_ftbasic_request_t *collreq)
{
    return OMPI_ERR_NOT_SUPPORTED;
}

static void ftbasic_agreement_allreduce_op_fn(void * invec, void *inoutvec, int *len, struct ompi_datatype_t **datatype)
{
    unsigned char *in  = (unsigned char *) invec;
    unsigned char *out = (unsigned char *) inoutvec;
#if 0
    int i;

    for(i = 0; i < *count - 1; ++i) {
        *(out++) |= *(in++);
    }
    *(out++) &= *(in++);
#else
    ompi_op_reduce(MPI_LOR, invec, inoutvec, (*len)-1, *datatype);

    in  += (*len)-1;
    out += (*len)-1;

    /* JJH: I do not know why the following will not work properly:
     *   ompi_op_reduce(MPI_LAND, in, out,       1, *datatype);
     * But do the following instead.
     */
    *out = (*in &= *out);
#endif
}
