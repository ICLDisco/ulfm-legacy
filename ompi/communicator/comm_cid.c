/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2008 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2007      Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2007      Voltaire All rights reserved.
 * Copyright (c) 2006-2010 University of Houston.  All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc.  All rights reserved.
 * Copyright (c) 2012      Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "ompi_config.h"

#include "opal/dss/dss.h"
#include "orte/types.h"
#include "ompi/proc/proc.h" 
#include "ompi/communicator/communicator.h"
#include "ompi/op/op.h"
#include "ompi/constants.h"
#include "opal/class/opal_pointer_array.h"
#include "opal/class/opal_list.h"
#include "opal/util/opal_sos.h"
#include "ompi/mca/pml/pml.h"
#include "ompi/mca/coll/base/base.h"
#include "ompi/request/request.h"
#include "ompi/runtime/ompi_module_exchange.h" 
#include "ompi/runtime/mpiruntime.h"
#include "ompi/mca/dpm/dpm.h"

#include "orte/mca/rml/rml.h"
#include "orte/mca/errmgr/errmgr.h"

BEGIN_C_DECLS

/**
 * These functions make sure, that we determine the global result over 
 * an intra communicators (simple), an inter-communicator and a
 * pseudo inter-communicator described by two separate intra-comms
 * and a bridge-comm (intercomm-create scenario).
 */


typedef int ompi_comm_cid_allredfct (int *inbuf, int* outbuf, 
                                     int count, struct ompi_op_t *op, 
                                     ompi_communicator_t *comm,
                                     ompi_communicator_t *bridgecomm, 
                                     void* lleader, void* rleader, 
                                     int send_first );

static int ompi_comm_allreduce_intra (int *inbuf, int* outbuf, 
                                      int count, struct ompi_op_t *op, 
                                      ompi_communicator_t *intercomm,
                                      ompi_communicator_t *bridgecomm, 
                                      void* local_leader, 
                                      void* remote_ledaer,
                                      int send_first );

static int ompi_comm_allreduce_inter (int *inbuf, int *outbuf, 
                                      int count, struct ompi_op_t *op, 
                                      ompi_communicator_t *intercomm,
                                      ompi_communicator_t *bridgecomm, 
                                      void* local_leader, 
                                      void* remote_leader,
                                      int send_first );

static int ompi_comm_allreduce_intra_bridge(int *inbuf, int* outbuf, 
                                            int count, struct ompi_op_t *op, 
                                            ompi_communicator_t *intercomm,
                                            ompi_communicator_t *bridgecomm, 
                                            void* local_leader, 
                                            void* remote_leader,
                                            int send_first);

static int ompi_comm_allreduce_intra_oob (int *inbuf, int* outbuf, 
                                          int count, struct ompi_op_t *op, 
                                          ompi_communicator_t *intercomm,
                                          ompi_communicator_t *bridgecomm, 
                                          void* local_leader, 
                                          void* remote_leader, 
                                          int send_first );

static int      ompi_comm_register_cid (uint32_t contextid);
static int      ompi_comm_unregister_cid (uint32_t contextid);
static uint32_t ompi_comm_lowest_cid ( void );

struct ompi_comm_reg_t{
    opal_list_item_t super;
    uint32_t           cid;
};
typedef struct ompi_comm_reg_t ompi_comm_reg_t;
OMPI_DECLSPEC OBJ_CLASS_DECLARATION(ompi_comm_reg_t);

static void ompi_comm_reg_constructor(ompi_comm_reg_t *regcom);
static void ompi_comm_reg_destructor(ompi_comm_reg_t *regcom);

OBJ_CLASS_INSTANCE (ompi_comm_reg_t,
                    opal_list_item_t,
                    ompi_comm_reg_constructor,
                    ompi_comm_reg_destructor );

static opal_mutex_t ompi_cid_lock;
static opal_list_t  ompi_registered_comms;
static ompi_op_t   *ompi_cid_redux_op = NULL;

static int cid_redux_nb_it = 0;
static int cid_redux_nb_calls = 0;

/* This variable is zero (false) if all processes in MPI_COMM_WORLD
 * did not require MPI_THREAD_MULTIPLE support, and is 1 (true) as
 * soon as at least one process requested support for THREAD_MULTIPLE */
static int ompi_comm_world_thread_level_mult=0;

typedef struct {
    uint32_t cid;
    uint32_t epoch;
} cid_redux_elem_t;
static int cid_redux_size = 4;
static cid_redux_elem_t *cid_lCIDs = NULL, *cid_gCIDs = NULL;

/**
 * cid 0 is hard-coded to MPI_COMM_WORLD that cannot be freed.
 * Thus, we use CID 0 to denote "unavailable CID".
 *
 * Input:  the first pair holds the max of the CIDs locally considered. The other pairs are CIDs
 *         (plus epochs) that are locally available
 * Output: The first pair holds the max of all considered, the other pairs are CID / Epoch that
 *         are globally available. CIDs that are not available on all ranks are simply removed.
 */
static void cid_redux_fn(void *_in, void *_out, int *dcount, struct ompi_datatype_t **dt)
{
    cid_redux_elem_t *in = (cid_redux_elem_t*)_in;
    cid_redux_elem_t *out = (cid_redux_elem_t*)_out;
    int i, j, m, n;

    assert(cid_redux_size * 2 == *dcount);

    /* first CID is simply a max. It's used to decide where to start
     * from if a second iteration is necessary */
    if( in[0].cid > out[0].cid )
        out[0].cid = in[0].cid;

    if( out[1].cid == 0 ) {
        return;
    }
    if( in[1].cid == 0 ) {
        /** Damned: we already reduced to no available CID in the proposed list.
         *  The output doesn't change.
         */
        for(i = 1; i < *dcount / 2; i++) out[i].cid = 0;
        return;
    }

    n = (*dcount / 2)-1;
    while( n > 0 && out[n].cid == 0 ) n--;
    m = (*dcount / 2)-1;
    while( m > 0 && in[m].cid == 0 ) m--;

    for(j = 1; j <= n; j++) {
        for(i = 1; i <= m; i++) {
            if( out[j].cid == in[i].cid ) {
                if( in[i].epoch > out[j].epoch )
                    out[j].epoch = in[i].epoch;
                break;
            }
        }
        if(i > m) {
            out[j].cid = out[n].cid;
            out[j].epoch = out[n].epoch;
            out[n].cid = 0;
            j--;
            n--;
        }
    }
}

int ompi_comm_cid_init (void)
{
    ompi_proc_t **procs, *thisproc;
    uint8_t thread_level;
    void *tlpointer;
    int ret;
    size_t i, size, numprocs;
    int value;

    mca_base_param_reg_int_name("ompi", "cid_redux_size",
                                "Number of context IDs that are considered in a single allreduce for collective allocation (>=2)",
                                false, false, 4, &value);
    if( value < 2 ) value = 2;
    cid_redux_size = value;
    cid_lCIDs = (cid_redux_elem_t *)malloc(cid_redux_size * sizeof(cid_redux_elem_t));
    cid_gCIDs = (cid_redux_elem_t *)malloc(cid_redux_size * sizeof(cid_redux_elem_t));

    /** Note that the following call only returns processes
     * with the same jobid. This is on purpose, since 
     * we switch for the dynamic communicators anyway 
     * to the original (slower) cid allocation algorithm.
     */ 
    procs = ompi_proc_world ( &numprocs );
    ompi_cid_redux_op = ompi_op_create_user( 1, (ompi_op_fortran_handler_fn_t*)cid_redux_fn );

    for ( i=0; i<numprocs; i++ ) {
        thisproc = procs[i];
       
        ret = ompi_modex_recv_string("MPI_THREAD_LEVEL", thisproc, &tlpointer, &size);
        if (OMPI_SUCCESS == ret) {
            thread_level = *((uint8_t *) tlpointer);
            if ( OMPI_THREADLEVEL_IS_MULTIPLE (thread_level) ) {
                ompi_comm_world_thread_level_mult = 1;
                break;
            }
        } else if (OMPI_ERR_NOT_IMPLEMENTED == OPAL_SOS_GET_ERROR_CODE(ret)) {
            if (ompi_mpi_thread_multiple) {
                ompi_comm_world_thread_level_mult = 1;
            }
            break;
        } else {
            return ret;
        }
    }
    free(procs);

    return OMPI_SUCCESS;
}

int ompi_comm_nextcid ( ompi_communicator_t* newcomm, 
                        ompi_communicator_t* comm, 
                        ompi_communicator_t* bridgecomm, 
                        void* local_leader,
                        void* remote_leader,
                        int mode, int send_first )
{
    int ret;
    int nextcid;
    bool flag;
    int done=0;
    int start;
    unsigned int i, nbredux, nextcid_i;
    ompi_comm_cid_allredfct* allredfnct;
    void *location;
#if OPAL_ENABLE_FT_MPI
    int nextepoch;
#endif  /* OPAL_ENABLE_FT_MPI */

    /**
     * Determine which implementation of allreduce we have to use
     * for the current scenario
     */
    cid_redux_nb_calls++;

    switch (mode)
        {
        case OMPI_COMM_CID_INTRA:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra;
            break;
        case OMPI_COMM_CID_INTER:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_inter;
            break;
        case OMPI_COMM_CID_INTRA_BRIDGE:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra_bridge;
            break;
        case OMPI_COMM_CID_INTRA_OOB:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra_oob;
            break;
#if OPAL_ENABLE_FT_MPI
        case OMPI_COMM_CID_INTRA_FT:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra_ft;
            break;
        case OMPI_COMM_CID_INTER_FT:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_inter_ft;
            break;
        case OMPI_COMM_CID_INTRA_BRIDGE_FT:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra_bridge_ft;
            break;
        case OMPI_COMM_CID_INTRA_OOB_FT:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra_oob_ft;
            break;
#endif /* OPAL_ENABLE_FT_MPI */
        default:
            return MPI_UNDEFINED;
            break;
        }

    do {
        /* Only one communicator function allowed in same time on the
         * same communicator.
         */
        OPAL_THREAD_LOCK(&ompi_cid_lock);
        ret = ompi_comm_register_cid (comm->c_contextid);
        OPAL_THREAD_UNLOCK(&ompi_cid_lock);
    } while (OMPI_SUCCESS != ret );
    start = ompi_mpi_communicators.lowest_free;

    while (!done) {
        OPAL_THREAD_LOCK(&ompi_cid_lock);
        if (comm->c_contextid != ompi_comm_lowest_cid() ) {
            /* if not lowest cid, we do not continue, but sleep and try again */
            OPAL_THREAD_UNLOCK(&ompi_cid_lock);
            continue;
        }
        OPAL_THREAD_UNLOCK(&ompi_cid_lock);
        cid_redux_nb_it++;

        nbredux = 1;
        for (i=start; i < mca_pml.pml_max_contextid ; i++) {
            flag = opal_pointer_array_test_and_set_item(&ompi_mpi_communicators, 
                                                        i, comm);
            if (true == flag) {
                cid_lCIDs[nbredux].cid = i;
#if OPAL_ENABLE_FT_MPI
                location = opal_pointer_array_get_item(&ompi_mpi_comm_epoch, i);
                cid_lCIDs[nbredux].epoch = (int)((uintptr_t)location);
#endif  /* OPAL_ENABLE_FT_MPI */
                nbredux++;
                if( nbredux == cid_redux_size )
                    break;
            }
        }
        for(; nbredux < cid_redux_size; nbredux++) 
            cid_lCIDs[nbredux].cid = 0;
        cid_lCIDs[0].cid = i;

        ret = (allredfnct)((int*)cid_lCIDs, (int*)cid_gCIDs, 2*nbredux, ompi_cid_redux_op, comm, bridgecomm,
                           local_leader, remote_leader, send_first );

        /* Take the min */
        nextcid_i = 1;
        for(i = 2; i < cid_redux_size; i++)
            if( cid_gCIDs[i].cid < cid_gCIDs[nextcid_i].cid )
                nextcid_i = i;
        nextcid = cid_gCIDs[nextcid_i].cid;

#if OPAL_ENABLE_FT_MPI
        nextepoch = cid_gCIDs[nextcid_i].epoch;
#endif /* OPAL_ENABLE_FT_MPI */

        if( OMPI_SUCCESS != ret ) {
            for(nbredux = 1; nbredux < cid_redux_size && cid_lCIDs[nbredux].cid != 0; nbredux++)
                opal_pointer_array_set_item(&ompi_mpi_communicators, cid_lCIDs[nbredux].cid, NULL);
            goto release_and_return;
        }

        if (nextcid == 0) {
            for(nbredux = 1; nbredux < cid_redux_size && cid_lCIDs[nbredux].cid != 0; nbredux++)
                opal_pointer_array_set_item(&ompi_mpi_communicators, cid_lCIDs[nbredux].cid, NULL);
            start = cid_gCIDs[0].cid;
            continue;
        }
        
        for(nbredux = 1; nbredux < cid_redux_size && cid_lCIDs[nbredux].cid != 0; nbredux++)
            if( cid_lCIDs[nbredux].cid != nextcid )
                opal_pointer_array_set_item(&ompi_mpi_communicators, cid_lCIDs[nbredux].cid, NULL);
        done = 1;  /* we are done */
    }

    /* set the according values to the newcomm */
    newcomm->c_contextid = nextcid;
    newcomm->c_f_to_c_index = newcomm->c_contextid;
    opal_pointer_array_set_item (&ompi_mpi_communicators, nextcid, newcomm);
#if OPAL_ENABLE_FT_MPI
    newcomm->c_epoch = nextepoch + 1;
    opal_pointer_array_set_item(&ompi_mpi_comm_epoch, nextcid, (void*)(uintptr_t)newcomm->c_epoch);
#endif  /* OPAL_ENABLE_FT_MPI */

 release_and_return:
    OPAL_THREAD_LOCK(&ompi_cid_lock);
    ompi_comm_unregister_cid (comm->c_contextid);
    OPAL_THREAD_UNLOCK(&ompi_cid_lock);

    return ret;
}

/**************************************************************************/
/**************************************************************************/
/**************************************************************************/
static void ompi_comm_reg_constructor (ompi_comm_reg_t *regcom)
{
    regcom->cid=MPI_UNDEFINED;
}

static void ompi_comm_reg_destructor (ompi_comm_reg_t *regcom)
{
}

void ompi_comm_reg_init (void)
{
    OBJ_CONSTRUCT(&ompi_registered_comms, opal_list_t);
    OBJ_CONSTRUCT(&ompi_cid_lock, opal_mutex_t);
}

void ompi_comm_reg_finalize (void)
{
    OBJ_DESTRUCT(&ompi_registered_comms);
    OBJ_DESTRUCT(&ompi_cid_lock);
}


static int ompi_comm_register_cid (uint32_t cid )
{
    opal_list_item_t *item;
    ompi_comm_reg_t *regcom;
    ompi_comm_reg_t *newentry = OBJ_NEW(ompi_comm_reg_t);

    newentry->cid = cid;
    if ( !(opal_list_is_empty (&ompi_registered_comms)) ) {
        for (item = opal_list_get_first(&ompi_registered_comms);
             item != opal_list_get_end(&ompi_registered_comms);
             item = opal_list_get_next(item)) {
            regcom = (ompi_comm_reg_t *)item;
            if ( regcom->cid > cid ) {
                break;
            }
#if OMPI_ENABLE_THREAD_MULTIPLE
            if( regcom->cid == cid ) {
                /**
                 * The MPI standard state that is the user responsability to
                 * schedule the global communications in order to avoid any
                 * kind of troubles. As, managing communicators involve several
                 * collective communications, we should enforce a sequential
                 * execution order. This test only allow one communicator
                 * creation function based on the same communicator.
                 */
                OBJ_RELEASE(newentry);
                return OMPI_ERROR;
            }
#endif  /* OMPI_ENABLE_THREAD_MULTIPLE */
        }
        opal_list_insert_pos (&ompi_registered_comms, item, 
                              (opal_list_item_t *)newentry);
    }
    else {
        opal_list_append (&ompi_registered_comms, (opal_list_item_t *)newentry);
    }

    return OMPI_SUCCESS;
}

static int ompi_comm_unregister_cid (uint32_t cid)
{
    ompi_comm_reg_t *regcom;
    opal_list_item_t *item;

    for (item = opal_list_get_first(&ompi_registered_comms);
         item != opal_list_get_end(&ompi_registered_comms);
         item = opal_list_get_next(item)) {
        regcom = (ompi_comm_reg_t *)item;
        if(regcom->cid == cid) {
            opal_list_remove_item(&ompi_registered_comms, item);
            OBJ_RELEASE(regcom);
            break;
        }
    }
    return OMPI_SUCCESS;
}

static uint32_t ompi_comm_lowest_cid (void)
{
    ompi_comm_reg_t *regcom=NULL;
    opal_list_item_t *item=opal_list_get_first (&ompi_registered_comms);

    regcom = (ompi_comm_reg_t *)item;
    return regcom->cid;
}
/**************************************************************************/
/**************************************************************************/
/**************************************************************************/
/* This routine serves two purposes:
 * - the allreduce acts as a kind of Barrier,
 *   which avoids, that we have incoming fragments 
 *   on the new communicator before everybody has set
 *   up the comm structure.
 * - some components (e.g. the collective MagPIe component
 *   might want to generate new communicators and communicate
 *   using the new comm. Thus, it can just be called after
 *   the 'barrier'.
 *
 * The reason that this routine is in comm_cid and not in
 * comm.c is, that this file contains the allreduce implementations
 * which are required, and thus we avoid having duplicate code...
 */
int ompi_comm_activate ( ompi_communicator_t** newcomm, 
                         ompi_communicator_t* comm,
                         ompi_communicator_t* bridgecomm,
                         void* local_leader,
                         void* remote_leader,
                         int mode,
                         int send_first )
{
    int ret = 0;

    int ok=0, gok=0;
    ompi_comm_cid_allredfct* allredfnct;

    /* Step 1: the barrier, after which it is allowed to
     * send messages over the new communicator
     */
    switch (mode)
        {
        case OMPI_COMM_CID_INTRA:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra;
            break;
        case OMPI_COMM_CID_INTER:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_inter;
            break;
        case OMPI_COMM_CID_INTRA_BRIDGE:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra_bridge;
            break;
        case OMPI_COMM_CID_INTRA_OOB:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra_oob;
            break;
#if OPAL_ENABLE_FT_MPI
        case OMPI_COMM_CID_INTRA_FT:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra_ft;
            break;
        case OMPI_COMM_CID_INTER_FT:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_inter_ft;
            break;
        case OMPI_COMM_CID_INTRA_BRIDGE_FT:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra_bridge_ft;
            break;
        case OMPI_COMM_CID_INTRA_OOB_FT:
            allredfnct=(ompi_comm_cid_allredfct*)ompi_comm_allreduce_intra_oob_ft;
            break;
#endif /* OPAL_ENABLE_FT_MPI */
        default:
            return MPI_UNDEFINED;
            break;
        }

    if (MPI_UNDEFINED != (*newcomm)->c_local_group->grp_my_rank) {

        /* Initialize the PML stuff in the newcomm  */
        if ( OMPI_SUCCESS != (ret = MCA_PML_CALL(add_comm(*newcomm))) ) {
            goto bail_on_error;
        }
        OMPI_COMM_SET_PML_ADDED(*newcomm);
    }


    ret = (allredfnct)(&ok, &gok, 1, MPI_MIN, comm, bridgecomm,
                       local_leader, remote_leader, send_first );
    if( OMPI_SUCCESS != ret ) {
        goto bail_on_error;
    }

    /**
     * Check to see if this process is in the new communicator.
     *
     * Specifically, this function is invoked by all proceses in the
     * old communicator, regardless of whether they are in the new
     * communicator or not.  This is because it is far simpler to use
     * MPI collective functions on the old communicator to determine
     * some data for the new communicator (e.g., remote_leader) than
     * to kludge up our own pseudo-collective routines over just the
     * processes in the new communicator.  Hence, *all* processes in
     * the old communicator need to invoke this function.
     *
     * That being said, only processes in the new communicator need to
     * select a coll module for the new communicator.  More
     * specifically, proceses who are not in the new communicator
     * should *not* select a coll module -- for example,
     * ompi_comm_rank(newcomm) returns MPI_UNDEFINED for processes who
     * are not in the new communicator.  This can cause errors in the
     * selection / initialization of a coll module.  Plus, it's
     * wasteful -- processes in the new communicator will end up
     * freeing the new communicator anyway, so we might as well leave
     * the coll selection as NULL (the coll base comm unselect code
     * handles that case properly).
     */
    if (MPI_UNDEFINED == (*newcomm)->c_local_group->grp_my_rank) {
        return OMPI_SUCCESS;
    }

    /* Let the collectives components fight over who will do
       collective on this new comm.  */
    if (OMPI_SUCCESS != (ret = mca_coll_base_comm_select(*newcomm))) {
        goto bail_on_error;
    }

    /* For an inter communicator, we have to deal with the potential
     * problem of what is happening if the local_comm that we created
     * has a lower CID than the parent comm. This is not a problem
     * as long as the user calls MPI_Comm_free on the inter communicator.
     * However, if the communicators are not freed by the user but released
     * by Open MPI in MPI_Finalize, we walk through the list of still available
     * communicators and free them one by one. Thus, local_comm is freed before
     * the actual inter-communicator. However, the local_comm pointer in the
     * inter communicator will still contain the 'previous' address of the local_comm
     * and thus this will lead to a segmentation violation. In order to prevent
     * that from happening, we increase the reference counter local_comm
     * by one if its CID is lower than the parent. We cannot increase however
     *  its reference counter if the CID of local_comm is larger than
     * the CID of the inter communicators, since a regular MPI_Comm_free would
     * leave in that the case the local_comm hanging around and thus we would not
     * recycle CID's properly, which was the reason and the cause for this trouble.
     */
    if ( OMPI_COMM_IS_INTER(*newcomm)) {
        if ( OMPI_COMM_CID_IS_LOWER(*newcomm, comm)) {
            OMPI_COMM_SET_EXTRA_RETAIN (*newcomm);
            OBJ_RETAIN (*newcomm);
        }
    }


    return OMPI_SUCCESS;

 bail_on_error:
    OBJ_RELEASE(*newcomm);
    *newcomm = MPI_COMM_NULL;
    return ret;
}                         

/**************************************************************************/
/**************************************************************************/
/**************************************************************************/
/* Arguments not used in this implementation:
 *  - bridgecomm
 *  - local_leader
 *  - remote_leader
 *  - send_first
 */
static int ompi_comm_allreduce_intra ( int *inbuf, int *outbuf, 
                                       int count, struct ompi_op_t *op, 
                                       ompi_communicator_t *comm,
                                       ompi_communicator_t *bridgecomm, 
                                       void* local_leader, 
                                       void* remote_leader, 
                                       int send_first )
{
    return comm->c_coll.coll_allreduce ( inbuf, outbuf, count, MPI_INT, 
                                         op,comm,
                                         comm->c_coll.coll_allreduce_module );
}

/* Arguments not used in this implementation:
 *  - bridgecomm
 *  - local_leader
 *  - remote_leader
 *  - send_first
 */
static int ompi_comm_allreduce_inter ( int *inbuf, int *outbuf, 
                                       int count, struct ompi_op_t *op, 
                                       ompi_communicator_t *intercomm,
                                       ompi_communicator_t *bridgecomm, 
                                       void* local_leader, 
                                       void* remote_leader, 
                                       int send_first )
{
    int local_rank, rsize;
    int i, rc;
    int *sbuf;
    int *tmpbuf=NULL;
    int *rcounts=NULL, scount=0;
    int *rdisps=NULL;

    if ( !OMPI_COMM_IS_INTER (intercomm)) {
        return MPI_ERR_COMM;
    }

    /* Allocate temporary arrays */
    rsize      = ompi_comm_remote_size (intercomm);
    local_rank = ompi_comm_rank ( intercomm );

    tmpbuf  = (int *) malloc ( count * sizeof(int));
    rdisps  = (int *) calloc ( rsize, sizeof(int));
    rcounts = (int *) calloc ( rsize, sizeof(int) );
    if ( OPAL_UNLIKELY (NULL == tmpbuf || NULL == rdisps || NULL == rcounts)) {
        rc = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

    /* Execute the inter-allreduce: the result of our group will
       be in the buffer of the remote group */
    rc = intercomm->c_coll.coll_allreduce ( inbuf, tmpbuf, count, MPI_INT,
                                            op, intercomm,
                                            intercomm->c_coll.coll_allreduce_module);
    if ( OMPI_SUCCESS != rc ) {
        goto exit;
    }

    if ( 0 == local_rank ) {
        MPI_Request req;

        /* for the allgatherv later */
        scount = count;

        /* local leader exchange their data and determine the overall result
           for both groups */
        rc = MCA_PML_CALL(irecv (outbuf, count, MPI_INT, 0, 
                                 OMPI_COMM_ALLREDUCE_TAG,
                                 intercomm, &req));
        if ( OMPI_SUCCESS != rc ) {
            goto exit;
        }
        rc = MCA_PML_CALL(send (tmpbuf, count, MPI_INT, 0,
                                OMPI_COMM_ALLREDUCE_TAG,
                                MCA_PML_BASE_SEND_STANDARD,
                                intercomm));
        if ( OMPI_SUCCESS != rc ) {
            goto exit;
        }
        rc = ompi_request_wait ( &req, MPI_STATUS_IGNORE );
        if ( OMPI_SUCCESS != rc ) {
            goto exit;
        }

        ompi_op_reduce( op, tmpbuf, outbuf, count, &ompi_mpi_int.dt);
    }

    /* distribute the overall result to all processes in the other group.
       Instead of using bcast, we are using here allgatherv, to avoid the
       possible deadlock. Else, we need an algorithm to determine, 
       which group sends first in the inter-bcast and which receives 
       the result first.
    */
    rcounts[0] = count;
    sbuf       = outbuf;
    rc = intercomm->c_coll.coll_allgatherv (sbuf, scount, MPI_INT, outbuf,
                                            rcounts, rdisps, MPI_INT, 
                                            intercomm,
                                            intercomm->c_coll.coll_allgatherv_module);

 exit:
    if ( NULL != tmpbuf ) {
        free ( tmpbuf );
    }
    if ( NULL != rcounts ) {
        free ( rcounts );
    }
    if ( NULL != rdisps ) {
        free ( rdisps );
    }
    
    return (rc);
}

/* Arguments not used in this implementation:
 * - send_first
 */
static int ompi_comm_allreduce_intra_bridge (int *inbuf, int *outbuf, 
                                             int count, struct ompi_op_t *op, 
                                             ompi_communicator_t *comm,
                                             ompi_communicator_t *bcomm, 
                                             void* lleader, void* rleader,
                                             int send_first )
{
    int *tmpbuf=NULL;
    int local_rank;
    int i;
    int rc;
    int local_leader, remote_leader;

    local_leader  = (*((int*)lleader));
    remote_leader = (*((int*)rleader));

    local_rank = ompi_comm_rank ( comm );
    tmpbuf     = (int *) malloc ( count * sizeof(int));
    if ( NULL == tmpbuf ) {
        rc = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

    /* Intercomm_create */
    rc = comm->c_coll.coll_allreduce ( inbuf, tmpbuf, count, MPI_INT,
                                       op, comm, comm->c_coll.coll_allreduce_module );
    if ( OMPI_SUCCESS != rc ) {
#if OPAL_ENABLE_FT_MPI
        if( local_rank != local_leader ) {
            goto exit;
        }
#else
        goto exit;
#endif  /* OPAL_ENABLE_FT_MPI */
    }

    if (local_rank == local_leader ) {
        MPI_Request req;

        rc = MCA_PML_CALL(irecv ( outbuf, count, MPI_INT, remote_leader,
                                  OMPI_COMM_ALLREDUCE_TAG, 
                                  bcomm, &req));
        if ( OMPI_SUCCESS == rc ) {
            rc = MCA_PML_CALL(send (tmpbuf, count, MPI_INT, remote_leader, 
                                    OMPI_COMM_ALLREDUCE_TAG,
                                    MCA_PML_BASE_SEND_STANDARD,  bcomm));
            if ( OMPI_SUCCESS != rc ) {
#if OPAL_ENABLE_FT_MPI
                if( MPI_ERR_PROC_FAILED == rc ) {
                    /* The peer is dead, continue with the local decision */
                    ompi_datatype_copy_content_same_ddt(MPI_INT, count, outbuf, tmpbuf);
                    goto skip_handshake;
                }
#endif  /* OPAL_ENABLE_FT_MPI */
                goto exit;
            }
  skip_handshake:
            rc = ompi_request_wait(&req, MPI_STATUS_IGNORE);
            if ( OMPI_SUCCESS != rc ) {
#if OPAL_ENABLE_FT_MPI
                if( MPI_ERR_PROC_FAILED == rc ) {
                    /* The peer is dead, continue with the local decision */
                    ompi_datatype_copy_content_same_ddt(MPI_INT, count, outbuf, tmpbuf);
                    /* Let it go don't break the leader execution flow here */
                } else
#endif  /* OPAL_ENABLE_FT_MPI */
                goto exit;
            }
        }

        ompi_op_reduce(op, tmpbuf, outbuf, count, &ompi_mpi_int.dt);
    }

    rc = comm->c_coll.coll_bcast ( outbuf, count, MPI_INT, local_leader, 
                                   comm, comm->c_coll.coll_bcast_module );

 exit:
    if (NULL != tmpbuf ) {
        free (tmpbuf);
    }

    return (rc);
}

/* Arguments not used in this implementation:
 *    - bridgecomm
 *
 * lleader is the local rank of root in comm
 * rleader is the OOB contact information of the
 * root processes in the other world.
 */
static int ompi_comm_allreduce_intra_oob (int *inbuf, int *outbuf, 
                                          int count, struct ompi_op_t *op, 
                                          ompi_communicator_t *comm,
                                          ompi_communicator_t *bridgecomm, 
                                          void* lleader, void* rleader,
                                          int send_first )
{
    int *tmpbuf=NULL;
    int i;
    int rc;
    int local_leader, local_rank;
    orte_process_name_t *remote_leader=NULL;
    orte_std_cntr_t size_count;

    local_leader  = (*((int*)lleader));
    remote_leader = (orte_process_name_t*)rleader;
    size_count = count;

    local_rank = ompi_comm_rank ( comm );
    tmpbuf     = (int *) malloc ( count * sizeof(int));
    if ( NULL == tmpbuf ) {
        rc = OMPI_ERR_OUT_OF_RESOURCE;
        goto exit;
    }

    /* comm is an intra-communicator */
    rc = comm->c_coll.coll_allreduce(inbuf,tmpbuf,count,MPI_INT,op, comm,
                                     comm->c_coll.coll_allreduce_module);
    if ( OMPI_SUCCESS != rc ) {
        goto exit;
    }
    
    if (local_rank == local_leader ) {
        opal_buffer_t *sbuf;
        opal_buffer_t *rbuf;

        sbuf = OBJ_NEW(opal_buffer_t);
        rbuf = OBJ_NEW(opal_buffer_t);
        
        if (OPAL_SUCCESS != (rc = opal_dss.pack(sbuf, tmpbuf, (orte_std_cntr_t)count, OPAL_INT))) {
            goto exit;
        }

        if ( send_first ) {
            if (0 > (rc = orte_rml.send_buffer(remote_leader, sbuf, OMPI_RML_TAG_COMM_CID_INTRA, 0))) {
                goto exit;
            }
            if (0 > (rc = orte_rml.recv_buffer(remote_leader, rbuf, OMPI_RML_TAG_COMM_CID_INTRA, 0))) {
                goto exit;
            }
        }
        else {
            if (0 > (rc = orte_rml.recv_buffer(remote_leader, rbuf, OMPI_RML_TAG_COMM_CID_INTRA, 0))) {
                goto exit;
            }
            if (0 > (rc = orte_rml.send_buffer(remote_leader, sbuf, OMPI_RML_TAG_COMM_CID_INTRA, 0))) {
                goto exit;
            }
        }

        if (OPAL_SUCCESS != (rc = opal_dss.unpack(rbuf, outbuf, &size_count, OPAL_INT))) {
            goto exit;
        }
        OBJ_RELEASE(sbuf);
        OBJ_RELEASE(rbuf);
        count = (int)size_count;

        ompi_op_reduce( op, tmpbuf, outbuf, count, &ompi_mpi_int.dt);
    }

    rc = comm->c_coll.coll_bcast (outbuf, count, MPI_INT, 
                                  local_leader, comm,
                                  comm->c_coll.coll_bcast_module);

 exit:
    if (NULL != tmpbuf ) {
        free (tmpbuf);
    }

    return (rc);
}

END_C_DECLS
