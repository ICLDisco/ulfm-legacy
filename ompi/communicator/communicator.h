/* -*- Mode: C; c-basic-offset:4 ; -*- */
/*
 * Copyright (c) 2004-2005 The Trustees of Indiana University and Indiana
 *                         University Research and Technology
 *                         Corporation.  All rights reserved.
 * Copyright (c) 2004-2011 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 * Copyright (c) 2004-2005 High Performance Computing Center Stuttgart, 
 *                         University of Stuttgart.  All rights reserved.
 * Copyright (c) 2004-2005 The Regents of the University of California.
 *                         All rights reserved.
 * Copyright (c) 2006-2010 Cisco Systems, Inc.  All rights reserved.
 * Copyright (c) 2006-2010 University of Houston.  All rights reserved.
 * Copyright (c) 2009      Sun Microsystems, Inc. All rights reserved.
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 *
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#ifndef OMPI_COMMUNICATOR_H
#define OMPI_COMMUNICATOR_H

#include "ompi_config.h"
#include "opal/class/opal_object.h"
#include "ompi/errhandler/errhandler.h"
#include "opal/threads/mutex.h"

#include "mpi.h"
#include "ompi/group/group.h"
#include "ompi/mca/coll/coll.h"
#include "ompi/proc/proc.h"

BEGIN_C_DECLS

OMPI_DECLSPEC OBJ_CLASS_DECLARATION(ompi_communicator_t);

#define OMPI_COMM_INTER      0x00000001
#define OMPI_COMM_CART       0x00000002
#define OMPI_COMM_GRAPH      0x00000004
#define OMPI_COMM_NAMEISSET  0x00000008
#define OMPI_COMM_ISFREED    0x00000010
#define OMPI_COMM_INTRINSIC  0x00000020
#define OMPI_COMM_DYNAMIC    0x00000040
#define OMPI_COMM_INVALID    0x00000080
#define OMPI_COMM_PML_ADDED  0x00000100
#define OMPI_COMM_EXTRA_RETAIN 0x00000200

/* some utility #defines */
#define OMPI_COMM_IS_INTER(comm) ((comm)->c_flags & OMPI_COMM_INTER)
#define OMPI_COMM_IS_INTRA(comm) (!((comm)->c_flags & OMPI_COMM_INTER))
#define OMPI_COMM_IS_CART(comm) ((comm)->c_flags & OMPI_COMM_CART)
#define OMPI_COMM_IS_GRAPH(comm) ((comm)->c_flags & OMPI_COMM_GRAPH)
#define OMPI_COMM_IS_INTRINSIC(comm) ((comm)->c_flags & OMPI_COMM_INTRINSIC)
#define OMPI_COMM_IS_FREED(comm) ((comm)->c_flags & OMPI_COMM_ISFREED)
#define OMPI_COMM_IS_DYNAMIC(comm) ((comm)->c_flags & OMPI_COMM_DYNAMIC)
#define OMPI_COMM_IS_INVALID(comm) ((comm)->c_flags & OMPI_COMM_INVALID)
#define OMPI_COMM_IS_PML_ADDED(comm) ((comm)->c_flags & OMPI_COMM_PML_ADDED)
#define OMPI_COMM_IS_EXTRA_RETAIN(comm) ((comm)->c_flags & OMPI_COMM_EXTRA_RETAIN)

#define OMPI_COMM_SET_DYNAMIC(comm) ((comm)->c_flags |= OMPI_COMM_DYNAMIC)
#define OMPI_COMM_SET_INVALID(comm) ((comm)->c_flags |= OMPI_COMM_INVALID)

#define OMPI_COMM_SET_PML_ADDED(comm) ((comm)->c_flags |= OMPI_COMM_PML_ADDED)
#define OMPI_COMM_SET_EXTRA_RETAIN(comm) ((comm)->c_flags |= OMPI_COMM_EXTRA_RETAIN)

/* a set of special tags: */

/*  to recognize an MPI_Comm_join in the comm_connect_accept routine. */

#define OMPI_COMM_ALLGATHER_TAG -31078
#define OMPI_COMM_BARRIER_TAG   -31079
#define OMPI_COMM_ALLREDUCE_TAG -31080

/**
 * Modes required for acquiring the new comm-id.
 * The first (INTER/INTRA) indicates whether the
 * input comm was an inter/intra-comm, the second
 * whether the new communicator will be an inter/intra
 * comm
 */
#define OMPI_COMM_CID_INTRA        0x00000020
#define OMPI_COMM_CID_INTER        0x00000040
#define OMPI_COMM_CID_INTRA_BRIDGE 0x00000080
#define OMPI_COMM_CID_INTRA_OOB    0x00000100
#if OPAL_ENABLE_FT_MPI
#define OMPI_COMM_CID_INTRA_FT        0x00000200
#define OMPI_COMM_CID_INTER_FT        0x00000400
#define OMPI_COMM_CID_INTRA_BRIDGE_FT 0x00000800
#define OMPI_COMM_CID_INTRA_OOB_FT    0x00001000
#endif /* OPAL_ENABLE_FT_MPI */

/**
 * The block of CIDs allocated for MPI_COMM_WORLD
 * and other communicators
 */
#define OMPI_COMM_BLOCK_WORLD      16
#define OMPI_COMM_BLOCK_OTHERS     8

/* A macro comparing two CIDs */
#define OMPI_COMM_CID_IS_LOWER(comm1,comm2) ( ((comm1)->c_contextid < (comm2)->c_contextid)? 1:0)

OMPI_DECLSPEC extern opal_pointer_array_t ompi_mpi_communicators;
#if OPAL_ENABLE_FT_MPI
/**
 * This array hold the number of time each id has been used. In the case where a communicator
 * is revoked this reference count act as a epoch, and prevent us from revoking newly created
 * communicators, that use similar id to others communicators that are still revoked in the
 * system.
 */
OMPI_DECLSPEC extern opal_pointer_array_t ompi_mpi_comm_epoch;

/*
 * Callback function that should be called when there is a fault.
 *
 * This callback function will be used anytime (other than during finalize) the
 * runtime or BTLs detects and handles a process failure. The function is called
 * once per communicator that possess the failed process, and per process failure.
 *
 * @param[in] comm the communicator to which the failed process belongs
 * @param[in] rank the rank of the failed process in that communicator
 * @param[in] remote is true iff rank is a remote process
 */
typedef void (ompi_comm_rank_failure_callback_t)(struct ompi_communicator_t *comm, int rank, bool remote);

OMPI_DECLSPEC extern ompi_comm_rank_failure_callback_t *ompi_rank_failure_cbfunc;
#endif  /* OPAL_ENABLE_FT_MPI */

struct ompi_communicator_t {
    opal_object_t              c_base;
    opal_mutex_t               c_lock; /* mutex for name and potentially
                                          attributes */
    char  c_name[MPI_MAX_OBJECT_NAME];
    uint32_t              c_contextid;
    int                     c_my_rank;
    uint32_t                  c_flags; /* flags, e.g. intercomm,
                                          topology, etc. */

    int                c_id_available; /* the currently available Cid for allocation 
                                          to a child*/
    int              c_id_start_index; /* the starting index of the block of cids 
                                          allocated to this communicator*/
    uint32_t                  c_epoch;  /**< Identifier used to differenciate between two communicators
                                         *   using the same c_contextid (not at the same time, obviously) */
    ompi_group_t        *c_local_group;
    ompi_group_t       *c_remote_group;

    struct ompi_communicator_t *c_local_comm; /* a duplicate of the local 
                                                 communicator in case the comm  
                                                 is an inter-comm*/
                     
    /* Attributes */
    struct opal_hash_table_t       *c_keyhash;

    /**< inscribing cube dimension */
    int c_cube_dim;

    /* Hooks for topo module to hang things */
    mca_base_component_t *c_topo_component;

    const struct mca_topo_base_module_1_0_0_t* c_topo; 
    /**< structure of function pointers */

    struct mca_topo_base_comm_1_0_0_t* c_topo_comm; 
    /**< structure containing basic information about the topology */

    struct mca_topo_base_module_comm_t *c_topo_module;
    /**< module specific data */

    /* index in Fortran <-> C translation array */

    int c_f_to_c_index;

#ifdef OMPI_WANT_PERUSE
    /*
     * Place holder for the PERUSE events.
     */
    struct ompi_peruse_handle_t** c_peruse_handles;
#endif

    /* Error handling.  This field does not have the "c_" prefix so
       that the OMPI_ERRHDL_* macros can find it, regardless of whether
       it's a comm, window, or file. */

    ompi_errhandler_t                  *error_handler;
    ompi_errhandler_type_t             errhandler_type;

    /* Hooks for PML to hang things */
    struct mca_pml_comm_t  *c_pml_comm;

    /* Collectives module interface and data */
    mca_coll_base_comm_coll_t c_coll;

#if OPAL_ENABLE_FT_MPI
    /** Are MPI_ANY_SOURCE operations enabled? - OMPI_Comm_failure_ack */
    bool                     any_source_enabled;
    /** MPI_ANY_SOURCE Failed Group Offset - OMPI_Comm_failure_get_acked */
    int                      any_source_offset;
    /** Has this communicator been revoked - OMPI_Comm_revoke() */
    bool                     comm_revoked;
    /** Force errors to collective pt2pt operations? */
    bool                     collectives_force_error;
    /** Quick lookup */
    int                      num_active_local;
    int                      num_active_remote;
    int                      lleader;
    int                      rleader;

    ompi_group_t            *agreed_failed_ranks;
#endif /* OPAL_ENABLE_FT_MPI */
};
typedef struct ompi_communicator_t ompi_communicator_t;

/**
 * Padded struct to maintain back compatibiltiy.
 *
 * The following ompi_predefined_xxx_t structure is used to maintain
 * backwards binary compatibility for MPI applications compiled
 * against one version of OMPI library but dynamically linked at
 * runtime with another.  The issue is between versions the actual
 * structure may change in size (even between debug and optimized
 * compilation -- the structure contents change, and therefore the
 * overall size changes).
 *
 * This is problematic with predefined handles because the storage of
 * the structure ends up being located to an application's BSS.  This
 * causes problems because if one version has the predefined as size X
 * and then the application is dynamically linked with a version that
 * has a size of Y (where X != Y) then the application will
 * unintentionally overrun the memory initially allocated for the
 * structure.
 *
 * The solution we are using below creates a parent structure
 * (ompi_predefined_xxx_t) that contains the base structure
 * (ompi_xxx_t) followed by a character padding that is the size of
 * the total size we choose to preallocate for the structure minus the
 * amount used by the base structure.  In this way, we've normalized
 * the size of each predefined handle across multiple versions and
 * configurations of Open MPI (e.g., MPI_COMM_WORLD will refer to a
 * back-end struct that is X bytes long, even if we change the
 * back-end ompi_communicator_t between version A.B and version C.D in
 * Open MPI).  When we come close to filling up the the padding we can
 * add a pointer at the back end of the base structure to point to an
 * extension of the type.  Or we can just increase the padding and
 * break backwards binary compatibility.
 * 
 * The above method was decided after several failed attempts
 * described below.
 *
 * - Original implementation - suffered that the base structure seemed
 *   to always change in size between Open MPI versions and/or
 *   configurations (e.g., optimized vs. debugging build).
 *
 * - Convert all predefined handles to run-time-assigned pointers
 *   (i.e., global variables) - This worked except in cases where an MPI
 *   application wanted to assign the predefined handle value to a
 *   global variable -- we could not guarantee to have the global
 *   variable filled until MPI_INIT was called (recall that MPI
 *   predefined handles must be assignable before MPI_INIT; e.g.,
 *   "MPI_Comm foo = MPI_COMM_WORLD").
 *
 * - union of struct and padding - Similar to current implementation
 *   except using a union for the parent.  This worked except in cases
 *   where the compilers did not support C99 union static initalizers.
 *   It would have been a pain to convert a bunch of the code to use
 *   non-static initializers (e.g., MPI datatypes).
 */

/* Define for the preallocated size of the predefined handle.  
 * Note that we are using a pointer type as the base memory chunk
 * size so when the bitness changes the size of the handle changes.
 * This is done so we don't end up needing a structure that is
 * incredibly larger than necessary because of the bitness.
 */
#define PREDEFINED_COMMUNICATOR_PAD (sizeof(void*) * 128)

struct ompi_predefined_communicator_t {
    struct ompi_communicator_t comm;
    char padding[PREDEFINED_COMMUNICATOR_PAD - sizeof(ompi_communicator_t)];
};
typedef struct ompi_predefined_communicator_t ompi_predefined_communicator_t;

OMPI_DECLSPEC extern ompi_communicator_t *ompi_mpi_comm_parent;
OMPI_DECLSPEC extern ompi_predefined_communicator_t ompi_mpi_comm_world;
OMPI_DECLSPEC extern ompi_predefined_communicator_t ompi_mpi_comm_self;
OMPI_DECLSPEC extern ompi_predefined_communicator_t ompi_mpi_comm_null;

/**
 * Is this a valid communicator?  This is a complicated question.
 * :-)
 *
 * According to MPI-1:5.2.4 (p137):
 *
 * "The predefined constant MPI_COMM_NULL is the value used for
 * invalid communicator handles."
 *
 * Hence, MPI_COMM_NULL is not valid.  However, MPI-2:4.12.4 (p50)
 * clearly states that the MPI_*_C2F and MPI_*_F2C functions
 * should treat MPI_COMM_NULL as a valid communicator -- it
 * distinctly differentiates between "invalid" handles and
 * "MPI_*_NULL" handles.  Some feel that the MPI-1 definition
 * still holds for all other MPI functions; others feel that the
 * MPi-2 definitions trump the MPI-1 definition.  Regardless of
 * who is right, there is ambiguity here.  So we have left
 * ompi_comm_invalid() as originally coded -- per the MPI-1
 * definition, where MPI_COMM_NULL is an invalid communicator.
 * The MPI_Comm_c2f() function, therefore, calls
 * ompi_comm_invalid() but also explictily checks to see if the
 * handle is MPI_COMM_NULL.
 */
static inline int ompi_comm_invalid(ompi_communicator_t* comm)
{
    if ((NULL == comm) || (MPI_COMM_NULL == comm) ||
        (OMPI_COMM_IS_FREED(comm)) || (OMPI_COMM_IS_INVALID(comm)) )
        return true;
    else
        return false;
}

/**
 * rank w/in the communicator
 */
static inline int ompi_comm_rank(ompi_communicator_t* comm)
{
    return comm->c_my_rank;
}

/**
 * size of the communicator
 */
static inline int ompi_comm_size(ompi_communicator_t* comm)
{
    return comm->c_local_group->grp_proc_count;
}

/**
 * size of the remote group for inter-communicators.
 * returns zero for an intra-communicator
 */
static inline int ompi_comm_remote_size(ompi_communicator_t* comm)
{
    return (comm->c_flags & OMPI_COMM_INTER ? comm->c_remote_group->grp_proc_count : 0);
}

/** 
 * Context ID for the communicator, suitable for passing to
 * ompi_comm_lookup for getting the communicator back 
 */
static inline uint32_t ompi_comm_get_cid(ompi_communicator_t* comm) 
{
    return comm->c_contextid;
}

/* return pointer to communicator associated with context id cid,
 * No error checking is done*/
static inline ompi_communicator_t *ompi_comm_lookup(uint32_t cid)
{
    /* array of pointers to communicators, indexed by context ID */
    return (ompi_communicator_t*)opal_pointer_array_get_item(&ompi_mpi_communicators, cid);
}

static inline struct ompi_proc_t* ompi_comm_peer_lookup(ompi_communicator_t* comm, int peer_id)
{
#if OPAL_ENABLE_DEBUG
    if(peer_id >= comm->c_remote_group->grp_proc_count) {
        opal_output(0, "ompi_comm_peer_lookup: invalid peer index (%d)", peer_id);
        return (struct ompi_proc_t *) NULL;
    }
#endif
    /*return comm->c_remote_group->grp_proc_pointers[peer_id];*/
    return ompi_group_peer_lookup(comm->c_remote_group,peer_id);
}

/* Determine the rank of the specified process in this communicator
 * @return -1 If not in communicator
 * @return >=0 If in communicator
 */
static inline int ompi_comm_peer_lookup_id(ompi_communicator_t* comm, ompi_proc_t *proc)
{
#if OPAL_ENABLE_DEBUG
    if(NULL == proc ) {
        opal_output(0, "ompi_comm_peer_lookup_id: invalid ompi_proc (NULL)");
        return -1;
    }
#endif
    return ompi_group_peer_lookup_id(comm->c_remote_group, proc);
}

#if OPAL_ENABLE_FT_MPI
#define OMPI_COMM_SET_FT(COMM, NPROCS, EPOCH)                           \
    do {                                                                \
        (COMM)->any_source_enabled  = true;                             \
        (COMM)->any_source_offset   = 0;                                \
        (COMM)->comm_revoked        = false;                            \
        (COMM)->collectives_force_error = false;                        \
        (COMM)->num_active_local    = (NPROCS);                         \
        (COMM)->num_active_remote   = (NPROCS);                         \
        (COMM)->lleader             = 0;                                \
        (COMM)->rleader             = 0;                                \
        (COMM)->c_epoch             = (EPOCH);                          \
        if( (NPROCS) > 0 ) {                                            \
            (COMM)->agreed_failed_ranks =                               \
                ompi_group_allocate_bmap( NPROCS, 0 );                  \
        } else {                                                        \
            (COMM)->agreed_failed_ranks = NULL;                         \
        }                                                               \
    } while (0)

/*
 * Support for MPI_ANY_SOURCE point-to-point operations
 */
static inline bool ompi_comm_is_any_source_enabled(ompi_communicator_t* comm)
{
    return (comm->any_source_enabled);
}

/*
 * Are collectives still active on this communicator?
 */
static inline bool ompi_comm_force_error_on_collectives(ompi_communicator_t* comm)
{
    return (comm->collectives_force_error);
}

/*
 * Has this communicator been revoked?
 */
static inline bool ompi_comm_is_revoked(ompi_communicator_t* comm)
{
    return (comm->comm_revoked);
}

/*
 * Acknowledge failures and re-enable MPI_ANY_SOURCE
 * Related to OMPI_Comm_failure_ack() and OMPI_Comm_failure_get_acked()
 */
OMPI_DECLSPEC int ompi_comm_failure_ack_internal(ompi_communicator_t* comm);

/*
 * Return the acknowledged group of failures
 * Related to OMPI_Comm_failure_ack() and OMPI_Comm_failure_get_acked()
 */
OMPI_DECLSPEC int ompi_comm_failure_get_acked_internal(ompi_communicator_t* comm, ompi_group_t **group );

/*
 * Setup/Shutdown 'revoke' handler
 */
OMPI_DECLSPEC int ompi_comm_init_revoke(void);
OMPI_DECLSPEC int ompi_comm_finalize_revoke(void);

/*
 * Revoke the communicator
 */
OMPI_DECLSPEC int ompi_comm_revoke_internal(ompi_communicator_t* comm);

/*
 * Shrink the communicator
 */
OMPI_DECLSPEC int ompi_comm_shrink_internal(ompi_communicator_t* comm, ompi_communicator_t** newcomm);

/*
 * Process and Communicator State Accessors
 */
static inline int ompi_comm_num_active_local(ompi_communicator_t* comm)
{
    return (comm->num_active_local);
}
static inline int ompi_comm_num_active_remote(ompi_communicator_t* comm)
{
    return (comm->num_active_remote);
}

/*
 * Check if the process is active
 */
OMPI_DECLSPEC bool ompi_comm_is_proc_active(ompi_communicator_t *comm, int peer_id, bool remote);

/*
 * Register a new process failure
 */
OMPI_DECLSPEC int ompi_comm_set_rank_failed(ompi_communicator_t *comm, int peer_id, bool remote);

/*
 * MPI Interface early checks
 */
static inline bool ompi_comm_iface_p2p_check_proc(ompi_communicator_t *comm, int peer_id, int *err)
{
    if( ompi_comm_is_revoked(comm) ) {
        *err = MPI_ERR_REVOKED;
        return false;
    }
    else if( !ompi_comm_is_proc_active(comm, peer_id, OMPI_COMM_IS_INTRA(comm)) ) {
        *err = MPI_ERR_PROC_FAILED;
        return false;
    }
    return true;
}

static inline bool ompi_comm_iface_coll_check(ompi_communicator_t *comm, int *err)
{
    if( ompi_comm_is_revoked(comm) ) {
        *err = MPI_ERR_REVOKED;
        return false;
    }
    else if( ompi_comm_force_error_on_collectives(comm) ) {
        *err = MPI_ERR_PROC_FAILED;
        return false;
    }
    return true;
}

static inline bool ompi_comm_iface_create_check(ompi_communicator_t *comm, int *err)
{
    return ompi_comm_iface_coll_check(comm, err);
}

/*
 * Communicator creation support collectives
 * - Agreement style allreduce
 */
int ompi_comm_allreduce_intra_ft( int *inbuf, int *outbuf, 
                                  int count, struct ompi_op_t *op, 
                                  ompi_communicator_t *comm,
                                  ompi_communicator_t *bridgecomm, 
                                  void* local_leader, 
                                  void* remote_leader, 
                                  int send_first );
int ompi_comm_allreduce_inter_ft( int *inbuf, int *outbuf, 
                                  int count, struct ompi_op_t *op, 
                                  ompi_communicator_t *intercomm,
                                  ompi_communicator_t *bridgecomm, 
                                  void* local_leader, 
                                  void* remote_leader, 
                                  int send_first );
int ompi_comm_allreduce_intra_bridge_ft(int *inbuf, int *outbuf, 
                                        int count, struct ompi_op_t *op, 
                                        ompi_communicator_t *comm,
                                        ompi_communicator_t *bcomm, 
                                        void* lleader, void* rleader,
                                        int send_first );
int ompi_comm_allreduce_intra_oob_ft(int *inbuf, int *outbuf, 
                                     int count, struct ompi_op_t *op, 
                                     ompi_communicator_t *comm,
                                     ompi_communicator_t *bridgecomm, 
                                     void* lleader, void* rleader,
                                     int send_first );
#else
#define OMPI_COMM_SET_FT(COMM, NPROCS, EPOCH)
#endif /* OPAL_ENABLE_FT_MPI */

static inline bool ompi_comm_peer_invalid(ompi_communicator_t* comm, int peer_id)
{
    if(peer_id < 0 || peer_id >= comm->c_remote_group->grp_proc_count) {
        return true;
    }
    return false;
}


/**
 * Initialise MPI_COMM_WORLD and MPI_COMM_SELF
 */
int ompi_comm_init(void);
OMPI_DECLSPEC int ompi_comm_link_function(void);

/**
 * extract the local group from a communicator
 */
OMPI_DECLSPEC int ompi_comm_group (ompi_communicator_t *comm, ompi_group_t **group);

/**
 * create a communicator based on a group
 */
int ompi_comm_create (ompi_communicator_t* comm, ompi_group_t *group,
                      ompi_communicator_t** newcomm);


/**
 * create a cartesian communicator
 */
int ompi_topo_create (ompi_communicator_t *old_comm,
                      int ndims_or_nnodes,
                      int *dims_or_index,
                      int *periods_or_edges,
                      bool reorder,
                      ompi_communicator_t **comm_cart,
                      int cart_or_graph);

/**
 * split a communicator based on color and key. Parameters
 * are identical to the MPI-counterpart of the function.
 *
 * @param comm: input communicator
 * @param color
 * @param key
 *
 * @
 */
OMPI_DECLSPEC int ompi_comm_split (ompi_communicator_t *comm, int color, int key,
                                   ompi_communicator_t** newcomm, bool pass_on_topo);

/**
 * split a communicator based on type and key. Parameters
 * are identical to the MPI-counterpart of the function.
 *
 * @param comm: input communicator
 * @param color
 * @param key
 *
 * @
 */
OMPI_DECLSPEC int ompi_comm_split_type(ompi_communicator_t *comm, 
                                       int split_type, int key,
                                       struct ompi_info_t *info,
                                       ompi_communicator_t** newcomm);

/**
 * dup a communicator. Parameter are identical to the MPI-counterpart
 * of the function. It has been extracted, since we need to be able
 * to dup a communicator internally as well.
 *
 * @param comm:      input communicator
 * @param newcomm:   the new communicator or MPI_COMM_NULL if any error is detected.
 */
OMPI_DECLSPEC int ompi_comm_dup (ompi_communicator_t *comm, ompi_communicator_t **newcomm);
/**
 * compare two communicators.
 *
 * @param comm1,comm2: input communicators
 *
 */
int ompi_comm_compare(ompi_communicator_t *comm1, ompi_communicator_t *comm2, int *result);

/**
 * free a communicator
 */
OMPI_DECLSPEC int ompi_comm_free (ompi_communicator_t **comm);

/**
 * allocate a new communicator structure
 * @param local_group_size
 * @param remote_group_size
 *
 * This routine allocates the structure, the according local and
 * remote groups, the proc-arrays in the local and remote group.
 * It furthermore sets the fortran index correctly,
 * and sets all other elements to zero.
 */
ompi_communicator_t* ompi_comm_allocate (int local_group_size,
                                         int remote_group_size);

/**
 * allocate new communicator ID
 * @param newcomm:    pointer to the new communicator
 * @param oldcomm:    original comm
 * @param bridgecomm: bridge comm for intercomm_create
 * @param mode: combination of input
 *              OMPI_COMM_CID_INTRA:        intra-comm
 *              OMPI_COMM_CID_INTER:        inter-comm
 *              OMPI_COMM_CID_INTRA_BRIDGE: 2 intracomms connected by
 *                                          a bridge comm. local_leader
 *                                          and remote leader are in this
 *                                          case an int (rank in bridge-comm).
 *              OMPI_COMM_CID_INTRA_OOB:    2 intracomms, leaders talk
 *                                          through OOB. lleader and rleader
 *                                          are the required contact information.
 * @param send_first: to avoid a potential deadlock for
 *                    the OOB version.
 * This routine has to be thread safe in the final version.
 */
OMPI_DECLSPEC int ompi_comm_nextcid ( ompi_communicator_t* newcomm,
                                      ompi_communicator_t* oldcomm,
                                      ompi_communicator_t* bridgecomm,
                                      void* local_leader,
                                      void* remote_leader,
                                      int mode,
                                      int send_first);

/**
 * shut down the communicator infrastructure.
 */
int ompi_comm_finalize (void);

/**
 * This is THE routine, where all the communicator stuff
 * is really set.
 */
OMPI_DECLSPEC int ompi_comm_set ( ompi_communicator_t** newcomm,
                                  ompi_communicator_t* oldcomm,
                                  int local_size,
                                  int *local_ranks,
                                  int remote_size,
                                  int *remote_ranks,
                                  opal_hash_table_t *attr,
                                  ompi_errhandler_t *errh,
                                  mca_base_component_t *topocomponent,
                                  ompi_group_t *local_group,
                                  ompi_group_t *remote_group   );
/**
 * This is a short-hand routine used in intercomm_create.
 * The routine makes sure, that all processes have afterwards
 * a list of ompi_proc_t pointers for the remote group.
 */
struct ompi_proc_t **ompi_comm_get_rprocs ( ompi_communicator_t *local_comm,
                                            ompi_communicator_t *bridge_comm,
                                            int local_leader,
                                            int remote_leader,
                                            int tag,
                                            int rsize);

/**
 * This routine verifies, whether local_group and remote group are overlapping
 * in intercomm_create
 */
int ompi_comm_overlapping_groups (int size, struct ompi_proc_t ** lprocs,
                                  int rsize, struct ompi_proc_t ** rprocs);

/**
 * This is a routine determining whether the local or the
 * remote group will be first in the new intra-comm.
 * Just used from within MPI_Intercomm_merge.
 */
int ompi_comm_determine_first ( ompi_communicator_t *intercomm,
                                int high );


OMPI_DECLSPEC int ompi_comm_activate ( ompi_communicator_t** newcomm, 
				       ompi_communicator_t* comm,
				       ompi_communicator_t* bridgecomm,
				       void* local_leader,
				       void* remote_leader,
				       int mode,
				       int send_first );
				       

/**
 * a simple function to dump the structure
 */
int ompi_comm_dump ( ompi_communicator_t *comm );

/* setting name */
int ompi_comm_set_name (ompi_communicator_t *comm, char *name );

/*
 * these are the init and finalize functions for the comm_reg
 * stuff. These routines are necessary for handling multi-threading
 * scenarious in the communicator_cid allocation
 */
void ompi_comm_reg_init(void);
void ompi_comm_reg_finalize(void);

/* global variable to save the number od dynamic communicators */
extern int ompi_comm_num_dyncomm;


/* check whether any of the processes has requested support for 
   MPI_THREAD_MULTIPLE. Note, that this produces global
   information across MPI_COMM_WORLD, in contrary to the local
   flag ompi_mpi_thread_provided 
*/
OMPI_DECLSPEC int ompi_comm_cid_init ( void );


END_C_DECLS

#endif /* OMPI_COMMUNICATOR_H */

