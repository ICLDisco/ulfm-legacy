/*
 * Copyright (c) 2014      The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include "mpi.h"
#include "ompi/mpiext/ftmpi/mpiext_ftmpi_c.h"

#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <sys/types.h>
#include <unistd.h>
#include <sched.h>
#include <signal.h>
#include <dlfcn.h>
#include <assert.h>

static char host[256];
static char *agree4everpath = NULL;

#define INITIAL_SIZE 0
#define AG_IDX       1
#define AG_CID       2
#define SEED         3
#define VERBOSE      4
#define FAILURES     5
#define DEBUG        6
#define STATE_SIZE   7

static void new_process(MPI_Comm *comm, int *state)
{
    int rc;
    MPI_Comm parent;

    MPI_Comm_get_parent(&parent);
    rc = MPI_Intercomm_merge(parent, 1, comm);
    MPI_Comm_set_errhandler(*comm, MPI_ERRORS_RETURN);
    if(rc != MPI_SUCCESS) {
        exit(0);
    }
    if(*comm == MPI_COMM_NULL) {
        exit(0);
    }
    
    MPI_Bcast(state, STATE_SIZE, MPI_INT, 0, *comm);
}

static void spawn_new_processes(MPI_Comm *comm, int *state)
{
    int size, size2, rc, ret, rank;
    MPI_Comm everyone, temp;
    
    while(1) {
        MPI_Comm_size(*comm, &size);
        MPI_Comm_rank(*comm, &rank);
        
        if(state[VERBOSE]) {
            fprintf(stderr, "%d/%d -- %s:%d: Spawning %d processes to replace dead ones.\n", rank, size, host, getpid(), state[INITIAL_SIZE] - size);
        }

        rc = MPI_Comm_spawn(agree4everpath, MPI_ARGV_NULL, state[INITIAL_SIZE] - size,
                            MPI_INFO_NULL, 0, *comm, &everyone,
                            MPI_ERRCODES_IGNORE);

        if(state[VERBOSE]) {
            fprintf(stderr, "%d/%d -- %s:%d: Spawn returned %d.\n", rank, size, host, getpid(), rc);
        }

        if( rc == MPI_SUCCESS ) {
            if(state[VERBOSE]) {
                fprintf(stderr, "%d/%d -- %s:%d: Merging intercommunicator.\n", rank, size, host, getpid());
            }
            rc = MPI_Intercomm_merge(everyone, 0, &temp);
            MPI_Comm_set_errhandler(temp, MPI_ERRORS_RETURN);
            MPI_Comm_free(&everyone);

            if(state[VERBOSE]) {
                MPI_Comm_size(temp, &size2);
                fprintf(stderr, "%d/%d -- %s:%d: Merged communicator is of size %d.\n", rank, size, host, getpid(), size2);
                fprintf(stderr, "%d/%d -- %s:%d: Checking agreement.\n", rank, size, host, getpid());
            }
            ret = OMPI_Comm_agree(*comm, &rc);
        } else {
            if(state[VERBOSE]) {
                fprintf(stderr, "%d/%d -- %s:%d: Participating to agreement.\n", rank, size, host, getpid());
            }
            ret = OMPI_Comm_agree(*comm, &rc);
        }
        if(state[VERBOSE]) {
            fprintf(stderr, "%d/%d -- %s:%d: Agreement returned %d and %d.\n", rank, size, host, getpid(), rc, ret);
        }

        if( ret == MPI_ERR_PROC_FAILED ||
            rc  != MPI_SUCCESS) {
            OMPI_Comm_shrink(*comm, &temp);
            MPI_Comm_free(comm);
            *comm = temp;
            continue;
        } else {
            break;
        }
    }

    state[AG_CID]++;
    state[AG_IDX] = 0;
    MPI_Bcast(state, STATE_SIZE, MPI_INT, 0, temp);
    
    MPI_Comm_free(comm);
    *comm = temp;
}

int main(int argc, char *argv[])
{
    int state[STATE_SIZE];
    int rank, size;
    MPI_Comm comm, parent;
    int c, rc, i, simultaneous, fail_this_round;
    int *victims;
    int common, flag, ret;
    void *handle;
    int *coll_ftbasic_era_debug_rank_may_fail;

    state[SEED]  = 1789;
    state[VERBOSE]  = 0;
    state[FAILURES] = 0;
    state[DEBUG]    = 0;

    agree4everpath = argv[0];

    while(1) {
        static struct option long_options[] = {
            { "verbose",      0, 0, 'v' },
            { "failures",     1, 0, 'F' },
            { "debug",        0, 0, 'd' },
            { NULL,           0, 0, 0   }
        };

        c = getopt_long(argc, argv, "vdF:", long_options, NULL);
        if (c == -1)
            break;

        switch(c) {
        case 'v':
            state[VERBOSE] = 1;
            break;
        case 'd':
            state[DEBUG] = 1;
            break;
        case 'F':
            state[FAILURES] = atoi(optarg);
            break;
        }
    }

    gethostname(host, 256);

    srand(getpid());

    MPI_Init(&argc, &argv);
    handle = dlopen(NULL, RTLD_LAZY|RTLD_LOCAL);
    coll_ftbasic_era_debug_rank_may_fail = dlsym(handle, "coll_ftbasic_era_debug_rank_may_fail");
    dlclose(handle);
    if( NULL == coll_ftbasic_era_debug_rank_may_fail ) {
        fprintf(stderr, "Could not find Open MPI internal symbol coll_ftbasic_era_debug_rank_may_fail. Resort to simple raise(SIGKILL) type of failure\n");
    }

    MPI_Comm_set_errhandler(MPI_COMM_WORLD,MPI_ERRORS_RETURN);

    state[AG_IDX]   = 0;
    state[AG_CID] = 0;
    MPI_Comm_get_parent(&parent);
    if( MPI_COMM_NULL != parent ) {
        new_process(&comm, state);
        MPI_Comm_size(comm, &size);
    } else {
        comm = MPI_COMM_WORLD;
        MPI_Comm_size(comm, &size);
        state[INITIAL_SIZE] = size;
    }
    MPI_Comm_rank(comm, &rank);

    if( state[DEBUG] ) {
        int stop = 0;
        fprintf(stderr, "ssh -t %s gdb -p %d\n", host, getpid());
        while( stop == 0 ) {
            sched_yield();
        }
    }
    
    if(state[VERBOSE]) {
        fprintf(stderr, "Process %d / %d runs on host %s:%d. (INITIAL_SIZE=%d, AG_IDX=%d, AG_CID=%d, SEED=%d, VERBOSE=%d, FAILURES=%d, DEBUG=%d)\n",
                rank, size, host, getpid(),
                state[INITIAL_SIZE], state[AG_IDX], state[AG_CID], state[SEED], state[VERBOSE], state[FAILURES], state[DEBUG]);
    }

    victims = (int*)malloc(state[FAILURES] * sizeof(int));

    while(1) {
        if( state[FAILURES] > 0 ) {
            simultaneous = (rand_r((unsigned int*)&state[SEED]) % state[FAILURES]);
            if( state[VERBOSE] && rank == 0 ) {
                printf("Decided to do %d failures at this round\n", simultaneous);
            }
            fail_this_round = 0;
            for(c = 0; c < simultaneous; c++) {
                do {
                    rc = rand_r((unsigned int*)&state[SEED]) % state[INITIAL_SIZE];
                    for(i = 0; i < c; i++) {
                        if( rc == victims[i] )
                            break;
                    }
                    if( i == c ) {
                        victims[i] = rc;
                        if( rc == rank ) {
                            fail_this_round = 1;
                        }
                    }
                } while(i != c);
            }
            if( fail_this_round ) {
                if( NULL != coll_ftbasic_era_debug_rank_may_fail ) {
                    if( state[VERBOSE] ) {
                        fprintf(stderr, "Rank %d/%d will fail\n", rank, size);
                    }
                    *coll_ftbasic_era_debug_rank_may_fail = 1;
                } else {
                    if( state[VERBOSE] ) {
                        fprintf(stderr, "Rank %d/%d fails\n", rank, size);
                    }
                    raise(SIGKILL);
                }
            } else {
                if( NULL != coll_ftbasic_era_debug_rank_may_fail ) {
                    *coll_ftbasic_era_debug_rank_may_fail = 0;
                }
            }
        }

        while( size > state[INITIAL_SIZE] - simultaneous ) {

            if( state[VERBOSE] ) {
                printf("Size = %d, INITIAL_SIZE = %d, simultaneous = %d\n", size, state[INITIAL_SIZE], simultaneous );
            }

            common = rand_r((unsigned int*)&state[SEED]);
            flag = common | rand();
            state[AG_IDX]++;
            if( state[VERBOSE] ) {
                printf("Rank %d/%d enters MPI_Comm_agree %d.%d with %08x\n", rank, size, state[AG_CID], state[AG_IDX], flag);
            }
            ret = OMPI_Comm_agree(comm, &flag);
            if( state[VERBOSE] ) {
                printf("Rank %d/%d leaves MPI_Comm_agree %d.%d with %08x and %d\n", rank, size, state[AG_CID], state[AG_IDX], flag, ret);
            }
            if( ret != MPI_SUCCESS ) {
                MPI_Comm temp;
                int orank = rank;
                int osize = size;
                if(state[VERBOSE]) {
                    printf("Rank %d/%d after Agree %d.%d needs to shrink after a failure.\n", rank, size, state[AG_CID], state[AG_IDX]);
                }
                rc = OMPI_Comm_shrink(comm, &temp);
                if( rc != MPI_SUCCESS ) {
                    fprintf(stderr, "MPI_Comm_shrink returned %d instead of MPI_SUCCESS on rank %d/%d!\n", rc, rank, size);
                }
                if( temp == MPI_COMM_NULL ) {
                    fprintf(stderr, "MPI_Comm_shrink returned MPI_COMM_NULL and MPI_SUCCESS on rank %d/%d!\n", rank, size);
                }
                if( comm != MPI_COMM_WORLD )
                    MPI_Comm_free(&comm);
                comm = temp;
                MPI_Comm_rank(comm, &rank);
                MPI_Comm_size(comm, &size);
                if(state[VERBOSE]) {
                    printf("Fix after Agree %d.%d: rank %d/%d in previous comm is now %d/%d after shrink (state[INITIAL_SIZE] is %d)\n",
                           state[AG_CID], state[AG_IDX], orank, osize, rank, size, state[INITIAL_SIZE]);
                }
            }
        }
        if( size < state[INITIAL_SIZE] ) {
            spawn_new_processes(&comm, state);
            MPI_Comm_rank(comm, &rank);
            MPI_Comm_size(comm, &size);
        }
    }
    
    if( MPI_COMM_WORLD != comm ) {
        MPI_Comm_free(&comm);
    }

    MPI_Finalize();

    return 0;
}
