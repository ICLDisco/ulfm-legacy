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
#include "mpi-ext.h"

#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <sys/types.h>
#include <unistd.h>
#include <sched.h>
#include <signal.h>
#include <dlfcn.h>

static void usleep_random(void)
{
    usleep( (int)(1000.0 * (double)rand() / (double)RAND_MAX) );
}

int main(int argc, char *argv[])
{
    int rank, size;
    int c;
    int common, flag, ret;
    unsigned int seed = 1789;
    int ag_nb = 0;
    int *coll_ftbasic_debug_rank_may_fail;

    int verbose = 1;
    int de_sync = 0;
    int debug = 0;
    char *help_msg = NULL;
    void *handle;

    while(1) {
        static struct option long_options[] = {
            { "verbose",      0, 0, 'v' },
            { "de-sync",      0, 0, 's' },
            { "help",         0, 0, 'h' },
            { "debug",        0, 0, 'd' },
            { NULL,           0, 0, 0   }
        };

        c = getopt_long(argc, argv, "vshd", long_options, NULL);
        if (c == -1)
            break;

        switch(c) {
        case 'v':
            verbose = 0;
            break;
        case 's':
            de_sync = 1;
            break;
        case 'd':
            debug = 1;
            break;
        case 'h':
            help_msg = "";
            break;
        default:
            help_msg = "Unrecognized option\n";
            break;
        }
    }

    if( debug ) {
        int stop = 0;
        char hostname[255];
        gethostname(hostname, 255);
        fprintf(stderr, "ssh -t %s gdb -p %d\n", hostname, getpid());
        while( stop == 0 ) {
            sched_yield();
        }
    }

    common = rand_r(&seed);

    srand(getpid());

    MPI_Init(&argc, &argv);

    MPI_Comm_set_errhandler(MPI_COMM_WORLD,MPI_ERRORS_RETURN);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    handle = dlopen(NULL, RTLD_LAZY|RTLD_LOCAL);
    coll_ftbasic_debug_rank_may_fail = dlsym(handle, "coll_ftbasic_debug_rank_may_fail");
    dlclose(handle);
    if( NULL == coll_ftbasic_debug_rank_may_fail ) {
        fprintf(stderr, "Could not find Open MPI internal symbol coll_ftbasic_debug_rank_may_fail. Resort to simple raise(SIGKILL) type of failure\n");
    }

    if( NULL != help_msg ) {
        if( 0 == rank ) {
            fprintf(stderr, "%s", help_msg);
            fprintf(stderr, 
                    "Usage: mpirun [MPI_OPTIONS] %s [OPTIONS]\n"
                    " Where typical MPI_OPTIONS for Open MPI are:\n"
                    "   -am ft-enable-mpi                     To enable FT-extensions\n"
                    "   -mca coll ftbasic,basic,tuned,self    To select FT version of the agreement\n"
                    "   -mca coll_ftbasic_method <I>          To choose which method is used for the agreement\n"
                    "                                           (0 - 4) 3 = ETA, 4 = ERA, see ompi_info -param all all\n"
                    " And OPTIONS are:\n"
                    "\n"
                    "--verbose         | -v        To disable printing information messages\n"
                    "--de-sync         | -s        To introduce process-specific random wait times before entering the consensus (or dying)\n"
                    "--debug           | -d        Wait that a gdb attaches to the process and call stop=1 to continue\n"
                    "--help            | -h        To display this help\n",
                    argv[0]);
        }
        MPI_Finalize();
        return 0;
    }

    if( de_sync ) {
        usleep_random();
    }

    ag_nb = 0;
    do {
        if( rand() % 10 < 5 ) {
            if( NULL != coll_ftbasic_debug_rank_may_fail ) {
                if( verbose ) {
                    fprintf(stderr, "Rank %d/%d may fail during Agreements starting with %d (and until they return 0)\n", rank, size, ag_nb);
                }
                *coll_ftbasic_debug_rank_may_fail = 1;
            } else {
                if( verbose ) {
                    fprintf(stderr, "Rank %d/%d fails before round %d\n", rank, size, ag_nb);
                }
                raise(SIGKILL);
            }
        } else {
            if( NULL != coll_ftbasic_debug_rank_may_fail ) {
                *coll_ftbasic_debug_rank_may_fail = 0;
            }
        }

        do {
            ag_nb++;
            flag = rand() | common;
            if( verbose ) {
                fprintf(stderr, "Agreement Nb %d: Rank %d/%d enters MPI_Comm_agree with %08x\n", ag_nb, rank, size, flag);
            }
            ret = MPIX_Comm_agree(MPI_COMM_WORLD, &flag);
            if( verbose ) {
                fprintf(stderr, "Agreement Nb %d: Rank %d/%d leaves MPI_Comm_agree with %08x and %d\n", ag_nb, rank, size, flag, ret);
            }
            if( ret == MPI_SUCCESS ) {
                MPI_Group failed_grp;
                int alive_size, alive_rank;
                MPI_Group world_grp;
                MPI_Group alive_grp;

                MPI_Comm_group(MPI_COMM_WORLD, &world_grp);
                MPIX_Comm_failure_get_acked(MPI_COMM_WORLD, &failed_grp);
                MPI_Group_difference(world_grp, failed_grp, &alive_grp);

                MPI_Group_size(alive_grp, &alive_size);
                MPI_Group_rank(alive_grp, &alive_rank);
                    
                fprintf(stderr, "Agreement Nb %d: Rank %d/%d is initial world is now Rank %d/%d in the alive processes\n",
                        ag_nb,
                        rank, size,
                        alive_rank, alive_size);

                if( alive_rank == 0 ) {
                    fprintf(stderr, "\n\n\n");
                }

                MPI_Group_free(&failed_grp);
                MPI_Group_free(&world_grp);
                MPI_Group_free(&alive_grp);
                    
            } else {
                MPIX_Comm_failure_ack(MPI_COMM_WORLD);
            }
        } while(ret != MPI_SUCCESS);
    } while(1);

    MPI_Finalize();

    return 0;
}
