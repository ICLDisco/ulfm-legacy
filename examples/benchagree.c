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

#include <stdlib.h>
#include <stdio.h>
#include <getopt.h>
#include <sys/types.h>
#include <unistd.h>
#include <sched.h>
#include <signal.h>
#include <dlfcn.h>
#include <math.h>

/** Knuth algorithm for online numerically stable computation of variance */
typedef struct {
    int     n;
    double  mean;
    double  m2;
    int     ks;
    double *samples;
} stat_t;

static inline double stat_get_mean(stat_t *s) {
    return s->mean;
}

static inline double stat_get_stdev(stat_t *s) {
    if( s->n > 1 )
        return sqrt(s->m2/(double)(s->n-1));
    return NAN;
}

static inline int stat_get_nbsamples(stat_t *s) {
    return s->n;
}

static inline void stat_record(stat_t *s, double v) {
    double delta;
    if( s->n < s->ks ) {
        s->samples[s->n] = v;
    }
    s->n++;
    delta = v - s->mean;
    s->mean += delta / (double)s->n;
    s->m2 += delta * (v - s->mean);
}

static inline void stat_init(stat_t *s, int keep_samples) {
    s->n    = 0;
    s->mean = 0.0;
    s->m2   = 0.0;
    s->samples = (double*)calloc(keep_samples, sizeof(double));
    s->ks = keep_samples;
}

int main(int argc, char *argv[])
{
    int rank, size;
    int c, i;
    int common, flag, ret;
    unsigned int seed = 1789;

    int  verbose = 0;
    int  before = 10;
    int  after  = 10;
    int *faults;

    double start, dfailure;
    stat_t sbefore, safter, sstab;

    MPI_Init(&argc, &argv);
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);

    faults = (int*)calloc(size, sizeof(int));

    while(1) {
        static struct option long_options[] = {
            { "verbose",      0, 0, 'v' },
            { "before",       0, 0, 'b' },
            { "after",        0, 0, 'a' },
            { "faults",       0, 0, 'f' },
            { NULL,           0, 0, 0   }
        };

        c = getopt_long(argc, argv, "vb:a:f:", long_options, NULL);
        if (c == -1)
            break;

        switch(c) {
        case 'v':
            verbose = 1;
            break;
        case 'b':
            before = atoi(optarg);
            break;
        case 'a':
            after = atoi(optarg);
            break;
        case 'f':
            faults[ atoi(optarg) ] = 1;
            break;
        }
    }

    stat_init(&sbefore, 0);
    stat_init(&sstab, 0);
    stat_init(&safter, 0);

    common = rand_r(&seed);
    srand(getpid());

    MPI_Comm_set_errhandler(MPI_COMM_WORLD,MPI_ERRORS_RETURN);

    MPI_Barrier(MPI_COMM_WORLD);
    for(i = 0; i < before; i++) {
        flag = rand() | common;

        start = MPI_Wtime();
        ret = OMPI_Comm_agree(MPI_COMM_WORLD, &flag);
        stat_record(&sbefore, MPI_Wtime() - start);

        if( ret != MPI_SUCCESS ) {
            fprintf(stderr, "Correctness error: no process should have failed at this point, and the agreement returned %d to rank %d\n",
                    ret, rank);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    printf("BEFORE_FAILURE %g s (stdev %g ) per agreement on rank %d (average over %d agreements)\n", 
           stat_get_mean(&sbefore), stat_get_stdev(&sbefore), rank, stat_get_nbsamples(&sbefore));

    if( faults[rank] ) {
        if(verbose) {
            fprintf(stderr, "Rank %d dies\n", rank);
        }
        raise(SIGKILL);
    }

    MPI_Barrier(MPI_COMM_WORLD);
    if(verbose) {
        fprintf(stderr, "Rank %d out of barrier after failure\n", rank);
    }
    flag = rand() | common;

    start = MPI_Wtime();
    ret = OMPI_Comm_agree(MPI_COMM_WORLD, &flag);
    dfailure = MPI_Wtime() - start;
    stat_record(&sstab, dfailure);

    OMPI_Comm_failure_ack(MPI_COMM_WORLD);
    if(verbose) {
        fprintf(stderr, "Rank %d out of first agreement after failure; ret = %d\n", rank, ret);
    }

    i = 1;
    while(ret != MPI_SUCCESS) {
        i++;
        start = MPI_Wtime();
        ret = OMPI_Comm_agree(MPI_COMM_WORLD, &flag);
        stat_record(&sstab, MPI_Wtime()-start);

        if( ret != MPI_SUCCESS ) {
            OMPI_Comm_failure_ack(MPI_COMM_WORLD);
        }
        if(verbose) {
            fprintf(stderr, "Rank %d out of %d agreement after failure; ret = %d\n", rank, i, ret);
        }
    }
    
    MPI_Barrier(MPI_COMM_WORLD);
    printf("FIRST_AGREEMENT_AFTER_FAILURE %g s to do that agreement on rank %d\n", dfailure, rank);
    printf("STABILIZE_AGREEMENT %g s (stdev %g ) per agreements in %d agreements to stabilize to SUCCESS on rank %d\n",
           stat_get_mean(&sstab), stat_get_stdev(&sstab), stat_get_nbsamples(&sstab), rank);

    MPI_Barrier(MPI_COMM_WORLD);
    for(i = 0; i < after; i++) {
        flag = rand() | common;

        start = MPI_Wtime();
        ret = OMPI_Comm_agree(MPI_COMM_WORLD, &flag);
        stat_record(&safter, MPI_Wtime() - start);

        if( ret != MPI_SUCCESS ) {
            fprintf(stderr, "Correctness error: no new process should have failed at this point, and the agreement returned %d to rank %d\n",
                    ret, rank);
        }
    }

    MPI_Barrier(MPI_COMM_WORLD);
    printf("AFTER_FAILURE %g s (stdev %g ) per agreement on rank %d (average over %d agreements) -- Precision is %g\n",
               stat_get_mean(&safter), stat_get_stdev(&safter), rank, stat_get_nbsamples(&safter), MPI_Wtick());

    if( verbose ) {
        for(i = 0; i < safter.n && i < safter.ks; i++) {
            fprintf(stderr, "%d %g\n", rank, safter.samples[i]);
        }
    }

    MPI_Finalize();

    return 0;
}