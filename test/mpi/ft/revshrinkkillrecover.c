#include <mpi.h>
#include <mpi-ext.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <signal.h>
#include <unistd.h>

#define FAIL_WINDOW 10000

void recover(MPI_Comm **comm) {
    int nprocs, *errcodes, rc;
    char command[] = "/home/wbland/test/invshrinkkillrecover";
    MPI_Comm intercomm, tmp_comm, *new_intra;
    MPI_Group tmp_group;
    
    new_intra = (MPI_Comm *) malloc(sizeof(MPI_Comm));
    
#if 0
    OMPI_Comm_shrink(**comm, new_intra);
    *comm = new_intra;
    return;
#else
    /* Figure out how many procs failed */
    OMPI_Comm_failure_ack(**comm);
    OMPI_Comm_failure_get_acked(**comm, &tmp_group);
    MPI_Group_size(tmp_group, &nprocs);
    MPI_Group_free(&tmp_group);
    
    errcodes = (int *) malloc(sizeof(int) * nprocs);

    /* Shrink the old communicator */
    OMPI_Comm_shrink(**comm, &tmp_comm);

    /* Spawn the new processes */
    rc = MPI_Comm_spawn(command, MPI_ARGV_NULL, nprocs, MPI_INFO_NULL, 0, tmp_comm, &intercomm, errcodes);

    free(errcodes);

    //printf("RC: %d\n", rc);

    MPI_Intercomm_merge(intercomm, 0, new_intra);
    *comm = new_intra;
    return;
    //return new_intra;
#endif
}

int main(int argc, char *argv[]) {
    int rank, size, rc, rnum, print = 0;
    MPI_Comm *world, parentcomm;
    pid_t pid;
    char *spawned;

    MPI_Init(&argc, &argv);

    pid = getpid();

    world = (MPI_Comm *) malloc(sizeof(MPI_Comm));
    
    MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);
    
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);

    srand((unsigned int) time(NULL) + (rank*1000));
    
    MPI_Comm_get_parent(&parentcomm);
    
    /* See if we were spawned and if so, recover */
    if (MPI_COMM_NULL != parentcomm) {
        printf("Spawned\n");
        MPI_Comm_set_errhandler(parentcomm, MPI_ERRORS_RETURN);
        MPI_Intercomm_merge(parentcomm, 1, world);
        print = 1;
        spawned = strdup("spawned");
    } else {
        /* Dup MPI_COMM_WORLD so we can continue to use the 
         * world handle if there is a failure */
        printf("Original\n");
        MPI_Comm_dup(MPI_COMM_WORLD, world);
        spawned = strdup("original");
    }

    MPI_Comm_rank(*world, &rank);
    MPI_Comm_size(*world, &size);

    /* Do a loop that keeps killing processes until there are none left */
    while(true) {
        rnum = rand();
        
        if (rank != 0) {
            /* If you're within the window, just kill yourself */
            if ((RAND_MAX / 2) + FAIL_WINDOW > rnum 
                    && (RAND_MAX / 2) - FAIL_WINDOW < rnum ) {
                printf("%d - Killing Self (%d in %d)\n", rank, rnum, (RAND_MAX / 2));
                fflush(stdout);
                kill(pid, 9);
            }
        }

        if (print) {
            printf("%d - Entering Barrier (%s)\n", rank, spawned);
        }

        rc = MPI_Barrier(*world);

        /* If comm was revoked, shrink world and try again */
        if (MPI_ERR_REVOKED == rc) {
            printf("%d - REVOKED\n", rank);
            recover(&world);
            print = 1;
        } 
        /* Otherwise check for a new process failure and recover
         * if necessary */
        else if (MPI_ERR_PROC_FAILED == rc) {
            printf("%d - FAILED\n", rank);
            OMPI_Comm_revoke(*world);
            recover(&world);
            print = 1;
        } else if (MPI_SUCCESS == rc) {
            print = 0;
        }

        //MPI_Comm_size(world, &size);
        MPI_Comm_rank(*world, &rank);
    }

    /* We'll reach here when only 0 is left */
    MPI_Finalize();

    return 0;
}
