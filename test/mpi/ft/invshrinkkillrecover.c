#include <mpi.h>
#include <mpi-ext.h>
#include <stdio.h>
#include <stdlib.h>
#include <time.h>
#include <signal.h>

#define FAIL_WINDOW 1000000

void recover(MPI_Comm *comm) {
    int nprocs, *errcodes, rc;
    char command[] = "/home/wbland/test/invshrinkkillrecover";
    MPI_Comm intercomm, tmp_comm, *new_intra;
    MPI_Group tmp_group;
    
    new_intra = (MPI_Comm *) malloc(sizeof(MPI_Comm));
    
    /* Figure out how many procs failed */
    OMPI_Comm_failure_ack(*comm);
    OMPI_Comm_failure_get_acked(*comm, &tmp_group);
    MPI_Group_size(tmp_group, &nprocs);
    MPI_Group_free(&tmp_group);
    
    errcodes = (int *) malloc(sizeof(int) * nprocs);
    
    /* Shrink the old communicator */
    OMPI_Comm_shrink(*comm, &tmp_comm);
    comm = &tmp_comm;
    
    /* Spawn the new processes */
    rc = MPI_Comm_spawn(command, MPI_ARGV_NULL, nprocs, MPI_INFO_NULL, 0, *comm, &intercomm, errcodes);

    //printf("RC: %d\n", rc);

    MPI_Intercomm_merge(intercomm, false, new_intra);
    comm = new_intra;
}

int main(int argc, char *argv[]) {
    int rank, size, rc, rnum;
    MPI_Comm world, parentcomm;
    pid_t pid;

    MPI_Init(&argc, &argv);

    pid = getpid();
    
    MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);

    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    srand((unsigned int) time(NULL) + rank);
    
    MPI_Comm_get_parent(&parentcomm);
    
    /* See if we were spawned and if so, recover */
    if (MPI_COMM_NULL == parentcomm) {
        MPI_Comm_set_errhandler(parentcomm, MPI_ERRORS_RETURN);
        MPI_Intercomm_merge(parentcomm, true, &world);
    } else {
        /* Dup MPI_COMM_WORLD so we can continue to use the 
         * world handle if there is a failure */
        MPI_Comm_dup(MPI_COMM_WORLD, &world);
    }

    /* Do a loop that keeps killing processes until there are none left */
    while(size > 1) {
        rnum = rand();
        
        if (rank != 0) {
            /* If you're within the window, just kill yourself */
            if ((RAND_MAX / 2) + FAIL_WINDOW > rnum 
                    && (RAND_MAX / 2) - FAIL_WINDOW < rnum ) {
                printf("%d - Killing Self (%d in %d)\n", rank, rnum, (RAND_MAX / 2));
                kill(pid, 9);
            }
        }

        rc = MPI_Barrier(world);

        /* If comm was invalidated, shrink world and try again */
        if (MPI_ERR_INVALIDATED == rc) {
            recover(&world);
        } 
        /* Otherwise check for a new process failure and recover
         * if necessary */
        else if (MPI_ERR_PROC_FAILED == rc) {
            OMPI_Comm_invalidate(world);
            recover(&world);
        }

        MPI_Comm_size(world, &size);
    }
    
    /* We'll reach here when only 0 is left */
    MPI_Finalize();
    
    return 0;
}
