#include <mpi.h>
#include <mpi-ext.h>
#include <stdio.h>

int main(int argc, char *argv[]) {
    int rank, size, rc;
    MPI_Comm world, tmp;
    
    MPI_Init(&argc, &argv);
    
    MPI_Comm_set_errhandler(MPI_COMM_WORLD, MPI_ERRORS_RETURN);
    
    MPI_Comm_rank(MPI_COMM_WORLD, &rank);
    MPI_Comm_size(MPI_COMM_WORLD, &size);
    
    /* Dup MPI_COMM_WORLD so we can continue to use the 
     * world handle if there is a failure */
    MPI_Comm_dup(MPI_COMM_WORLD, &world);
    
    /* Have rank 0 cause some trouble for later */
    if (0 == rank) {
        OMPI_Comm_invalidate(world);
        OMPI_Comm_shrink(world, &tmp);
        MPI_Comm_free(&world);
        world = tmp;
    } else {
        rc = MPI_Barrier(world);
        
        /* If world was invalidated, shrink world and try again */
        if (MPI_ERR_INVALIDATED == rc) {
            printf("Rank %d - Barrier INVALIDATED\n", rank);
            OMPI_Comm_shrink(world, &tmp);
            MPI_Comm_free(&world);
            world = tmp;
        } 
        /* Otherwise check for a new process failure and recover
         * if necessary */
        else if (MPI_ERR_PROC_FAILED == rc) {
            printf("Rank %d - Barrier FAILED\n", rank);
            OMPI_Comm_invalidate(world);
            OMPI_Comm_shrink(world, &tmp);
            MPI_Comm_free(&world);
            world = tmp;
        }
    }

    rc = MPI_Barrier(world);
    
    printf("Rank %d - RC = %d\n", rank, rc);

    MPI_Finalize();
    
    return 0;
}
