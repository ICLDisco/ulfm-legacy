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
        rc = OMPI_Comm_revoke(world);
    } else {
        rc = MPI_Barrier(world);        
        if (MPI_ERR_REVOKED == rc) {
            printf("Rank %d - Barrier REVOKED\n", rank);
        } 
    }
    
    printf("Rank %d - RC = %d\n", rank, rc);

    MPI_Finalize();
    
    return 0;
}
