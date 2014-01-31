/*
 * Copyright (c) 2012-2014 The University of Tennessee and The University
 *                         of Tennessee Research Foundation.  All rights
 *                         reserved.
 *
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */

#include <stdlib.h>
#include <stdio.h>
#include <signal.h>
#include <math.h>
#include <mpi.h>

int main( int argc, char* argv[] ) { 
    int np, rank;
    int rc;
    double start, end;
    char estr[MPI_MAX_ERROR_STRING]=""; int strl;
    MPI_Comm scomm;
    
    MPI_Init( &argc, &argv );

    MPI_Comm_size( MPI_COMM_WORLD, &np );
    MPI_Comm_rank( MPI_COMM_WORLD, &rank );

    MPI_Comm_split( MPI_COMM_WORLD, (rank==np-1)?1:0, rank, &scomm );
    MPI_Barrier( MPI_COMM_WORLD );
    MPI_Comm_set_errhandler( MPI_COMM_WORLD, MPI_ERRORS_RETURN );    

    if( rank == np-1 ) {
        printf( "Rank %04d: committing suicide\n", rank );
        raise( SIGKILL );
    }
    else {
        start=MPI_Wtime();
        printf( "Rank %04d: entering Barrier\n", rank );
        rc = MPI_Barrier( MPI_COMM_WORLD );
        end=MPI_Wtime();
        MPI_Error_string( rc, estr, &strl );
        printf( "Rank %04d: Barrier1 completed (rc=%s) duration %g (s)\n", rank, estr, end-start );
        int st = ceil(fmax(5., 5.*(end-start)));
        /* operation on scomm should not raise an error, only procs 
         * not appearing in scomm are dead */
        MPI_Allreduce( MPI_IN_PLACE, &st, 1, MPI_INT, MPI_MAX, scomm );
        if( 0 == rank ) printf( "Sleeping for %ds ... ... ...\n", st );
        sleep( st );
        start=MPI_Wtime();
        rc = MPI_Barrier( MPI_COMM_WORLD );
        end=MPI_Wtime();
        MPI_Error_string( rc, estr, &strl );
        printf( "Rank %04d: Barrier2 completed (rc=%s) duration %g (s)\n", rank, estr, end-start );
    }
 
    MPI_Barrier( scomm );
    MPI_Finalize();
    return EXIT_SUCCESS;
}