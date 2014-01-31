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
#include <mpi.h>

int main( int argc, char* argv[] ) { 
    int np, rank;
    int rc;
    double start, end;
    char estr[MPI_MAX_ERROR_STRING]=""; int strl;
    
    MPI_Init( &argc, &argv );
    MPI_Comm_set_errhandler( MPI_COMM_WORLD, MPI_ERRORS_RETURN );
    
    MPI_Comm_size( MPI_COMM_WORLD, &np );
    MPI_Comm_rank( MPI_COMM_WORLD, &rank );
    
    MPI_Barrier( MPI_COMM_WORLD );

    if( rank == np-1 ) {
        printf( "Rank %04d: %g committing suicide\n", rank, MPI_Wtime() );
        raise( SIGKILL );
    }
    else {
        start=MPI_Wtime();
        printf( "Rank %04d: date %g entering Barrier\n", rank, start );
        rc = MPI_Barrier( MPI_COMM_WORLD );
        end=MPI_Wtime();
        MPI_Error_string( rc, estr, &strl );
        printf( "Rank %04d: duration %g (s) Barrier1 completed (rc=%s)\n", rank, end-start, estr );
        sleep( 5 );
        start=MPI_Wtime();
        rc = MPI_Barrier( MPI_COMM_WORLD );
        end=MPI_Wtime();
        MPI_Error_string( rc, estr, &strl );
        printf( "Rank %04d: duration %g (s) Barrier2 completed (rc=%s)\n", rank, end-start, estr );
    }
    
    MPI_Finalize();
    return EXIT_SUCCESS;
}