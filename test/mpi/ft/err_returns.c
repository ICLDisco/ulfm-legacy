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
    double start, tb, mtb, Mtb, tbf1, Mtbf1, mtbf1, tbf2, Mtbf2, mtbf2;
    int st;
    char estr[MPI_MAX_ERROR_STRING]=""; int strl;
    MPI_Comm scomm;
    int verbose=0;
    
    MPI_Init( &argc, &argv );
    
    if( !strcmp( argv[argc-1], "-v" ) ) verbose=1;

    MPI_Comm_size( MPI_COMM_WORLD, &np );
    MPI_Comm_rank( MPI_COMM_WORLD, &rank );

    MPI_Comm_split( MPI_COMM_WORLD, (rank==np-1)?1:0, rank, &scomm );
    start=MPI_Wtime();
    MPI_Barrier( MPI_COMM_WORLD );
    tb=MPI_Wtime()-start;
    MPI_Comm_set_errhandler( MPI_COMM_WORLD, MPI_ERRORS_RETURN );

    if( rank == np-1 ) {
        printf( "Rank %04d: committing suicide\n", rank );
        raise( SIGKILL );
        while(1); /* wait for the signal */
    }
    
    if(verbose) printf( "Rank %04d: entering Barrier\n", rank );
    start=MPI_Wtime();
    rc = MPI_Barrier( MPI_COMM_WORLD );
    tbf1=MPI_Wtime()-start;
    if(verbose) { 
        MPI_Error_string( rc, estr, &strl );
        printf( "Rank %04d: Barrier1 completed (rc=%s) duration %g (s)\n", rank, estr, tbf1 );
    }
    if( rc != MPI_ERR_PROC_FAILED ) MPI_Abort( scomm, rc );
    st = ceil(10*fmax(1., tb));

    /* operation on scomm should not raise an error, only procs 
     * not appearing in scomm are dead */
    MPI_Allreduce( MPI_IN_PLACE, &st, 1, MPI_INT, MPI_MAX, scomm );
    if( 0 == rank ) printf( "Sleeping for %ds ... ... ...\n", st );
    sleep( st );

    if(verbose) printf( "Rank %04d: entering Barrier\n", rank );
    start=MPI_Wtime();
    rc = MPI_Barrier( MPI_COMM_WORLD );
    tbf2=MPI_Wtime()-start;
    if(verbose) { 
        MPI_Error_string( rc, estr, &strl );
        printf( "Rank %04d: Barrier2 completed (rc=%s) duration %g (s)\n", rank, estr, tbf2 );
    }
    if( rc != MPI_ERR_PROC_FAILED ) MPI_Abort( scomm, rc );

    MPI_Reduce( &tb, &mtb, 1, MPI_DOUBLE, MPI_MIN, 0, scomm );
    MPI_Reduce( &tb, &Mtb, 1, MPI_DOUBLE, MPI_MAX, 0, scomm );
    MPI_Reduce( &tbf1, &mtbf1, 1, MPI_DOUBLE, MPI_MIN, 0, scomm );
    MPI_Reduce( &tbf1, &Mtbf1, 1, MPI_DOUBLE, MPI_MAX, 0, scomm );
    MPI_Reduce( &tbf2, &mtbf2, 1, MPI_DOUBLE, MPI_MIN, 0, scomm );
    MPI_Reduce( &tbf2, &Mtbf2, 1, MPI_DOUBLE, MPI_MAX, 0, scomm );

    if( 0 == rank ) printf( 
        "## Timings ########### Min         ### Max         ##\n"
        "Barrier (no fault)  # %13.5e # %13.5e\n"
        "Barrier (new fault) # %13.5e # %13.5e\n"
        "Barrier (old fault) # %13.5e # %13.5e\n",
        mtb, Mtb, mtbf1, Mtbf1, mtbf2, Mtbf2 );

    MPI_Finalize();
    return EXIT_SUCCESS;
}