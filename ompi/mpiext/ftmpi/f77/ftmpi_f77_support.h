/*
 * Copyright (c) 2010-2012 Oak Ridge National Labs.  All rights reserved.
 * $COPYRIGHT$
 * 
 * Additional copyrights may follow
 * 
 * $HEADER$
 */
#if OMPI_PROFILING_DEFINES
#define F77_STAMP_PROFILE_FN(fn_name_core, fn_name_lower, fn_name_upper, fn_args, pass_args) \
    static void fn_name_core fn_args;                                   \
                                                                        \
    OMPI_DECLSPEC void P##fn_name_upper fn_args;                        \
    OMPI_DECLSPEC void p##fn_name_lower fn_args;                        \
    OMPI_DECLSPEC void p##fn_name_lower##_ fn_args;                     \
    OMPI_DECLSPEC void p##fn_name_lower##__ fn_args;                    \
                                                                        \
    void P##fn_name_upper fn_args {                                     \
        fn_name_core pass_args;                                         \
    }                                                                   \
    void p##fn_name_lower fn_args {                                     \
        fn_name_core pass_args;                                         \
    }                                                                   \
    void p##fn_name_lower##_ fn_args {                                  \
        fn_name_core pass_args;                                         \
    }                                                                   \
    void p##fn_name_lower##__ fn_args {                                 \
        fn_name_core pass_args;                                         \
    }
#define F77_STAMP_FN(fn_name_core, fn_name_lower, fn_name_upper, fn_args, pass_args) \
    F77_STAMP_PROFILE_FN(fn_name_core, fn_name_lower, fn_name_upper, fn_args, pass_args)

#else
#define F77_STAMP_FN(fn_name_core, fn_name_lower, fn_name_upper, fn_args, pass_args) \
    static void fn_name_core fn_args;                                   \
                                                                        \
    OMPI_DECLSPEC void fn_name_upper fn_args;                           \
    OMPI_DECLSPEC void fn_name_lower fn_args;                           \
    OMPI_DECLSPEC void fn_name_lower##_ fn_args;                        \
    OMPI_DECLSPEC void fn_name_lower##__ fn_args;                       \
                                                                        \
    void fn_name_upper fn_args {                                        \
        fn_name_core pass_args;                                         \
    }                                                                   \
    void fn_name_lower fn_args {                                        \
        fn_name_core pass_args;                                         \
    }                                                                   \
    void fn_name_lower##_ fn_args {                                     \
        fn_name_core pass_args;                                         \
    }                                                                   \
    void fn_name_lower##__ fn_args {                                    \
        fn_name_core pass_args;                                         \
    }
#endif /* OMPI_PROFILING_DEFINES */
