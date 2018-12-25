    AC_INIT([OceanBase],
            [0.4.2.21],
            [qushan@taobao.com],
            [oceanbase],
			[http://oceanbase.taobao.org/])

    obapi_version="1.0.0"
    AC_SUBST(obapi_version)

    AC_PROG_LIBTOOL
    AC_PROG_RANLIB
    AM_INIT_AUTOMAKE(oceanbase, 0.4.2.21)
    ac_default_prefix=${HOME}/oceanbase_bin# for convenience
    AC_PROG_CXX
    AC_LANG([C++])
    AC_PROG_CC

	AC_MSG_CHECKING([drc-root])
    AC_ARG_WITH([drc-root],
           AS_HELP_STRING([--with-drc-root],
           [drc installation path]),
           DRC_ROOT=${withval})

    if  ! test -d "${DRC_ROOT}"; then
       AC_MSG_ERROR([DRC_ROOT have not been set])
       exit 1;
    fi
	AC_MSG_RESULT([${DRC_ROOT}])
    dnl drc include path supplied
    AC_SUBST([DRC_ROOT])

	AC_MSG_CHECKING([tblib-root])
    AC_ARG_WITH([tblib-root],
           AS_HELP_STRING([--with-tblib-root],
           [tbnet installation path]),
           TBLIB_ROOT=${withval})

    if  ! test -d "${TBLIB_ROOT}/include"; then
       AC_MSG_ERROR([TBLIB_ROOT have not been set])
       exit 1;
    fi
	AC_MSG_RESULT([${TBLIB_ROOT}])
    dnl tblib include path supplied
    AC_SUBST([TBLIB_ROOT])

	AC_MSG_CHECKING([easy-root])
    AC_ARG_WITH([easy-root],
           AS_HELP_STRING([--with-easy-root],
           [libeasy installation path]),
           EASY_ROOT=${withval})

    if  ! test -d "${EASY_ROOT}/include"; then
       AC_MSG_ERROR([EASY_ROOT have not been set, ${EASY_ROOT}/include])
       exit 1;
    fi
	AC_MSG_RESULT([${EASY_ROOT}])
    dnl libeasy include path supplied
    AC_SUBST([EASY_ROOT])

	AC_MSG_CHECKING([easy-lib-path])
    AC_ARG_WITH([easy-lib-path],
           AS_HELP_STRING([--with-easy-lib-path],
           [libeasy.a path]),
           EASY_LIB_PATH=${withval})

    if test -z ${EASY_LIB_PATH};
	then
		EASY_LIB_PATH=${EASY_ROOT}/lib
    fi
    if  ! test -d "${EASY_LIB_PATH}"; then
       AC_MSG_ERROR([EASY_LIB_PATH ${EASY_LIB_PATH} does not exist])
       exit 1;
    fi
	AC_MSG_RESULT([${EASY_LIB_PATH}])
    dnl libeasy include path supplied
    AC_SUBST([EASY_LIB_PATH])

    AC_ARG_WITH([release],
    AS_HELP_STRING([--with-release],
    [use optimize (default is NO)]),
    [
    if test "$withval" = "yes"; then
    AM_CXXFLAGS="-g -O2 -D_OB_VERSION=400 -D_NO_EXCEPTION -finline-functions \
	-D__STDC_LIMIT_MACROS -D__STDC_CONSTANT_MACROS -DNDEBUG\
	-Wall -Werror -Wextra -Wunused-parameter -Wformat -fno-strict-aliasing -fno-omit-frame-pointer \
	-Wconversion -Wno-deprecated -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64 \
	-D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE" #-lprofiler
	AM_CFLAGS="-g -O2 -D_OB_VERSION=400 -DCOMPATIBLE -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64\
	-D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -DNDEBUG\
	-finline-functions -fno-strict-aliasing -Werror -Wall"
    fi
    ],
    [ AM_CXXFLAGS="-D__STDC_LIMIT_MACROS -D_OB_VERSION=400 -D__STDC_CONSTANT_MACROS -D_NO_EXCEPTION \
	-Wall -Werror -Wextra -Wunused-parameter \
	-Wformat -Wconversion -Wno-deprecated -fno-strict-aliasing -g" #-D__OB_MTRACE__ -rdynamic
	AM_CFLAGS="-g -D_OB_VERSION=400 -DCOMPATIBLE -D__USE_LARGEFILE64 -D_FILE_OFFSET_BITS=64\
	-D_LARGE_FILE -D_LARGEFILE_SOURCE -D_LARGEFILE64_SOURCE -Werror -Wall"
	]
    )

    #check gcc version, add -Wno-ignored-qualifiers flag for gcc version greater than 4.3.0
    GCC_VERSION=`$CC -dumpversion`
    if test $? -eq 0; then
        major=`echo $GCC_VERSION | cut -d. -f1`
        minor=`echo $GCC_VERSION | cut -d. -f2`
    fi
    if test $major -eq 4 -a $minor -ge 3 ; then
        AM_CXXFLAGS="${AM_CXXFLAGS} -Wno-ignored-qualifiers"
    fi

    AC_ARG_WITH([test_case],
           AS_HELP_STRING([--with-test-case],
		          [with test case (default is YES)]),
	   		  [
			    if test "$withval" = "no"; then
			    	test_case=no
			    fi
			  ],
			  [ test_case=yes ]
	   )

    AC_ARG_WITH([perf],
           AS_HELP_STRING([--with-perf],
		          [with perf (default is NO)]),
	   		  [
			    if test "$withval" = "yes"; then
			    	test_perf=yes
                                AM_CXXFLAGS="${AM_CXXFLAGS} -D__NEED_PERF__"
                                AM_LDFLAGS="${AM_LDFLAGS} -lprofiler"
			    fi
			  ],
			  [with_perf=no]
	   )

    AC_ARG_WITH([preload],
           AS_HELP_STRING([--with-preload],
		          [with preload (default is NO)]),
	   		  [
			    if test "$withval" = "yes"; then
			    	with_preload=yes
                                AM_CXXFLAGS="${AM_CXXFLAGS} -D__ENABLE_PRELOAD__"
			    fi
			  ],
			  [
			  with_preload=no
                          ]
	   )
    AC_ARG_WITH([coverage],
           AS_HELP_STRING([--with-coverage],
		          [with coverage (default is NO)]),
	   		  [
			    if test "$withval" = "yes"; then
			    	coverage=yes
			    fi
			  ],
			  [coverage=no]
	   )

	AC_ARG_VAR([RELEASEID], [release number when building in Taobao ABS])
	AC_DEFINE_UNQUOTED([RELEASEID], "$RELEASEID")

    AM_CONDITIONAL([WITH_TEST_CASE], test x$test_case = xyes )

    AM_CONDITIONAL([COVERAGE], test x$coverage = xyes )

    dnl see whether subversion is installed
    AC_PATH_PROG(svnversioncommand, svnversion)

    AC_ARG_WITH([svnfile],
           AS_HELP_STRING([--with-svnfile],
		          [use svnfile svn_dist_version instead of svnversion -n (default is NO)]),
	   		  [
			    if test "$withval" = "yes"; then
			    	use_svnfile=yes
			    fi
			  ],
			  [use_svnfile=no]
	   )
    dnl use svnversion to record the current repository revision only if
    dnl subversion is installed and we are in a working copy
    if test "X$svnversioncommand" = "X" || test "X$use_svnfile" = "Xyes"; then
        mysvnversion="cat \$(top_srcdir)/svn_dist_version"
        AM_CONDITIONAL([HAVESVNWC], false)
    else
        mysvnversion="svnversion -n '.'"
        AM_CONDITIONAL([HAVESVNWC], true)
    fi

    AC_SUBST(mysvnversion)

  AC_ARG_WITH([mem-trace],
           AS_HELP_STRING([--with-mem-trace],
		          [with memory trace (default is NO)]),
	   		  [
			    if test "$withval" = "yes"; then
			    	memtrace=yes
			    fi
			  ],
			  [memtrace=no]
	   )
	if test $memtrace = "yes"
	then
        AM_CXXFLAGS="${AM_CXXFLAGS}  -D__OB_MTRACE__ -rdynamic"
	fi
    AM_CONDITIONAL([MEMTRACE], test x$memtrace = xyes )

	AC_ARG_WITH([mem-debug],
           AS_HELP_STRING([--with-mem-debug],
		          [with memory debug features (default is NO)]),
	   		  [
			    if test "$withval" = "yes"; then
			    	memdebug=yes
			    fi
			  ],
			  [memdebug=no]
	   )
	if test $memdebug = "yes"
	then
        AM_CXXFLAGS="${AM_CXXFLAGS}  -D__OB_MDEBUG__"
	fi
    AM_CONDITIONAL([MEMDEBUG], test x$memdebug = xyes )
	AC_SUBST([AM_CXXFLAGS])
	AC_SUBST([AM_CFLAGS])
	AC_SUBST([AM_LDFLAGS])

    if test "X${CXXFLAGS}" = "X-g -O2"; then
        CXXFLAGS=""
    fi
    AC_SUBST([CXXFLAGS])
    AC_MSG_RESULT([CXXFLAGS=${CXXFLAGS}])

    AC_ARG_WITH([mysqltest],
    AS_HELP_STRING([--with-mysqltest],
    [whether to build mysqltest (default is NO)]),
    [],
	[with_mysqltest="no"])
	AM_CONDITIONAL([WITH_MYSQLTEST], test x$with_mysqltest = xyes)

    AC_CONFIG_FILES([\
                     Makefile \
                     script/Makefile \
                     script/supervisor/Makefile \
                     tools/Makefile \
                     tools/syschecker/Makefile \
                     tools/syschecker/client/Makefile \
                     tools/dump/Makefile \
                     src/Makefile \
                     src/common/Makefile \
                     src/common/compress/Makefile \
                     src/common/btree/Makefile \
                     src/common/btree/test/Makefile \
                     src/common/roottable/Makefile \
                     src/rootserver/Makefile \
                     src/chunkserver/Makefile \
                     src/proxyserver/Makefile \
                     src/updateserver/Makefile \
                     src/mergeserver/Makefile \
                     src/importserver/Makefile \
                     src/sstable/Makefile \
                     src/mrsstable/Makefile \
                     src/lsync/Makefile \
                     src/compactsstable/Makefile \
                     src/compactsstablev2/Makefile \
                     src/sql/Makefile \
                     src/obmysql/Makefile \
                     src/client/Makefile \
                     src/client/obsql/Makefile \
                     src/client/obsql/src/Makefile \
                     src/client/obsql/demo/Makefile \
                     src/client/obsql/test/Makefile \
                     src/common/timezone/Makefile \
		     src/backupserver/Makefile\
                     tests/Makefile \
                     tests/chunkserver/Makefile \
                     tests/common/Makefile \
                     tests/common/compress/Makefile \
                     tests/common/hash/Makefile \
                     tests/rootserver/Makefile \
                     tests/updateserver/Makefile \
                     tests/updateserver/stress_test/Makefile \
                     tests/mergeserver/Makefile \
                     tests/sql/Makefile \
                     tests/obmysql/Makefile \
                     tests/sstable/Makefile \
                     tests/compactsstablev2/Makefile \
                     tests/lsync/Makefile \
                     tools/log_tool/Makefile \
                     tools/mixed_test/Makefile \
                     tools/newsqltest/Makefile \
                     tools/sstable_builder/Makefile \
                     tools/compactsstablev2/Makefile \
                     tools/getquerytest/Makefile \
                     tools/mysqltest/Makefile\
                     tools/benchmark/runner/Makefile\
                     tools/liboblog/Makefile\
					 tools/benchmark/Makefile
                     ])
    AC_OUTPUT
