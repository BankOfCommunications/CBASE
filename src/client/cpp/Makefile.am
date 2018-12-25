
LIBOCEANBASE_VERSION=2:0:1

lib_LTLIBRARIES = libobapi.la
libobapi_la_SOURCES = libobjoin.cpp libobapi.cpp ob_client.cpp obapi_main.c 
nodist_libobapi_la_SOURCES= $(top_srcdir)/svn_version.c

libobapi_la_CPPFLAGS = -g -fPIC \
                       -I${TBLIB_ROOT}/include/tbsys \
                       -I${TBLIB_ROOT}/include/tbnet \
                       -I${top_srcdir}/src \
					   -DOBAPI_SO_VERSION=\"@obapi_version@\"

libobapi_la_LDFLAGS = -version-info $(LIBOCEANBASE_VERSION) \
                      -L${top_builddir}/src/common/ \
                      -L${TBLIB_ROOT}/lib/ \
                      -lcommon \
                      -Wl,-static,-ltbnet \
                      -Wl,-static,-ltbsys \
					  -Wl,-e,so_main \
                      -lpthread -lc -lm -lrt -ldl -laio

AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbnet \
			  -I${TBLIB_ROOT}/include/tbsys \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src 

LDADD = $(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/rootserver/librootserver.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a 

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lboost_regex

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = set_sync_demo scan_sync_demo set_async_demo scan_async_demo \
							 sum_async_demo simple_test \
							 join_demo new_set_sync_demo new_scan_sync_demo
set_sync_demo_SOURCES = set_sync_demo.c ${libobapi_la_SOURCES} ${nodist_libobapi_la_SOURCES}

scan_sync_demo_SOURCES = scan_sync_demo.c ${libobapi_la_SOURCES} ${nodist_libobapi_la_SOURCES}

new_scan_sync_demo_SOURCES = new_scan_sync_demo.c ${libobapi_la_SOURCES} ${nodist_libobapi_la_SOURCES}

new_set_sync_demo_SOURCES = new_set_sync_demo.c ${libobapi_la_SOURCES} ${nodist_libobapi_la_SOURCES}

set_async_demo_SOURCES = set_async_demo.c ${libobapi_la_SOURCES} ${nodist_libobapi_la_SOURCES}

scan_async_demo_SOURCES = scan_async_demo.c ${libobapi_la_SOURCES} ${nodist_libobapi_la_SOURCES}

sum_async_demo_SOURCES = sum_async_demo.c ${libobapi_la_SOURCES} ${nodist_libobapi_la_SOURCES}

#scan_all_SOURCES = scan_all.c ${libobapi_la_SOURCES} ${nodist_libobapi_la_SOURCES}

#test_expr_SOURCES = test_expr.c ${libobapi_la_SOURCES} ${nodist_libobapi_la_SOURCES}

#time_test_SOURCES = time_test.c ${libobapi_la_SOURCES} ${nodist_libobapi_la_SOURCES}

simple_test_SOURCES = simple_test.c ${libobapi_la_SOURCES} ${nodist_libobapi_la_SOURCES}

join_demo_SOURCES = join_demo.c ${libobapi_la_SOURCES} ${nodist_libobapi_la_SOURCES}

#test_stream_SOURCES = test_stream.c ${libobapi_la_SOURCES} ${nodist_libobapi_la_SOURCES}

EXTRA_DIST = \
	oceanbase.h \
	libobapi.h \
	libobjoin.h \
	ob_client.h

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk

