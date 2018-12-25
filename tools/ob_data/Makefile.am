AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
							-I${TBLIB_ROOT}/include/tbnet \
							-I$(top_builddir)/src/common/ \
							-I$(top_builddir)/src/mergeserver \
							-I${top_srcdir}/src

LDADD = $(top_builddir)/src/mergeserver/libmergeserver.a \
				$(top_builddir)/src/common/libcommon.a \
				${TBLIB_ROOT}/lib/libtbnet.a \
				${TBLIB_ROOT}/lib/libtbsys.a 

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -lboost_regex
CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall -DORDER_SERIALIZE
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = ob_data mock_merge_server
ob_data_SOURCES =   main.cpp \
										ob_client.cpp \
                    rpc_stub.cpp \
									  base_client.cpp
mock_merge_server_SOURCES = test_merge_server_main.cpp \
														mock_merge_server.cpp \
														mock_server.cpp

EXTRA_DIST =  base_client.h \
              ob_client.h \
							rpc_stub.h \
							mock_merge_server.h \
							mock_server.h

clean-local:
	-rm -f *.gcov *.gcno *.gcda
