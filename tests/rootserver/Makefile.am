AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/sql \
			  -I${top_srcdir}/src/rootserver
LIBTOOLFLAGS=--preserve-dup-deps
LDADD = $(top_builddir)/src/rootserver/librootserver.a \
		$(top_builddir)/src/common/roottable/libroottable.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/sql/libsql.a \
		$(top_builddir)/src/common/libcommon.a \
		${EASY_LIB_PATH}/libeasy.a \
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm  -lgtest -lgmock -lrt -lboost_regex ${GCOV_LIB} -lssl -laio
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS =  base_main_test \
			  test_root_monitor_table \
				ob_obi_role_test\
        test_batch_create_table\
				chunk_servers_manager_test \
				test_root_table2 \
				ob_ups_manager_test \
				test_tablet_info_manager\
       test_root_table2_test\
				root_server_test\
       ob_new_balance_test\
       ob_delete_replicas_test

test_root_monitor_table_SOURCES = test_root_monitor_table.cpp
nodist_test_root_monitor_table_SOURCES = $(top_srcdir)/svn_version.cpp    
chunk_servers_manager_test_SOURCES = chunk_servers_manager_test.cpp
base_main_test_SOURCES = base_main_test.cpp test_main.cpp
root_server_test_SOURCES = root_server_test.cpp
nodist_root_server_test_SOURCES = $(top_srcdir)/svn_version.cpp    
#root_server_log_test_SOURCES = root_server_log_test.cpp
test_tablet_info_manager_SOURCES = test_tablet_info_manager.cpp
test_root_table2_SOURCES = test_root_table2.cpp $(top_srcdir)/svn_version.cpp
test_root_table2_test_SOURCES = test_root_table2_test.cpp $(top_srcdir)/svn_version.cpp
ob_obi_role_test_SOURCES = ob_obi_role_test.cpp
ob_ups_manager_test_SOURCES = ob_ups_manager_test.cpp
nodist_ob_ups_manager_test_SOURCES = $(top_srcdir)/svn_version.cpp
ob_new_balance_test_SOURCES = ob_new_balance_test.cpp
nodist_ob_new_balance_test_SOURCES = $(top_srcdir)/svn_version.cpp    
ob_delete_replicas_test_SOURCES = ob_delete_replicas_test.cpp
nodist_ob_delete_replicas_test_SOURCES = $(top_srcdir)/svn_version.cpp    
test_batch_create_table_SOURCES = test_batch_create_table.cpp
#EXTRA_DIST = mock_chunk_server.h  mock_server.h  mock_update_server.h  root_server_tester.h  test_main.h mock_root_rpc_stub.h
EXTRA_DIST = test_main.h \
						 mock_root_rpc_stub.h \
						 mock_chunk_server.h  \
						 mock_server.h \
						 mock_update_server.h  \
						 root_server_tester.h

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS)

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda
