#SUBDIRS=olap
#DIST_SUBDIRS=olap
AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/common/btree \
			  -I${top_srcdir}/src/mergeserver

LIBTOOLFLAGS=--preserve-dup-deps

LDADD = $(top_builddir)/src/mergeserver/libmergeserver.a \
				$(top_builddir)/src/obmysql/libobmysql.a  \
				$(top_builddir)/src/sql/libsql.a \
				$(top_builddir)/src/mergeserver/libmergeserver.a \
				$(top_builddir)/src/common/libcommon.a \
				$(top_builddir)/src/sql/libsql.a \
				$(top_builddir)/src/common/libcommon.a \
				$(top_builddir)/src/common/btree/libbtree.a \
				${EASY_LIB_PATH}/libeasy.a \
				${TBLIB_ROOT}/lib/libtbsys.a

CXXFLAGS += -D_MS_MOCK_WHOLE_  -D_BTREE_ENGINE_ -D__UNIT_TEST__
AM_LDFLAGS = -laio -lpthread -lc -lm  -lgtest -lrt -lboost_regex ${GCOV_LIB} -ldl -lssl
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

pub_source = mock_server.cpp \
			 mock_root_server.cpp \
			 mock_update_server.cpp \
			 mock_chunk_server.cpp
new_source = ob_scan_param_loader.cpp \
       ob_scanner_loader.cpp \
       ob_location_list_cache_loader.cpp

bin_PROGRAMS = \
			   test_merger_cache_table            \
			   test_ob_frozen_data_cache

#test_session_manager_SOURCES = test_session_manager.cpp
test_merger_cache_table_SOURCES = test_merger_cache_table.cpp
test_ob_frozen_data_cache_SOURCES = test_ob_frozen_data_cache.cpp
#monitor_tool_SOURCES = monitor_tool.cpp
#test_ups_client_SOURCES = test_ups_client.cpp
#test_mutate_client_SOURCES = test_mutate_client.cpp
#test_merger_scanner_encoder_SOURCES = test_merger_scanner_encoder.cpp
#test_merger_schema_proxy_SOURCES = test_merger_schema_proxy.cpp $(pub_source)
#test_merger_timer_task_SOURCES = test_merger_timer_task.cpp ${pub_source}
#test_merger_root_proxy_SOURCES = test_merger_root_proxy.cpp ${pub_source}
#test_merger_rpc_proxy_SOURCES = test_merger_rpc_proxy.cpp ${pub_source}
#test_merger_rpc_stub_SOURCES = test_merger_rpc_stub.cpp ${pub_source}
#test_merger_version_proxy_SOURCES = test_merger_version_proxy.cpp ${pub_source}
#test_merger_stlmap_SOURCES = test_merger_stlmap.cpp
#test_merger_btreemap_SOURCES = test_merger_btreemap.cpp
#test_merger_tablet_location_SOURCES = test_merger_tablet_location.cpp
#test_merger_schema_manager_SOURCES = test_merger_schema_manager.cpp
#test_groupby_operator_SOURCES = test_groupby_operator.cpp
#test_merger_param_decoder_SOURCES = test_merger_param_decoder.cpp
#test_merger_mutator_decoder_SOURCES = test_merger_mutator_decoder.cpp
#test_read_param_decoder_SOURCES = test_read_param_decoder.cpp
#test_read_param_modifier_SOURCES = test_read_param_modifier.cpp
#test_param_modifier_SOURCES = test_param_modifier.cpp
#test_scan_backward_SOURCES = test_scan_backward.cpp
#test_cell_operator_SOURCES=test_cell_operator.cpp
#test_ob_ms_scan_event_SOURCES=test_ob_ms_scan_event.cpp $(pub_source)
#test_ms_scan_param_SOURCES=test_ms_scan_param.cpp
#test_mj_client_SOURCES = test_mj_client.cpp
#test_merger_sorted_operator_SOURCES = test_merger_sorted_operator.cpp
#test_merger_reverse_operator_SOURCES = test_merger_reverse_operator.cpp
#test_merger_groupby_operator_SOURCES = test_merger_groupby_operator.cpp
#test_merger_operator_SOURCES = test_merger_operator.cpp
#test_ob_scan_param_loader_SOURCES = test_ob_scan_param_loader.cpp $(new_source)
#test_ob_scanner_loader_SOURCES = test_ob_scanner_loader.cpp $(new_source)
#test_ob_location_list_cache_loader_SOURCES = test_ob_location_list_cache_loader.cpp $(new_source)
#test_merger_scan_SOURCES = test_merger_scan.cpp
#test_ms_sub_get_request_SOURCES = test_ms_sub_get_request.cpp
#test_get_merger_SOURCES = test_get_merger.cpp
#test_ms_get_event_parellel_SOURCES=test_ms_get_event_parellel.cpp
#sql_get_merger_test_SOURCES = sql_get_merger_test.cpp
#sql_get_request_test_SOURCES = sql_get_request_test.cpp $(new_source)

check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS)
CLEANFILES = $(check_SCRIPTS)
EXTRA_DIST=mock_chunk_server.h  mock_define.h  mock_root_server.h  mock_server.h  mock_update_server.h mock_client.h \
					 ob_scan_param_loader.h ob_location_list_cache_loader.h  ob_scanner_loader.h
clean-local:
	-rm -f *.gcov *.gcno *.gcda
