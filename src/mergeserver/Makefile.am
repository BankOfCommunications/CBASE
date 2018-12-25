AM_CPPFLAGS := $(AM_CPPFLAGS) -I${EASY_ROOT}/include/easy   \
              -I${TBLIB_ROOT}/include/tbsys \
              -I${top_srcdir}/include       \
              -I${top_srcdir}/src

LIBTOOLFLAGS=--preserve-dup-deps

LDADD = $(top_builddir)/src/common/roottable/libroottable.a \
        $(top_builddir)/src/sql/libsql.a            \
        $(top_builddir)/src/obmysql/libobmysql.a    \
        $(top_builddir)/src/common/libcommon.a      \
        $(top_builddir)/src/sql/libsql.a            \
        $(top_builddir)/src/common/libcommon.a      \
        $(top_builddir)/src/sql/libsql.a            \
        $(top_builddir)/src/common/btree/libbtree.a \
				$(top_builddir)/src/common/timezone/libtimezone.a \
        ${EASY_LIB_PATH}/libeasy.a                  \
        ${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -lssl -laio -lboost_regex
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

if MEMDEBUG
AM_LDFLAGS+=-ldmalloc
endif

pub_source =                                                            \
  mergeserver.conf.template                                             \
  ob_ms_tsi.h                                                           \
  ob_chunk_server_task_dispatcher.h ob_chunk_server_task_dispatcher.cpp \
  ob_merge_callback.h               ob_merge_callback.cpp               \
  ob_merge_server.h                 ob_merge_server.cpp                 \
  ob_merge_server_main.h            ob_merge_server_main.cpp            \
  ob_merge_server_service.h         ob_merge_server_service.cpp         \
  ob_merger_groupby_operator.h      ob_merger_groupby_operator.cpp      \
  ob_ms_server_counter.h            ob_ms_server_counter.cpp            \
  ob_merger_operator.h              ob_merger_operator.cpp              \
  ob_merger_reverse_operator.h      ob_merger_reverse_operator.cpp      \
  ob_merger_sorted_operator.h       ob_merger_sorted_operator.cpp       \
  ob_ms_lease_task.h                ob_ms_lease_task.cpp                \
  ob_ms_monitor_task.h              ob_ms_monitor_task.cpp              \
  ob_ms_rpc_proxy.h                 ob_ms_rpc_proxy.cpp                 \
  ob_ms_async_rpc.h                 ob_ms_async_rpc.cpp                 \
  ob_rpc_event.h                    ob_rpc_event.cpp                    \
  ob_ms_rpc_event.h                 ob_ms_rpc_event.cpp                 \
  ob_ms_request.h                   ob_ms_request.cpp                   \
  ob_ms_scan_request.h              ob_ms_scan_request.cpp              \
  ob_ms_sub_scan_request.h          ob_ms_sub_scan_request.cpp          \
  ob_ms_get_request.h               ob_ms_get_request.cpp               \
  ob_ms_sub_get_request.h           ob_ms_sub_get_request.cpp           \
  ob_ms_scan_param.h                ob_ms_scan_param.cpp                \
  ob_ms_scanner_encoder.h           ob_ms_scanner_encoder.cpp           \
  ob_ms_schema_proxy.h              ob_ms_schema_proxy.cpp              \
  ob_ms_schema_task.h               ob_ms_schema_task.cpp               \
  ob_ms_service_monitor.h           ob_ms_service_monitor.cpp           \
  ob_ms_sql_sorted_operator.h       ob_ms_sql_sorted_operator.cpp       \
  ob_ms_sql_operator.h              ob_ms_sql_operator.cpp              \
  ob_sql_rpc_event.h                ob_sql_rpc_event.cpp                \
  ob_ms_sql_rpc_event.h             ob_ms_sql_rpc_event.cpp             \
  ob_ms_sql_request.h               ob_ms_sql_request.cpp               \
  ob_ms_sql_scan_request.h          ob_ms_sql_scan_request.cpp          \
  ob_ms_sql_sub_scan_request.h      ob_ms_sql_sub_scan_request.cpp      \
  ob_ms_sql_get_request.h           ob_ms_sql_get_request.cpp           \
  ob_ms_sql_sub_get_request.h       ob_ms_sql_sub_get_request.cpp       \
  ob_ms_ups_task.h                  ob_ms_ups_task.cpp                  \
  ob_mutator_param_decoder.h        ob_mutator_param_decoder.cpp        \
  ob_param_decoder.h                ob_param_decoder.cpp                \
  ob_query_cache.h                  ob_query_cache.cpp                  \
  ob_read_param_decoder.h           ob_read_param_decoder.cpp           \
  ob_read_param_modifier.h          ob_read_param_modifier.cpp          \
  ob_rs_rpc_proxy.h                 ob_rs_rpc_proxy.cpp                 \
  ob_merge_server_config.h          ob_merge_server_config.cpp          \
  ob_ms_sql_proxy.h                 ob_ms_sql_proxy.cpp                 \
  ob_get_privilege_task.h           ob_get_privilege_task.cpp           \
	ob_bloom_filter_task_queue_thread.h ob_bloom_filter_task_queue_thread.cpp\
	ob_insert_cache.h                 ob_insert_cache.cpp\
  ob_merge_reload_config.h          ob_merge_reload_config.cpp          \
  ob_frozen_data_cache.h            ob_frozen_data_cache.cpp \
	ob_ms_request_result.h            ob_ms_request_result.cpp


# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk
noinst_LIBRARIES = libmergeserver.a
bin_PROGRAMS = mergeserver
libmergeserver_a_SOURCES= ${pub_source} $(top_srcdir)/svn_version.cpp
mergeserver_SOURCES= main.cpp ${pub_source}
nodist_mergeserver_SOURCES= $(top_srcdir)/svn_version.cpp

#pkgdata_data_DATA=mergeserver.conf.template
#pkgdata_datadir=${prefix}/etc
clean-local:
	-rm -f *.gcov *.gcno *.gcda
