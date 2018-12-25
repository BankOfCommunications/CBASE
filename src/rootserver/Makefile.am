AM_CPPFLAGS := $(AM_CPPFLAGS) -I${TBLIB_ROOT}/include/tbsys \
              -I${EASY_ROOT}/include/easy   \
              -I${top_srcdir}/include       \
              -I${top_srcdir}/src           \
              -fPIC

LIBTOOLFLAGS=--preserve-dup-deps

LDADD = $(top_builddir)/src/common/roottable/libroottable.a \
        $(top_builddir)/src/common/libcommon.a              \
        $(top_builddir)/src/sql/libsql.a                    \
        $(top_builddir)/src/common/libcommon.a              \
				$(top_builddir)/src/common/timezone/libtimezone.a          \
        ${EASY_LIB_PATH}/libeasy.a                          \
        ${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -lssl -laio -lboost_regex
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

pub_source =                                                                \
  rootserver.conf.template                                                  \
  ob_root_main.h                                                            \
  ob_root_admin_cmd.h                                                       \
  ob_root_admin2.h                                                          \
  ob_root_stat.h                                                            \
  ob_root_server_state.h               ob_root_server_state.cpp             \
	ob_daily_merge_checker.h             ob_daily_merge_checker.cpp           \
  ob_heartbeat_checker.h               ob_heartbeat_checker.cpp             \
  ob_rs_trigger_event_util.h           ob_rs_trigger_event_util.cpp         \
  ob_root_inner_table_task.h           ob_root_inner_table_task.cpp         \
  ob_root_async_task_queue.h           ob_root_async_task_queue.cpp         \
  ob_chunk_server_manager.h            ob_chunk_server_manager.cpp          \
  ob_root_sql_proxy.h                  ob_root_sql_proxy.cpp                \
  ob_migrate_info.h                    ob_migrate_info.cpp                  \
  ob_restart_server.h                  ob_restart_server.cpp                \
  ob_root_balancer.h                   ob_root_balancer.cpp                 \
  ob_root_balancer_runnable.h          ob_root_balancer_runnable.cpp        \
  ob_root_bootstrap.h                  ob_root_bootstrap.cpp                \
  ob_root_callback.h                   ob_root_callback.cpp                 \
  ob_root_ddl_operator.h               ob_root_ddl_operator.cpp             \
  ob_root_fetch_thread.h               ob_root_fetch_thread.cpp             \
  ob_root_log_manager.h                ob_root_log_manager.cpp              \
  ob_root_log_replay.h                 ob_root_log_replay.cpp               \
  ob_root_log_worker.h                 ob_root_log_worker.cpp               \
  ob_root_meta2.h                      ob_root_meta2.cpp                    \
  ob_root_ms_provider.h                ob_root_ms_provider.cpp              \
  ob_root_rpc_stub.h                   ob_root_rpc_stub.cpp                 \
  ob_root_server2.h                    ob_root_server2.cpp                  \
  ob_root_stat_key.h                   ob_root_stat_key.cpp                 \
  ob_root_table2.h                     ob_root_table2.cpp                   \
  ob_root_ups_provider.h               ob_root_ups_provider.cpp             \
  ob_root_util.h                       ob_root_util.cpp                     \
  ob_root_worker.h                     ob_root_worker.cpp                   \
  ob_schema_service_ms_provider.h      ob_schema_service_ms_provider.cpp    \
  ob_schema_service_ups_provider.h     ob_schema_service_ups_provider.cpp   \
  ob_server_balance_info.h             ob_server_balance_info.cpp           \
  ob_tablet_info_manager.h             ob_tablet_info_manager.cpp           \
  ob_ups_check_runnable.h              ob_ups_check_runnable.cpp            \
  ob_ups_heartbeat_runnable.h          ob_ups_heartbeat_runnable.cpp        \
  ob_ups_manager.h                     ob_ups_manager.cpp                   \
  ob_root_server_config.h              ob_root_server_config.cpp            \
  ob_root_monitor_table.h              ob_root_monitor_table.cpp            \
  ob_rs_after_restart_task.h           ob_rs_after_restart_task.cpp         \
  ob_rs_get_server_status_task.h       ob_rs_get_server_status_task.cpp     \
  ob_root_operation_helper.h           ob_root_operation_helper.cpp         \
	ob_root_operation_data.h             ob_root_operation_data.cpp           \
	ob_rs_schema_operation.h             ob_rs_schema_operation.cpp           \
	ob_root_table_operation.h            ob_root_table_operation.cpp          \
	ob_root_timer_task.h                 ob_root_timer_task.cpp               \
	ob_root_reload_config.h              ob_root_reload_config.cpp            \
  	ob_data_source_mgr.h                 ob_data_source_mgr.cpp	          \
	ob_index_monitor.h		     ob_index_monitor.cpp                 \
  	ob_root_election_node_mgr.h	     ob_root_election_node_mgr.cpp	  \
  	ob_root_election_checker.h	     ob_root_election_checker.cpp	  \
	ob_root_cluster_manager.h        ob_root_cluster_manager.cpp      \
 	 ob_rs_heartbeat_runnable.h	     ob_rs_heartbeat_runnable.cpp       


noinst_LIBRARIES = librootserver.a
bin_PROGRAMS = rootserver schema_reader checkpoint2str \
               str2checkpoint rs_admin rs_stress

librootserver_a_SOURCES = ${pub_source}
rootserver_SOURCES = ob_root_main.cpp main.cpp ${pub_source}
nodist_rootserver_SOURCES = $(top_srcdir)/svn_version.cpp
schema_reader_SOURCES = ob_schema_reader.cpp ${pub_source}
nodist_schema_reader_SOURCES = $(top_srcdir)/svn_version.cpp
checkpoint2str_SOURCES = ob_check_point2str.cpp ${pub_source}
nodist_checkpoint2str_SOURCES =  $(top_srcdir)/svn_version.cpp
str2checkpoint_SOURCES = ob_str2check_point.cpp ${pub_source}
nodist_str2checkpoint_SOURCES =  $(top_srcdir)/svn_version.cpp
rs_admin_SOURCES = ob_root_admin2.cpp ob_root_admin2_main.cpp ${pub_source}
nodist_rs_admin_SOURCES = $(top_srcdir)/svn_version.cpp
rs_stress_SOURCES = ob_rs_stress.cpp ${pub_source}
nodist_rs_stress_SOURCES = $(top_srcdir)/svn_version.cpp

conf_DATA=schema.ini
confdir=${prefix}/etc
EXTRA_DIST = schema.ini

include Flymake.mk

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk
clean-local:
	-rm -f *.gcov *.gcno *.gcda
