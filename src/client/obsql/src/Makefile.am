noinst_LIBRARIES=libobsql.a

libobsql_a_SOURCES =                                                   \
	libobsql.h											 libobsql.cpp													\
  ob_sql_struct.h                  ob_sql_struct.cpp                    \
  ob_sql_atomic.h                  ob_sql_atomic.cpp                    \
  ob_sql_server_info.h                                                  \
  ob_sql_cluster_info.h                                                 \
  ob_sql_curl.h                    ob_sql_curl.cpp                      \
  ob_sql_cluster_config.h          ob_sql_cluster_config.cpp            \
  ob_sql_update_worker.h           ob_sql_update_worker.cpp             \
  ob_sql_data_source.h                                                  \
  ob_sql_data_source_utility.h     ob_sql_data_source_utility.cpp       \
  ob_sql_group_data_source.h       ob_sql_group_data_source.cpp         \
  ob_sql_conn_acquire.h            ob_sql_conn_acquire.cpp              \
  ob_sql_util.h                    ob_sql_util.cpp                      \
  ob_sql_real_func.h               ob_sql_real_func.cpp                 \
  ob_sql_cluster_select.h          ob_sql_cluster_select.cpp            \
  ob_sql_ms_select.h               ob_sql_ms_select.cpp                 \
  ob_sql_mysql_adapter.h           ob_sql_mysql_adapter.cpp             \
  ob_sql_define.h                                                       \
  ob_sql_list.h                    ob_sql_list.cpp                      \
  ob_sql_select_method.h                                                \
  ob_sql_select_method_impl.h     ob_sql_select_method_impl.cpp         \
  ob_sql_global.h                 ob_sql_global.cpp                     \
  ob_sql_parser.h                 ob_sql_parser.cpp




LDADD = $(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/rootserver/librootserver.a

#\
#${TBLIB_ROOT}/lib/libtbnet.a \
#${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lboost_regex

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

libobsql_a_CFLAGS = -g -fPIC \
                       -I${TBLIB_ROOT}/include/tbsys \
                       -I${TBLIB_ROOT}/include/tbnet \
                       -I/usr/local/include \
                       -I${top_srcdir}/include\
                       -I${top_srcdir}/src

libobsql_a_LDFLAGS = -L${top_builddir}/src/common/ \
                      -L${TBLIB_ROOT}/lib/ \
                      -L/usr/lib64/ \
                      -L/usr/local/lib \
                      -lcommon \
                      -Wl,-static,-ltbnet \
                      -Wl,-static,-ltbsys \
                      -Wl,-e,so_main \
                      -lpthread -lc -lm -lrt -ldl -lcrypt -lssl -lcurl
# -version-info 1:0:1


AM_CPPFLAGS= -I${TBLIB_ROOT}/include/tbnet \
             -I${TBLIB_ROOT}/include/tbsys \
             -I${top_srcdir}/include\
             -I /usr/local/lib \
             -I${top_srcdir}/src \
						 -fPIC
EXTRA_DIST= libobsql.conf libobsqlrc
pkgdata_data_DATA= libobsql.conf libobsqlrc
pkgdata_datadir=${prefix}/etc
