LIBOBLOG_VERSION=1:0:0

lib_LTLIBRARIES = liboblog.la
liboblog_la_SOURCES = liboblog.cpp liboblog.h															\
											ob_log_common.h	 ob_log_common.cpp									\
											ob_log_rpc_stub.cpp ob_log_rpc_stub.h								\
											ob_log_filter.cpp ob_log_filter.h										\
											ob_log_router.cpp	ob_log_router.h										\
											ob_log_meta_manager.cpp ob_log_meta_manager.h				\
											ob_log_dml_info.cpp ob_log_dml_info.h								\
											ob_log_formator.cpp ob_log_formator.h								\
											ob_log_fetcher.cpp ob_log_fetcher.h									\
											ob_log_spec.cpp ob_log_spec.h												\
											ob_log_config.cpp ob_log_config.h										\
						 					ob_log_entity.cpp ob_log_entity.h										\
											ob_log_partitioner.cpp ob_log_partitioner.h					\
											ob_log_server_selector.cpp ob_log_server_selector.h	\
											ob_log_mysql_adaptor.cpp ob_log_mysql_adaptor.h			\
											ob_log_perf.cpp ob_log_perf.h												\
											ob_log_binlog_record.cpp ob_log_binlog_record.h     \
											ob_log_schema_getter.cpp ob_log_schema_getter.h     \
						 					ob_log_utils.h	ob_log_utils.cpp										\
											ob_log_mysql_query_result.h  ob_log_mysql_query_result.cpp \
											ob_log_mysql_query_engine.h  ob_log_mysql_query_engine.cpp \
											ob_log_main.c

nodist_liboblog_la_SOURCES= $(top_srcdir)/svn_version.c

liboblog_la_CPPFLAGS = -g -fPIC 																				\
                       -I${TBLIB_ROOT}/include/tbsys 										\
                       -I${TBLIB_ROOT}/include/tbnet 										\
											 -I${EASY_ROOT}/include/easy											\
                       -I${top_srcdir}/src															\
                       -I${top_srcdir}/src/common												\
											 -I${top_srcdir}/src/client/obsql/src							\
											 -I${DRC_ROOT}/include/drcmessage

liboblog_la_LDFLAGS = -fPIC -rdynamic 																	\
											-version-info $(LIBOBLOG_VERSION) 								\
											-Wl,-Bsymbolic																		\
					  					-Wl,-e,so_main

liboblog_la_LIBADD = -lpthread -lrt -lcurl -llua -lboost_regex												\
										 -L${DRC_ROOT}/lib64 -ldrcmsg												\
										 -L$(top_builddir)/src/updateserver/ -lupdateserver	\
										 -L$(top_builddir)/src/obmysql/ -lobmysql					  \
										 -L$(top_builddir)/src/common/ -lcommon							\
										 $(top_builddir)/src/client/obsql/src/libobsql.a \
										 ${EASY_LIB_PATH}/libeasy.a													\
										 ${TBLIB_ROOT}/lib/libtbsys.a												\
										 ${TBLIB_ROOT}/lib/libtbnet.a

if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
liboblog_la_LIBADD+=-lgcov
endif

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk

EXTRA_DIST= liboblog.conf liboblog.partition.lua liboblog.h
pkgdata_data_DATA= liboblog.conf liboblog.partition.lua
pkgdata_datadir=${prefix}/etc

pkgdata_include_DATA= liboblog.h
pkgdata_includedir=${prefix}/include

AM_CPPFLAGS = -g -fPIC                              \
							-I${TBLIB_ROOT}/include/tbsys					\
							-I${TBLIB_ROOT}/include/tbnet         \
							-I${EASY_ROOT}/include/easy           \
							-I${top_srcdir}/src   	              \
							-I${top_srcdir}/src/common						\
							-I${top_srcdir}/src/client/obsql/src	\
							-I${DRC_ROOT}/include/drcmessage
LDADD=liboblog.la
bin_PROGRAMS = oblog_tailf
oblog_tailf_SOURCES = oblog_tailf.cpp ${liboblog_la_SOURCES}
