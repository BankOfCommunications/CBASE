AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
							-I${TBLIB_ROOT}/include/tbnet \
							-I${EASY_ROOT}/include/easy \
							-I$(top_builddir)/src/common/ \
							-I${top_srcdir}/src\
							-I$(top_builddir)/tools/benchmark/runner/mysql_5.5/include

LDADD = $(top_builddir)/src/common/libcommon.a \
				$(top_builddir)/src/sql/libsql.a \
				$(top_builddir)/src/common/libcommon.a \
				$(top_builddir)/src/common/btree/libbtree.a \
				${EASY_LIB_PATH}/libeasy.a \
				${TBLIB_ROOT}/lib/libtbnet.a \
				${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lm -lrt -ldl -lstdc++ -L/usr/lib64/mysql/ -lmysqlclient -lboost_regex
CXXFLAGS = -D__STDC_LIMIT_MACROS -g -Wall -DORDER_SERIALIZE
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

pub_source =  \
              load_query.cpp \
              load_parser.cpp \
              load_util.cpp \
              load_filter.cpp \
              load_runner.cpp \
              load_manager.cpp \
              load_producer.cpp \
              load_consumer.cpp \
              load_watcher.cpp \
              load_statics.cpp\
							load_rpc.cpp\
							packet_queue.cpp\
              load_client.h\
              load_client_producer.h\
              load_consumer.h\
              load_define.h\
              load_filter.h\
              load_manager.h\
              load_packet.h\
              load_param.h\
              load_parser.h\
              load_producer.h\
              load_query.h\
              load_rpc.h\
              load_runner.h\
              load_server.h\
              load_statics.h\
              load_util.h\
              load_watcher.h\
              packet_queue.h

noinst_LIBRARIES = libloadrunner.a
libloadrunner_a_SOURCES = $(pub_source)

bin_PROGRAMS = ob_load_meter
ob_load_meter_SOURCES = load_meter_read_only.cpp $(pub_source)
EXTRA_DIST =  mysql_5.5

clean-local:
	-rm -f *.gcov *.gcno *.gcda
