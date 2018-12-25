AM_CPPFLAGS := $(AM_CPPFLAGS) -I${TBLIB_ROOT}/include/tbsys \
              -I${EASY_ROOT}/include/easy   \
              -I${top_srcdir}/include       \
              -I${top_srcdir}/src

LIBTOOLFLAGS=--preserve-dup-deps

LDADD = $(top_builddir)/src/sstable/libsstable.a                \
		$(top_builddir)/src/compactsstable/libcompactsstable.a  \
        $(top_builddir)/src/common/libcommon.a                  \
        $(top_builddir)/src/sql/libsql.a                        \
		$(top_builddir)/src/compactsstablev2/libcompactsstablev2.a  \
        $(top_builddir)/src/common/compress/libcomp.a           \
        $(top_builddir)/src/common/btree/libbtree.a             \
        $(top_builddir)/src/common/libcommon.a                  \
        $(top_builddir)/src/sql/libsql.a                        \
        ${EASY_LIB_PATH}/libeasy.a                              \
        ${TBLIB_ROOT}/lib/libtbsys.a


AM_LDFLAGS = -lpthread -lm -lrt -ldl -laio -lssl -lboost_regex
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libproxyserver.a


libproxyserver_a_SOURCES =                                               \
                                  $(top_srcdir)/svn_version.cpp          \
 ob_proxy_server.h                ob_proxy_server.cpp                    \
 ob_proxy_server_main.h           ob_proxy_server_main.cpp               \
 ob_proxy_service.h               ob_proxy_service.cpp                   \
 ob_proxy_server_config.h         ob_proxy_server_config.cpp             \
 ob_yunti_proxy.h                 ob_yunti_proxy.cpp                     \
 ob_yunti_meta.h                  ob_yunti_meta.cpp                      \
 ob_proxy_callback.h              ob_proxy_callback.cpp                  \
 ob_proxy_reader.h                ob_proxy_reader.cpp

bin_PROGRAMS = proxyserver
confdir=${prefix}/etc
proxyserver_SOURCES = main.cpp ${libproxyserver_a_SOURCES}

conf_DATA=proxyserver.conf.template
EXTRA_DIST = \
						 proxyserver.conf.template

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk

clean-local:
	-rm -f *.gcov *.gcno *.gcda
