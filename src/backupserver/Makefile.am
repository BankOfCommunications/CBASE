AM_CPPFLAGS := $(AM_CPPFLAGS) -I${TBLIB_ROOT}/include/tbsys \
              -I${EASY_ROOT}/include/easy   \
              -I${top_srcdir}/include       \
              -I${top_srcdir}/src

LIBTOOLFLAGS=--preserve-dup-deps

LDADD = ${top_builddir}/src/chunkserver/libchunkserver.a\
        ${top_builddir}/src/rootserver/librootserver.a\
		${top_builddir}/src/updateserver/libupdateserver.a\
		$(top_builddir)/src/common/roottable/libroottable.a \
		$(top_builddir)/src/sstable/libsstable.a                \
		$(top_builddir)/src/compactsstable/libcompactsstable.a  \
        $(top_builddir)/src/common/libcommon.a                  \
        $(top_builddir)/src/sql/libsql.a                        \
        $(top_builddir)/src/mergeserver/libmergeserver.a \
	$(top_builddir)/src/obmysql/libobmysql.a    \
        $(top_builddir)/src/common/libcommon.a  \
		$(top_builddir)/src/compactsstablev2/libcompactsstablev2.a  \
        $(top_builddir)/src/common/compress/libcomp.a           \
        $(top_builddir)/src/common/btree/libbtree.a             \
        $(top_builddir)/src/common/libcommon.a                  \
        $(top_builddir)/src/sql/libsql.a                        \
	$(top_builddir)/src/mergeserver/libmergeserver.a \
        $(top_builddir)/src/common/libcommon.a  \
				$(top_builddir)/src/common/timezone/libtimezone.a       \
        ${EASY_LIB_PATH}/libeasy.a                              \
        ${TBLIB_ROOT}/lib/libtbsys.a -lcrypt 

AM_LDFLAGS = -lpthread -lc -lm -lrt -ldl -laio -lssl -lboost_regex
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libbackupserver.a

 
libbackupserver_a_SOURCES =                                              \
                                  $(top_srcdir)/svn_version.cpp          \
 ob_backup_server.h               ob_backup_server.cpp                   \
 ob_backup_server_main.h          ob_backup_server_main.cpp              \
 ob_tablet_backup_service.h       ob_tablet_backup_service.cpp           \
 ob_backup_server_config.h        ob_backup_server_config.cpp            \
 ob_backup_callback.h             ob_backup_callback.cpp                 \
 ob_tablet_backup_runnable.h      ob_tablet_backup_runnable.cpp          \
 ob_tablet_backup_manager.h       ob_tablet_backup_manager.cpp           \
 ob_backup_rs_fetch_runnable.h    ob_backup_rs_fetch_runnable.cpp          \
 ob_backup_ups_fetch_runnable.h   ob_backup_ups_fetch_runnable.cpp

bin_PROGRAMS = backupserver
confdir=${prefix}/etc
backupserver_SOURCES =ob_disk_path.cpp main.cpp ${libbackupserver_a_SOURCES} 

conf_DATA=backupserver.conf.template
EXTRA_DIST = \
						 backupserver.conf.template

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk

clean-local:
	-rm -f *.gcov *.gcno *.gcda
