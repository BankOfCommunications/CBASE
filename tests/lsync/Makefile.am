AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/tests/updateserver \
			  -I${top_srcdir}/src

LDADD = $(top_builddir)/src/updateserver/libupdateserver.a \
		$(top_builddir)/src/rootserver/librootserver.a \
		$(top_builddir)/src/chunkserver/libchunkserver.a  \
		$(top_builddir)/src/mergeserver/libmergeserver.a \
		$(top_builddir)/src/sstable/libsstable.a \
		$(top_builddir)/src/lsync/liblsyncserver.a \
		$(top_builddir)/src/common/libcommon.a \
		$(top_builddir)/src/common/compress/libcomp.a \
		$(top_builddir)/src/common/btree/libbtree.a \
		${EASY_LIB_PATH}/libeasy.a \
		${TBLIB_ROOT}/lib/libtbnet.a \
		${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lgtest -lpthread -lc -lm -lrt -ldl -laio -lboost_regex ${GCOV_LIB}
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = test_lsync
test_lsync_SOURCES = test_lsync.cpp
lsync_test_CPPFLAGS = -DUNITTEST $(AM_CPPFLAGS)

run-test: test_lsync
	./$< --gtest_filter="" --log_dir=/local/ob/ --start=1 --dev=eth0 --port=2500 --timeout=1000
clean-local:
	-rm -f *.gcov *.gcno *.gcda
