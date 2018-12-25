AM_CPPFLAGS = -I${TBLIB_ROOT}/include/tbsys \
			  -I${TBLIB_ROOT}/include/tbnet \
			  -I${EASY_ROOT}/include/easy \
			  -I${top_srcdir}/include \
			  -I${top_srcdir}/src \
			  -I${top_srcdir}/src/common \
			  -I${top_srcdir}/src/common/hash

LDADD =  \
		 ${TBLIB_ROOT}/lib/libtbnet.a \
		 ${top_builddir}/src/compactsstable/libcompactsstable.a \
		 ${top_builddir}/src/sstable/libsstable.a \
		 ${top_builddir}/src/chunkserver/libchunkserver.a \
		 $(top_builddir)/src/common/compress/libcomp.a \
		 ${top_builddir}/src/common/libcommon.a \
		 ${EASY_LIB_PATH}/libeasy.a \
		 ${TBLIB_ROOT}/lib/libtbsys.a -lcrypt

AM_LDFLAGS=-lpthread -lc -lm -lrt -lgtest -lboost_regex ${GCOV_LIB} -lcrypt 
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

bin_PROGRAMS = test_compact_row \
			   test_blockmem \
			   test_compactsstable_mem

test_compact_row_SOURCES = test_compact_row.cpp
test_blockmem_SOURCES = test_blockmem.cpp
test_compactsstable_mem = test_compactsstable_mem.cpp


check_SCRIPTS = $(bin_PROGRAMS)
TESTS = $(check_SCRIPTS)

CLEANFILES = $(check_SCRIPTS)
clean-local:
	-rm -f *.gcov *.gcno *.gcda
