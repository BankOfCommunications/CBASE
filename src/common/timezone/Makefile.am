AM_CPPFLAGS := $(AM_CPPFLAGS) -I${TBLIB_ROOT}/include/tbsys\
             -I${EASY_ROOT}/include/easy\
             -I${top_srcdir}/include\
             -I${top_srcdir}/src\
             -fPIC

LDADD = ${EASY_LIB_PATH}/libeasy.a \
        ${TBLIB_ROOT}/lib/libtbsys.a \
        $(top_builddir)/src/common/btree/libbtree.a \
        ${top_builddir}/src/sql/libsql.a

AM_LDFLAGS=-lpthread -lc -lm  -lrt -lcrypt -lssl -laio -lboost_regex #-pg
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

AM_CFLAGS+=-DUSG_COMPAT

noinst_LIBRARIES = libtimezone.a

timezone_sources=				                                                  \
	private.h \
	tzfile.h \
	localtime.c

libtimezone_a_SOURCES = ${timezone_sources} 

clean-local:
	-rm -f *.gcov *.gcno *.gcda
