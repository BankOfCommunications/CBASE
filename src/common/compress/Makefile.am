AM_CPPFLAGS= -fPIC
AM_LDFLAGS=-lc -lm -lrt -lboost_regex
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

noinst_LIBRARIES = libcomp.a
lib_LTLIBRARIES = liblzo_1.0.la \
		  libsnappy_1.0.la \
		  libnone.la

libcomp_a_SOURCES = ob_compressor.cpp

liblzo_1_0_la_SOURCES = lzo_compressor.cpp
liblzo_1_0_la_LDFLAGS = -ldl -lm -llzo2

libsnappy_1_0_la_SOURCES = snappy_compressor.cpp
libsnappy_1_0_la_LDFLAGS = -ldl -lm -lsnappy

libnone_la_SOURCES = none_compressor.cpp
libnone_la_LDFLAGS = -ldl

if COVERAGE
libsnappy_1_0_la_LDFLAGS +=-lgcov
libnone_la_LDFLAGS +=-lgcov
liblzo_1_0_la_LDFLAGS +=-lgcov
endif
EXTRA_DIST = \
	lzo_compressor.h \
	ob_compressor.h \
	snappy_compressor.h \
	none_compressor.h
clean-local:
	-rm -f *.gcov *.gcno *.gcda/Users/liuyun/taobao/oceanbase/src/common/compress//Makefile.am
