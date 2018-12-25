
LIBSUITE_VERSION=1:0:0

lib_LTLIBRARIES = libsuite.la
libsuite_la_SOURCES = suite.c
nodist_libsuite_la_SOURCES= $(top_srcdir)/svn_version.c
libsuite_la_CPPFLAGS = -g -fPIC
libsuite_la_LDFLAGS = -version-info $(LIBSUITE_VERSION) \
                      -Wl,-e,so_main

bin_PROGRAMS = suite_cntl test
suite_cntl_SOURCES = suite_cntl.c
test_SOURCES = test.c

# rules to auto generate svn_version.cpp
include $(top_srcdir)/svn_version.mk

