noinst_LIBRARIES = libbtree.a
AM_CPPFLAGS=-I${TBLIB_ROOT}/include/tbsys 
#-DOCEAN_BASE_BTREE_USE_SYS_MALLOC -DOCEAN_BASE_BTREE_DEBUG
AM_LDFLAGS=-lc -lm -lrt -lboost_regex
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif

libbtree_a_SOURCES = btree_default_alloc.cpp \
		     btree_node.cpp \
		     btree_read_param.cpp \
		     btree_write_handle.cpp \
		     id_btree.cpp \
		     key_btree.cpp \
		     btree_base.cpp \
		     btree_read_handle_new.cpp \
		     btree_base_handle.cpp \
		     btree_root_pointer.cpp \
		     btree_array_list.cpp \
		     btree_write_param.cpp \
		     id_key_btree.cpp
DIST_SUBDIRS=test
EXTRA_DIST = \
	btree_alloc.h \
	btree_base.h \
	btree_base_handle.h \
	btree_define.h \
	btree_node.h \
	btree_read_handle_new.h \
	btree_read_param.h \
	btree_root_pointer.h \
	btree_array_list.h \
	btree_write_handle.h \
	btree_write_param.h \
	id_btree.h \
	id_key_btree.h \
	btree_default_alloc.h \
	key_btree.h
clean-local:
	-rm -f *.gcov *.gcno *.gcda
