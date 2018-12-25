AM_CPPFLAGS = -I${top_srcdir}/src/common/btree -I${TBLIB_ROOT}/include/tbsys 
#-DOCEAN_BASE_BTREE_USE_SYS_MALLOC -DOCEAN_BASE_BTREE_DEBUG

LDADD=$(top_builddir)/src/common/btree/libbtree.a ${TBLIB_ROOT}/lib/libtbsys.a

AM_LDFLAGS = -lpthread -lboost_regex
btree_test_LDFLAGS = -lgtest

bin_PROGRAMS = btree_test test_batch

btree_test_SOURCES = btree_alloc_test.cpp btree_base_test.cpp btree_node_test.cpp \
  btree_read_handle_test.cpp btree_root_pointer_test.cpp btree_test.cpp \
  key_btree_mthread.cpp key_btree_test.cpp btree_write_handle_test.cpp \
  keytree_put_test.cpp keytree_get_test.cpp keytree_remove_test.cpp keytree_getnext_test.cpp

test_batch_SOURCES = test_batch.cpp

EXTRA_DIST = test_key.h
