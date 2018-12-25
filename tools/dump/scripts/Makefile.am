LDADD = $(top_builddir)/src/updateserver/libupdateserver.a \
				$(top_builddir)/src/sstable/libsstable.a \
				$(top_builddir)/src/chunkserver/libchunkserver.a \
				$(top_builddir)/src/rootserver/librootserver.a \
				$(top_builddir)/src/mergeserver/libmergeserver.a \
				$(top_builddir)/src/common/compress/libcomp.a \
				$(top_builddir)/src/common/libcommon.a \
				$(top_builddir)/src/common/btree/libbtree.a \
				${TBLIB_ROOT}/lib/libtbnet.a \
				${TBLIB_ROOT}/lib/libtbsys.a
EXTRA_DIST= dumper.ini dumper.ini_master obdump.sh ob_check_done.sh
pkgdata_data_DATA= dumper.ini dumper.ini_master obdump.sh ob_check_done.sh
pkgdata_datadir=${prefix}/etc
