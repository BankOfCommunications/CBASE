top-src-dir ?= ../../..
src-dir = $(top-src-dir)/tools/
src-files = $(wildcard $(top-src-dir)/*.cpp $(top-src-dir)/*.h)
all-bin-files = gen_sstable syschecker
all: $(all-bin-files)
gen_sstable: $(src-dir)/gen_sstable.cpp $(src-dir)/gen_sstable.h
	make $@ -C $(src-dir) && cp $(src-dir)/$@ .
syschecker: $(wildcard $(src-dir)/syschecker/*.cpp $(src-dir)/syschecker/*.h)
	make $@ -C $(src-dir)/syschecker && cp $(src-dir)/syschecker/$@ .
clean:
	rm -rf $(all-bin-files)
