top-src-dir ?= ../../..
src-dir = $(top-src-dir)/tools/mixed_test
src-files = $(wildcard $(src-dir)/*.cpp $(src-dir)/*.h)
all-bin-files = launcher multi_write random_read total_scan
all: $(all-bin-files)
$(all-bin-files): $(src-files)
	make -C $(src-dir) && cp $(src-dir)/$@ .
clean:
	rm -rf $(all-bin-files)
