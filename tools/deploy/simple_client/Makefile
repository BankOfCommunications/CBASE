top-src-dir ?= ../../..
src-dir = $(top-src-dir)/tools/log_tool
src-files = $(wildcard $(src-dir)/*.cpp $(src-dir)/*.h)
all-bin-files = client
all: $(all-bin-files)
$(all-bin-files): $(src-files)
	make -C $(src-dir) && cp $(src-dir)/$@ .
clean:
	rm -rf $(all-bin-files)
