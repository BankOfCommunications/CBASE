top-src-dir ?= ../../..
src-dir = $(top-src-dir)/tools/
src-files = $(wildcard $(top-src-dir)/*.cpp $(top-src-dir)/*.h)
all-bin-files = bigquery
all: $(all-bin-files)
bigquery: $(wildcard $(src-dir)/bigquerytest/*.cpp $(src-dir)/bigquerytest/*.h)
	make $@ -C $(src-dir)/bigquerytest && cp $(src-dir)/bigquerytest/$@ .
clean:
	rm -rf $(all-bin-files)
