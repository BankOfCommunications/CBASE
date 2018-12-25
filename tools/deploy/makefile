help:
	@echo -e 'make daily-test.redo'
all: tools/iof
	make -C mixed_test
	make -C syschecker
	make -C bigquery
	make -C sqltest
clean:
	make clean -C mixed_test
	make clean -C syschecker
	make clean -C bigquery
	make clean -C sqltest
%.rm:
	rm -rf $(dir $*)
%.mkdir:
	mkdir -p $(dir $*)

src_dir ?= ../..
iof-dir ?= $(src_dir)/tools/io_fault
ups_admin-dir ?= $(src_dir)/tools
rs_admin-dir ?= $(src_dir)/src/rootserver
FORCE:
tools.%/ups_admin: FORCE
	mkdir -p tools.$* && make ups_admin -C $(ups_admin-dir) && cp $(ups_admin-dir)/ups_admin $@
tools.%/rs_admin: FORCE
	mkdir -p tools.$* && make rs_admin -C $(rs_admin-dir) && cp $(rs_admin-dir)/rs_admin $@
tools.%/iof: FORCE
	mkdir -p tools.$* && make iof -C $(iof-dir) && cp $(iof-dir)/iof $@
log_tool-dir ?= $(src_dir)/tools/log_tool
log_tool-src = $(wildcard $(log_tool-dir)/*.cpp)
tools.%/client tools.%/log_tool: $(log_tool-src) FORCE
	mkdir -p tools.$* && make client log_tool -C $(log_tool-dir) && cp $(log_tool-dir)/{client,log_tool} tools.$*
daily-test.redo:
	./deploy.py p0.[cleanup,profile]

