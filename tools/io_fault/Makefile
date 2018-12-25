target = iof
args = test
cc = gcc
cflags =
ldflags = -ldl

EXTRA_DIST = Makefile iof.c func-list.c extra-func-list.c gen-iof-preload-func.py

all: $(target)
%.test: %
	./$< set . 1.0 EDELAY 1000000
	date >date.txt
	LD_PRELOAD=./$< cat date.txt
	./$< set tcp:10001 1.0 EDELAY 1000000
	LD_PRELOAD=./$< nc -l 10001 &
	nc 127.0.0.1 10001 <date.txt
func-list.c: gen-iof-preload-func.py
	./$< >$@
$(target): $(target).c func-list.c extra-func-list.c
	$(cc) -o $@ $^ -fPIC --shared -Wl,-e,mystart $(cflags) $(ldflags)
clean:
	rm -f $(target) *.o
