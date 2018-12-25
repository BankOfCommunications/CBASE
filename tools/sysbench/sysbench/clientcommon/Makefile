TOPDIR=../

## the Simon client library
LIBRARIES = libsimon.a libBasicsSubset.a
libsimon_a_OBJECTS = simonClient.o simonContext.o simonPacket.o \
                     callbackRegistry.o simonSession.o
libBasicsSubset_a_OBJECTS = basinaddr.o bassockudp.o basthr.o

HEADERS = simonUtils.h simonClient.h simonContext.h simonPacket.h
OBJECTS = $(libsimon_a_OBJECTS) $(libBasicsSubset_a_OBJECTS)

#AR = @AR@
#RANLIB = @RANLIB@
AR=ar
RANLIB=ranlib
DIST_DIR = ../dist

# Override this by invoking "make SIMONDEBUG=1" to get debugging messages
SIMONDEBUG=0

CC=gcc
CXXFLAGS = -pipe -g -Wall -rdynamic -Wno-deprecated -fPIC
CXXLINK = $(CXX) $(CXXFLAGS) -o
DEFS = -D_GNU_SOURCE -D_REENTRANT -D_PTHREADS -DLinux=1 -DSIMONDEBUG=$(SIMONDEBUG)
CXXCOMPILE = $(CXX) $(DEFS) $(CXXFLAGS)
CCOMPILE = $(CC) $(DEFS) $(CFLAGS) -fPIC

all: $(LIBRARIES)

install: all
	mkdir -p $(DIST_DIR)/lib $(DIST_DIR)/include
	cp $(LIBRARIES) $(DIST_DIR)/lib/
	cp simonContext.h $(DIST_DIR)/include/

.SUFFIXES : .o .cc .o .c

.cc.o :
	@echo '$(CXXCOMPILE) -c $<'
	@$(CXXCOMPILE) -c $<

.c.o :
	$(CCOMPILE) -c $<

clean:
	rm -f version.ic
	rm -f $(LIBRARIES)
	rm -f *.o core *.core

distclean: clean
	rm Makefile

###########################################
#### libsimon.a library:

libsimon.a: $(libsimon_a_OBJECTS)
	-rm -f $@
	$(AR) cru $@ $^
	$(RANLIB) $@

$(libsimon_a_OBJECTS): simonUtils.h simonClient.h simonContext.h simonPacket.h

simonContext.o: version.ic
.PHONY: version.ic
version.ic:
	sh ./mkversion.sh @SIMON_VERSION@


###########################################
#### libBasicsSubset.a library.  This is a local copy of a small subset of 
#### libBasics.  Applications using libBasics need not and should not use this.

libBasicsSubset.a: $(libBasicsSubset_a_OBJECTS)
	-rm -f $@
	$(AR) cru $@ $^
	$(RANLIB) $@

$(libBasicsSubset_a_OBJECTS): basatomdefs.h  basinaddr.h  bassockudp.h  basthr.h
