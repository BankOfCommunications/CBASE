all: mysql_stress

CXXFLAGS := -I/usr/include/mysql -I$(TBLIB_ROOT)/include -I$(TBLIB_ROOT)/include/tbsys -I$(LUALIB_PATH)/include -g -Wno-deprecated -O2
LDFLAGS := -L/usr/lib64/mysql -L$(LUALIB_PATH)/lib

mysql_stress: mysql_stress.o han.o
	$(CXX) $(LDFLAGS)  -o $@ $^ $(TBLIB_ROOT)/lib/libtbsys.a -lmysqlclient_r -llua

mysql_stress.o: mysql_stress.cpp han.h
	$(CXX) $(CXXFLAGS) -c -o $@ $<

han.o: han.cpp han.h
	$(CXX) $(CXXFLAGS) -c -o $@ $<

.PHONY: clean
clean:
	-rm mysql_stress *.o
