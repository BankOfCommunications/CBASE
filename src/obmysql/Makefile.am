AM_CPPFLAGS= -I${TBLIB_ROOT}/include/tbsys \
			 -I${EASY_ROOT}/include/easy \
			 -I${top_srcdir}/include \
			 -I${top_srcdir}/src \
			 -fPIC

LDADD = ${EASY_LIB_PATH}/libeasy.a \
				${TBLIB_ROOT}/lib/libtbsys.a \
	${top_srcdir}/src/sql/libsql.a \
	${top_srcdir}/src/common/libcommon.a
#CXXFLAGS+=-O2
AM_LDFLAGS=-lpthread -lc -lm  -lrt -lcrypt -lssl -lboost_regex #-pg
if COVERAGE
CXXFLAGS+=-fprofile-arcs -ftest-coverage
AM_LDFLAGS+=-lgcov
endif



noinst_LIBRARIES = libobmysql.a
libobmysql_a_SOURCES =                                                        \
  ob_mysql_state.map                                                          \
  ob_mysql_callback.h                  ob_mysql_callback.cpp                  \
  ob_mysql_command_queue.h             ob_mysql_command_queue.cpp             \
  ob_mysql_command_queue_thread.h      ob_mysql_command_queue_thread.cpp      \
  ob_mysql_define.h                                                           \
  ob_mysql_dtoa.h                      ob_mysql_dtoa.cpp                      \
  ob_mysql_field.h                     ob_mysql_field.cpp                     \
  ob_mysql_global.h                                                           \
  ob_mysql_loginer.h                   ob_mysql_loginer.cpp                   \
  ob_mysql_packet_queue_handler.h                                             \
  ob_mysql_result_set.h                ob_mysql_result_set.cpp                \
  ob_mysql_row.h                       ob_mysql_row.cpp                       \
  ob_mysql_server.h                    ob_mysql_server.cpp                    \
  ob_mysql_state.h                     ob_mysql_state.cpp                     \
  ob_mysql_util.h                      ob_mysql_util.cpp                      \
  packet/ob_mysql_command_packet.h     packet/ob_mysql_command_packet.cpp     \
  packet/ob_mysql_eof_packet.h         packet/ob_mysql_eof_packet.cpp         \
  packet/ob_mysql_error_packet.h       packet/ob_mysql_error_packet.cpp       \
  packet/ob_mysql_field_packet.h       packet/ob_mysql_field_packet.cpp       \
  packet/ob_mysql_handshake_packet.h   packet/ob_mysql_handshake_packet.cpp   \
  packet/ob_mysql_ok_packet.h          packet/ob_mysql_ok_packet.cpp          \
  packet/ob_mysql_packet.h             packet/ob_mysql_packet.cpp             \
  packet/ob_mysql_packet_header.h                                             \
  packet/ob_mysql_resheader_packet.h   packet/ob_mysql_resheader_packet.cpp   \
  packet/ob_mysql_row_packet.h         packet/ob_mysql_row_packet.cpp         \
  packet/ob_mysql_spr_packet.h         packet/ob_mysql_spr_packet.cpp


#bin_PROGRAMS = obmysql
#obmysql_SOURCES =main.cpp ${libobmysql_a_SOURCES}
#SUBDIRS=packet
#DIST_SUDIRS=packet

