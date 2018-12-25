/*
  Copyright (c) 2001, 2012, 2013, Oracle and/or its affiliates. All rights reserved.

  This program is free software; you can redistribute it and/or modify
  it under the terms of the GNU General Public License as published by
  the Free Software Foundation; version 2 of the License.

  This program is distributed in the hope that it will be useful,
  but WITHOUT ANY WARRANTY; without even the implied warranty of
  MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
  GNU General Public License for more details.

  You should have received a copy of the GNU General Public License
  along with this program; if not, write to the Free Software
  Foundation, Inc., 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA */

/* This file is a simplified version of MySQL `include/my_global.h'.*/

#ifndef _OB_MYSQL_GLOBAL_H_
#define _OB_MYSQL_GLOBAL_H_

#define sint1korr(A)    (*((int8_t*)(A)))

#define sint2korr(A)	(int16_t) (((int16_t) ((uint8_t) (A)[0])) +     \
                                   ((int16_t) ((int16_t) (A)[1]) << 8))
#define sint3korr(A)	((int32_t) ((((uint8_t) (A)[2]) & 128) ?        \
                                    (((uint32_t) 255L << 24) |          \
                                     (((uint32_t) (uint8_t) (A)[2]) << 16) | \
                                     (((uint32_t) (uint8_t) (A)[1]) << 8) | \
                                     ((uint32_t) (uint8_t) (A)[0])) :   \
                                    (((uint32_t) (uint8_t) (A)[2]) << 16) | \
                                    (((uint32_t) (uint8_t) (A)[1]) << 8) | \
                                    ((uint32_t) (uint8_t) (A)[0])))
#define sint4korr(A)	(int32_t) (((int32_t) ((uint8_t) (A)[0])) +     \
                                   (((int32_t) ((uint8_t) (A)[1]) << 8)) + \
                                   (((int32_t) ((uint8_t) (A)[2]) << 16)) + \
                                   (((int32_t) ((int16_t) (A)[3]) << 24)))
#define sint8korr(A)	(int64_t) uint8korr(A)
#define uint1korr(A)    (*((uint8_t *)A))
#define uint2korr(A)	(uint16_t) (((uint16_t) ((uint8_t) (A)[0])) +   \
                                    ((uint16_t) ((uint8_t) (A)[1]) << 8))
#define uint3korr(A)	(uint32_t) (((uint32_t) ((uint8_t) (A)[0])) +   \
                                    (((uint32_t) ((uint8_t) (A)[1])) << 8) + \
                                    (((uint32_t) ((uint8_t) (A)[2])) << 16))
#define uint4korr(A)	(uint32_t) (((uint32_t) ((uint8_t) (A)[0])) +   \
                                    (((uint32_t) ((uint8_t) (A)[1])) << 8) + \
                                    (((uint32_t) ((uint8_t) (A)[2])) << 16) + \
                                    (((uint32_t) ((uint8_t) (A)[3])) << 24))
#define uint5korr(A)	((uint64_t)(((uint32_t) ((uint8_t) (A)[0])) +   \
				    (((uint32_t) ((uint8_t) (A)[1])) << 8) + \
				    (((uint32_t) ((uint8_t) (A)[2])) << 16) + \
				    (((uint32_t) ((uint8_t) (A)[3])) << 24)) + \
                         (((uint64_t) ((uint8_t) (A)[4])) << 32))
#define uint6korr(A)	((uint64_t)(((uint32_t)    ((uint8_t) (A)[0]))          + \
                                    (((uint32_t)    ((uint8_t) (A)[1])) << 8)   + \
                                    (((uint32_t)    ((uint8_t) (A)[2])) << 16)  + \
                                    (((uint32_t)    ((uint8_t) (A)[3])) << 24)) + \
                         (((uint64_t) ((uint8_t) (A)[4])) << 32) +      \
                         (((uint64_t) ((uint8_t) (A)[5])) << 40))
#define uint8korr(A)	((uint64_t)(((uint32_t) ((uint8_t) (A)[0])) +   \
				    (((uint32_t) ((uint8_t) (A)[1])) << 8) + \
				    (((uint32_t) ((uint8_t) (A)[2])) << 16) + \
				    (((uint32_t) ((uint8_t) (A)[3])) << 24)) + \
                         (((uint64_t) (((uint32_t) ((uint8_t) (A)[4])) + \
                                       (((uint32_t) ((uint8_t) (A)[5])) << 8) + \
                                       (((uint32_t) ((uint8_t) (A)[6])) << 16) + \
                                       (((uint32_t) ((uint8_t) (A)[7])) << 24))) << \
                          32))
#define int1store(T,A)       do { *((uint8_t *)(T)) = (uint8_t)(A); } while (0)
#define int2store(T,A)       do { uint def_temp= (uint) (A) ;   \
    *((uint8_t*) (T))=  (uint8_t)(def_temp);                    \
    *((uint8_t*) (T)+1)=(uint8_t)((def_temp >> 8));             \
  } while(0)
#define int3store(T,A)       do { /*lint -save -e734 */ \
    *((uint8_t*)(T))=(uint8_t) ((A));                   \
    *((uint8_t*) (T)+1)=(uint8_t) (((A) >> 8));         \
    *((uint8_t*)(T)+2)=(uint8_t) (((A) >> 16));         \
    /*lint -restore */} while(0)
#define int4store(T,A)       do { *((char *)(T))=(char) ((A));  \
    *(((char *)(T))+1)=(char) (((A) >> 8));                     \
    *(((char *)(T))+2)=(char) (((A) >> 16));                    \
    *(((char *)(T))+3)=(char) (((A) >> 24)); } while(0)
#define int5store(T,A)       do { *((char *)(T))=     (char)((A));      \
    *(((char *)(T))+1)= (char)(((A) >> 8));                             \
    *(((char *)(T))+2)= (char)(((A) >> 16));                            \
    *(((char *)(T))+3)= (char)(((A) >> 24));                            \
    *(((char *)(T))+4)= (char)(((A) >> 32));                            \
  } while(0)
#define int6store(T,A)       do { *((char *)(T))=     (char)((A));      \
    *(((char *)(T))+1)= (char)(((A) >> 8));                             \
    *(((char *)(T))+2)= (char)(((A) >> 16));                            \
    *(((char *)(T))+3)= (char)(((A) >> 24));                            \
    *(((char *)(T))+4)= (char)(((A) >> 32));                            \
    *(((char *)(T))+5)= (char)(((A) >> 40));                            \
  } while(0)
#define int8store(T,A)       do { uint def_temp= (uint) (A), def_temp2= (uint) ((A) >> 32); \
    int4store((T),def_temp);                                            \
    int4store((T+4),def_temp2); } while(0)
#define float4get(V,M)   memcpy(&V, (M), sizeof(float))
#define float4store(V,M) memcpy(V, (&M), sizeof(float))

#define float8get(V,M)   doubleget((V),(M))
#define float8store(V,M) doublestore((V),(M))

#define int4net(A)        (int32_t) (((uint32_t) ((uint8_t) (A)[3]))        | \
                                     (((uint32_t) ((uint8_t) (A)[2])) << 8)  | \
                                     (((uint32_t) ((uint8_t) (A)[1])) << 16) | \
                                     (((uint32_t) ((uint8_t) (A)[0])) << 24))
/*
  Define-funktions for reading and storing in machine format from/to
  short/long to/from some place in memory V should be a (not
  register) variable, M is a pointer to byte
*/
#define ushortget(V,M)	do { V = uint2korr(M); } while(0)
#define shortget(V,M)	do { V = sint2korr(M); } while(0)
#define longget(V,M)	do { V = sint4korr(M); } while(0)
#define ulongget(V,M)   do { V = uint4korr(M); } while(0)
#define shortstore(T,V) int2store(T,V)
#define longstore(T,V)	int4store(T,V)
#ifndef floatstore
#define floatstore(T,V)  memcpy((T), (void *) (&V), sizeof(float))
#define floatget(V,M)    memcpy(&V, (M), sizeof(float))
#endif
#ifndef doubleget
#define doubleget(V,M)	 memcpy(&V, (M), sizeof(double))
#define doublestore(T,V) memcpy((T), (void *) &V, sizeof(double))
#endif /* doubleget */
#define longlongget(V,M) memcpy(&V, (M), sizeof(uint64_t))
#define longlongstore(T,V) memcpy((T), &V, sizeof(uint64_t))

#define MY_ALIGN(A,L)	(((A) + (L) - 1) & ~((L) - 1))
#define ALIGN_SIZE(A)	MY_ALIGN((A),sizeof(double))

#define SIZEOF_CHARP 8

#define NOT_FIXED_DEC 31

/*
  The longest string my_fcvt can return is 311 + "precision" bytes.
  Here we assume that we never cal my_fcvt() with precision >= NOT_FIXED_DEC
  (+ 1 byte for the terminating '\0').
*/
#define FLOATING_POINT_BUFFER (311 + NOT_FIXED_DEC)

#define DOUBLE_TO_STRING_CONVERSION_BUFFER_SIZE FLOATING_POINT_BUFFER
/* The fabs(float) < 10^39 */
#define FLOAT_TO_STRING_CONVERSION_BUFFER_SIZE (39 + NOT_FIXED_DEC)

#define MAX_TINYINT_WIDTH       3       /* Max width for a TINY w.o. sign */
#define MAX_SMALLINT_WIDTH      5       /* Max width for a SHORT w.o. sign */
#define MAX_MEDIUMINT_WIDTH     8       /* Max width for a INT24 w.o. sign */
#define MAX_INT_WIDTH           10      /* Max width for a LONG w.o. sign */
#define MAX_BIGINT_WIDTH        20      /* Max width for a LONGLONG */
#define MAX_CHAR_WIDTH		255	/* Max length for a CHAR colum */
#define MAX_BLOB_WIDTH		16777216	/* Default width for blob */

#define MAX_DECPT_FOR_F_FORMAT DBL_DIG
#define MAX_DATETIME_WIDTH	19	/* YYYY-MM-DD HH:MM:SS */
//add peiouya [DATE_TIME] 20150906:b
#define MAX_DATE_WIDTH	10	/* YYYY-MM-DD */
#define MAX_TIME_WIDTH	8	/* HH:MM:SS */
//add 20150906:e

/* -[digits].E+## */
#define MAX_FLOAT_STR_LENGTH (FLT_DIG + 6)
/* -[digits].E+### */
#define MAX_DOUBLE_STR_LENGTH (DBL_DIG + 7)

namespace oceanbase {
  namespace obmysql {
    enum EMySQLFieldType
    {
      MYSQL_TYPE_DECIMAL,
      MYSQL_TYPE_TINY,
      MYSQL_TYPE_SHORT,
      MYSQL_TYPE_LONG,
      MYSQL_TYPE_FLOAT,
      MYSQL_TYPE_DOUBLE,
      MYSQL_TYPE_NULL,
      MYSQL_TYPE_TIMESTAMP,
      MYSQL_TYPE_LONGLONG,
      MYSQL_TYPE_INT24,
      MYSQL_TYPE_DATE,
      MYSQL_TYPE_TIME,
      MYSQL_TYPE_DATETIME,
      MYSQL_TYPE_YEAR,
      MYSQL_TYPE_NEWDATE,
      MYSQL_TYPE_VARCHAR,
      MYSQL_TYPE_BIT,
      MYSQL_TYPE_NEWDECIMAL=246,
      MYSQL_TYPE_ENUM=247,
      MYSQL_TYPE_SET=248,
      MYSQL_TYPE_TINY_BLOB=249,
      MYSQL_TYPE_MEDIUM_BLOB=250,
      MYSQL_TYPE_LONG_BLOB=251,
      MYSQL_TYPE_BLOB=252,
      MYSQL_TYPE_VAR_STRING=253,
      MYSQL_TYPE_STRING=254,
      MYSQL_TYPE_GEOMETRY=255,
      MYSQL_TYPE_NOT_DEFINED=65535
    };
  } // end of namespace obmysql
}   // end of namespace oceanbase

#endif /* _OB_MYSQL_GLOBAL_H_ */
