#!/usr/bin/env python3
# author: zhuweng.yzf@taobao.comZhifeng YANG
# description:
class SqlKeywords:
    reserved_92 = '''
         ABSOLUTE  ACTION  ADD  ALL  ALLOCATE  ALTER  AND  ANY  ARE
          AS  ASC  ASSERTION  AT  AUTHORIZATION  AVG
          BEGIN  BETWEEN  BIT  BIT_LENGTH  BOTH  BY
          CASCADE  CASCADED  CASE  CAST  CATALOG  CHAR  CHARACTER  CHARACTER_LENGTH
          CHAR_LENGTH  CHECK  CLOSE  COALESCE  COLLATE  COLLATION  COLUMN  COMMIT
          CONNECT  CONNECTION  CONSTRAINT  CONSTRAINTS  CONTINUE  CONVERT  CORRESPONDING
          CREATE  CROSS  CURRENT  CURRENT_DATE  CURRENT_TIME  CURRENT_TIMESTAMP  CURRENT_USER  CURSOR
          DATE  DAY  DEALLOCATE  DEC  DECIMAL  DECLARE  DEFAULT
          DEFERRABLE  DEFERRED  DELETE  DESC  DESCRIBE  DESCRIPTOR  DIAGNOSTICS
          DISCONNECT  DISTINCT  DOMAIN  DOUBLE  DROP
          ELSE  END  END-EXEC  ESCAPE  EXCEPT  EXCEPTION  EXEC  EXECUTE  EXISTS  EXTERNAL  EXTRACT
          FALSE  FETCH  FIRST  FLOAT  FOR  FOREIGN  FOUND  FROM  FULL
          GET  GLOBAL  GO  GOTO  GRANT  GROUP
          HAVING  HOUR
          IDENTITY  IMMEDIATE  IN  INDICATOR  INITIALLY  INNER  INPUT  INSENSITIVE
          INSERT  INT  INTEGER  INTERSECT  INTERVAL  INTO  IS  ISOLATION
          JOIN
          KEY
          LANGUAGE  LAST  LEADING  LEFT  LEVEL  LIKE  LOCAL  LOWER
          MATCH  MAX  MIN  MINUTE  MODULE  MONTH
          NAMES  NATIONAL  NATURAL  NCHAR  NEXT  NO  NOT  NULL  NULLIF  NUMERIC
          OCTET_LENGTH  OF  ON  ONLY  OPEN  OPTION  OR  ORDER  OUTER  OUTPUT  OVERLAPS
          PAD  PARTIAL  POSITION  PRECISION  PREPARE  PRESERVE  PRIMARY  PRIOR  PRIVILEGES  PROCEDURE  PUBLIC
          READ  REAL  REFERENCES  RELATIVE  RESTRICT  REVOKE  RIGHT  ROLLBACK  ROWS
          SCHEMA  SCROLL  SECOND  SECTION  SELECT  SESSION  SESSION_USER  SET
          SIZE  SMALLINT  SOME  SPACE  SQL  SQLCODE  SQLERROR  SQLSTATE  SUBSTRING  SUM  SYSTEM_USER
          TABLE  TEMPORARY  THEN  TIME  TIMESTAMP  TIMEZONE_HOUR  TIMEZONE_MINUTE
          TO  TRAILING  TRANSACTION  TRANSLATE  TRANSLATION  TRIM  TRUE
          UNION  UNIQUE  UNKNOWN  UPDATE  UPPER  USAGE  USER  USING
          VALUE  VALUES  VARCHAR  VARYING  VIEW
          WHEN  WHENEVER  WHERE  WITH  WORK  WRITE
          YEAR
          ZONE'''
    non_reserved_92 = '''
         ADA
          C  CATALOG_NAME  CHARACTER_SET_CATALOG  CHARACTER_SET_NAME  CHARACTER_SET_SCHEMA
          CLASS_ORIGIN  COBOL  COLLATION_CATALOG  COLLATION_NAME  COLLATION_SCHEMA
          COLUMN_NAME  COMMAND_FUNCTION  COMMITTED  CONDITION_NUMBER  CONNECTION_NAME
          CONSTRAINT_CATALOG  CONSTRAINT_NAME  CONSTRAINT_SCHEMA  CURSOR_NAME
          DATA  DATETIME_INTERVAL_CODE  DATETIME_INTERVAL_PRECISION  DYNAMIC_FUNCTION
          FORTRAN
          LENGTH
          MESSAGE_LENGTH  MESSAGE_OCTET_LENGTH  MESSAGE_TEXT  MORE  MUMPS
          NAME  NULLABLE  NUMBER
          PASCAL  PLI
          REPEATABLE  RETURNED_LENGTH  RETURNED_OCTET_LENGTH  RETURNED_SQLSTATE  ROW_COUNT
          SCALE  SCHEMA_NAME  SERIALIZABLE  SERVER_NAME  SUBCLASS_ORIGIN
          TABLE_NAME  TYPE
          UNCOMMITTED  UNNAMED'''
    reserved_99 = '''
         ABSOLUTE  ACTION  ADD  AFTER  ALL  ALLOCATE  ALTER  AND  ANY  ARE
          ARRAY  AS  ASC  ASSERTION  AT  AUTHORIZATION
          BEFORE  BEGIN  BETWEEN  BINARY  BIT  BLOB  BOOLEAN  BOTH
          BREADTH  BY
          CALL  CASCADE  CASCADED  CASE  CAST  CATALOG  CHAR  CHARACTER
          CHECK  CLOB  CLOSE  COLLATE  COLLATION  COLUMN  COMMIT
          CONDITION  CONNECT  CONNECTION  CONSTRAINT  CONSTRAINTS
          CONSTRUCTOR  CONTINUE  CORRESPONDING  CREATE  CROSS  CUBE
          CURRENT  CURRENT_DATE  CURRENT_DEFAULT_TRANSFORM_GROUP
          CURRENT_TRANSFORM_GROUP_FOR_TYPE  CURRENT_PATH  CURRENT_ROLE
          CURRENT_TIME  CURRENT_TIMESTAMP  CURRENT_USER  CURSOR  CYCLE
          DATA  DATE  DAY  DEALLOCATE  DEC  DECIMAL  DECLARE  DEFAULT
          DEFERRABLE  DEFERRED  DELETE  DEPTH  DEREF  DESC
          DESCRIBE  DESCRIPTOR  DETERMINISTIC
          DIAGNOSTICS  DISCONNECT  DISTINCT  DO  DOMAIN  DOUBLE
          DROP  DYNAMIC
          EACH  ELSE  ELSEIF  END  END-EXEC  EQUALS  ESCAPE  EXCEPT
          EXCEPTION  EXEC  EXECUTE  EXISTS  EXIT  EXTERNAL
          FALSE  FETCH  FIRST  FLOAT  FOR  FOREIGN  FOUND  FROM  FREE
          FULL  FUNCTION
          GENERAL  GET  GLOBAL  GO  GOTO  GRANT  GROUP  GROUPING
          HANDLE  HAVING  HOLD  HOUR
          IDENTITY  IF  IMMEDIATE  IN  INDICATOR
          INITIALLY  INNER  INOUT  INPUT  INSERT  INT  INTEGER
          INTERSECT  INTERVAL  INTO  IS  ISOLATION
          JOIN
          KEY
          LANGUAGE  LARGE  LAST  LATERAL  LEADING  LEAVE  LEFT
          LEVEL  LIKE  LOCAL  LOCALTIME  LOCALTIMESTAMP  LOCATOR  LOOP
          MAP  MATCH  METHOD  MINUTE  MODIFIES  MODULE  MONTH
          NAMES  NATIONAL  NATURAL  NCHAR  NCLOB  NESTING  NEW  NEXT
          NO  NONE  NOT  NULL  NUMERIC
          OBJECT  OF  OLD  ON  ONLY  OPEN  OPTION
          OR  ORDER  ORDINALITY  OUT  OUTER  OUTPUT  OVERLAPS
          PAD  PARAMETER  PARTIAL  PATH  PRECISION
          PREPARE  PRESERVE  PRIMARY  PRIOR  PRIVILEGES  PROCEDURE  PUBLIC
          READ  READS  REAL  RECURSIVE  REDO  REF  REFERENCES  REFERENCING
          RELATIVE  RELEASE  REPEAT  RESIGNAL  RESTRICT  RESULT  RETURN
          RETURNS  REVOKE  RIGHT  ROLE  ROLLBACK  ROLLUP  ROUTINE
          ROW  ROWS
          SAVEPOINT  SCHEMA  SCROLL  SEARCH  SECOND  SECTION  SELECT
          SESSION  SESSION_USER  SET  SETS  SIGNAL  SIMILAR  SIZE
          SMALLINT  SOME  SPACE  SPECIFIC  SPECIFICTYPE  SQL  SQLEXCEPTION
          SQLSTATE  SQLWARNING  START  STATE  STATIC  SYSTEM_USER
          TABLE  TEMPORARY  THEN  TIME  TIMESTAMP
          TIMEZONE_HOUR  TIMEZONE_MINUTE  TO  TRAILING  TRANSACTION
          TRANSLATION  TREAT  TRIGGER  TRUE
          UNDER  UNDO  UNION  UNIQUE  UNKNOWN  UNNEST  UNTIL  UPDATE
          USAGE  USER  USING
          VALUE  VALUES  VARCHAR  VARYING  VIEW
          WHEN  WHENEVER  WHERE  WHILE  WITH  WITHOUT  WORK  WRITE
          YEAR
          ZONE'''
    non_reserved_99 = '''
         ABS  ADA  ADMIN  ASENSITIVE  ASSIGNMENT  ASYMMETRIC  ATOMIC
          ATTRIBUTE  AVG
          BIT_LENGTH
          C  CALLED  CARDINALITY  CATALOG_NAME  CHAIN  CHAR_LENGTH
          CHARACTERISTICS  CHARACTER_LENGTH  CHARACTER_SET_CATALOG
          CHARACTER_SET_NAME  CHARACTER_SET_SCHEMA  CHECKED  CLASS_ORIGIN
          COALESCE  COBOL  COLLATION_CATALOG  COLLATION_NAME  COLLATION_SCHEMA
          COLUMN_NAME  COMMAND_FUNCTION  COMMAND_FUNCTION_CODE  COMMITTED
          CONDITION_IDENTIFIER  CONDITION_NUMBER  CONNECTION_NAME
          CONSTRAINT_CATALOG  CONSTRAINT_NAME  CONSTRAINT_SCHEMA  CONTAINS
          CONVERT  COUNT  CURSOR_NAME
          DATETIME_INTERVAL_CODE  DATETIME_INTERVAL_PRECISION  DEFINED
          DEFINER  DEGREE  DERIVED  DISPATCH
          EVERY  EXTRACT
          FINAL  FORTRAN
          G  GENERATED  GRANTED
          HIERARCHY
          IMPLEMENTATION  INSENSITIVE  INSTANCE  INSTANTIABLE  INVOKER
          K  KEY_MEMBER  KEY_TYPE
          LENGTH  LOWER
          M  MAX  MIN  MESSAGE_LENGTH  MESSAGE_OCTET_LENGTH  MESSAGE_TEXT
          MOD  MORE  MUMPS
          NAME  NULLABLE  NUMBER  NULLIF
          OCTET_LENGTH  ORDERING  OPTIONS  OVERLAY  OVERRIDING
          PASCAL  PARAMETER_MODE  PARAMETER_NAME
          PARAMETER_ORDINAL_POSITION  PARAMETER_SPECIFIC_CATALOG
          PARAMETER_SPECIFIC_NAME  PARAMETER_SPECIFIC_SCHEMA  PLI  POSITION
          REPEATABLE  RETURNED_CARDINALITY  RETURNED_LENGTH
          RETURNED_OCTET_LENGTH  RETURNED_SQLSTATE  ROUTINE_CATALOG
          ROUTINE_NAME  ROUTINE_SCHEMA  ROW_COUNT
          SCALE  SCHEMA_NAME  SCOPE  SECURITY  SELF  SENSITIVE  SERIALIZABLE
          SERVER_NAME  SIMPLE  SOURCE  SPECIFIC_NAME  STATEMENT  STRUCTURE
          STYLE  SUBCLASS_ORIGIN  SUBSTRING  SUM  SYMMETRIC  SYSTEM
          TABLE_NAME  TOP_LEVEL_COUNT  TRANSACTIONS_COMMITTED
          TRANSACTIONS_ROLLED_BACK  TRANSACTION_ACTIVE  TRANSFORM
          TRANSFORMS  TRANSLATE  TRIGGER_CATALOG  TRIGGER_SCHEMA
          TRIGGER_NAME  TRIM  TYPE
          UNCOMMITTED  UNNAMED  UPPER '''
    non_reserved_03 = '''
         A
          ABS
          ABSOLUTE
          ACTION
          ADA
          ADMIN
          AFTER
          ALWAYS
          ASC
          ASSERTION
          ASSIGNMENT
          ATTRIBUTE
          ATTRIBUTES
          AVG
          BEFORE
          BERNOULLI
          BREADTH
          C
          CARDINALITY
          CASCADE
          CATALOG
          CATALOG_NAME
          CEIL
          CEILING
          CHAIN
          CHARACTERISTICS
          CHARACTERS
          CHARACTER_LENGTH
          CHARACTER_SET_CATALOG
          CHARACTER_SET_NAME
          CHARACTER_SET_SCHEMA
          CHAR_LENGTH
          CHECKED
          CLASS_ORIGIN
          COALESCE
          COBOL
          CODE_UNITS
          COLLATION
          COLLATION_CATALOG
          COLLATION_NAME
          COLLATION_SCHEMA
          COLLECT
          COLUMN_NAME
          COMMAND_FUNCTION
          COMMAND_FUNCTION_CODE
          COMMITTED
          CONDITION
          CONDITION_NUMBER
          CONNECTION_NAME
          CONSTRAINTS
          CONSTRAINT_CATALOG
          CONSTRAINT_NAME
          CONSTRAINT_SCHEMA
          CONSTRUCTORS
          CONTAINS
          CONVERT
          CORR
          COUNT
          COVAR_POP
          COVAR_SAMP
          CUME_DIST
          CURRENT_COLLATION
          CURSOR_NAME
          DATA
          DATETIME_INTERVAL_CODE
          DATETIME_INTERVAL_PRECISION
          DEFAULTS
          DEFERRABLE
          DEFERRED
          DEFINED
          DEFINER
          DEGREE
          DENSE_RANK
          DEPTH
          DERIVED
          DESC
          DESCRIPTOR
          DIAGNOSTICS
          DISPATCH
          DOMAIN
          DYNAMIC_FUNCTION
          DYNAMIC_FUNCTION_CODE
          EQUALS
          EVERY
          EXCEPTION
          EXCLUDE
          EXCLUDING
          EXP
          EXTRACT
          FINAL
          FIRST
          FLOOR
          FOLLOWING
          FORTRAN
          FOUND
          FUSION
          G
          GENERAL
          GO
          GOTO
          GRANTED
          HIERARCHY
          IMPLEMENTATION
          INCLUDING
          INCREMENT
          INITIALLY
          INSTANCE
          INSTANTIABLE
          INTERSECTION
          INVOKER
          ISOLATION
          K
          KEY
          KEY_MEMBER
          KEY_TYPE
          LAST
          LENGTH
          LEVEL
          LN
          LOCATOR
          LOWER
          M
          MAP
          MATCHED
          MAX
          MAXVALUE
          MESSAGE_LENGTH
          MESSAGE_OCTET_LENGTH
          MESSAGE_TEXT
          MIN
          MINVALUE
          MOD
          MORE
          MUMPS
          NAME
          NAMES
          NESTING
          NEXT
          NORMALIZE
          NORMALIZED
          NULLABLE
          NULLIF
          NULLS
          NUMBER
          OBJECT
          OCTETS
          OCTET_LENGTH
          OPTION
          OPTIONS
          ORDERING
          ORDINALITY
          OTHERS
          OVERLAY
          OVERRIDING
          PAD
          PARAMETER_MODE
          PARAMETER_NAME
          PARAMETER_ORDINAL_POSITION
          PARAMETER_SPECIFIC_CATALOG
          PARAMETER_SPECIFIC_NAME
          PARAMETER_SPECIFIC_SCHEMA
          PARTIAL
          PASCAL
          PATH
          PERCENTILE_CONT
          PERCENTILE_DISC
          PERCENT_RANK
          PLACING
          PLI
          POSITION
          POWER
          PRECEDING
          PRESERVE
          PRIOR
          PRIVILEGES
          PUBLIC
          RANK
          READ
          RELATIVE
          REPEATABLE
          RESTART
          RETURNED_CARDINALITY
          RETURNED_LENGTH
          RETURNED_OCTET_LENGTH
          RETURNED_SQLSTATE
          ROLE
          ROUTINE
          ROUTINE_CATALOG
          ROUTINE_NAME
          ROUTINE_SCHEMA
          ROW_COUNT
          ROW_NUMBER
          SCALE
          SCHEMA
          SCHEMA_NAME
          SCOPE_CATALOG
          SCOPE_NAME
          SCOPE_SCHEMA
          SECTION
          SECURITY
          SELF
          SEQUENCE
          SERIALIZABLE
          SERVER_NAME
          SESSION
          SETS
          SIMPLE
          SIZE
          SOURCE
          SPACE
          SPECIFIC_NAME
          SQRT
          STATE
          STATEMENT
          STDDEV_POP
          STDDEV_SAMP
          STRUCTURE
          STYLE
          SUBCLASS_ORIGIN
          SUBSTRING
          SUM
          TABLESAMPLE
          TABLE_NAME
          TEMPORARY
          TIES
          TOP_LEVEL_COUNT
          TRANSACTION
          TRANSACTIONS_COMMITTED
          TRANSACTIONS_ROLLED_BACK
          TRANSACTION_ACTIVE
          TRANSFORM
          TRANSFORMS
          TRANSLATE
          TRIGGER_CATALOG
          TRIGGER_NAME
          TRIGGER_SCHEMA
          TRIM
          TYPE
          UNBOUNDED
          UNCOMMITTED
          UNDER
          UNNAMED
          USAGE
          USER_DEFINED_TYPE_CATALOG
          USER_DEFINED_TYPE_CODE
          USER_DEFINED_TYPE_NAME
          USER_DEFINED_TYPE_SCHEMA
          VIEW
          WORK
          WRITE
          ZONE '''
    reserved_03 = '''
         ADD
          ALL
          ALLOCATE
          ALTER
          AND
          ANY
          ARE
          ARRAY
          AS
          ASENSITIVE
          ASYMMETRIC
          AT
          ATOMIC
          AUTHORIZATION
          BEGIN
          BETWEEN
          BIGINT
          BINARY
          BLOB
          BOOLEAN
          BOTH
          BY
          CALL
          CALLED
          CASCADED
          CASE
          CAST
          CHAR
          CHARACTER
          CHECK
          CLOB
          CLOSE
          COLLATE
          COLUMN
          COMMIT
          CONNECT
          CONSTRAINT
          CONTINUE
          CORRESPONDING
          CREATE
          CROSS
          CUBE
          CURRENT
          CURRENT_DATE
          CURRENT_DEFAULT_TRANSFORM_GROUP
          CURRENT_PATH
          CURRENT_ROLE
          CURRENT_TIME
          CURRENT_TIMESTAMP
          CURRENT_TRANSFORM_GROUP_FOR_TYPE
          CURRENT_USER
          CURSOR
          CYCLE
          DATE
          DAY
          DEALLOCATE
          DEC
          DECIMAL
          DECLARE
          DEFAULT
          DELETE
          DEREF
          DESCRIBE
          DETERMINISTIC
          DISCONNECT
          DISTINCT
          DOUBLE
          DROP
          DYNAMIC
          EACH
          ELEMENT
          ELSE
          END
          END-EXEC
          ESCAPE
          EXCEPT
          EXEC
          EXECUTE
          EXISTS
          EXTERNAL
          FALSE
          FETCH
          FILTER
          FLOAT
          FOR
          FOREIGN
          FREE
          FROM
          FULL
          FUNCTION
          GET
          GLOBAL
          GRANT
          GROUP
          GROUPING
          HAVING
          HOLD
          HOUR
          IDENTITY
          IMMEDIATE
          IN
          INDICATOR
          INNER
          INOUT
          INPUT
          INSENSITIVE
          INSERT
          INT
          INTEGER
          INTERSECT
          INTERVAL
          INTO
          IS
          ISOLATION
          JOIN
          LANGUAGE
          LARGE
          LATERAL
          LEADING
          LEFT
          LIKE
          LOCAL
          LOCALTIME
          LOCALTIMESTAMP
          MATCH
          MEMBER
          MERGE
          METHOD
          MINUTE
          MODIFIES
          MODULE
          MONTH
          MULTISET
          NATIONAL
          NATURAL
          NCHAR
          NCLOB
          NEW
          NO
          NONE
          NOT
          NULL
          NUMERIC
          OF
          OLD
          ON
          ONLY
          OPEN
          OR
          ORDER
          OUT
          OUTER
          OUTPUT
          OVER
          OVERLAPS
          PARAMETER
          PARTITION
          PRECISION
          PREPARE
          PRIMARY
          PROCEDURE
          RANGE
          READS
          REAL
          RECURSIVE
          REF
          REFERENCES
          REFERENCING
          REGR_AVGX
          REGR_AVGY
          REGR_COUNT
          REGR_INTERCEPT
          REGR_R2
          REGR_SLOPE
          REGR_SXX
          REGR_SXY
          REGR_SYY
          RELEASE
          RESULT
          RETURN
          RETURNS
          REVOKE
          RIGHT
          ROLLBACK
          ROLLUP
          ROW
          ROWS
          SAVEPOINT
          SCROLL
          SEARCH
          SECOND
          SELECT
          SENSITIVE
          SESSION_USER
          SET
          SIMILAR
          SMALLINT
          SOME
          SPECIFIC
          SPECIFICTYPE
          SQL
          SQLEXCEPTION
          SQLSTATE
          SQLWARNING
          START
          STATIC
          SUBMULTISET
          SYMMETRIC
          SYSTEM
          SYSTEM_USER
          TABLE
          THEN
          TIME
          TIMESTAMP
          TIMEZONE_HOUR
          TIMEZONE_MINUTE
          TO
          TRAILING
          TRANSLATION
          TREAT
          TRIGGER
          TRUE
          UESCAPE
          UNION
          UNIQUE
          UNKNOWN
          UNNEST
          UPDATE
          UPPER
          USER
          USING
          VALUE
          VALUES
          VAR_POP
          VAR_SAMP
          VARCHAR
          VARYING
          WHEN
          WHENEVER
          WHERE
          WIDTH_BUCKET
          WINDOW
          WITH
          WITHIN
          WITHOUT
          YEAR'''


ob_keywords = '''
ADD
AND
ANY
ALL
AS
ASC
AUTO_INCREMENT
BETWEEN
BIGINT
BINARY
BOOLEAN
BY
CASE
CHARACTER
CNNOP
COLUMNS
COMPRESS_METHOD
CREATE
CREATETIME
DATE
DATETIME
DECIMAL
DEFAULT
DELETE
DESC
DESCRIBE
DISTINCT
DOUBLE
DROP
ELSE
END
END_P
ERROR
EXCEPT
EXISTS
EXPIRE_INFO
EXPLAIN
FLOAT
FROM
FULL
GLOBAL
GROUP
HAVING
IF
IN
INNER
INTEGER
INTERSECT
INSERT
INTO
IS
JOIN
JOIN_INFO
KEY
LEFT
LIMIT
LIKE
MEDIUMINT
MOD
MODIFYTIME
NOT
NUMERIC
OFFSET
ON
OR
ORDER
OUTER
PRECISION
PRIMARY
REAL
REPLACE
REPLICA_NUM
RIGHT
SCHEMA
SELECT
SERVER
SESSION
SET
SHOW
SMALLINT
STATUS
TABLE
TABLES
TABLET_MAX_SIZE
THEN
TIME
TIMESTAMP
TINYINT
UNION
UPDATE
USE_BLOOM_FILTER
VALUES
VARCHAR
VARBINARY
VARIABLES
VERBOSE
WHERE
WHEN
USER
IDENTIFIED
PASSWORD
FOR
ALTER
RENAME
TO
LOCKED
UNLOCKED
GRANT
PRIVILEGES
OPTION
REVOKE
'''

if (__name__ == '__main__'):
    a = SqlKeywords()
    for k in ob_keywords.split():
        print("%s " % k, end = "")
        for c in [a.reserved_92, a.reserved_99, a.reserved_03]:
            try:
                i = c.split().index(k)
                print("R ", end = "")
            except ValueError:
                print("U ", end = "")
        for n in [a.non_reserved_92, a.non_reserved_99, a.non_reserved_03]:
            try:
                i = n.split().index(k)
                print("N ", end = "")
            except ValueError:
                print("U ", end = "")
        print("")
