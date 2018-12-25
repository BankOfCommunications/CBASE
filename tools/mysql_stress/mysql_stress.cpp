#include <mysql.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <getopt.h>

#include <memory>
#include <vector>
#include <string>

extern "C" {
#include "lua.h"
#include "lauxlib.h"
#include "lualib.h"
}

#include "tbsys.h"
#include "han.h"

using std::auto_ptr;
using std::vector;
using std::string;

const int ERR_ITER_END = 10001;

class CONST
{
  public:
    static const char * COMMAND_STRESS;
    static const char * COMMAND_PREPARE;

    static const char * STRESS_SQLS_CK;
    static const char * PREPARE_SQLS_CK;
    static const char * SQL_CK;
    static const char * PARAMS_CK;
    static const char * TYPE_CK;
    static const char * WEIGHT_CK;
    static const char * RANGE_MIN_CK;
    static const char * RANGE_MAX_CK;
    static const char * RANGE_STEP_CK;
    static const char * DEPENDENT_INDEX_CK;
    static const char * DIFFERENCE_CK;
    static const char * GEN_TYPE_CK;

    static const char * DATA_TYPE_NAME_INT_STR;
    static const char * DATA_TYPE_NAME_LONG_STR;
    static const char * DATA_TYPE_NAME_DISTINCT_INT_STR;
    static const char * DATA_TYPE_NAME_DEPENDENT_INT_STR;
    static const char * DATA_TYPE_NAME_VARCHAR_STR;
    static const char * GEN_TYPE_NAME_PARALLEL_LOOP_STR;
    static const char * GEN_TYPE_NAME_NESTED_LOOP_STR;
};

const char * CONST::COMMAND_STRESS             = "stress";
const char * CONST::COMMAND_PREPARE            = "prepare";

const char * CONST::STRESS_SQLS_CK             = "stress_sqls";
const char * CONST::PREPARE_SQLS_CK            = "prepare_sqls";
const char * CONST::SQL_CK                     = "sql";
const char * CONST::PARAMS_CK                  = "params";
const char * CONST::WEIGHT_CK                  = "weight";
const char * CONST::TYPE_CK                    = "type";
const char * CONST::RANGE_MIN_CK               = "range_min";
const char * CONST::RANGE_MAX_CK               = "range_max";
const char * CONST::RANGE_STEP_CK              = "range_step";
const char * CONST::DEPENDENT_INDEX_CK         = "dependent_index";
const char * CONST::DIFFERENCE_CK              = "difference";
const char * CONST::GEN_TYPE_CK                = "gen_type";

const char * CONST::DATA_TYPE_NAME_INT_STR           = "int";
const char * CONST::DATA_TYPE_NAME_LONG_STR          = "long";
const char * CONST::DATA_TYPE_NAME_DISTINCT_INT_STR  = "distinct_int";
const char * CONST::DATA_TYPE_NAME_DEPENDENT_INT_STR = "dependent_int";
const char * CONST::DATA_TYPE_NAME_VARCHAR_STR       = "varchar";
const char * CONST::GEN_TYPE_NAME_PARALLEL_LOOP_STR  = "parallel_loop";
const char * CONST::GEN_TYPE_NAME_NESTED_LOOP_STR    = "nested_loop";

class Global
{
  public:
    enum RunningType
    {
      RUNNING_TYPE_STRESS,
      RUNNING_TYPE_PREPARE,
      RUNNING_TYPE_NOT_DEFINED=65535
    };
    enum DataType
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
      MYSQL_TYPE_DISTINCT_LONGLONG=1001,
      MYSQL_TYPE_DEPENDENT_LONGLONG=1002,
      MYSQL_TYPE_NOT_DEFINED=65535
    };
    enum GenType
    {
      GEN_TYPE_PARALLEL_LOOP,
      GEN_TYPE_NESTED_LOOP,
      GEN_TYPE_NOT_DEFINED = 65535,
    };
    static void init()
    {
      iClientStartTs = microseconds();
    }
    static DataType getDataType(const char * name)
    {
      if (strcmp(name, CONST::DATA_TYPE_NAME_INT_STR) == 0)
      {
        return MYSQL_TYPE_LONGLONG;
      }
      else if (strcmp(name, CONST::DATA_TYPE_NAME_LONG_STR) == 0)
      {
        return MYSQL_TYPE_LONGLONG;
      }
      else if (strcmp(name, CONST::DATA_TYPE_NAME_DISTINCT_INT_STR) == 0)
      {
        return MYSQL_TYPE_DISTINCT_LONGLONG;
      }
      else if (strcmp(name, CONST::DATA_TYPE_NAME_DEPENDENT_INT_STR) == 0)
      {
        return MYSQL_TYPE_DEPENDENT_LONGLONG;
      }
      else if (strcmp(name, CONST::DATA_TYPE_NAME_VARCHAR_STR) == 0)
      {
        return MYSQL_TYPE_VARCHAR;
      }
      else
      {
        return MYSQL_TYPE_NOT_DEFINED;
      }
    }
    static const char * getDataTypeStr(const DataType & iDataType)
    {
      switch (iDataType)
      {
        case MYSQL_TYPE_LONGLONG           : return "int";
        case MYSQL_TYPE_DISTINCT_LONGLONG  : return "distinct_int";
        case MYSQL_TYPE_DEPENDENT_LONGLONG : return "dependent_int";
        default                            : return "unknown_type";
      }
    }
    static GenType getGenType(const char * name)
    {
      if (strcmp(name, CONST::GEN_TYPE_NAME_PARALLEL_LOOP_STR) == 0)
      {
        return GEN_TYPE_PARALLEL_LOOP;
      }
      else if (strcmp(name, CONST::GEN_TYPE_NAME_NESTED_LOOP_STR) == 0)
      {
        return GEN_TYPE_NESTED_LOOP;
      }
      else
      {
        return GEN_TYPE_NOT_DEFINED ;
      }
    }
    static const char * getGenTypeStr(const GenType & iGenType)
    {
      switch (iGenType)
      {
        case GEN_TYPE_PARALLEL_LOOP       : return "parallel_loop";
        case GEN_TYPE_NESTED_LOOP         : return "nested_loop";
        default                           : return "unknown_gen_type";
      }
    }
    static enum_field_types getMySQLType(DataType iDataType)
    {
      if (iDataType == MYSQL_TYPE_DISTINCT_LONGLONG)
      {
        return ::MYSQL_TYPE_LONGLONG;
      }
      else if (iDataType == MYSQL_TYPE_DEPENDENT_LONGLONG)
      {
        return ::MYSQL_TYPE_LONGLONG;
      }
      else
      {
        return static_cast<enum_field_types>(iDataType);
      }
    }
    static int64_t getDigitNum(int64_t n)
    {
      int64_t ret = 0;
      do
      {
        n /= 10;
        ret++;
      }
      while (n > 0);
      return ret;
    }
    static int64_t powerOfTen(int64_t n)
    {
      int64_t ret = 1;
      for (int64_t i = 0; i < n; i++)
      {
        ret *= 10;
      }
      return ret;
    }
    static int64_t iClientStartTs;
    static int64_t getAutoIncId()
    {
      return __sync_fetch_and_add(&iAutoIncId_, 1);
    }
    static bool isStop()
    {
      return bStop_;
    }
    static void stop()
    {
      bStop_ = true;
    }
  protected:
    static int64_t iAutoIncId_;
    static bool    bStop_;
};
int64_t Global::iClientStartTs = 0;
int64_t Global::iAutoIncId_ = 0;
bool    Global::bStop_ = false;

class Params
{
  public:
    static void parse(int argc, char * argv[]);
    static void usage(const char * pszProg);
    static void print();
  public:
    static Global::RunningType iRunningType;
    static const char * pszHost;
    static unsigned int iPort;
    static const char * pszUser;
    static const char * pszPasswd;
    static const char * pszDB;
    static int64_t iNumThreads;
    static int64_t iRunningTime;
    static const char * pszConfig;
    static int64_t iClientId;
    static bool bVerbose;
    static int64_t iLoopTimes;
};

class Stats
{
  public:
    static int64_t  iQueryCounts;
    static int64_t  iElapsedTime;
    static int64_t  iRT;
  public:
    static void print()
    {
      printf("Total Querys : %ld\n"
             "Elapsed Time : %fs\n"
             "QPS          : %fs\n"
             "Average RT   : %f\n\n",
             iQueryCounts,
             static_cast<double>(iElapsedTime) / 1000000,
             iElapsedTime == 0 ? 0 : static_cast<double>(iQueryCounts) / static_cast<double>(iElapsedTime) * 1000000.0,
             iElapsedTime == 0 ? 0 : static_cast<double>(iRT) / static_cast<double>(iElapsedTime) * 1000000.0);
    }
};

int64_t  Stats::iQueryCounts = 0;
int64_t  Stats::iElapsedTime = 0;
int64_t  Stats::iRT = 0;

class CMysql;
/**
 * Wrapper of prepared statement related functions of MySQL C API
 */
class CStmt
{
    friend class CMysql;
  public:
    void prepare(const char * pszQuery)
    {
      int iQueryLen = strlen(pszQuery);
      int err = mysql_stmt_prepare(phStmt_, pszQuery, iQueryLen);
      if (0 != err)
      {
        THROW_EXCEPTION(ERR_FATAL, "mysql_stmt_prepare error, err: %d sql: %s",
            err, pszQuery);
      }
      else
      {
        if (iQueryLen < iQueryLenMax_)
        {
          strcpy(pszQuery_, pszQuery);
        }
        else
        {
          strncpy(pszQuery_, pszQuery, iQueryLenMax_ - 1);
          pszQuery_[iQueryLenMax_ - 1] = '\0';
        }
      }
    }
    void bind_param(MYSQL_BIND * phBind)
    {
      int err = mysql_stmt_bind_param(phStmt_, phBind);
      if (0 != err)
      {
        THROW_EXCEPTION(ERR_FATAL, "mysql_stmt_bind_param error, err: %d phBind: %p sql: %s",
            err, phBind, pszQuery_);
      }
    }
    void execute()
    {
      int err = mysql_stmt_execute(phStmt_);
      if (0 != err)
      {
        THROW_EXCEPTION(ERR_FATAL, "mysql_stmt_execute error, err: %d msg: %s sql: %s",
            err, mysql_stmt_error(phStmt_), pszQuery_);
      }
    }
    void bind_result(MYSQL_BIND * phBind)
    {
      int err = mysql_stmt_bind_result(phStmt_, phBind);
      if (0 != err)
      {
        THROW_EXCEPTION(ERR_FATAL, "mysql_stmt_bind_result error, err: %d phBind: %p sql: %s",
            err, phBind, pszQuery_);
      }
    }
    int fetch()
    {
      int ret = 0;
      int err = mysql_stmt_fetch(phStmt_);
      if (MYSQL_NO_DATA == err)
      {
        ret = ERR_NO_DATA;
      }
      else if (0 != err)
      {
        THROW_EXCEPTION(ERR_FATAL, "mysql_stmt_fetch error, err: %d sql: %s",
            err, pszQuery_);
      }
      return ret;
    }
    void free_result()
    {
      int ret = 0;
      mysql_stmt_free_result(phStmt_);
    }
    int64_t fetch_num_rows()
    {
      int64_t num = 0;
      while (fetch() == 0) num++;
      return num;
    }
  protected:
    CStmt(MYSQL_STMT * phStmt)
    {
      if (NULL == phStmt)
      {
        THROW_EXCEPTION(ERR_FATAL, "CStmt constructor phStmt is NULL");
      }
      else
      {
        phStmt_ = phStmt;
      }
    }
  public:
    ~CStmt()
    {
      if (NULL != phStmt_)
      {
        mysql_stmt_close(phStmt_);
      }
    }
  protected:
    MYSQL_STMT * phStmt_;
    static const int iQueryLenMax_ = 4096;
    char pszQuery_[iQueryLenMax_];
};

/**
 * Wrapper of connection related functions of MySQL C API
 */
class CMysql
{
  public:
    CMysql(const char * pszHost, unsigned int iPort,
           const char * pszUser, const char * pszPasswd,
           const char * pszDB)
    {
      if (NULL == (phMysql_ = mysql_init(NULL)))
      {
        THROW_EXCEPTION(ERR_FATAL, "mysql_init error");
      }
      if (NULL == mysql_real_connect(
            phMysql_,
            pszHost,
            pszUser,
            pszPasswd,
            pszDB,
            iPort,
            NULL,
            0))
      {
        THROW_EXCEPTION(ERR_FATAL, "mysql_real_connect error");
      }
    }
    ~CMysql()
    {
      mysql_close(phMysql_);
    }
    void query(const char * pszQuery)
    {
      if (0 != mysql_query(phMysql_, pszQuery))
      {
        THROW_EXCEPTION(ERR_FATAL, "mysql_query error, sql: %s", pszQuery);
      }
      MYSQL_RES * result = mysql_store_result(phMysql_);
      if (NULL == result)
      {
        THROW_EXCEPTION(ERR_FATAL, "mysql_store_result error, sql: %s", pszQuery);
      }
      MYSQL_FIELD *fields = mysql_fetch_fields(result);
      int num_fields = mysql_num_fields(result);
      for(int i = 0; i < num_fields; i++)
      {
         TBSYS_LOG(INFO, "Field %u is %s\n", i, fields[i].name);
      }
      MYSQL_ROW row;
      while ((row = mysql_fetch_row(result)))
      {
        unsigned long *lengths;
        lengths = mysql_fetch_lengths(result);
        for(int i = 0; i < num_fields; i++)
        {
          TBSYS_LOG(INFO, "[%.*s] ", (int) lengths[i], row[i] ? row[i] : "NULL");
        }
      }
      mysql_free_result(result);
    }
    auto_ptr<CStmt> getStmt()
    {
      MYSQL_STMT * ph_stmt = mysql_stmt_init(phMysql_);
      if (NULL == ph_stmt)
      {
        THROW_EXCEPTION(ERR_FATAL, "mysql_stmt_init error");
      }
      else
      {
        return auto_ptr<CStmt>(new CStmt(ph_stmt));
      }
    }
  protected:
    MYSQL * phMysql_;
};

/**
 * Wrapper of buffer for prepared statement
 * when doing copy constructor or assignment,
 * the source will pass the ownership to the receiver
 */
class CPrBuffer
{
  public:
    CPrBuffer(unsigned long uBufLen)
      : uBufLen_(uBufLen)
    {
      pBuf_ = new char[uBufLen];
    }
    ~CPrBuffer()
    {
      if (NULL != pBuf_)
      {
        delete[] pBuf_;
        pBuf_ = NULL;
      }
    }
    CPrBuffer(const CPrBuffer & r)
      : pBuf_(r.pBuf_), uLen_(r.uLen_), uBufLen_(r.uBufLen_)
    {
      const_cast<CPrBuffer &>(r).pBuf_ = NULL;
    }
    CPrBuffer & operator = (const CPrBuffer & r)
    {
      if (NULL != pBuf_)
      {
        delete[] pBuf_;
      }
      pBuf_ = r.pBuf_;
      uLen_ = r.uLen_;
      uBufLen_ = r.uBufLen_;
      const_cast<CPrBuffer &>(r).pBuf_ = NULL;
    }
    char * getBuf()
    {
      return pBuf_;
    }
    const char * getBuf() const
    {
      return pBuf_;
    }
    unsigned long & getLen()
    {
      return uLen_;
    }
    const unsigned long getLen() const
    {
      return uLen_;
    }
    const unsigned long getBufLen() const
    {
      return uBufLen_;
    }
  protected:
    char *         pBuf_;
    unsigned long  uLen_;
    unsigned long  uBufLen_;
};

/**
 * representing the binded parameter parsed from the config file
 */
class CBndParam
{
  public:
    CBndParam(Global::DataType iDataType)
      : iDataType_(iDataType), iRangeMin_(-1), iRangeMax_(-1), iRangeStep_(1),
        iDependentIndex_(-1), iDifference_(0)
    {
    }
    CBndParam(Global::DataType iDataType, int64_t iRangeMin, int64_t iRangeMax, int64_t iRangeStep)
      : iDataType_(iDataType), iRangeMin_(iRangeMin), iRangeMax_(iRangeMax), iRangeStep_(iRangeStep),
        iDependentIndex_(-1), iDifference_(0)
    {
    }
    CBndParam(Global::DataType iDataType, int64_t iRangeMin, int64_t iRangeMax, int64_t iRangeStep,
        int64_t iDependentIndex, int64_t iDifference)
      : iDataType_(iDataType), iRangeMin_(iRangeMin), iRangeMax_(iRangeMax), iRangeStep_(iRangeStep),
        iDependentIndex_(iDependentIndex), iDifference_(iDifference)
    {
    }
    void print() const
    {
      printf(" %s(%ld %ld %ld %ld %ld)", Global::getDataTypeStr(iDataType_),
          iRangeMin_, iRangeMax_, iRangeStep_, iDependentIndex_, iDifference_);
    }
    Global::DataType  getDataType() const
    {
      return iDataType_;
    }
    int64_t getRangeMin() const
    {
      return iRangeMin_;
    }
    int64_t getDependentIndex() const
    {
      return iDependentIndex_;
    }
    void getRndValue(CPrBuffer & rBuffer) const
    {
      int64_t iAutoIncId;
      int64_t iV = static_cast<int64_t>(Random::rand() & 0x7fffffffffffffff);
      if (-1 != iRangeMin_ || -1 != iRangeMax_)
      {
        iV = iV % (iRangeMax_ - iRangeMin_ + 1) + iRangeMin_;
      }
      switch (iDataType_)
      {
        case Global::MYSQL_TYPE_DISTINCT_LONGLONG:
          iV = Global::iClientStartTs;
          iV *= Global::powerOfTen(Global::getDigitNum(Params::iClientId));
          iV += Params::iClientId;
          iAutoIncId = Global::getAutoIncId();
          iV *= Global::powerOfTen(Global::getDigitNum(iAutoIncId));
          iV += iAutoIncId;
        case Global::MYSQL_TYPE_LONGLONG:
          writeIntValue(iV, rBuffer);
          break;
        case Global::MYSQL_TYPE_DEPENDENT_LONGLONG:
          break;
        case Global::MYSQL_TYPE_VARCHAR:
          writeVarcharValue(iV, rBuffer);
          break;
        default:
          THROW_EXCEPTION(ERR_FATAL, "Unknown data type: %d", iDataType_);
      }
    }
    int getNextValue(int64_t & iV, CPrBuffer & rBuffer) const
    {
      if (iV > iRangeMax_)
      {
        return ERR_ITER_END;
      }
      writeIntValue(iV, rBuffer);
      iV += iRangeStep_;
      return 0;
    }
    void getDependentIntValue(const CPrBuffer & rDependent, CPrBuffer & rBuffer) const
    {
      const int64_t * iDV = reinterpret_cast<const int64_t *>(rDependent.getBuf());
      writeIntValue(*iDV + iDifference_, rBuffer);
    }
  protected:
    void writeIntValue(int64_t iV, CPrBuffer & rBuffer) const
    {
      if (rBuffer.getBufLen() < sizeof(iV))
      {
        THROW_EXCEPTION(ERR_FATAL, "CPrBuffer is not enough");
      }
      else
      {
        memcpy(rBuffer.getBuf(), &iV, sizeof(iV));
        rBuffer.getLen() = sizeof(iV);
      }
    }
    void writeVarcharValue(int64_t iV, CPrBuffer & rBuffer) const
    {
      int64_t iDigitNum;
      iDigitNum = snprintf(rBuffer.getBuf(), rBuffer.getBufLen(), "%ld", iV);
      rBuffer.getLen() = iDigitNum;
      if (iDigitNum < rBuffer.getBufLen())
      {
        for (int64_t i = 0; i < rBuffer.getBufLen() / iDigitNum - 1; i++)
        {
          rBuffer.getLen() += snprintf(rBuffer.getBuf(), rBuffer.getBufLen(), "%ld", iV);
        }
      }
    }
  protected:
    Global::DataType  iDataType_;
    int64_t           iRangeMin_, iRangeMax_, iRangeStep_;
    int64_t           iDependentIndex_, iDifference_;
};

class CSQLParams
{
  public:
    CSQLParams()
    {
    }
    CSQLParams(Global::GenType iGenType, const vector<CBndParam> & aParams)
      : iGenType_(iGenType), aParams_(aParams),
        aNextValues_(aParams.size()), aNextEnds_(aParams.size())
    {
      for (int64_t i = 0; i < aParams_.size(); i++)
      {
        aNextValues_[i] = aParams_[i].getRangeMin();
        aNextEnds_[i] = false;
      }
    }
    void print()
    {
      for (int64_t i = 0; i < aParams_.size(); i++)
      {
        aParams_[i].print();
      }
      printf("    %s", Global::getGenTypeStr(iGenType_));
    }
    void prepare(CStmt * phStmt, vector<CPrBuffer> & aBuffers)
    {
      if (NULL == phStmt)
      {
        THROW_EXCEPTION(ERR_ARG, "phStmt is NULL");
      }
      int64_t iParamNum = aParams_.size();
      MYSQL_BIND * input = new MYSQL_BIND[iParamNum];
      memset(input, 0x00, sizeof(MYSQL_BIND) * iParamNum);
      for (int64_t i = 0; i < iParamNum; i++)
      {
        CPrBuffer buffer(sizeof(int64_t));
        input[i].buffer_type = Global::getMySQLType(aParams_[i].getDataType());
        input[i].buffer = buffer.getBuf();
        input[i].length = &buffer.getLen();
        input[i].buffer_length = sizeof(int64_t);
        aBuffers.push_back(buffer);
      }
      phStmt->bind_param(input);
    }
    int64_t getParamNum() const
    {
      return aParams_.size();
    }
    Global::GenType getGenType() const
    {
      return iGenType_;
    }
    void rnd(vector<CPrBuffer> & aBuffers)
    {
      checkValidBuffers(aBuffers);
      int64_t iParamNum = aParams_.size();
      for (int64_t i = 0; i < iParamNum; i++)
      {
        aParams_[i].getRndValue(aBuffers[i]);
      }
      calDependentInt(aBuffers);
    }
    int next(vector<CPrBuffer> & aBuffers)
    {
      int ret = 0;
      int err = 0;
      checkValidBuffers(aBuffers);
      int64_t iParamNum = aParams_.size();
      if (iGenType_ == Global::GEN_TYPE_PARALLEL_LOOP)
      {
        for (int64_t i = 0; i < iParamNum; i++)
        {
          err = aParams_[i].getNextValue(aNextValues_[i], aBuffers[i]);
          if (ERR_ITER_END == err)
          {
            aNextEnds_[i] = true;
            aNextValues_[i] = aParams_[i].getRangeMin();
            err = aParams_[i].getNextValue(aNextValues_[i], aBuffers[i]);
            if (0 != err)
            {
              THROW_EXCEPTION(ERR_FATAL, "misconfigued range, index: %ld", i);
            }
          }
        }
        ret = ERR_ITER_END;
        for (int64_t i = 0; i < iParamNum; i++)
        {
          if (!aNextEnds_[i])
          {
            ret = 0;
            break;
          }
        }
      }
      else if (iGenType_ == Global::GEN_TYPE_NESTED_LOOP)
      {
        for (int64_t i = 0; i < iParamNum; i++)
        {
          err = aParams_[i].getNextValue(aNextValues_[i], aBuffers[i]);
          if (ERR_ITER_END == err)
          {
            if (i == iParamNum - 1)
            {
              ret = ERR_ITER_END;
              break;
            }
            aNextValues_[i] = aParams_[i].getRangeMin();
            err = aParams_[i].getNextValue(aNextValues_[i], aBuffers[i]);
            if (0 != err)
            {
              THROW_EXCEPTION(ERR_FATAL, "misconfigued range, index: %ld", i);
            }
          }
          else
          {
            break;
          }
        }
      }
      else
      {
        ret = ERR_ITER_END;
      }
      if (0 == ret)
      {
        calDependentInt(aBuffers);
      }
      return ret;
    }
  protected:
    void calDependentInt(vector<CPrBuffer> & aBuffers)
    {
      int64_t iParamNum = aParams_.size();
      for (int64_t i = 0; i < iParamNum; i++)
      {
        if (aParams_[i].getDataType() == Global::MYSQL_TYPE_DEPENDENT_LONGLONG)
        {
          int64_t iDependentIndex = aParams_[i].getDependentIndex();
          if (iDependentIndex > iParamNum)
          {
            THROW_EXCEPTION(ERR_FATAL, "error dependent_index");
          }
          else
          {
            aParams_[i].getDependentIntValue(aBuffers[iDependentIndex - 1], aBuffers[i]);
          }
        }
      }
    }
    void checkValidBuffers(vector<CPrBuffer> & aBuffers)
    {
      if (aParams_.size() != aBuffers.size())
      {
        THROW_EXCEPTION(ERR_FATAL, "size dismatch between BindedParams and Buffers");
      }
    }
  protected:
    Global::GenType          iGenType_;
    vector<CBndParam>        aParams_;
    vector<int64_t>          aNextValues_;
    vector<bool>             aNextEnds_;
};

class CCfgSQL
{
  public:
    CCfgSQL()
    {
    }
    CCfgSQL(const string & pszSQL, int64_t iWeight, const CSQLParams & rParams)
      : pszSQL_(pszSQL), iWeight_(iWeight), oParams_(rParams)
    {
      printf("SQL: %s\n", pszSQL_.c_str());
      printf("Weight: %ld\n", iWeight_);
      printf("Binded Param Types:");
      oParams_.print();
      printf("\n\n");
    }
    void prepare(CStmt * phStmt, vector<CPrBuffer> & aBuffers)
    {
      if (NULL == phStmt)
      {
        THROW_EXCEPTION(ERR_ARG, "phStmt is NULL");
      }
      phStmt->prepare(pszSQL_.c_str());
      oParams_.prepare(phStmt, aBuffers);
    }
    void rnd(vector<CPrBuffer> & aBuffers)
    {
      oParams_.rnd(aBuffers);
    }
    int next(vector<CPrBuffer> & aBuffers)
    {
      return oParams_.next(aBuffers);
    }
    int64_t getWeight() const
    {
      return iWeight_;
    }
  protected:
    string                   pszSQL_;
    int64_t                  iWeight_;
    CSQLParams               oParams_;
};

class CPrStmt
{
  public:
    CPrStmt()
    {
    }
    CPrStmt(auto_ptr<CStmt> phStmt, const CCfgSQL & oSQL)
      : phStmt_(phStmt), oSQL_(oSQL)
    {
      oSQL_.prepare(phStmt_.get(), aBuffers_);
    }
    CPrStmt(const CPrStmt & r)
      : phStmt_(const_cast<CPrStmt &>(r).phStmt_),
        oSQL_(r.oSQL_), aBuffers_(r.aBuffers_)
    {
    }
    CPrStmt & operator = (const CPrStmt & r)
    {
      phStmt_ = const_cast<CPrStmt &>(r).phStmt_;
      oSQL_ = r.oSQL_;
      aBuffers_ = r.aBuffers_;
      return *this;
    }
    void stress_execute()
    {
      oSQL_.rnd(aBuffers_);
      try
      {
        phStmt_->execute();
      }
      catch (ErrMsgException & ex)
      {
        phStmt_->free_result();
        RETHROW(ex);
      }
      phStmt_->free_result();
    }
    int prepare_execute()
    {
      int ret = 0;
      ret = oSQL_.next(aBuffers_);
      if (0 == ret)
      {
        try
        {
          phStmt_->execute();
        }
        catch (ErrMsgException & ex)
        {
          phStmt_->free_result();
          RETHROW(ex);
        }
        phStmt_->free_result();
      }
      return ret;
    }
    int64_t getWeight() const
    {
      return oSQL_.getWeight();
    }
  protected:
    auto_ptr<CStmt>   phStmt_;
    CCfgSQL           oSQL_;
    vector<CPrBuffer> aBuffers_;
};

class CConfig
{
  public:
    CConfig()
    {
    }
    void open(const char * fn)
    {
      parseConfig_(fn);
    }
    const vector<CCfgSQL> & getStressSQLList() const
    {
      return aStressSQLList_;
    }
    const vector<CCfgSQL> & getPrepareSQLList() const
    {
      return aPrepareSQLList_;
    }
  protected:
    void parseConfig_(const char * fn)
    {
      lua_State *L = luaL_newstate();
      luaL_openlibs(L);
      if (luaL_loadfile(L, fn) || lua_pcall(L, 0, 0, 0))
      {
        THROW_EXCEPTION(ERR_ARG, "cannot parse config file: %s", lua_tostring(L, -1));
      }
      try
      {
        parseSQLs_(L, CONST::STRESS_SQLS_CK, aStressSQLList_);
      }
      catch (ErrMsgException & ex)
      {
        ex.logException();
        printf("Do not load `stress_sqls': %s\n", ex.what());
      }
      try
      {
        parseSQLs_(L, CONST::PREPARE_SQLS_CK, aPrepareSQLList_);
      }
      catch (ErrMsgException & ex)
      {
        ex.logException();
        printf("Do not load `prepare_sqls': %s\n", ex.what());
      }
      lua_close(L);
    }
    void parseSQLs_(lua_State * L, const char * key, vector<CCfgSQL> & aSQLList)
    {
      lua_getglobal(L, key); // push STRESS_SQLS: [ STRESS_SQLS
      if (!lua_istable(L, -1))
      {
        lua_pop(L, 1); // pop STRESS_SQLS: [
        THROW_EXCEPTION(ERR_ARG, "parse `%s' error", key);
      }
      int64_t iSQLNum = getTableLen_(L, -1);
      for (int64_t i = 0; i < iSQLNum; i++)
      {
        lua_pushinteger(L, i + 1);
        lua_gettable(L, -2); // push STRESS_SQLS[i]: [ STRESS_SQLS STRESS_SQLS[i]
        if (!lua_istable(L, -1))
        {
          lua_pop(L, 2); // pop STRESS_SQLS[i] and STRESS_SQLs: [
          THROW_EXCEPTION(ERR_ARG, "parse `%s' error: `%s' and `%s' should be "
              "enclosed by `{}'", key, CONST::SQL_CK, CONST::PARAMS_CK);
        }
        string pszSQL = getSQL_(L, -1);
        int64_t iWeight = getWeight_(L, -1);
        CSQLParams oSQLParams = getParams_(L, -1);
        aSQLList.push_back(CCfgSQL(pszSQL, iWeight, oSQLParams));
        lua_pop(L, 1); // pop STRESS_SQLS[i]: [ STRESS_SQLS
      }
      lua_pop(L, 1); // pop STRESS_SQLS: [
    }
    int64_t getTableLen_(lua_State * L, int iTableIdx)
    {
      lua_len(L, iTableIdx);
      int64_t ret = lua_tointeger(L, -1);
      lua_pop(L, 1);
      return ret;
    }
    string getSQL_(lua_State * L, int iTableIdx)
    {
      string ret;
      lua_getfield(L, iTableIdx, CONST::SQL_CK);
      if (!lua_isstring(L, -1))
      {
        lua_pop(L, 1);
        THROW_EXCEPTION(ERR_ARG, "parse `%s' error", CONST::SQL_CK);
      }
      ret = lua_tostring(L, -1);
      lua_pop(L, 1);
      return ret;
    }
    CSQLParams getParams_(lua_State * L, int iTableIdx)
    {
      vector<CBndParam> aParams;
      lua_getfield(L, iTableIdx, CONST::PARAMS_CK); // push STRESS_SQLS[i].PARAMS: [ STRESS_SQLS STRESS_SQLS[i] STRESS_SQLS[i].PARAMS
      if (!lua_istable(L, -1))
      {
        lua_pop(L, 1); // pop STRESS_SQLS[i].PARAMS: [ STRESS_SQLS STRESS_SQLS[i]
        THROW_EXCEPTION(ERR_ARG, "parse `%s' error", CONST::PARAMS_CK);
      }
      int64_t iParamNum = getTableLen_(L, -1);
      for (int64_t j = 0; j < iParamNum; j++)
      {
        lua_pushinteger(L, j + 1);
        lua_gettable(L, -2); // push STRESS_SQLS[i].PARAMS[j]: [ STRESS_SQLS STRESS_SQLS[i] STRESS_SQLS[i].PARAMS STRESS_SQLS[i].PARAMS[j]
        if (!lua_istable(L, -1))
        {
          lua_pop(L, 2); // pop STRESS_SQLS[i].PARAMS[j] and STRESS_SQLS[i].PARAMS: [ STRESS_SQLS STRESS_SQLS[i]
          THROW_EXCEPTION(ERR_ARG, "parse `%s' error: `%s' should be "
              "enclosed by `{}'", CONST::PARAMS_CK, CONST::TYPE_CK);
        }
        aParams.push_back(getParam_(L, -1, iParamNum));
        lua_pop(L, 1); // pop STRESS_SQLS[i].PARAMS[j]: [ STRESS_SQLS STRESS_SQLS[i] STRESS_SQLS[i].PARAMS
      }
      Global::GenType iGenType;
      lua_getfield(L, -1, CONST::GEN_TYPE_CK);
      if (lua_isstring(L, -1))
      {
        iGenType = Global::getGenType(lua_tostring(L, -1));
      }
      else
      {
        iGenType = Global::GEN_TYPE_NOT_DEFINED;
      }
      lua_pop(L, 1); // pop STRESS_SQLS[i].PARAMS.GEN_TYPE: [ STRESS_SQLS STRESS_SQLS[i] STRESS_SQLS[i].PARAMS
      lua_pop(L, 1); // pop STRESS_SQLS[i].PARAMS: [ STRESS_SQLS STRESS_SQLS[i]
      return CSQLParams(iGenType, aParams);
    }
    CBndParam getParam_(lua_State * L, int iTableIdx, int64_t iParamNum)
    {
      Global::DataType iDataType;
      int64_t iRangeMin, iRangeMax, iRangeStep;
      int64_t iDependentIndex, iDifference;
      lua_getfield(L, iTableIdx, CONST::TYPE_CK);
      if (!lua_isstring(L, -1))
      {
        lua_pop(L, 1);
        THROW_EXCEPTION(ERR_ARG, "parse `%s' error in `%s'",
            CONST::TYPE_CK, CONST::PARAMS_CK);
      }
      iDataType = Global::getDataType(lua_tostring(L, -1));
      lua_pop(L, 1);
      lua_getfield(L, iTableIdx, CONST::RANGE_MIN_CK);
      if (lua_isnumber(L, -1))
      {
        iRangeMin = lua_tointeger(L, -1);
      }
      else
      {
        iRangeMin = -1;
      }
      lua_pop(L, 1);
      lua_getfield(L, iTableIdx, CONST::RANGE_MAX_CK);
      if (lua_isnumber(L, -1))
      {
        iRangeMax= lua_tointeger(L, -1);
      }
      else
      {
        iRangeMax= -1;
      }
      lua_pop(L, 1);
      lua_getfield(L, iTableIdx, CONST::RANGE_STEP_CK);
      if (lua_isnumber(L, -1))
      {
        iRangeStep = lua_tointeger(L, -1);
      }
      else
      {
        iRangeStep = 1;
      }
      lua_pop(L, 1);
      if (iRangeMin > iRangeMax || iRangeStep <= 0)
      {
        THROW_EXCEPTION(ERR_ARG, "iRangeMin `%ld' iRangeMax `%ld' iRangeStep `%ld' values error",
            iRangeMin, iRangeMax, iRangeStep);
      }
      lua_getfield(L, iTableIdx, CONST::DEPENDENT_INDEX_CK);
      if (lua_isnumber(L, -1))
      {
        iDependentIndex = lua_tointeger(L, -1);
      }
      else
      {
        iDependentIndex = -1;
      }
      lua_pop(L, 1);
      lua_getfield(L, iTableIdx, CONST::DIFFERENCE_CK);
      if (lua_isnumber(L, -1))
      {
        iDifference = lua_tointeger(L, -1);
      }
      else
      {
        iDifference = 0;
      }
      lua_pop(L, 1);
      if (iDependentIndex != -1 && iDependentIndex > iParamNum)
      {
        THROW_EXCEPTION(ERR_ARG, "dependent_index `%ld' is bigger than parameter number", iDependentIndex);
      }
      return CBndParam(iDataType, iRangeMin, iRangeMax, iRangeStep,
                 iDependentIndex, iDifference);
    }
    int64_t getWeight_(lua_State * L, int iTableIdx)
    {
      int64_t ret;
      lua_getfield(L, iTableIdx, CONST::WEIGHT_CK);
      if (lua_isnumber(L, -1))
      {
        ret = lua_tointeger(L, -1);
      }
      else
      {
        ret = 1;
      }
      lua_pop(L, 1);
      return ret;
    }
  protected:
    vector<CCfgSQL> aStressSQLList_;
    vector<CCfgSQL> aPrepareSQLList_;
};
CConfig rConfig;

class CBall
{
  public:
    CBall(CMysql & rhMysql, const CConfig & rConfig)
      : iTotalWeight_(0), iIndex_(0)
    {
      const vector<CCfgSQL> & aSQLList = rConfig.getStressSQLList();
      for (vector<CCfgSQL>::const_iterator iter = aSQLList.begin();
          iter != aSQLList.end(); iter++)
      {
        aPrStmts_.push_back(CPrStmt(rhMysql.getStmt(), *iter));
        iTotalWeight_ += iter->getWeight();
      }
    }
    void bat()
    {
      iIndex_++;
      int64_t p = iIndex_ % iTotalWeight_;
      for (vector<CPrStmt>::iterator iter = aPrStmts_.begin();
          iter != aPrStmts_.end(); iter++)
      {
        if (p < iter->getWeight())
        {
          int64_t iStartTime = microseconds();
          iter->stress_execute();
          __sync_fetch_and_add(&Stats::iRT, microseconds() - iStartTime);
          __sync_fetch_and_add(&Stats::iQueryCounts, 1);
          break;
        }
        else
        {
          p -= iter->getWeight();
        }
      }
    }
  protected:
    vector<CPrStmt> aPrStmts_;
    int64_t iTotalWeight_;
    int64_t iIndex_;
};

class CPaver
{
  public:
    void init(CMysql & rhMysql, const CConfig & rConfig, int64_t iPaverIndex)
    {
      oPrStmt_ = CPrStmt(rhMysql.getStmt(), rConfig.getPrepareSQLList()[iPaverIndex]);
    }
    int pave()
    {
      return oPrStmt_.prepare_execute();
    }
  protected:
    CPrStmt oPrStmt_;
};

class CWorker : public tbsys::CDefaultRunnable
{
  public:
    CWorker()
      : hMysql_(Params::pszHost, Params::iPort, Params::pszUser, Params::pszPasswd, Params::pszDB),
        oBall_(hMysql_, rConfig),
        iWorkerIndex_(0)
    {
    }
    void run(tbsys::CThread * thread, void * arg)
    {
      try
      {
        Random::srand(microseconds() + iWorkerIndex_);
        init();
        if (Params::iLoopTimes > 0)
        {
          for (int64_t i = 0; i < Params::iLoopTimes && !Global::isStop(); i++)
          {
            if (Params::iRunningType == Global::RUNNING_TYPE_STRESS)
            {
              oBall_.bat();
            }
            else if (Params::iRunningType == Global::RUNNING_TYPE_PREPARE)
            {
              oPaver_.pave();
            }
            else
            {
              // do nothing
            }
          }
        }
        else
        {
          if (Params::iRunningType == Global::RUNNING_TYPE_STRESS)
          {
            while (!_stop)
            {
              oBall_.bat();
            }
          }
          else if (Params::iRunningType == Global::RUNNING_TYPE_PREPARE)
          {
            int err = 0;
            while (ERR_ITER_END != err && !Global::isStop())
            {
              try
              {
                err = oPaver_.pave();
              }
              catch (ErrMsgException & ex)
              {
                ex.logException();
                sleep(2);
              }
            }
              ;
          }
          else
          {
            // do nothing
          }
        }
      }
      catch (ErrMsgException & ex)
      {
        ex.logException();
      }
    }
    void setWorkerIndex(int64_t iWorkerIndex)
    {
      iWorkerIndex_ = iWorkerIndex;
    }
  protected:
    void init()
    {
      if (Params::iRunningType == Global::RUNNING_TYPE_STRESS)
      {
        // do nothing
      }
      else if (Params::iRunningType == Global::RUNNING_TYPE_PREPARE)
      {
        oPaver_.init(hMysql_, rConfig, iWorkerIndex_);
      }
      else
      {
        // do nothing
      }
    }
  protected:
    CMysql    hMysql_;
    CBall     oBall_;
    CPaver    oPaver_;
    int64_t   iWorkerIndex_;
};

void Params::parse(int argc, char * argv[])
{
  struct option long_options[] = {
    {"num_threads", 1, 0, 201},
    {"running_time", 1, 0, 203},
    {"host", 1, 0, 204},
    {"port", 1, 0, 205},
    {"user", 1, 0, 206},
    {"passwd", 1, 0, 207},
    {"db", 1, 0, 208},
    {"config", 1, 0, 209},
    {"client_id", 1, 0, 210},
    {"verbose", 0, 0, 211},
    {"loop_times", 1, 0, 301},  // options more than 300 are hidden values
  };
  int c = 0;
  int option_index;
  while ((c = getopt_long(argc, argv, "h", long_options, &option_index)) != -1)
  {
    switch (c)
    {
      case 'h':
        usage(argv[0]);
        THROW_EXCEPTION(0, "print usage");
      case 201:
        iNumThreads = atol(optarg);
        break;
      case 203:
        iRunningTime = atol(optarg);
        break;
      case 204:
        pszHost = optarg;
        break;
      case 205:
        iPort = atol(optarg);
        break;
      case 206:
        pszUser = optarg;
        break;
      case 207:
        pszPasswd = optarg;
        break;
      case 208:
        pszDB = optarg;
        break;
      case 209:
        pszConfig = optarg;
        break;
      case 210:
        iClientId = atol(optarg);
        break;
      case 211:
        bVerbose = true;
        break;
      case 301:
        iLoopTimes = atol(optarg);
        break;
      default:
        usage(argv[0]);
        THROW_EXCEPTION(ERR_ARG, "unknown opt: %d", c);
    }
  }
  rConfig.open(Params::pszConfig);
  if (optind < argc && strcmp(argv[optind], CONST::COMMAND_PREPARE) == 0)
  {
    iRunningType = Global::RUNNING_TYPE_PREPARE;
    iNumThreads = rConfig.getPrepareSQLList().size();
    iRunningTime = 0;
  }
  print();
}
void Params::usage(const char * pszProg)
{
  fprintf(stderr, "\n%s [--num_threads=? --running_time=? "
                        "--host=? --port=? --user=? --passwd=? --db=? "
                        "--config=? --client_id=?]\n\n",
                        pszProg);
}
void Params::print()
{
  printf("num_threads  = %ld\n"
         "running_time = %ld\n"
         "host         = %s\n"
         "port         = %ld\n"
         "user         = %s\n"
         "passwd       = %s\n"
         "db           = %s\n"
         "config       = %s\n"
         "client_id    = %ld\n"
         "verbose      = %s\n",
         iNumThreads, iRunningTime,
         pszHost, iPort, pszUser, pszPasswd, pszDB,
         pszConfig, iClientId, bVerbose ? "true" : "false");
}

Global::RunningType Params::iRunningType = Global::RUNNING_TYPE_STRESS;
const char * Params::pszHost      = "127.0.0.1";
unsigned int Params::iPort        = 3248;
const char * Params::pszUser      = "admin";
const char * Params::pszPasswd    = "admin";
const char * Params::pszDB        = "";
int64_t      Params::iNumThreads  = 1;
int64_t      Params::iRunningTime = 1;
const char * Params::pszConfig    = "";
int64_t      Params::iClientId    = 0;
bool         Params::bVerbose     = false;
int64_t      Params::iLoopTimes   = 0;

void term_handler(const int sig)
{
  Global::stop();
}

int main(int argc, char * argv[])
{
  TBSYS_LOG(INFO, "start main");
  Global::init();
  signal(SIGPIPE, SIG_IGN);
  signal(SIGHUP,  SIG_IGN);
  signal(SIGTERM, term_handler);
  signal(SIGINT, term_handler);
  int64_t iStartTime = microseconds();
  CWorker * w = NULL;
  try
  {
    Params::parse(argc, argv);
    w = new CWorker[Params::iNumThreads];
    for (int64_t i = 0; i < Params::iNumThreads; i++)
    {
      w[i].setWorkerIndex(i);
      w[i].start();
    }
    if (Params::iLoopTimes == 0)
    {
      iStartTime = microseconds();
      int64_t iEndTime = iStartTime + Params::iRunningTime * 1000000;
      while (microseconds() < iEndTime && !Global::isStop())
      {
        usleep(100000);
      }
    }
    for (int64_t i = 0; i < Params::iNumThreads; i++)
    {
      w[i].stop();
    }
    for (int64_t i = 0; i < Params::iNumThreads; i++)
    {
      w[i].wait();
    }
    delete[] w;
  }
  catch (ErrMsgException & ex)
  {
    ex.logException();
  }
  Stats::iElapsedTime = microseconds() - iStartTime;
  Stats::print();
  return 0;
}

