#include "sql/ob_result_set.h"
#include "ob_mysql_field.h"
#include "ob_mysql_util.h"

namespace oceanbase {
  namespace obmysql {
    ObMySQLField::ObMySQLField()
      : catalog_("def"), db_(""),
        length_(0x20000), flags_(0), decimals_(0), charsetnr_(28)
    {
    }

    int ObMySQLField::serialize_pro41(char *buf, const int64_t len, int64_t &pos) const
    {
      EMySQLFieldType type = MYSQL_TYPE_GEOMETRY; //init with type unsupported
      int ret = OB_SUCCESS;
      uint8_t num_decimals = decimals_;
      uint32_t length = length_;
      if (OB_SUCCESS != (ret = ObMySQLUtil::get_mysql_type(type_.get_type(), type, num_decimals, length)))
      {
        TBSYS_LOG(WARN, "get mysql type failed, type is %d mysql type is %d", type_.get_type(), type);
      }
      else if (OB_SUCCESS != (ret = ObMySQLUtil::store_str(buf, len, catalog_, pos)))
      {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
        {
          TBSYS_LOG(WARN, "serialize catalog failed ret is %d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = ObMySQLUtil::store_str(buf, len, db_, pos)))
      {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
        {
          TBSYS_LOG(WARN, "serialize db failed ret is %d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = ObMySQLUtil::store_str_v(buf, len, tname_.ptr(), tname_.length(), pos)))
      {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
        {
          TBSYS_LOG(WARN, "serialize tname failed ret is %d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = ObMySQLUtil::store_str_v(buf, len, org_tname_.ptr(), org_tname_.length(), pos)))
      {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
        {
          TBSYS_LOG(WARN, "serialize org_tname failed ret is %d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = ObMySQLUtil::store_str_v(buf, len, cname_.ptr(), cname_.length(), pos)))
      {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
        {
          TBSYS_LOG(WARN, "serialize cname failed ret is %d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = ObMySQLUtil::store_str_v(buf, len, org_cname_.ptr(), org_cname_.length(), pos)))
      {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
        {
          TBSYS_LOG(WARN, "serialize org_cname failed ret is %d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buf, len, 0xc, pos))) // length of belows
      {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
        {
          TBSYS_LOG(WARN, "serialize 0xc failed ret is %d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int2(buf, len, charsetnr_, pos)))
      {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
        {
          TBSYS_LOG(WARN, "serialize charsetnr failed ret is %d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int4(buf, len, length, pos)))
      {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
        {
          TBSYS_LOG(WARN, "serialize length failed ret is %d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buf, len, type, pos)))
      {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
        {
          TBSYS_LOG(WARN, "serialize type failed ret is %d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int2(buf, len, flags_, pos)))
      {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
        {
          TBSYS_LOG(WARN, "serialize flags failed ret is %d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int1(buf, len, num_decimals, pos)))
      {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
        {
          TBSYS_LOG(WARN, "serialize num_decimals failed ret is %d", ret);
        }
      }
      else if (OB_SUCCESS != (ret = ObMySQLUtil::store_int2(buf, len, 0, pos)))
      {
        if (OB_UNLIKELY(OB_SIZE_OVERFLOW != ret))
        {
          TBSYS_LOG(WARN, "serialize 0 failed ret is %d", ret);
        }
      }

      return ret;
    }

  } // end of namespace obmysql
} // end of namespace oceanbase
