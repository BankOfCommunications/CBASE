#include <string.h>
#include <algorithm>
#include <math.h>
#include <sstream>//add gaojt [ListAgg][JHOBv0.1]20150108

#include "ob_object.h"
#include "serialization.h"
#include "tbsys.h"
#include "ob_action_flag.h"
#include "utility.h"
#include "ob_crc64.h"
#include "murmur_hash.h"
//add  wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
#include "Ob_Decimal.h"
//add e
using namespace oceanbase;
using namespace oceanbase::common;

const uint8_t ObObj::INVALID_OP_FLAG;
const uint8_t ObObj::ADD;


bool ObObj::is_true() const
{
  bool ret = false;
  switch (get_type())
  {
    case ObBoolType:
      ret = value_.bool_val;
      break;
    case ObVarcharType:
      ret = (val_len_ > 0);
      break;
    case ObIntType:
      ret = (value_.int_val != 0);
      break;
    //add lijianqiang [INT_32] 20151008:b
    case ObInt32Type:
      ret = (value_.int32_val_ != 0);
      break;
    //add 20151008:e
    case ObDecimalType:
      {
       //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
       // ObNumber dec;
        ObString dec;
        ObDecimal od;

        //bool is_add = false;
        if (OB_SUCCESS == get_decimal(dec))
        {
          if(OB_SUCCESS!=(ret=od.from(dec.ptr(),dec.length()))){
            TBSYS_LOG(ERROR,"failed in covert decimal from buf");
          }
          else if(OB_SUCCESS!=(ret=od.modify_value(get_precision(),get_scale()))){

            TBSYS_LOG(ERROR,"failed in modify decimal value");
          }
          else
          ret = !od.is_zero();
        }
        //modify e
        break;

      }
    case ObFloatType:
      ret = (fabsf(value_.float_val) > FLOAT_EPSINON);
      break;
    case ObDoubleType:
      ret = (fabs(value_.double_val) > DOUBLE_EPSINON);
      break;
    //add peiouya [DATE_TIME] 20150906:b
    case ObDateType:
    case ObTimeType:
    //add 20150906:e
    case ObIntervalType: //add peiouya [DATE_TIME] 20150909
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
      {
        int64_t ts1 = 0;
        get_timestamp(ts1);
        ret = (0 != ts1);
        break;
      }
    default:
      break;
  }
  return ret;
}


int ObObj::get_decimal(ObNumber &num, bool &is_add) const
{
  int ret = OB_OBJ_TYPE_ERROR;
  if (ObDecimalType == meta_.type_)
  {
    ret = OB_SUCCESS;
    is_add = (ADD == meta_.op_flag_);
    int8_t nwords = static_cast<int8_t>(meta_.dec_nwords_ + 1);
    int8_t vscale = meta_.dec_vscale_;
    if (nwords <= 3)
    {
      num.from(vscale, nwords, reinterpret_cast<const uint32_t*>(&val_len_));
    }
    else
    {
      num.from(vscale, nwords, value_.dec_words_);
    }
  }
  return ret;
}


//add zhaoqiong [bugfix::trim rowkey in varchar type in case of memtable checksum mismatch] 20160612:b
int ObObj::get_varchar_trim_length() const
{
  if (meta_.type_ != ObVarcharType)
  {
    return 0;
  }
  else
  {
    int32_t actual_len = val_len_;
    while (actual_len > 0)
    {
      if (value_.varchar_val[actual_len-1] != ' ')
      {
        break;
      }
      actual_len --;
    }
    return actual_len;
  }
}
//add:e

int ObObj::get_decimal(ObNumber &num) const
{
  bool is_add;
  return get_decimal(num, is_add);
}

int ObObj::set_decimal(const ObNumber &num, int8_t precision, int8_t scale, bool is_add /*= false*/)
{
  int ret = OB_SUCCESS;
  set_flag(is_add);
  meta_.type_ = ObDecimalType;
  meta_.dec_precision_ = static_cast<uint8_t>(precision) & META_PREC_MASK;
  meta_.dec_scale_ = static_cast<uint8_t>(scale) & META_SCALE_MASK;
  int8_t nwords = 0;
  int8_t vscale = 0;
  uint32_t words[ObNumber::MAX_NWORDS];
  ret = num.round_to(precision, scale, nwords, vscale, words);
  if (OB_SUCCESS == ret)
  {
    if (nwords <= 3)
    {
      meta_.dec_nwords_ = static_cast<uint8_t>(nwords - 1) & META_NWORDS_MASK;
      meta_.dec_vscale_ = static_cast<uint8_t>(vscale) & META_VSCALE_MASK;
      memcpy(reinterpret_cast<uint32_t*>(&val_len_), words, sizeof(uint32_t)*nwords);
    }
    else
    {
      //@todo, use ob_pool.h to allocate memory
      ret = OB_NOT_IMPLEMENT;
    }
  }
  return ret;
}
//add  wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
int ObObj::get_decimal(ObString& value) const{

   int res = OB_OBJ_TYPE_ERROR;
   if(meta_.type_==ObDecimalType){

    value.assign_ptr(const_cast<char *>(value_.word), val_len_);
    res = OB_SUCCESS;

   }

   return res;
}

int ObObj::set_decimal(const ObString& value){
    int ret=OB_SUCCESS;
    ObDecimal od;
    if(OB_SUCCESS!=(ret=od.from(value.ptr(),value.length()))){

        TBSYS_LOG(WARN, "invalid buffer for string to decimal,buffer =%.*s,",value.length(),value.ptr());
    }
    else
    {
        uint32_t p,s,v;
        p=od.get_precision();
        s=od.get_scale();
        v=od.get_vscale();
        meta_.type_ = ObDecimalType;
        meta_.dec_precision_ = static_cast<uint8_t>(p) & META_PREC_MASK;
        meta_.dec_scale_ = static_cast<uint8_t>(s) & META_SCALE_MASK;
        meta_.dec_vscale_ = static_cast<uint8_t>(v) & META_VSCALE_MASK;
        meta_.op_flag_ = INVALID_OP_FLAG;
        value_.word = value.ptr();
        val_len_ = value.length();
    }
	return ret;
}

    int ObObj::set_decimal(const ObString& value,uint32_t p,uint32_t s,uint32_t v){
     int ret=OB_SUCCESS;
     meta_.type_ = ObDecimalType;
     value_.word = value.ptr();
     val_len_ = value.length();
     meta_.dec_precision_ = static_cast<uint8_t>(p) & META_PREC_MASK;
     meta_.dec_scale_ = static_cast<uint8_t>(s) & META_SCALE_MASK;
     meta_.dec_vscale_ = static_cast<uint8_t>(v) & META_VSCALE_MASK;
     meta_.op_flag_ = INVALID_OP_FLAG;

     return ret;
}


//add e
int ObObj::compare_same_type(const ObObj &other) const
{
  int cmp = 0;
  switch(get_type())
  {
    case ObIntType:
      if (this->value_.int_val < other.value_.int_val)
      {
        cmp = -1;
      }
      else if (this->value_.int_val == other.value_.int_val)
      {
        cmp = 0;
      }
      else
      {
        cmp = 1;
      }
      break;
    //add lijianqiang [INT_32] 20151008:b
    case ObInt32Type:
      if (this->value_.int32_val_ < other.value_.int32_val_)
      {
        cmp = -1;
      }
      else if (this->value_.int32_val_ == other.value_.int32_val_)
      {
        cmp = 0;
      }
      else
      {
        cmp = 1;
      }
      break;
    //add 20151008:e
    case ObDecimalType:
      {
         /*ObNumber n1, n2;
       get_decimal(n1);
        other.get_decimal(n2);
        cmp = n1.compare(n2);
        */
        //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
        ObString str1,str2;
        ObDecimal od1, od2;
        get_decimal(str1);
        other.get_decimal(str2);
        int ret=OB_SUCCESS;

        if(OB_SUCCESS!=(ret=od1.from(str1.ptr(),str1.length()))){
            TBSYS_LOG(WARN,"failed to covert buff to decimal buf=%.*s",str1.length(),str1.ptr());
        }
        else if(OB_SUCCESS!=(ret=od2.from(str2.ptr(),str2.length()))){
            TBSYS_LOG(WARN,"failed to covert buff to decimal buf=%.*s",str2.length(),str2.ptr());
        }
        else if(OB_SUCCESS!=(ret=od1.modify_value(get_precision(),get_scale()))){
            char err1[MAX_PRINTABLE_SIZE];
            memset(err1,0,MAX_PRINTABLE_SIZE);
            od1.to_string(err1,MAX_PRINTABLE_SIZE);
            TBSYS_LOG(WARN,"failed to modify value decimal=%s",err1);
        }
        else if(OB_SUCCESS!=(ret=od2.modify_value(other.get_precision(),other.get_scale())))
        {
            char err2[MAX_PRINTABLE_SIZE];
            memset(err2,0,MAX_PRINTABLE_SIZE);
            od2.to_string(err2,MAX_PRINTABLE_SIZE);
            TBSYS_LOG(WARN,"failed to modify value decimal=%s",err2);
        }

        cmp = od1.compare(od2);
        break;
        //modify e
      }
    case ObVarcharType:
      {
        ObString varchar1, varchar2;
        this->get_varchar(varchar1);
        other.get_varchar(varchar2);
        cmp = varchar1.compare(varchar2);
        break;
      }
    case ObFloatType:
      {
        bool float_eq = fabsf(value_.float_val - other.value_.float_val) < FLOAT_EPSINON;
        if (float_eq)
        {
          cmp = 0;
        }
        else if (this->value_.float_val < other.value_.float_val)
        {
          cmp = -1;
        }
        else
        {
          cmp = 1;
        }
        break;
      }
    case ObDoubleType:
      {
        bool double_eq = fabs(value_.double_val - other.value_.double_val) < DOUBLE_EPSINON;
        if (double_eq)
        {
          cmp = 0;
        }
        else if (this->value_.double_val < other.value_.double_val)
        {
          cmp = -1;
        }
        else
        {
          cmp = 1;
        }
        break;
      }
    //add peiouya [DATE_TIME] 20150906:b
    case ObDateType:
    case ObTimeType:
    //add 20150906:e
    case ObIntervalType: //add peiouya [DATE_TIME] 20150909
    case ObDateTimeType:
    case ObPreciseDateTimeType:
    case ObCreateTimeType:
    case ObModifyTimeType:
      {
        int64_t ts1 = 0;
        int64_t ts2 = 0;
        get_timestamp(ts1);
        other.get_timestamp(ts2);
        if (ts1 < ts2)
        {
          cmp = -1;
        }
        else if (ts1 == ts2)
        {
          cmp = 0;
        }
        else
        {
          cmp = 1;
        }
        break;
      }
    case ObBoolType:
      cmp = this->value_.bool_val - other.value_.bool_val;
      break;
    default:
      TBSYS_LOG(ERROR, "invalid type=%d", get_type());
      break;
  }
  return cmp;
}

int ObObj::compare(const ObObj &other) const
{
  int cmp = 0;
  ObObjType this_type = get_type();
  ObObjType other_type = other.get_type();
  if (!can_compare(other))
  {
    TBSYS_LOG(ERROR, "can not be compared, this_type=%d other_type=%d",
        get_type(), other.get_type());
    cmp = this_type - other_type;
  }
  else
  {

    // compare principle : min value < null type < normal object < max value;
    if (is_min_value())
    {
      // min value == min value and less than any else
      if (other.is_min_value())
      {
        cmp = 0;
      }
      else
      {
        cmp = -1;
      }
    }
    else if (is_max_value())
    {
      // max value == max value and great than any else
      if (other.is_max_value())
      {
        cmp = 0;
      }
      else
      {
        cmp = 1;
      }
    }
    //mod peiouya [NULL_type_compare] 20151102:b
    else if (this_type == ObNullType)
    {
      // null type == null type but less than any else type object.
      // null type > min type
      if (other.is_min_value())
      {
        // null type > min value
        cmp = 1;
      }
      else if (other_type == ObNullType)
      {
        // null type == null type.
        cmp = 0;
      }
      else if (other.is_max_value())
      {
        cmp = -1;
      }
      else
      {
        //// null type < any of normal object type.
        //cmp = -1;
        // null type > any of normal object type.
        cmp = 1;
      }
    }
    else if (other.is_min_value())// || other_type == ObNullType)
    {
      // any of normal type (except null type) > (min value  & null type)
      cmp = 1;
    }
    else if ( other_type == ObNullType)
    {
      cmp = -1;
    }
    //mod:e
    else if (other.is_max_value())
    {
      cmp = -1;
    }
    /*
       else if (this_type == ObNullType || other_type == ObNullType)
       {
       cmp = this_type - other_type;
       }
       */
    else
    {
      // okay finally, two object are normal object type.
      cmp = this->compare_same_type(other);
    }
  }
  return cmp;
}

bool ObObj::operator < (const ObObj &other) const
{
  return this->compare(other) < 0;
}

bool ObObj::operator>(const ObObj &other) const
{
  return this->compare(other) > 0;
}

bool ObObj::operator>=(const ObObj &other) const
{
  return this->compare(other) >= 0;
}

bool ObObj::operator<=(const ObObj &other) const
{
  return this->compare(other) <= 0;
}

bool ObObj::operator==(const ObObj &other) const
{
  return this->compare(other) == 0;
}

bool ObObj::operator!=(const ObObj &other) const
{
  return this->compare(other) != 0;
}

int ObObj::apply(const ObObj &mutation)
{
  int err = OB_SUCCESS;
  int org_type = get_type();
  int org_ext = get_ext();
  int mut_type = mutation.get_type();
  ObCreateTime create_time = 0;
  ObModifyTime modify_time = 0;
  bool is_add = false;
  bool org_is_add = false;
  if (ObSeqType == mut_type
      || ObMinType >= mut_type
      || ObMaxType <= mut_type)
  {
    TBSYS_LOG(WARN,"unsupported type [type:%d]", mut_type);
    err = OB_INVALID_ARGUMENT;
  }
  if (OB_SUCCESS == err
      && ObExtendType != org_type
      && ObNullType != org_type
      && ObExtendType != mut_type
      && ObNullType != mut_type
      && org_type != mut_type)
  {
    TBSYS_LOG(WARN,"type not coincident [this->type:%d,mutation.type:%d]",
        org_type, mut_type);
    err = OB_INVALID_ARGUMENT;
  }
  _ObjValue value, mutation_value;
  if (OB_SUCCESS == err)
  {
    bool ext_val_can_change =  (ObActionFlag::OP_ROW_DOES_NOT_EXIST == org_ext)  ||  (ObNullType == org_type);
    bool org_is_nop = (ObActionFlag::OP_NOP == org_ext);
    switch (mut_type)
    {
      case ObNullType:
        set_null();
        break;
      case ObVarcharType:
        *this = mutation;
        break;
      case ObBoolType:
        *this = mutation;
        break;
      case ObIntType:
        if (ext_val_can_change || org_is_nop)
        {
          set_int(0);
        }
        err = get_int(value.int_val,org_is_add);
        if (OB_SUCCESS == err)
        {
          err = mutation.get_int(mutation_value.int_val,is_add);
        }
        if (OB_SUCCESS == err)
        {
          if (is_add)
          {
            value.int_val += mutation_value.int_val; // @bug value overflow
          }
          else
          {
            value.int_val = mutation_value.int_val;
          }
          set_int(value.int_val, (org_is_add || org_is_nop) && is_add);
        }
        break;
      //add lijianqiang [INT_32] 20151008:b
      case ObInt32Type:
        if (ext_val_can_change || org_is_nop)
        {
          set_int32(0);
        }
        err = get_int32(value.int32_val,org_is_add);
        if (OB_SUCCESS == err)
        {
          err = mutation.get_int32(mutation_value.int32_val,is_add);
        }
        if (OB_SUCCESS == err)
        {
          if (is_add)
          {
            value.int32_val += mutation_value.int32_val; // @bug value overflow
          }
          else
          {
            value.int32_val = mutation_value.int32_val;
          }
          set_int32(value.int32_val, (org_is_add || org_is_nop) && is_add);
        }
        break;
      //add 20151008:e
      case ObFloatType:
        if (ext_val_can_change || org_is_nop)
        {
          set_float(0);
        }
        err = get_float(value.float_val,org_is_add);
        if (OB_SUCCESS == err)
        {
          err = mutation.get_float(mutation_value.float_val, is_add);
        }
        if (OB_SUCCESS == err)
        {
          if (is_add)
          {
            value.float_val += mutation_value.float_val;
          }
          else
          {
            value.float_val = mutation_value.float_val;
          }
          set_float(value.float_val,is_add && (org_is_add || org_is_nop));
        }
        break;
      case ObDoubleType:
        if (ext_val_can_change || org_is_nop)
        {
          set_double(0);
        }
        err = get_double(value.double_val,org_is_add);
        if (OB_SUCCESS == err)
        {
          err = mutation.get_double(mutation_value.double_val,is_add);
        }
        if (OB_SUCCESS == err)
        {
          if (is_add)
          {
            value.double_val += mutation_value.double_val;
          }
          else
          {
            value.double_val = mutation_value.double_val;
          }
          set_double(value.double_val, (org_is_add || org_is_nop) && is_add);
        }
        break;
      case ObDateTimeType:
        if (ext_val_can_change || org_is_nop)
        {
          set_datetime(0);
        }
        err = get_datetime(value.second_val,org_is_add);
        if (OB_SUCCESS == err)
        {
          err = mutation.get_datetime(mutation_value.second_val,is_add);
        }
        if (OB_SUCCESS == err)
        {
          if (is_add)
          {
            value.second_val += mutation_value.second_val;
          }
          else
          {
            value.second_val = mutation_value.second_val;
          }
          set_datetime(value.second_val,is_add && (org_is_add || org_is_nop));
        }
        break;
      //add peiouya [DATE_TIME] 20150906:b
      case ObDateType:
        if (ext_val_can_change || org_is_nop)
        {
          set_date(0);
        }
        err = get_date(value.microsecond_val,org_is_add);
        if (OB_SUCCESS == err)
        {
          err = mutation.get_date(mutation_value.microsecond_val,is_add);
        }
        if (OB_SUCCESS == err)
        {
          if (is_add)
          {
            value.microsecond_val += mutation_value.microsecond_val;
          }
          else
          {
            value.microsecond_val = mutation_value.microsecond_val;
          }
          set_date(value.microsecond_val,is_add && (org_is_add || org_is_nop));
        }
        break;
      case ObTimeType:
        if (ext_val_can_change || org_is_nop)
        {
          set_time(0);
        }
        err = get_time(value.microsecond_val,org_is_add);
        if (OB_SUCCESS == err)
        {
          err = mutation.get_time(mutation_value.microsecond_val,is_add);
        }
        if (OB_SUCCESS == err)
        {
          if (is_add)
          {
            value.microsecond_val += mutation_value.microsecond_val;
          }
          else
          {
            value.microsecond_val = mutation_value.microsecond_val;
          }
          set_time(value.microsecond_val,is_add && (org_is_add || org_is_nop));
        }
        break;
      //add 20150906:e
      case ObPreciseDateTimeType:
        if (ext_val_can_change || org_is_nop)
        {
          set_precise_datetime(0);
        }
        err = get_precise_datetime(value.microsecond_val,org_is_add);
        if (OB_SUCCESS == err)
        {
          err = mutation.get_precise_datetime(mutation_value.microsecond_val,is_add);
        }
        if (OB_SUCCESS == err)
        {
          if (is_add)
          {
            value.microsecond_val += mutation_value.microsecond_val;
          }
          else
          {
            value.microsecond_val = mutation_value.microsecond_val;
          }
          set_precise_datetime(value.microsecond_val,is_add && (org_is_add || org_is_nop));
        }
        break;
      case ObExtendType:
        switch (mutation.get_ext())
        {
          case ObActionFlag::OP_DEL_ROW:
          case ObActionFlag::OP_DEL_TABLE:
            /// used for join, if right row was deleted, set the cell to null
            set_null();
            break;
          case ObActionFlag::OP_ROW_DOES_NOT_EXIST:
            /// do nothing
            break;
          case ObActionFlag::OP_NOP:
            if (org_ext == ObActionFlag::OP_ROW_DOES_NOT_EXIST
                || org_ext == ObActionFlag::OP_DEL_ROW)
            {
              set_null();
            }
            break;
          default:
            TBSYS_LOG(ERROR,"unsupported ext value [value:%ld]", mutation.get_ext());
            err = OB_INVALID_ARGUMENT;
            break;
        }

        break;
      case ObCreateTimeType:
        err = mutation.get_createtime(create_time);
        if (OB_SUCCESS == err)
        {
          if (ext_val_can_change || org_is_nop)
          {
            set_createtime(create_time);
          }
        }
        if (OB_SUCCESS == err)
        {
          err = get_createtime(value.create_time_val);
        }
        if (OB_SUCCESS == err)
        {
          err = mutation.get_createtime(mutation_value.create_time_val);
        }
        if (OB_SUCCESS == err)
        {
          set_createtime(std::min<ObCreateTime>(value.create_time_val,mutation_value.create_time_val));
        }
        break;
      case ObModifyTimeType:
        err = mutation.get_modifytime(modify_time);
        if (OB_SUCCESS == err)
        {
          if (ext_val_can_change || org_is_nop)
          {
            set_modifytime(modify_time);
          }
        }
        if (OB_SUCCESS == err)
        {
          err = get_modifytime(value.modify_time_val);
        }
        if (OB_SUCCESS == err)
        {
          err = mutation.get_modifytime(mutation_value.modify_time_val);
        }
        if (OB_SUCCESS == err)
        {
          set_modifytime(std::max<ObCreateTime>(value.modify_time_val,mutation_value.modify_time_val));
        }
        break;
      case ObDecimalType:
        {
          /*ObNumber num, mutation_num, res;
          if (ext_val_can_change || org_is_nop)
          {
            num.set_zero();
          }
          else
          {
            err = get_decimal(num, org_is_add);
          }
          if (OB_SUCCESS == err)
          {
            err = mutation.get_decimal(mutation_num, is_add);
          }
          if (OB_SUCCESS == err)
          {
            if (is_add)
            {
              err = num.add(mutation_num, res);
            }
            else
            {
              res = mutation_num;
            }
          }
          if (OB_SUCCESS == err)
          {
            set_decimal(res, meta_.dec_precision_, meta_.dec_scale_, (org_is_add || org_is_nop) && is_add);
          }
          */
          //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b

          *this=mutation;
            break;
          //modify e
        }
      default:
        /* case ObSeqType: */
        TBSYS_LOG(ERROR,"unsupported type [type:%d]", mut_type);
        err = OB_INVALID_ARGUMENT;
        break;
    }
  }
  if(OB_SUCCESS != err)
  {
    TBSYS_LOG(WARN,"fail to apply [this->type:%d,this->ext:%d,"
        "mutation->type:%d,mutation->ext:%ld, err:%d]",
        org_type, org_ext, mut_type, mutation.get_ext(), err);
  }
  return err;
}
//add dolphin[in post-expression optimization]@20151121:b
int ObObj::obj_copy(ObObj& nobj) const
{
    int ret = OB_SUCCESS;
    ObObjType type = get_type();
    if(ObMinType >= type || ObMaxType <= type)
    {
        TBSYS_LOG(WARN,"unsupported obobj copy type [type:%d]", type);
        ret = OB_INVALID_ARGUMENT;
    }
    switch(type)
    {
        case ObNullType:
            nobj.set_null();
            break;
        case ObIntType:
        {
            int64_t v = 0;
            get_int(v);
            nobj.set_int(v);
            break;
        }

        case ObFloatType:
        {
            float f = 0;
            get_float(f);
            nobj.set_float(f);
            break;
        }

        case ObDoubleType:
        {
            double d =0;
            get_double(d);
            nobj.set_double(d);
            break;
        }
        case ObDateTimeType:
        {
            ObDateTime dt =0;
            get_datetime(dt);
            nobj.set_datetime(dt);
            break;
        }
        case ObPreciseDateTimeType:
        {
            ObPreciseDateTime pdt=0;
            get_precise_datetime(pdt);
            nobj.set_precise_datetime(pdt);
            break;
        }
        case ObVarcharType:
        {
            ObString str1;
            get_varchar(str1);
            nobj.set_varchar(str1);
            break;
        }

        case ObSeqType:
            nobj.set_seq();
            break;
        case ObCreateTimeType:
        {
            ObCreateTime ct=0;
            get_createtime(ct);
            nobj.set_createtime(ct);
            break;
        }
        case ObModifyTimeType:
        {
            ObModifyTime mt=0;
            get_modifytime(mt);
            nobj.set_modifytime(mt);
            break;
        }
        case ObExtendType:
        {
            int64_t et=0;
            get_ext(et);
            nobj.set_ext(et);
            break;
        }
        case ObBoolType:
        {
            bool bt= false;
            get_bool(bt);
            nobj.set_bool(bt);
            break;
        }
        case ObDecimalType:
        {
            ObString ds ;
            get_decimal(ds);
            nobj.set_decimal(ds);
            break;
        }

        case ObDateType:
        {
            ObDate od=0;
            get_date(od);
            nobj.set_date(od);
            break;
        }
        case ObTimeType:
        {
            ObTime ot=0;
            get_time(ot);
            nobj.set_time(ot);
            break;
        }
        case ObIntervalType:
        {
            ObInterval iv=0;
            get_interval(iv);
            nobj.set_interval(iv);
            break;
        }
        case ObInt32Type:
        {
            int32_t value=0;
            get_int32(value);
            nobj.set_int32(value);
            break;
        }
        default:
            TBSYS_LOG(ERROR,"unsupported obobj copy type [type:%d]", type);
            ret = OB_INVALID_ARGUMENT;
            break;
    }
    return ret;
}
//add:e
void ObObj::dump(const int32_t log_level /*=TBSYS_LOG_LEVEL_DEBUG*/) const
{
  int64_t int_val = 0;
  //add lijianqiang [INT_32] 20151008:b
  int32_t int32_val = 0;
  //add 20151008:e
  bool bool_val = false;
  bool is_add = false;
  float float_val = 0.0f;
  double double_val = 0.0f;
  ObString str_val;
  //ObNumber num;
  //add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
   ObString bstring;
   ObDecimal od;
  //add e
  char num_buf[ObNumber::MAX_PRINTABLE_SIZE];
  switch (get_type())
  {
    case ObNullType:
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level), "[%ld] type:ObNull",pthread_self());
      break;
    case ObIntType:
      get_int(int_val,is_add);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObInt, val:%ld,is_add:%s",pthread_self(),int_val,is_add ? "true" : "false");
      break;
      //add lijianqiang [INT_32] 20151008:b
    case ObInt32Type:
      get_int32(int32_val,is_add);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObInt32, val:%d,is_add:%s",pthread_self(),int32_val,is_add ? "true" : "false");
      break;
      //add 20151008:e
    case ObVarcharType:
      get_varchar(str_val);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObVarChar,len :%d,val:",pthread_self(),str_val.length());
      common::hex_dump(str_val.ptr(),str_val.length(),true,log_level);
      break;
    case ObFloatType:
      get_float(float_val,is_add);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObFloat, val:%f,is_add:%s",pthread_self(),float_val,is_add ? "true" : "false");
      break;
    case ObDoubleType:
      get_double(double_val,is_add);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObDouble, val:%f,is_add:%s",pthread_self(),double_val,is_add ? "true" : "false");
      break;
    case ObDateTimeType:
      get_datetime(int_val,is_add);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObDateTime(seconds), val:%ld,is_add:%s",pthread_self(),int_val,is_add ? "true" : "false");
      break;
    //add peiouya [DATE_TIME] 20150906:b
    case ObDateType:
      get_date(int_val,is_add);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObDate(microseconds), val:%ld,is_add:%s",pthread_self(),int_val,is_add ? "true" : "false");
      break;
    case ObTimeType:
      get_time(int_val,is_add);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObTime(microseconds), val:%ld,is_add:%s",pthread_self(),int_val,is_add ? "true" : "false");
      break;
    //add 20150906:e
    //add peiouya [DATE_TIME] 20150909:b
    case ObIntervalType:
      get_interval(int_val,is_add);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObTime(microseconds), val:%ld,is_add:%s",pthread_self(),int_val,is_add ? "true" : "false");
      break;
    //add 20150909:e
    case ObPreciseDateTimeType:
      get_precise_datetime(int_val,is_add);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObPreciseDateTime(microseconds), val:%ld,is_add:%s",pthread_self(),int_val,is_add ? "true" : "false");
      break;
    case ObSeqType:
      //TODO
      break;
    case ObCreateTimeType:
      get_createtime(int_val);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObCreateTime, val:%ld",pthread_self(),int_val);
      break;
    case ObModifyTimeType:
      get_modifytime(int_val);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObModifyTime, val:%ld",pthread_self(),int_val);
      break;
    case ObBoolType:
      get_bool(bool_val);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObBool, val:%s",pthread_self(),bool_val?"true":"false");
      break;
    case ObExtendType:
      get_ext(int_val);
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObExt, val:%ld",pthread_self(),int_val);
      break;
    case ObDecimalType:
       /*get_decimal(num, is_add);
      num.to_string(num_buf, ObNumber::MAX_PRINTABLE_SIZE);
      */
       //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
      get_decimal(bstring);
      if(OB_SUCCESS!=od.from(bstring.ptr(),bstring.length())){
          TBSYS_LOG(ERROR,"failed to convert decimal from buff!");
      }
      else{
      od.set_precision(get_precision());
      od.set_scale(get_scale());
      od.to_string(num_buf, MAX_PRINTABLE_SIZE);
      }
      //modify e
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level),
          "[%ld] type:ObDecimalType, val:%s, is_add:%s",
          pthread_self(), num_buf, is_add ? "true" : "false");
      break;
    default:
      TBSYS_LOGGER.logMessage(TBSYS_LOG_NUM_LEVEL(log_level)," [%ld] unexpected type (%d)",pthread_self(),get_type());
      break;
  }
}


void ObObj::print_value(FILE* fd)
{
  switch (get_type())
  {
    case ObNullType:
      fprintf(fd, "nil");
      break;
    case ObIntType:
      fprintf(fd, "%ld", value_.int_val);
      break;
    //add lijianqiang [INT_32] 20151008:b
    case ObInt32Type:
      fprintf(fd, "%d", value_.int32_val_);
      break;
    //add 20151008:e
    case ObVarcharType:
      fprintf(fd, "%.*s", val_len_, value_.varchar_val);
      break;
    case ObFloatType:
      fprintf(fd, "%2f", value_.float_val);
      break;
    case ObDoubleType:
      fprintf(fd, "%2lf", value_.double_val);
      break;
    case ObDateTimeType:
      fprintf(fd, "%s", time2str(value_.time_val));
      break;
    //add peiouya [DATE_TIME] 20150906:b
    case ObDateType:
    //case ObTimeType:
    //add 20150906:e
    case ObPreciseDateTimeType:
      fprintf(fd, "%s", time2str(value_.precisetime_val));
      break;
    //add peiouya [DATE_TIME] 20150909:b
    case ObTimeType:
    case ObIntervalType:
      fprintf(fd, "%ld", value_.precisetime_val);
      break;
    //add 20150909:e
    case ObModifyTimeType:
      fprintf(fd, "%s", time2str(value_.modifytime_val));
      break;
    case ObCreateTimeType:
      fprintf(fd, "%s", time2str(value_.createtime_val));
      break;
    case ObSeqType:
      fprintf(fd, "seq");
      break;
    case ObExtendType:
      fprintf(fd, "%lde", value_.ext_val);
      break;
    case ObBoolType:
      fprintf(fd, "%c", value_.bool_val ? 'Y': 'N');
      break;
    case ObDecimalType:
      {
        /*char num_buf[ObNumber::MAX_PRINTABLE_SIZE];
        ObNumber num;
        get_decimal(num);
        num.to_string(num_buf, ObNumber::MAX_PRINTABLE_SIZE);
        fprintf(fd, "%s", num_buf);
        */
        //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
        ObString num;
        get_decimal(num);
        fprintf(fd, "%*s", num.length(),num.ptr());
        //modify e
        break;
      }
    default:
      break;
  }
}

const char* ObObj::get_sql_type(ObObjType type)
{
  const char* sql_type = NULL;
  static const char* sql_type_name[] =
  {
    "NULL",
    "INT",
    "FLOAT",
    "DOUBLE",
    "DATETIME",
    "DATETIME",
    "VARCHAR",
    "SEQENCE",
    "CREATETIME",
    "MODIFYTIME",
    "INT",
    "BOOL",
    "DECIMAL",
    //add peiouya [DATE_TIME] 20150906:b
    "date",
    "time",
    //add 20150906:e
    "interval", //add peiouya [DATE_TIME] 20150909
    //add lijianqiang [INT_32] 20151008:b
    "INT32",
    //add 20151008:e
    ""
  };
  if (type > ObMinType && type < ObMaxType)
  {
    sql_type = sql_type_name[type];
  }
  else
  {
    sql_type = sql_type_name[ObMaxType];
  }
  return sql_type;
}
//add gaojt [ListAgg][JHOBv0.1]20150104:b
int64_t ObObj::to_string_v2(std::string &s) const
{
  int64_t int_val = 0;
  //add lijianqiang [INT_32] 20151008:b
  int32_t int32_val = 0;
  //add 20151008:e
  float float_val = 0.0;
  double double_val = 0.0;
  bool is_add = false;
  bool bool_val = false;
  ObString str_val;
  std::stringstream temp_result;
  if(meta_.type_ == ObVarcharType)
  {
      get_varchar(str_val);
      char result[str_val.length()+1];
      snprintf(result,str_val.length()+1,"%.*s",str_val.length()+1, str_val.ptr());
      temp_result<<result;
      s = temp_result.str();
  }
  else
  {
      switch(meta_.type_)
      {
      case ObNullType:
          break;
      case ObIntType:
          get_int(int_val,is_add);
          temp_result<<int_val;
          s = temp_result.str();
          break;
      //add lijianqiang [INT_32] 20151008:b
      case ObInt32Type:
          get_int32(int32_val,is_add);
          temp_result<<int32_val;
          s = temp_result.str();
          break;
      //add 20151008:e
      case ObFloatType:
          get_float(float_val,is_add);
          temp_result<<float_val;
          s = temp_result.str();
          break;
      case ObDoubleType:
          get_double(double_val,is_add);
          temp_result<<double_val;
          s = temp_result.str();
          break;
      case ObDateTimeType:
          get_datetime(int_val,is_add);
          temp_result<<int_val;
          s = temp_result.str();
          break;
      //add peiouya [DATE_TIME] 20150906:b
      case ObDateType:
          get_date(int_val,is_add);
          temp_result<<int_val;
          s = temp_result.str();
          break;
      case ObTimeType:
          get_time(int_val,is_add);
          temp_result<<int_val;
          s = temp_result.str();
          break;
      //add 20150906:e
      case ObPreciseDateTimeType:
          get_precise_datetime(int_val,is_add);
          temp_result<<int_val;
          s = temp_result.str();
          break;
      case ObSeqType:
          //TODO
          break;
      case ObCreateTimeType:
          get_createtime(int_val);
          temp_result<<int_val;
          s = temp_result.str();
          break;
      case ObModifyTimeType:
          get_modifytime(int_val);
          temp_result<<int_val;
          s = temp_result.str();
          break;
      case ObExtendType:
          get_ext(int_val);
          temp_result<<int_val;
          s = temp_result.str();
          break;
      case ObBoolType:
          get_bool(bool_val);
          temp_result<<bool_val;
          s = temp_result.str();
          break;
      case ObDecimalType:
      {
			ObString num;
			get_decimal(num);
			ObDecimal od;
			char res[MAX_PRINTABLE_SIZE];
			memset(res,0,MAX_PRINTABLE_SIZE);
			if(OB_SUCCESS!=od.from(num.ptr(),num.length())){
				TBSYS_LOG(WARN,"failed to convert decimal from buf");
			}
			else if (OB_SUCCESS!=od.modify_value(get_precision(),get_scale())){
				TBSYS_LOG(WARN,"failed to modify decimal");
			}
			else{
				od.to_string(res,MAX_PRINTABLE_SIZE);
			}
			temp_result<<res;
			s = temp_result.str();
			//modify e
			break;
      }
      default:
          break;
      }
  }
  return 0;
}
//add 20150104:e
int64_t ObObj::to_string(char* buffer, const int64_t length) const
{
  static const char* obj_type_name[] =
  {
    "null",
    "int",
    "float",
    "double",
    "datetime",
    "precisedatetime",
    "varchar",
    "seq",
    "createtime",
    "modifytime",
    "extend",
    "bool",
    "decimal",
    //add peiouya [DATE_TIME] 20150906:b
    "date",
    "time",
    //add 20150906:e
    "interval", //add peiouya [DATE_TIME] 20150909
    //add lijianqiang [INT_32] 20151008:b
    "int32"
    //add 20151008:e
  };

  int64_t int_val = 0;
  //add lijianqiang [INT_32] 20151008:b
  int32_t int32_val = 0;
  //add 20151008:e
  float float_val = 0.0;
  double double_val = 0.0;
  bool is_add = false;
  ObString str_val;
  int32_t type = meta_.type_;
  int64_t pos = 0;

  if (type > ObMinType && type < ObMaxType)
  {
    databuff_printf(buffer, length, pos, "%s:", obj_type_name[meta_.type_]);
  }
  else
  {
    databuff_printf(buffer, length, pos, "%s", "unknown");
  }

  {
    switch(meta_.type_)
    {
      case ObNullType:
        break;
      case ObIntType:
        get_int(int_val,is_add);
        databuff_printf(buffer, length, pos,  "%s%ld",  is_add ? "+" : "", int_val);
        break;
      //add lijianqiang [INT_32] 20151008:b
      case ObInt32Type:
        get_int32(int32_val,is_add);
        databuff_printf(buffer, length, pos,  "%s%d",  is_add ? "+" : "", int32_val);
        break;
      //add 20151008:e
      case ObVarcharType:
        get_varchar(str_val);
        //mod Yukun [delete ' add by zhujun] 20160728:b
        //databuff_printf(buffer, length, pos, "'%.*s'", str_val.length(), str_val.ptr());
        //mod 20160620:e
        //mod zhujun [CDC support] 20160620:b
        databuff_printf(buffer, length, pos, "%.*s", str_val.length(), str_val.ptr());
        //mod 20160728:e
        break;
      case ObFloatType:
        get_float(float_val,is_add);
        databuff_printf(buffer, length, pos, "%s%f",  is_add ? "+" : "", float_val);
        break;
      case ObDoubleType:
        get_double(double_val,is_add);
        databuff_printf(buffer, length, pos, "%s%.12lf",  is_add ? "+" : "", double_val);
        break;
      case ObDateTimeType:
        get_datetime(int_val,is_add);
        databuff_printf(buffer, length, pos, "%s%ld",  is_add ? "+" : "", int_val);
        break;
      //add peiouya [DATE_TIME] 20150906:b
      case ObDateType:
        get_date(int_val,is_add);
        databuff_printf(buffer, length, pos, "%s%ld",  is_add ? "+" : "", int_val);
        break;
      case ObTimeType:
        get_time(int_val,is_add);
        databuff_printf(buffer, length, pos, "%s%ld",  is_add ? "+" : "", int_val);
        break;
      //add 20150906:e
      //add peiouya [DATE_TIME] 20150909
      case ObIntervalType:
        get_interval(int_val,is_add);
        databuff_printf(buffer, length, pos, "%s%ld",  is_add ? "+" : "", int_val);
        break;
      //add 20150909:e
      case ObPreciseDateTimeType:
        get_precise_datetime(int_val,is_add);
        databuff_printf(buffer, length, pos, "%s%ld",  is_add ? "+" : "", int_val);
        break;
      case ObSeqType:
        //TODO
        break;
      case ObCreateTimeType:
        get_createtime(int_val);
        databuff_printf(buffer, length, pos, "%ld", int_val);
        break;
      case ObModifyTimeType:
        get_modifytime(int_val);
        databuff_printf(buffer, length, pos, "%ld", int_val);
        break;
      case ObExtendType:
        get_ext(int_val);
        if (MIN_OBJECT_VALUE == int_val)
        {
          databuff_printf(buffer, length, pos, "min");
        }
        else if (MAX_OBJECT_VALUE == int_val)
        {
          databuff_printf(buffer, length, pos, "max");
        }
        else
        {
          databuff_printf(buffer, length, pos, "%ld", int_val);
        }
        break;
      case ObBoolType:
        databuff_printf(buffer, length, pos, "%c", value_.bool_val ? 'Y': 'N');
        break;
       case ObDecimalType:
        {
       /*char num_buf[ObNumber::MAX_PRINTABLE_SIZE];
        ObNumber num;
        get_decimal(num);
        num.to_string(num_buf, ObNumber::MAX_PRINTABLE_SIZE);
        databuff_printf(buffer, length, pos, "%s", num_buf);
        */
        //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
        ObString num;
        get_decimal(num);
        ObDecimal od;
        char res[MAX_PRINTABLE_SIZE];
        memset(res,0,MAX_PRINTABLE_SIZE);
        if(OB_SUCCESS!=od.from(num.ptr(),num.length())){
            TBSYS_LOG(WARN,"failed to convert decimal from buf");
        }
        else if (OB_SUCCESS!=od.modify_value(get_precision(),get_scale())){
            TBSYS_LOG(WARN,"failed to modify decimal");
        }
        else{
            od.to_string(res,MAX_PRINTABLE_SIZE);
        }
        databuff_printf(buffer, length, pos, "%s",res);
        //modify e
        break;
      }
      default:
        break;
    }
  }
  return pos;
}

DEFINE_SERIALIZE(ObObj)
{
  ObObjType type = get_type();
  int ret = 0;
  int64_t tmp_pos = pos;
  int8_t obj_op_flag = meta_.op_flag_;

  if (OB_SUCCESS == ret)
  {
    switch (type)
    {
      case ObNullType:
        ret = serialization::encode_null(buf,buf_len,tmp_pos);
        break;
      case ObIntType:
        ret = serialization::encode_int(buf,buf_len,tmp_pos,value_.int_val,obj_op_flag == ADD);
        break;
      case ObVarcharType:
        ret = serialization::encode_str(buf,buf_len,tmp_pos,value_.varchar_val,val_len_);
        break;
      case ObFloatType:
        ret = serialization::encode_float_type(buf,buf_len,tmp_pos,value_.float_val,obj_op_flag == ADD);
        break;
      case ObDoubleType:
        ret = serialization::encode_double_type(buf,buf_len,tmp_pos,value_.double_val,obj_op_flag == ADD);
        break;
      case ObDateTimeType:
        ret = serialization::encode_datetime_type(buf,buf_len,tmp_pos,value_.time_val,obj_op_flag == ADD);
        break;
      //add peiouya [DATE_TIME] 20150906:b
      case ObDateType:
        ret = serialization::encode_date_type(buf,buf_len,tmp_pos,value_.precisetime_val,obj_op_flag == ADD);
        break;
      case ObTimeType:
        ret = serialization::encode_time_type(buf,buf_len,tmp_pos,value_.precisetime_val,obj_op_flag == ADD);
        break;
      //add 20150906:e
      //add peiouya [DATE_TIME] 20150909:b
      case ObIntervalType:
        ret = serialization::encode_interval_type(buf,buf_len,tmp_pos,value_.precisetime_val,obj_op_flag == ADD);
        break;
      //add 20150909:e
      //add lijianqiang [INT_32] 20150929:b
      case ObInt32Type:
        ret = serialization::encode_int32_type(buf,buf_len,tmp_pos,value_.int32_val_,obj_op_flag == ADD);
        break;
      //add 20150929:e
      case ObPreciseDateTimeType:
        ret = serialization::encode_precise_datetime_type(buf,buf_len,tmp_pos,value_.precisetime_val,obj_op_flag == ADD);
        break;
      case ObModifyTimeType:
        ret = serialization::encode_modifytime_type(buf,buf_len,tmp_pos,value_.modifytime_val);
        break;
      case ObCreateTimeType:
        ret = serialization::encode_createtime_type(buf,buf_len,tmp_pos,value_.createtime_val);
        break;
      case ObSeqType:
        //TODO
        break;
      case ObExtendType:
        ret = serialization::encode_extend_type(buf,buf_len,tmp_pos,value_.ext_val);
        break;
      case ObBoolType:
        ret = serialization::encode_bool_type(buf, buf_len, tmp_pos, value_.bool_val);
        break;
      case ObDecimalType:
        /*
        if (meta_.dec_nwords_ + 1 <= 3)
        {
          ret = serialization::encode_decimal_type(buf, buf_len, tmp_pos, obj_op_flag == ADD, meta_.dec_precision_,
              meta_.dec_scale_, meta_.dec_vscale_,
              static_cast<int8_t>(meta_.dec_nwords_ + 1),
              reinterpret_cast<const uint32_t*>(&val_len_));
        }
        else
        {
          ret = serialization::encode_decimal_type(buf, buf_len, tmp_pos, obj_op_flag == ADD, meta_.dec_precision_,
              meta_.dec_scale_, meta_.dec_vscale_, static_cast<int8_t>(meta_.dec_nwords_ + 1),
              value_.dec_words_);
        }
        //old code
        */
        //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
         ret=serialization::encode_comm_decimal(buf,buf_len,tmp_pos,obj_op_flag == ADD,meta_.dec_precision_,
             meta_.dec_scale_, meta_.dec_vscale_,value_.word,val_len_);

        //modify e
        break;
      default:
        TBSYS_LOG(ERROR, "invalid obj_type=%d", type);
        ret = OB_ERR_UNEXPECTED;
        break;
    }
  }
  if (OB_SUCCESS == ret)
    pos = tmp_pos;
  return ret;
}

DEFINE_DESERIALIZE(ObObj)
{
  int ret = OB_SUCCESS;
  int64_t tmp_pos = pos;
  int8_t first_byte = 0;
  bool is_add = false;

  //better reset
  if (OB_SUCCESS == ret)
  {
    reset();
  }

  if (OB_SUCCESS == (ret = serialization::decode_i8(buf,data_len,tmp_pos,&first_byte)))
  {
    if ( serialization::OB_EXTEND_TYPE == first_byte )  // is extend type
    {
      meta_.type_ = ObExtendType;
      ret = serialization::decode_vi64(buf,data_len,tmp_pos,&value_.ext_val);
    }
    else
    {
      int8_t type = static_cast<int8_t>((first_byte & 0xc0) >> 6);
      switch (type)
      {
        case 0:
        case 1: //int
          meta_.type_ = ObIntType;
          ret = serialization::decode_int(buf,data_len,first_byte,tmp_pos,value_.int_val,is_add);
          break;
        case 2: //str
          meta_.type_ = ObVarcharType;
          value_.varchar_val = serialization::decode_str(buf,data_len,first_byte,tmp_pos,val_len_);
          if (NULL == value_.varchar_val)
          {
            ret = OB_ERROR;
          }
          break;
        case 3: //other
          {
            int8_t  sub_type = static_cast<int8_t>((first_byte & 0x30) >> 4); //00 11 00 00
            switch (sub_type)
            {
              case 0: //TODO seq & reserved
                break;
              case 1: //ObDatetime
                meta_.type_ = ObDateTimeType;
                ret = serialization::decode_datetime_type(buf,data_len,first_byte,tmp_pos,value_.time_val,is_add);
                break;
              case 2: //ObPreciseDateTime
                //mod peiouya [DATE_TIME] 20150906:b
                //meta_.type_ = ObIntervalType;
                //ret = serialization::decode_interval_type(buf,data_len,first_byte,second_byte,tmp_pos,value_.precisetime_val,is_add);
                {
                  int8_t  sub_type = static_cast<int8_t>(first_byte & 0xe3);
                  if (serialization::OB_PRECISE_DATETIME_TYPE_EX == sub_type)
                  {
                    int8_t second_byte = 0;
                    if (OB_SUCCESS != (ret = serialization::decode_i8(buf,data_len,tmp_pos,&second_byte)))
                    {
                    }
                    else
                    {
                      int8_t  sub_type = static_cast<int8_t>((second_byte & 0xc0) >> 6); //00 11 00 00
                      switch (sub_type)
                      {
                        case 0:  //ObDate
                        {
                          meta_.type_ = ObDateType;
                          ret = serialization::decode_date_type(buf,data_len,first_byte,second_byte,tmp_pos,value_.precisetime_val,is_add);
                          break;
                        }
                        case 1:  //ObTime
                        {
                          meta_.type_ = ObTimeType;
                          ret = serialization::decode_time_type(buf,data_len,first_byte,second_byte,tmp_pos,value_.precisetime_val,is_add);
                          break;
                        }
                        //add peiouya [DATE_TIME] 20150909:b
                        case 2:  //ObIntervalType
                        {
                          meta_.type_ = ObIntervalType;
                          ret = serialization::decode_interval_type(buf,data_len,first_byte,second_byte,tmp_pos,value_.precisetime_val,is_add);
                          break;
                        }
                        //add 20150909:e
                        //add lijianqiang [INT_32] 20150929:b
                        case 3://other
                        {
                          int8_t  sub_type = static_cast<int8_t>((second_byte & 0x30) >> 4);//0011 0000
                          //we add the int32 type here,we use the second byte :11 00 00 00(0xc0)
                          switch (sub_type)
                          {
                            case 0:
                              meta_.type_ = ObInt32Type;
                              ret = serialization::decode_int32_type(buf,data_len,first_byte,tmp_pos,value_.int32_val_,is_add);
                              break;
                            default:
                              TBSYS_LOG(ERROR, "invalid obj_type=%d", sub_type);
                              ret = OB_ERR_UNEXPECTED;
                              break;
                          }
                          break;
                        }
                        //add 20150929:e
                        default:
                          ret = OB_ERR_UNEXPECTED;
                          break;
                      }
                    }
                  }
                  else
                  {
                    meta_.type_ = ObPreciseDateTimeType;
                    ret = serialization::decode_precise_datetime_type(buf,data_len,first_byte,tmp_pos,value_.precisetime_val,is_add);
                  }
                  break;
                }
                //mod 20150906:e
              case 3: //other
                {
                  int8_t sub_sub_type = static_cast<int8_t>((first_byte & 0x0c) >> 2); // 00 00 11 00
                  switch (sub_sub_type)
                  {
                    case 0: //ObModifyTime
                      meta_.type_ = ObModifyTimeType;
                      ret = serialization::decode_modifytime_type(buf,data_len,first_byte,tmp_pos,value_.modifytime_val);
                      break;
                    case 1: //ObCreateTime
                      meta_.type_ = ObCreateTimeType;
                      ret = serialization::decode_createtime_type(buf,data_len,first_byte,tmp_pos,value_.createtime_val);
                      break;
                    case 2:
                      if (first_byte & 0x02) //ObDouble
                      {
                        meta_.type_ = ObDoubleType;
                        ret = serialization::decode_double_type(buf,data_len,first_byte,tmp_pos,value_.double_val,is_add);
                      }
                      else //ObFloat
                      {
                        meta_.type_ = ObFloatType;
                        ret = serialization::decode_float_type(buf,data_len,first_byte,tmp_pos,value_.float_val,is_add);
                      }
                      break;
                    case 3: //Other
                      {

                        int8_t sub_sub_sub_type = first_byte & 0x03;
                        switch (sub_sub_sub_type)
                        {
                          case 0:
                            meta_.type_ = ObNullType;
                            break;
                          case 1:
                            meta_.type_ = ObBoolType;
                            ret = serialization::decode_bool_type(buf,data_len,first_byte,tmp_pos,value_.bool_val);
                            break;
                          case 3: //obdecimaltype
                            {
                              /*
                              meta_.type_ = ObDecimalType;
                              uint32_t words[ObNumber::MAX_NWORDS];
                              int8_t p = 0;
                              int8_t s = 0;
                              int8_t vs = 0;
                              int8_t n = 0;
                              ret = serialization::decode_decimal_type(buf, data_len, tmp_pos, is_add, p, s, vs, n, words);
                              if(OB_SUCCESS == ret)
                              {
                                meta_.dec_precision_ = static_cast<uint8_t>(p) & META_PREC_MASK;
                                meta_.dec_scale_ = static_cast<uint8_t>(s) & META_SCALE_MASK;
                                meta_.dec_vscale_ = static_cast<uint8_t>(vs) & META_VSCALE_MASK;
                                meta_.dec_nwords_ = static_cast<uint8_t>(n - 1) & META_NWORDS_MASK;
                                if (n <= 3)
                                {
                                  memcpy(reinterpret_cast<char*>(&val_len_), words, n * sizeof(uint32_t));
                                }
                                else
                                {
                                  //@todo
                                  ret = OB_NOT_IMPLEMENT;
                                }
                              }
                              //old code
                              */
                              //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
                              meta_.type_ = ObDecimalType;
                              char *o_ptr=NULL;
                              int8_t p = 0;
                              int8_t s = 0;
                              int8_t vs = 0;
                              int32_t l=0;
                              ret=serialization::decode_comm_decimal(buf,data_len,tmp_pos,is_add,p,s,vs,o_ptr,l);
                              meta_.dec_precision_ = static_cast<uint8_t>(p) & META_PREC_MASK;
                              meta_.dec_scale_ = static_cast<uint8_t>(s) & META_SCALE_MASK;
                              meta_.dec_vscale_ = static_cast<uint8_t>(vs) & META_VSCALE_MASK;
                              val_len_=l;
                              if(NULL==o_ptr){
                                ret=OB_ERROR;
                                TBSYS_LOG(ERROR,"decode_comm_decimal error, the ob_string for decimal is NULL!");
                              }
                              if(ret==OB_SUCCESS)value_.word=o_ptr;
                              //modify e
                              break;
                            }
                          default:
                            TBSYS_LOG(ERROR, "invalid obj_type=%d", sub_sub_sub_type);
                            ret = OB_ERR_UNEXPECTED;
                            break;
                        }
                        break;
                      }
                    default:
                      TBSYS_LOG(ERROR, "invalid obj_type=%d", sub_sub_type);
                      ret = OB_ERR_UNEXPECTED;
                      break;
                  }
                  break;
                }
              default:
                TBSYS_LOG(ERROR, "invalid obj_type=%d", sub_type);
                ret = OB_ERR_UNEXPECTED;
                break;
            }
          }
          break;
        default:
          TBSYS_LOG(ERROR, "invalid obj_type=%d", type);
          ret = OB_ERR_UNEXPECTED;
          break;
      }
      //
      if (is_add)
      {
        meta_.op_flag_ = ADD;
      }
      else
      {
        meta_.op_flag_ = INVALID_OP_FLAG;
      }
    }
    if (OB_SUCCESS == ret)
      pos = tmp_pos;
  }
  return ret;
}
/**Name:from_hash
*input: buf,buf_len
*function:This function is only for obj hash,which used by row_key index hash for get method
**/
//add wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_14:b
int ObObj::from_hash(TTCInt &tc,const char* buff, int64_t buf_len)const{

int ret = OB_SUCCESS;
	char int_buf[MAX_PRINTABLE_SIZE];
	char frac_buf[MAX_PRINTABLE_SIZE];
	memset(int_buf, 0, MAX_PRINTABLE_SIZE);
	memset(frac_buf, 0, MAX_PRINTABLE_SIZE);
	const char* s;
	int got_dot = 0;
	int got_digit = 0;
	int got_frac = 0;
	int got_num=0;
	int i = 0;
	int op = 0;
	int length = 0;
	bool is_valid=false;
	short int sign;
	if(buff==NULL){
        ret=OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR, "failed to convert decimal to string buff,err=NULL buff ptr!");
	}
	if(buf_len>(int)MAX_PRINTABLE_SIZE)
    {
        ret=OB_ERR_UNEXPECTED;
        TBSYS_LOG(ERROR,"failed to convert decimal from string buff for hash,since buf_len is too long ");
    }
	if(OB_SUCCESS==ret){
        s = buff;
        sign = *s == '-' ? 1 : 0;
        if(buf_len==1&&!isdigit(*s)){
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "failed to convert char to decimal,invalid num=%c", *s);
        }
        else if(buf_len==2&&((*s == '-' || *s == '+')&&!isdigit(*(s+1)))){
            ret = OB_ERR_UNEXPECTED;
            TBSYS_LOG(ERROR, "failed to convert char to decimal,invalid num=%c", *s);
        };
        if(OB_SUCCESS==ret){
        if (*s == '-' || *s == '+') {
		op = 1;
		i++;
		++s;
        }

        for (;; ++s) {

		if (i == buf_len)
			break;

		if (isdigit(*s)) {
            if(*s!='0'||got_dot)is_valid=true;
			if(is_valid)got_digit++;
			got_num++;
			if (got_dot)
				got_frac++;
		} else {
			if (!got_dot && *s == '.')
				got_dot = 1;
			else if (*s != '\0') {

				ret = OB_ERR_UNEXPECTED;
				TBSYS_LOG(ERROR, "failed to convert char to decimal,invalid num=%c", *s);
			}
		}
		i++;

            }
        }
	}
    if (got_digit > MAX_DECIMAL_DIGIT || got_frac > MAX_DECIMAL_SCALE) {
		   ret = OB_DECIMAL_UNLEGAL_ERROR;
		   TBSYS_LOG(ERROR, "decimal overflow!got_digit=%d,got_frac=%d", got_digit,got_frac);
	}
	if(OB_SUCCESS==ret){
        TTCInt whole;
        length = got_num + got_dot + op;
	  if (!got_dot) {
		memcpy(int_buf, buff, buf_len);
		whole.FromString(int_buf);
        whole = whole * kMaxScaleFactor;
        //word[0] = whole;
	  } else {
	      int point_pos = length - got_frac;
          memcpy(int_buf, buff, point_pos);
         // TTCInt whole;
          whole.FromString(int_buf);
          whole = whole * kMaxScaleFactor;
          memcpy(frac_buf, buff + (point_pos), got_frac);
	      TTCInt p(MAX_DECIMAL_SCALE-got_frac);
          TTCInt BASE(10);
          TTCInt part_frac;
          part_frac.FromString(frac_buf);
          BASE.Pow(p);
          part_frac=part_frac*BASE;
          whole=whole+part_frac;

	  }
          tc=whole;

	}

	return ret;

}
//add e
DEFINE_GET_SERIALIZE_SIZE(ObObj)
{
  ObObjType type = get_type();
  int64_t len = 0;

  switch (type)
  {
    case ObNullType:
      len += serialization::encoded_length_null();
      break;
    case ObIntType:
      len += serialization::encoded_length_int(value_.int_val);
      break;
    //add lijianqiang [INT_32] 20151008:b
    case ObInt32Type:
      len += serialization::encoded_length_i32(value_.int32_val_);
      len += 2;//for the first byte and the seconde byte
      break;
    //add 20151008:e
    case ObVarcharType:
      len += serialization::encoded_length_str(val_len_);
      break;
    case ObFloatType:
      len += serialization::encoded_length_float_type();
      break;
    case ObDoubleType:
      len += serialization::encoded_length_double_type();
      break;
    case ObDateTimeType:
      len += serialization::encoded_length_datetime(value_.time_val);
      break;
    //add peiouya [DATE_TIME] 20150906:b
    case ObDateType:
    case ObTimeType:
    case ObIntervalType:  //add peiouya [DATE_TIME] 20150909
      len += serialization::encoded_length_precise_datetime(value_.precisetime_val);
      len += serialization::encoded_length_i8(static_cast<int8_t>(serialization::encoded_length_precise_datetime(value_.precisetime_val)));
      break;
    //add 20150906:e
    case ObPreciseDateTimeType:
      len += serialization::encoded_length_precise_datetime(value_.precisetime_val);
      break;
    case ObModifyTimeType:
      len += serialization::encoded_length_modifytime(value_.modifytime_val);
      break;
    case ObCreateTimeType:
      len += serialization::encoded_length_createtime(value_.createtime_val);
      break;
    case ObSeqType:
      //TODO (maoqi)
      break;
    case ObExtendType:
      len += serialization::encoded_length_extend(value_.ext_val);
      break;
    case ObBoolType:
      len += serialization::encoded_length_bool_type(value_.bool_val);
      break;
    case ObDecimalType:
      /* if (meta_.dec_nwords_+1 <= 3)
      {
        len += serialization::encoded_length_decimal_type(meta_.dec_nwords_, reinterpret_cast<const uint32_t*>(&val_len_));
      }
      else
      {
        len += serialization::encoded_length_decimal_type(static_cast<int8_t>(meta_.dec_nwords_+1), value_.dec_words_);
      }
      */
      //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
      /*to caculate size of seriliazation decimalType ObObject
      		  except TTInt,we seriliazation that:
      		  first byte:1byte the flag of object type
      		  is_add:1 byte bool is_add
      		  precision:1 byte,precision of decimal
      		  scale:1 byte,scale of decimal
      		  vscale: 1 byte,scale of decimal
        */

      		{
      		    len += serialization::encoded_length_decimal_comm(val_len_);
      		}
      		//modify:e
      break;
    default:
      TBSYS_LOG(ERROR,"unexpected obj type [obj.type:%d]", type);
      break;
  }
  return len;
}


uint32_t ObObj::murmurhash2(const uint32_t hash) const
{
  uint32_t result = hash;
  ObObjType type = get_type();
  ObObjMeta meta;
  memset(&meta, 0, sizeof(meta));
  meta.type_ = meta_.type_;
  meta.op_flag_ = meta_.op_flag_;

  result = ::murmurhash2(&meta,sizeof(meta),result);
  switch (type)
  {
    case ObNullType:
      break;
    case ObIntType:
      result = ::murmurhash2(&value_.int_val,sizeof(value_.int_val),result);
      break;
    //add lijianqiang [INT_32] 20151008:b
    case ObInt32Type:
      result = ::murmurhash2(&value_.int32_val_,sizeof(value_.int32_val_),result);
      break;
    //add 20151008:e
    case ObVarcharType:
    //mod zhaoqiong [varchar extend] 20151103:b
    //result = ::murmurhash2(value_.varchar_val,val_len_,result);
    //break;
    {
      int32_t actual_len = val_len_;
      while (actual_len > 0)
      {
        if (value_.varchar_val[actual_len-1] != ' ')
        {
          break;
        }
        actual_len --;
      }
      result = ::murmurhash2(value_.varchar_val,actual_len,result);
      break;
    }
    case ObFloatType:
      result = ::murmurhash2(&value_.float_val,sizeof(value_.float_val),result);
      break;
    case ObDoubleType:
      result = ::murmurhash2(&value_.double_val,sizeof(value_.double_val),result);
      break;
    case ObDateTimeType:
      result = ::murmurhash2(&value_.time_val,sizeof(value_.time_val),result);
      break;
    //add peiouya [DATE_TIME] 20150906:b
    case ObDateType:
    case ObTimeType:
    case ObIntervalType:  //add peiouya [DATE_TIME] 20150909:b
    //add 20150906:e
    case ObPreciseDateTimeType:
      result = ::murmurhash2(&value_.precisetime_val,sizeof(value_.precisetime_val),result);
      break;
    case ObModifyTimeType:
      result = ::murmurhash2(&value_.modifytime_val,sizeof(value_.modifytime_val),result);
      break;
    case ObCreateTimeType:
      result = ::murmurhash2(&value_.createtime_val,sizeof(value_.createtime_val),result);
      break;
    case ObSeqType:
      //TODO (maoqi)
      break;
    case ObExtendType:
      result = ::murmurhash2(&value_.ext_val,sizeof(value_.ext_val),result);
      break;
    case ObBoolType:
      result = ::murmurhash2(&value_.bool_val,sizeof(value_.bool_val),result);
      break;
    case ObDecimalType:
      {
       /*int8_t nwords = static_cast<int8_t>(meta_.dec_nwords_+1);
        if (nwords <= 3)
        {
          result = ::murmurhash2(reinterpret_cast<const uint32_t*>(&val_len_), static_cast<int32_t>(sizeof(uint32_t)*nwords), result);
        }
        else
        {
          result = ::murmurhash2(value_.dec_words_, static_cast<int32_t>(sizeof(uint32_t)*nwords), result);
        }
        */
        //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_14:b
        TTCInt ct;
        ObString os;
        get_decimal(os);
        if(OB_SUCCESS!=from_hash(ct,os.ptr(),os.length())){
            TBSYS_LOG(ERROR,"failed calculate ttint for hash!buff is=%.*s",os.length(),os.ptr());
            result=0;
        }
        else{
        int length=32;
        result = ::murmurhash2(&ct,length,result);
        }
        //modify e
        break;
      }
    default:
      TBSYS_LOG(ERROR, "invalid obj_type=%d", type);
      result = 0;
      break;
  }
  return result;
}
uint64_t ObObj::murmurhash64A(const uint64_t hash) const
{
  uint64_t result = hash;
  ObObjType type = get_type();
  ObObjMeta meta;
  memset(&meta, 0, sizeof(meta));
  meta.type_ = meta_.type_;
  meta.op_flag_ = meta_.op_flag_;

  result = ::murmurhash64A(&meta, sizeof(meta), result);
  switch (type)
  {
    case ObNullType:
      break;
    case ObIntType:
      result = ::murmurhash64A(&value_.int_val, sizeof(value_.int_val), result);
      break;
    //add lijianqiang [INT_32] 20151008:b
    case ObInt32Type:
      result = ::murmurhash64A(&value_.int32_val_, sizeof(value_.int32_val_), result);
      break;
    //add 20151008:e
    case ObVarcharType:
    //mod zhaoqiong [varchar extend] 20151103:b
    //result = ::murmurhash64A(value_.varchar_val,val_len_,result);
    //break;
    {
      int32_t actual_len = val_len_;
      while (actual_len > 0)
      {
        if (value_.varchar_val[actual_len-1] != ' ')
        {
          break;
        }
        actual_len --;
      }
      result = ::murmurhash64A(value_.varchar_val,actual_len,result);
      break;
    }
    case ObFloatType:
      result = ::murmurhash64A(&value_.float_val, sizeof(value_.float_val), result);
      break;
    case ObDoubleType:
      result = ::murmurhash64A(&value_.double_val, sizeof(value_.double_val), result);
      break;
    case ObDateTimeType:
      result = ::murmurhash64A(&value_.time_val, sizeof(value_.time_val), result);
      break;
    //add peiouya [DATE_TIME] 20150906:b
    case ObDateType:
    case ObTimeType:
    case ObIntervalType:  //add peiouya [DATE_TIME] 20150909:b
    //add 20150906:e
    case ObPreciseDateTimeType:
      result = ::murmurhash64A(&value_.precisetime_val, sizeof(value_.precisetime_val), result);
      break;
    case ObModifyTimeType:
      result = ::murmurhash64A(&value_.modifytime_val, sizeof(value_.modifytime_val), result);
      break;
    case ObCreateTimeType:
      result = ::murmurhash64A(&value_.createtime_val, sizeof(value_.createtime_val), result);
      break;
    case ObSeqType:
      //TODO
      break;
    case ObExtendType:
      result = ::murmurhash64A(&value_.ext_val, sizeof(value_.ext_val), result);
      break;
    case ObBoolType:
      result = ::murmurhash64A(&value_.bool_val, sizeof(value_.bool_val), result);
      break;
    case ObDecimalType:
      {
        /*int8_t nwords = static_cast<int8_t>(meta_.dec_nwords_ + 1);
        if (nwords <= 3)
        {
          result = ::murmurhash64A(reinterpret_cast<const uint64_t*>(&val_len_), static_cast<int32_t>(sizeof(uint32_t) * nwords), result);
        }
        else
        {
          result = ::murmurhash64A(value_.dec_words_, static_cast<int32_t>(sizeof(uint32_t) * nwords), result);
        }
        */

        //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_14:b
        TTCInt ct;
        ObString os;
        get_decimal(os);
        if(OB_SUCCESS!=from_hash(ct,os.ptr(),os.length())){
            TBSYS_LOG(ERROR,"failed calculate ttint for hash!buff is=%.*s",os.length(),os.ptr());
            result=0;
        }
        else{
        int length=32;
        result = ::murmurhash64A(&ct,length,result);
        }
        //modify e
        break;
      }
    default:
      TBSYS_LOG(WARN, "invalid obj type: obj_type[%d]", type);
      result = 0;
  }

  return result;
}

int64_t ObObj::checksum(const int64_t current) const
{
  int64_t ret = current;
  ObObjType type = get_type();
  ObObjMeta meta;
  memset(&meta, 0, sizeof(meta));
  meta.type_ = meta.type_;
  meta.op_flag_ = meta.op_flag_;

  ret = ob_crc64(ret, &meta, sizeof(meta));
  switch (type)
  {
    case ObNullType:
      break;
    case ObIntType:
      ret = ob_crc64(ret, &value_.int_val, sizeof(value_.int_val));
      break;
    //add lijianqiang [INT_32] 20151008:b
    case ObInt32Type:
      ret = ob_crc64(ret, &value_.int32_val_, sizeof(value_.int32_val_));
      break;
    //add 20151008:e
    case ObVarcharType:
    //mod zhaoqiong [varchar extend] 20151103:b
    //ret = ob_crc64(ret, value_.varchar_val, val_len_);
    //break;
      {
      int32_t actual_len = val_len_;
      while (actual_len > 0)
      {
        if (value_.varchar_val[actual_len-1] != ' ')
        {
          break;
        }
        actual_len --;
      }
      ret = ob_crc64(ret, value_.varchar_val, actual_len );
      }
      break;
    case ObFloatType:
      ret = ob_crc64(ret, &value_.float_val, sizeof(value_.float_val));
      break;
    case ObDoubleType:
      ret = ob_crc64(ret, &value_.double_val, sizeof(value_.double_val));
      break;
    case ObDateTimeType:
      ret = ob_crc64(ret, &value_.time_val, sizeof(value_.time_val));
      break;
    //add peiouya [DATE_TIME] 20150906:b
    case ObDateType:
    case ObTimeType:
    //add 20150906:e
    case ObIntervalType:  //add peiouya [DATE_TIME] 20150909:b
    case ObPreciseDateTimeType:
      ret = ob_crc64(ret, &value_.precisetime_val, sizeof(value_.precisetime_val));
      break;
    case ObModifyTimeType:
      ret = ob_crc64(ret, &value_.modifytime_val, sizeof(value_.modifytime_val));
      break;
    case ObCreateTimeType:
      ret = ob_crc64(ret, &value_.createtime_val, sizeof(value_.createtime_val));
      break;
    case ObSeqType:
      //TODO (maoqi)
      break;
    case ObExtendType:
      ret = ob_crc64(ret, &value_.ext_val, sizeof(value_.ext_val));
      break;
    case ObBoolType:
      ret = ob_crc64(ret, &value_.bool_val, sizeof(value_.bool_val));
      break;
    case ObDecimalType:
      {
        /*int8_t nwords = static_cast<int8_t>(meta_.dec_nwords_+1);
        if (nwords <= 3)
        {
          ret = ob_crc64(ret, reinterpret_cast<const uint32_t*>(&val_len_), sizeof(uint32_t)*nwords);
        }
        else
        {
          ret = ob_crc64(ret, value_.dec_words_, sizeof(uint32_t)*nwords);
        }
        */
        //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
        ret = ob_crc64(ret, value_.word, val_len_);
        //modify e
        break;
      }
    default:
      TBSYS_LOG(ERROR, "invalid obj_type=%d", type);
      ret = 0;
      break;
  }
  return ret;
}

void ObObj::checksum(ObBatchChecksum &bc) const
{
  ObObjType type = get_type();
  ObObjMeta meta;
  memset(&meta, 0, sizeof(meta));
  meta.type_ = meta.type_;
  meta.op_flag_ = meta.op_flag_;

  bc.fill(&meta, sizeof(meta));
  switch (type)
  {
    case ObNullType:
      break;
    case ObIntType:
      bc.fill(&value_.int_val, sizeof(value_.int_val));
      break;
    //add lijianqiang [INT_32] 20151008:b
    case ObInt32Type:
      bc.fill(&value_.int32_val_, sizeof(value_.int32_val_));
      break;
    //add 20151008:e
    case ObVarcharType:
    //mod zhaoqiong [varchar extend] 20151103:b
    //bc.fill(value_.varchar_val, val_len_);
    //break;
      {
      int32_t actual_len = val_len_;
      while (actual_len > 0)
      {
        if (value_.varchar_val[actual_len-1] != ' ')
        {
          break;
        }
        actual_len --;
      }
      bc.fill(value_.varchar_val, actual_len);
      }
      break;
    case ObFloatType:
      bc.fill(&value_.float_val, sizeof(value_.float_val));
      break;
    case ObDoubleType:
      bc.fill(&value_.double_val, sizeof(value_.double_val));
      break;
    case ObDateTimeType:
      bc.fill(&value_.time_val, sizeof(value_.time_val));
      break;
    //add peiouya [DATE_TIME] 20150906:b
    case ObDateType:
    case ObTimeType:
    //add 20150906:e
    case ObIntervalType:  //add peiouya [DATE_TIME] 20150909:b
    case ObPreciseDateTimeType:
      bc.fill(&value_.precisetime_val, sizeof(value_.precisetime_val));
      break;
    case ObModifyTimeType:
      bc.fill(&value_.modifytime_val, sizeof(value_.modifytime_val));
      break;
    case ObCreateTimeType:
      bc.fill(&value_.createtime_val, sizeof(value_.createtime_val));
      break;
    case ObSeqType:
      //TODO (maoqi)
      break;
    case ObExtendType:
      bc.fill(&value_.ext_val, sizeof(value_.ext_val));
      break;
    case ObBoolType:
      bc.fill(&value_.bool_val, sizeof(value_.bool_val));
      break;
    case ObDecimalType:
      {
        /*int8_t nwords = static_cast<int8_t>(meta_.dec_nwords_+1);
        if (nwords <= 3)
        {
          bc.fill(reinterpret_cast<const uint32_t*>(&val_len_), sizeof(uint32_t)*nwords);
        }
        else
        {
          bc.fill(value_.dec_words_, sizeof(uint32_t)*nwords);
        }
        */
      //modify wenghaixing DECIMAL OceanBase_BankCommV0.3 2014_7_10:b
        bc.fill(value_.word, val_len_);
        //modify e
        break;
      }
    default:
      TBSYS_LOG(ERROR, "invalid obj_type=%d", type);
      break;
  }
}
