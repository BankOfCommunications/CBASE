#ifndef OLAP_SELECT_WHERE_ORDER_LIMIT_H_ 
#define OLAP_SELECT_WHERE_ORDER_LIMIT_H_
#include "base_case.h" 
#include <string>
class SelectWhereOrderLimit:public OlapBaseCase
{
public:
  SelectWhereOrderLimit()
  {
    char case_name[512];
    int64_t size = 0;
    size = snprintf(case_name,sizeof(case_name), "%s:%d:%s", __FILE__, __LINE__, __FUNCTION__);
    case_name_.append(case_name, case_name + size);
  }
  virtual bool check_result(const uint32_t min_key_include, const uint32_t max_key_include, oceanbase::common::ObScanner &result,
    void *arg);
  virtual int form_scan_param(oceanbase::common::ObScanParam &param, const uint32_t min_key_include, const uint32_t max_key_include,
    void *&arg);
  virtual ob_req_t get_req_type()const
  {
    return SCAN;
  }
  static OlapBaseCase * get_instance()
  {
    return &static_case_;
  }
private:
  static SelectWhereOrderLimit static_case_;
};  
#endif /* OLAP_SELECT_WHERE_ORDER_LIMIT_H_ */
