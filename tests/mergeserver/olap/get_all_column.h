#ifndef OLAP_GET_ALL_COLUMN_H_ 
#define OLAP_GET_ALL_COLUMN_H_

#include "base_case.h" 
#include <string>
class GetSingleRowAllColumn:public OlapBaseCase
{
public:
  GetSingleRowAllColumn()
  {
    char case_name[512];
    int64_t size = 0;
    size = snprintf(case_name,sizeof(case_name), "%s:%d:%s", __FILE__, __LINE__, __FUNCTION__);
    case_name_.append(case_name, case_name + size);
  }
  virtual bool check_result(const uint32_t min_key_include, const uint32_t max_key_include, oceanbase::common::ObScanner &result,
    void *arg);
  virtual int form_get_param(oceanbase::common::ObGetParam & param, const uint32_t min_key_include, const uint32_t max_key_include, 
    void *&arg);

  virtual ob_req_t get_req_type()const
  {
    return GET;
  }
  static OlapBaseCase * get_instance()
  {
    return &static_case_;
  }
private:
  static GetSingleRowAllColumn static_case_;
};

#endif /* OLAP_GET_ALL_COLUMN_H_ */
