#ifndef OLAP_BASE_CASE_H_ 
#define OLAP_BASE_CASE_H_
#include <vector>
#include "common/ob_define.h"
#include "common/ob_scan_param.h" 
#include "common/ob_scanner.h" 
/// this class is static class, should not have members
class OlapBaseCase
{
public:
  OlapBaseCase()
  {
  };
  virtual ~OlapBaseCase()
  {
  };
  virtual int form_scan_param(oceanbase::common::ObScanParam & param, const uint32_t min_key_include, 
    const uint32_t max_key_include, void *& arg)
  {
    UNUSED(param);
    UNUSED(min_key_include);
    UNUSED(max_key_include);
    UNUSED(arg);
    return oceanbase::common::OB_NOT_SUPPORTED;
  }
  virtual int form_get_param(oceanbase::common::ObGetParam & param, const uint32_t min_key_include, 
    const uint32_t max_key_include, void *& arg)
  {
    UNUSED(param);
    UNUSED(min_key_include);
    UNUSED(max_key_include);
    UNUSED(arg);
    return oceanbase::common::OB_NOT_SUPPORTED;
  }
  virtual bool check_result(const uint32_t min_key_include, const uint32_t max_key_include, 
    oceanbase::common::ObScanner &result, void *arg) = 0;
  typedef enum
  {
    SCAN,
    GET
  }ob_req_t;

  virtual ob_req_t get_req_type()const =0;
  const char *get_name() const 
  {
    return case_name_.c_str();
  }
  ///virtual OlapBaseCase * get_instance() = 0;
private:
  OlapBaseCase(const OlapBaseCase &);
  OlapBaseCase &operator =(const OlapBaseCase &);
protected:
  std::string case_name_;
};

#endif /* OLAP_BASE_CASE_H_ */
