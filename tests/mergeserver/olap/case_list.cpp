#include <stdlib.h>
#include "case_list.h"
#include "select_where_order_limit.h"
#include "select_where_comp_order_limit.h"
#include "get_all_column.h"
#include "get_not_exist_rows.h"
#include "count_all.h"
#include "multi_parellel_get.h"
namespace 
{
  OlapBaseCase* case_list[] =
  {
    /// add cases here
    SelectWhereCompOrderLimit::get_instance(),
    SelectWhereOrderLimit::get_instance(),
    GetSingleRowAllColumn::get_instance(),
    CountALL::get_instance(),
    MultiGetP::get_instance(),
    GetNotExistRows::get_instance(),
  };
}


OlapBaseCase & random_select_case()
{
  static size_t case_list_size = sizeof(case_list)/sizeof(void*);
  return *case_list[random()%case_list_size];
}
