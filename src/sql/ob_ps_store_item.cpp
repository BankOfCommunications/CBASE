#include "ob_ps_store_item.h"
#include "common/ob_atomic.h"

namespace oceanbase
{
  namespace sql
  {
    ObPsStoreItem::ObPsStoreItem():status_(PS_ITEM_INVALID), ps_count_(0)
    {
      pthread_rwlock_init(&rwlock_, NULL);
    }

    ObPsStoreItem::~ObPsStoreItem()
    {
      TBSYS_LOG(DEBUG, "item dctor %p", this);
    }

    int64_t ObPsStoreItem::inc_ps_count()
    {
      return common::atomic_inc((uint64_t*)&ps_count_);
    }

    int64_t ObPsStoreItem::dec_ps_count()
    {
      return common::atomic_dec((uint64_t*)&ps_count_);
    }

    ObPhysicalPlan& ObPsStoreItem::get_physical_plan()
    {
      return value_.plan_;
    }

    void ObPsStoreItem::store_ps_sql(const common::ObString &stmt)
    {
      value_.str_buf_.write_string(stmt, &value_.sql_);
    }

    REGISTER_CREATOR(oceanbase::sql::ObPsStoreItemGFactory, ObPsStoreItem, ObPsStoreItem, 1);
  }
}
