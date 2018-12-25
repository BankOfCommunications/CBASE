
#ifndef OCEANBASE_UPS_ROW_H_
#define OCEANBASE_UPS_ROW_H_

#include "common/ob_row.h"

namespace oceanbase
{
  namespace common 
  {
    class ObUpsRow : public ObRow
    {
      public:
        ObUpsRow();
        virtual ~ObUpsRow();

        virtual int reset();
        virtual void set_is_delete_row(bool is_delete_row);
        virtual bool get_is_delete_row() const;

        int reuse();
        bool get_is_all_nop() const;
        void set_is_all_nop(bool is_all_nop);
        
      private:
        bool is_delete_row_;
        bool is_all_nop_;
    };

    inline void ObUpsRow::set_is_all_nop(bool is_all_nop)
    {
      is_all_nop_ = is_all_nop;
    }

    inline bool ObUpsRow::get_is_all_nop() const
    {
      return is_all_nop_;
    }

    inline int ObUpsRow::reuse()
    {
      set_is_delete_row(false);
      return reset();
    }
  }
}


#endif /* OCEANBASE_UPS_ROW_H_ */


