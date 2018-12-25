#ifndef TABLET_LOCATION_H_
#define TABLET_LOCATION_H_

#include "common/ob_tablet_info.h"

namespace oceanbase
{
  namespace tools
  {
    class TabletLocation 
    {
    public:
      TabletLocation();
      virtual ~TabletLocation();

    public:
      static const int64_t MAX_REPLICA_COUNT = 3;
      
      /// add a tablet location to item list
      int add(const common::ObTabletLocation & location);
      
      /// del the index pos TabletLocation
      int del(const int64_t index, common::ObTabletLocation & location);
      
      /// operator random access 
      const common::ObTabletLocation & operator[] (const uint64_t index) const;
      
      /// sort the server list
      int sort(const common::ObServer & server); 
      
      /// current tablet locastion server count 
      int64_t size(void) const;
      
      /// clear all items 
      void clear(void);
      
      /// operator ==
      bool operator == (const TabletLocation & other);

      /// dump all info
      void print_info(void) const;
    
      /// serailize or deserialization
      NEED_SERIALIZE_AND_DESERIALIZE;
    private:
      int64_t cur_count_;
      common::ObTabletLocation locations_[MAX_REPLICA_COUNT];
    };

    inline int64_t TabletLocation::size(void) const
    {
      return cur_count_;
    }

    inline void TabletLocation::clear(void)
    {
      cur_count_ = 0;
    }
    
    inline const common::ObTabletLocation & TabletLocation::operator[] (const uint64_t index) const
    {
      return locations_[index];
    }
  }
}


#endif // TABLET_LOCATION_H_

