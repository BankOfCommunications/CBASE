#ifndef OB_CS_SCAN_CELL_STREAM_H
#define OB_CS_SCAN_CELL_STREAM_H
#include "common/ob_read_common_data.h"
#include "ob_cell_stream.h"

namespace oceanbase
{
  namespace chunkserver
  {
    /**
     * this is one of ObCellStream subclasses, it can provide cell
     * stream through scan operation from chunkserver or update
     * server according to scan param. it encapsulates many rpc
     * calls when one server not serving all the required data or
     * the packet is too large.
     */
    class ObCSScanCellStream : public ObCellStream
    {
    public:
      ObCSScanCellStream(ObMergerRpcProxy * rpc_proxy,
        const common::ObServerType server_type = common::MERGE_SERVER,
        const int64_t time_out = 0);
      virtual ~ObCSScanCellStream();

    public:
      // get next cell
      virtual int next_cell(void);
      // scan init
      virtual int scan(const common::ObScanParam & param);

      /**
       * get the current scan data version, this function must
       * be called after next_cell()
       *
       * @return int64_t return data version
       */
      virtual int64_t get_data_version() const;

      void set_chunkserver(const ObTabletLocationList server);

      // reset inner stat
      void reset_inner_stat(void);
      inline void set_self(ObServer self){self_ = self;}
      inline ObServer get_self(){return self_;}

    private:

      // check whether finish scan, if finished return server's servering tablet range
      // param  @param current scan param
      int check_finish_scan(const common::ObScanParam & param);

      // scan for get next cell
      int get_next_cell(void);

      // scan data
      // param @param scan data param
      int scan_row_data();

      // check inner stat
      bool check_inner_stat(void) const;





    private:
      DISALLOW_COPY_AND_ASSIGN(ObCSScanCellStream);

      bool finish_;                             // finish all scan routine status
      common::ObMemBuf range_buffer_;           // for modify param range
      const common::ObScanParam * scan_param_;  // orignal scan param
      common::ObScanParam cur_scan_param_;      // current scan param
      ObTabletLocationList chunkserver_;                    // 选择需要发送的CS
      int64_t   cur_rep_index_;
      ObServer self_;
    };

    // check inner stat
    inline bool ObCSScanCellStream::check_inner_stat(void) const
    {
      //finish_ = false;
      return(ObCellStream::check_inner_stat() && (NULL != scan_param_));
    }

    // reset inner stat
    inline void ObCSScanCellStream::reset_inner_stat(void)
    {
      ObCellStream::reset_inner_stat();
      finish_ = false;
      scan_param_ = NULL;
      cur_scan_param_.reset();
    }

    inline void ObCSScanCellStream::set_chunkserver(const ObTabletLocationList server)
    {
      chunkserver_ = server;
    }
  }
}
#endif // OB_CS_SCAN_CELL_STREAM_H
