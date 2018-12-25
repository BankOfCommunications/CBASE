/**
 * (C) 2007-2010 Taobao Inc.
 *
 * This program is free software; you can redistribute it and/or modify
 * it under the terms of the GNU General Public License version 2 as
 * published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * Authors:
 *   yuanqi <yuanqi.xhf@taobao.com>
 *     - some work details if you want
 */
#ifndef __OB_COMMON_OB_PCAP_H__
#define __OB_COMMON_OB_PCAP_H__
#include "data_buffer.h"
#include "utility.h"

namespace oceanbase
{
  namespace common
  {
    class ObPacket;
    class ObPCap
    {
      public:
        ObPCap(const char* pcap_cmd);
        ~ObPCap();
        int handle_packet(ObPacket* packet);
      protected:
        int flush_buf();
        int do_capture_packet(ObPacket& packet);
      private:
        uint64_t lock_;
        int fd_;
        char cbuf_[16 * OB_MAX_PACKET_LENGTH];
        ObDataBuffer buf_;
        CountReporter count_reporter_;
        int64_t total_size_;
    };

    class IRLock
    {
      public:
        IRLock(uint64_t bs_shift, uint64_t bcount);
        ~IRLock();
      public:
        uint64_t get_start();
        uint64_t get_end();
        int use(uint64_t pos);
        int done(uint64_t pos);
        void debug();
      protected:
        inline int64_t update_ref(uint64_t pos, int64_t n){ return __sync_add_and_fetch(&refs_[(pos>>bs_shift_) % bcount_], n); }
        inline int64_t get_ref(uint64_t pos){ return refs_[(pos>>bs_shift_) % bcount_]; }
      private:
        uint64_t bs_shift_;
        uint64_t bcount_;
        volatile uint64_t start_;
        volatile uint64_t end_;
        volatile int64_t* refs_;
    };

    class QBuf
    {
      public:
        QBuf(uint64_t bs_shift, uint64_t bcount);
        ~QBuf();
        int pop(uint64_t& pos, char*& buf, int64_t& len);
        int pop_done(uint64_t pos);
        int append(char* buf, int64_t len);
        int64_t get_free();
      protected:
        int write(uint64_t pos, char* buf, int64_t len);
        int read(uint64_t pos, char* buf, int64_t len);
        int cpytail(uint64_t pos, uint64_t len);
      private:
        uint64_t size_mask_;
        uint64_t reserved_size_;
        uint64_t lock_;
        IRLock irlock_;
        volatile uint64_t push_;
        volatile uint64_t pop_;
        char* buf_;
    };

    class ObPFetcher
    {
      public:
        ObPFetcher(const char* pfetch_cmd, uint64_t bs_shift, uint64_t bcount);
        ~ObPFetcher();
        int get_packet(ObPacket* packet);
        int get_match_packet(ObPacket* packet, int pkt_code);
      protected:
        int try_read_buf();
      private:
        uint64_t lock_;
        int fd_;
        QBuf qbuf_;
        char cbuf_[OB_MAX_PACKET_LENGTH];
        CountReporter count_reporter_;
        int64_t total_size_;
    };

  }; // end namespace common
}; // end namespace oceanbase

#endif /* __OB_COMMON_OB_PCAP_H__ */
