/**
 * (C) 2010-2012 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License
 * version 2 as published by the Free Software Foundation.
 *
 * Version: $Id$
 *
 * ob_values.h
 *
 * Authors:
 *   Zhifeng YANG <zhuweng.yzf@taobao.com>
 *
 */
#ifndef _OB_VALUES_H
#define _OB_VALUES_H 1

#include "sql/ob_single_child_phy_operator.h"
#include "common/ob_row_store.h"
//add wenghaixing[decimal] for fix delete bug 2014/10/15
#include "common/ob_se_array.h"
#include "common/ob_define.h"
//add e
namespace oceanbase
{
  namespace sql
  {
    //add wenghaixing[decimal] for fix delete bug 2014/10/15
    class ObValuesKeyInfo{

    public:

        ObValuesKeyInfo();
        ~ObValuesKeyInfo();
        void set_key_info(uint64_t tid,uint64_t cid,uint32_t p,uint32_t s,common::ObObjType type);
        void get_type(common::ObObjType& type);
        void get_key_info(uint32_t& p,uint32_t& s);

        bool is_rowkey(uint64_t tid,uint64_t cid);
    private:
        uint32_t precision_;
        uint32_t scale_;
        uint64_t cid_;
        uint64_t tid_;
        common::ObObjType type_;
    };
    inline void ObValuesKeyInfo::set_key_info(uint64_t tid,uint64_t cid,uint32_t p,uint32_t s,common::ObObjType type){
        tid_=tid;
        cid_=cid;
        type_=type;
        precision_=p;
        scale_=s;
    }
    inline void ObValuesKeyInfo::get_type(common::ObObjType& type){
        type=type_;
    }
    inline void ObValuesKeyInfo::get_key_info(uint32_t& p,uint32_t& s){
        p=precision_;
        s=scale_;
    }
    inline bool ObValuesKeyInfo::is_rowkey(uint64_t tid,uint64_t cid){
        bool ret =false;
        if(tid==tid_&&cid==cid_)ret=true;
        return ret;
    }
    //add e
    class ObValues: public ObSingleChildPhyOperator
    {
      public:
        ObValues();
        virtual ~ObValues();
        virtual void reset();
        virtual void reuse();
        int set_row_desc(const common::ObRowDesc &row_desc);
        int add_values(const common::ObRow &value);
        const common::ObRowStore &get_row_store() {return row_store_;};

        virtual int open();
        virtual int close();
        virtual int get_next_row(const common::ObRow *&row);
        virtual int get_row_desc(const common::ObRowDesc *&row_desc) const;
        virtual int64_t to_string(char* buf, const int64_t buf_len) const;
        enum ObPhyOperatorType get_type() const{return PHY_VALUES;}
        DECLARE_PHY_OPERATOR_ASSIGN;
        NEED_SERIALIZE_AND_DESERIALIZE;
        //add wenghaixing[decimal] for fix delete bug 2014/10/10
        void set_fix_obvalues();
        void add_rowkey_array(uint64_t tid,uint64_t cid,common::ObObjType type,uint32_t p,uint32_t s);
        bool is_rowkey_column(uint64_t tid,uint64_t cid);
        int get_rowkey_schema(uint64_t tid,uint64_t cid,common::ObObjType& type,uint32_t& p,uint32_t& s);
        typedef common::ObSEArray<ObValuesKeyInfo, common::OB_MAX_ROWKEY_COLUMN_NUMBER> RowkeyInfo;
        //add e
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20161108:b
        int  add_values_for_ud(common::ObRow *value);
        void set_rowkey_num(int64_t rowkey_num);
        void set_table_id(uint64_t table_id);
        int  set_column_ids(uint64_t column_id);
        //add gaojt 20161108:e
      private:
        // types and constants
        int load_data();
      private:
        // disallow copy
        ObValues(const ObValues &other);
        ObValues& operator=(const ObValues &other);
        // function members
      private:
        // data members
        common::ObRowDesc row_desc_;
        common::ObRow curr_row_;
        common::ObRowStore row_store_;
        //add wenghaixing[decimal] for fix delete bug 2014/10/10
        bool is_need_fix_obvalues;
        RowkeyInfo obj_array_;
        //e
        //add gaojt [Delete_Update_Function] [JHOBv0.1] 20161108:b
        int64_t  rowkey_num_;
        uint64_t table_id_;
        common::ObArray<uint64_t> column_ids_;
        //add gaojt 20161108:e
    };
  } // end namespace sql
} // end namespace oceanbase

#endif /* _OB_VALUES_H */
