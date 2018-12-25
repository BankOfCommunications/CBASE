#include <pthread.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <assert.h>
#include "ob_malloc.h"
#include "ob_action_flag.h"
#include "ob_iterator_adaptor.h"
#include "utility.h"
#include "gtest/gtest.h"

using namespace oceanbase;
using namespace common;

ObRow g_row;

class MockIterator : public ObIterator
{
  struct CellNode
  {
    ObCellInfo cell_info;
    bool is_row_changed;
    bool is_row_finished;
    CellNode *next;
  };
  public:
    MockIterator() : head_(NULL), tail_(NULL), iter_(NULL)
    {
    };
    ~MockIterator()
    {
    };
  public:
    int next_cell()
    {
      int ret = OB_SUCCESS;
      if (NULL == iter_)
      {
        if (NULL == (iter_ = head_))
        {
          ret = OB_ITER_END;
        }
      }
      else if (NULL == iter_->next)
      {
        ret = OB_ITER_END;
      }
      else
      {
        iter_ = iter_->next;
      }
      return ret;
    };
    int get_cell(ObCellInfo **cell_info)
    {
      int ret = OB_SUCCESS;
      if (NULL == iter_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        *cell_info = &(iter_->cell_info);
      }
      return ret;
    };
    int get_cell(ObCellInfo **cell_info, bool *is_row_changed)
    {
      int ret = OB_SUCCESS;
      if (NULL == iter_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        *cell_info = &(iter_->cell_info);
        *is_row_changed = iter_->is_row_changed;
      }
      return ret;
    };
    int is_row_finished(bool *is_row_finished)
    {
      int ret = OB_SUCCESS;
      if (NULL == iter_)
      {
        ret = OB_ITER_END;
      }
      else
      {
        *is_row_finished = iter_->is_row_finished;
      }
      return ret;
    };
    void reset_iter()
    {
      iter_ = NULL;
    };
  public:
    void add_cell(ObCellInfo cell_info, bool is_row_changed, bool is_row_finished)
    {
      CellNode *node = (CellNode*)allocator_.alloc(sizeof(CellNode));
      if (NULL != node)
      {
        node->cell_info = cell_info;
        node->is_row_changed = is_row_changed;
        node->is_row_finished = is_row_finished;
        cell_info.row_key_.deep_copy(node->cell_info.row_key_, allocator_);
        node->next = NULL;
        if (NULL == tail_)
        {
          head_ = node;
        }
        else
        {
          tail_->next = node;
        }
        tail_ = node;
      }
    };
  private:
    PageArena<char> allocator_;
    CellNode *head_;
    CellNode *tail_;
    CellNode *iter_;
};

ObRowkeyInfo get_rki()
{
  ObRowkeyInfo rki;
  ObRowkeyColumn rkc1;
  ObRowkeyColumn rkc2;
  rkc1.length_ = 0;
  rkc1.column_id_ = 16;
  rkc1.type_ = ObIntType;
  rkc2.length_ = 0;
  rkc2.column_id_ = 17;
  rkc2.type_ = ObIntType;
  rki.set_column(0, rkc1);
  rki.set_column(1, rkc2);
  return rki;
}

TEST(TestObIteratorAdapter, zero_cell)
{
  ObRowkeyInfo rki = get_rki();
  ObRowDesc row_desc;
  ObRow &row = g_row;
  uint64_t table_id = 1001;

  ObObj rkc[2];
  uint64_t rk1_cid = 16;
  rkc[0].set_int(16);
  uint64_t rk2_cid = 17;
  rkc[1].set_int(17);
  ObRowkey rk;
  rk.assign(rkc, 2);

  row_desc.add_column_desc(table_id, rk1_cid);
  row_desc.add_column_desc(table_id, rk2_cid);
  row.set_row_desc(row_desc);
  row.set_cell(table_id, rk1_cid, rkc[0]);
  row.set_cell(table_id, rk2_cid, rkc[1]);

  ObCellAdaptor iteradp;
  iteradp.set_row(&row, rki.get_size());

  ObCellInfo *ci = NULL;
  ObCellInfo *nil = NULL;
  bool irc = false;
  bool irf = false;
  int64_t nop_flag = ObActionFlag::OP_NOP;
  int ret = iteradp.next_cell();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = iteradp.get_cell(&ci, &irc);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE(nil, ci);
  EXPECT_EQ(true, irc);
  EXPECT_EQ(table_id, ci->table_id_);
  EXPECT_EQ(rk, ci->row_key_);
  EXPECT_EQ(OB_INVALID_ID, ci->column_id_);
  EXPECT_EQ(nop_flag, ci->value_.get_ext());
  ret = iteradp.is_row_finished(&irf);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(true, irf);

  ret = iteradp.next_cell();
  EXPECT_EQ(OB_ITER_END, ret);
}

TEST(TestObIteratorAdapter, single_cell)
{
  ObRowkeyInfo rki = get_rki();
  ObRowDesc row_desc;
  ObRow &row = g_row;
  uint64_t table_id = 1001;

  ObObj rkc[2];
  uint64_t rk1_cid = 16;
  rkc[0].set_int(16);
  uint64_t rk2_cid = 17;
  rkc[1].set_int(17);
  ObRowkey rk;
  rk.assign(rkc, 2);
  uint64_t v1_cid = 101;
  ObObj v1;
  v1.set_varchar(ObString(2, 2, "v1"));

  row_desc.add_column_desc(table_id, rk1_cid);
  row_desc.add_column_desc(table_id, rk2_cid);
  row_desc.add_column_desc(table_id, v1_cid);
  row.set_row_desc(row_desc);
  row.set_cell(table_id, rk1_cid, rkc[0]);
  row.set_cell(table_id, rk2_cid, rkc[1]);
  row.set_cell(table_id, v1_cid, v1);

  ObCellAdaptor iteradp;
  iteradp.set_row(&row, rki.get_size());

  ObCellInfo *ci = NULL;
  ObCellInfo *nil = NULL;
  bool irc = false;
  bool irf = false;
  int ret = iteradp.next_cell();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = iteradp.get_cell(&ci, &irc);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE(nil, ci);
  EXPECT_EQ(true, irc);
  EXPECT_EQ(table_id, ci->table_id_);
  EXPECT_EQ(rk, ci->row_key_);
  EXPECT_EQ(v1_cid, ci->column_id_);
  EXPECT_TRUE(v1 == ci->value_);
  ret = iteradp.is_row_finished(&irf);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(true, irf);

  ret = iteradp.next_cell();
  EXPECT_EQ(OB_ITER_END, ret);
}

TEST(TestObIteratorAdapter, multi_cell)
{
  ObRowkeyInfo rki = get_rki();
  ObRowDesc row_desc;
  ObRow &row = g_row;
  uint64_t table_id = 1001;

  ObObj rkc[2];
  uint64_t rk1_cid = 16;
  rkc[0].set_int(16);
  uint64_t rk2_cid = 17;
  rkc[1].set_int(17);
  ObRowkey rk;
  rk.assign(rkc, 2);
  uint64_t v1_cid = 101;
  ObObj v1;
  v1.set_varchar(ObString(2, 2, "v1"));
  uint64_t v2_cid = 102;
  ObObj v2;
  v2.set_varchar(ObString(2, 2, "v2"));

  row_desc.add_column_desc(table_id, rk1_cid);
  row_desc.add_column_desc(table_id, rk2_cid);
  row_desc.add_column_desc(table_id, v1_cid);
  row_desc.add_column_desc(table_id, v2_cid);
  row.set_row_desc(row_desc);
  row.set_cell(table_id, rk1_cid, rkc[0]);
  row.set_cell(table_id, rk2_cid, rkc[1]);
  row.set_cell(table_id, v1_cid, v1);
  row.set_cell(table_id, v2_cid, v2);

  ObCellAdaptor iteradp;
  iteradp.set_row(&row, rki.get_size());

  ObCellInfo *ci = NULL;
  ObCellInfo *nil = NULL;
  bool irc = false;
  bool irf = false;
  int ret = iteradp.next_cell();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = iteradp.get_cell(&ci, &irc);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE(nil, ci);
  EXPECT_EQ(true, irc);
  EXPECT_EQ(table_id, ci->table_id_);
  EXPECT_EQ(rk, ci->row_key_);
  EXPECT_EQ(v1_cid, ci->column_id_);
  EXPECT_TRUE(v1 == ci->value_);
  ret = iteradp.is_row_finished(&irf);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_TRUE(false == irf);

  ret = iteradp.next_cell();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = iteradp.get_cell(&ci, &irc);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_NE(nil, ci);
  EXPECT_TRUE(false == irc);
  EXPECT_EQ(table_id, ci->table_id_);
  EXPECT_EQ(rk, ci->row_key_);
  EXPECT_EQ(v2_cid, ci->column_id_);
  EXPECT_TRUE(v2 == ci->value_);
  ret = iteradp.is_row_finished(&irf);
  EXPECT_EQ(OB_SUCCESS, ret);
  EXPECT_EQ(true, irf);

  ret = iteradp.next_cell();
  EXPECT_EQ(OB_ITER_END, ret);
}

TEST(TestObIteratorAdapter, invalid_rowkey)
{
  ObRowkeyInfo rki = get_rki();
  ObRowDesc row_desc;
  ObRow &row = g_row;
  uint64_t table_id = 1001;

  ObObj rkc;
  uint64_t rk1_cid = 16;
  rkc.set_int(16);

  row_desc.add_column_desc(table_id, rk1_cid);
  row.set_row_desc(row_desc);
  row.set_cell(table_id, rk1_cid, rkc);

  ObCellAdaptor iteradp;
  iteradp.set_row(&row, rki.get_size());

  int ret = iteradp.next_cell();
  EXPECT_EQ(OB_NOT_INIT, ret);
}

TEST(TestObIteratorAdapter, get_rowkey_fail)
{
  ObRowkeyInfo rki = get_rki();
  ObRowDesc row_desc;
  ObRow &row  = g_row;
  uint64_t table_id = 1001;

  ObObj rkc[2];
  uint64_t rk1_cid = 16;
  rkc[0].set_int(16);
  uint64_t rk2_cid = 17;
  rkc[1].set_int(17);
  ObRowkey rk;
  rk.assign(rkc, 2);

  row_desc.add_column_desc(table_id, rk1_cid);
  row_desc.add_column_desc(table_id, rk2_cid);
  row.set_row_desc(row_desc);
  row.set_cell(table_id, rk1_cid, rkc[0]);
  row.set_cell(table_id, rk2_cid, rkc[1]);
  row_desc.reset();

  ObCellAdaptor iteradp;
  iteradp.set_row(&row, rki.get_size());

  int ret = iteradp.next_cell();
  EXPECT_EQ(OB_NOT_INIT, ret);
}

TEST(TestObIteratorAdapter, get_value_fail)
{
  ObRowkeyInfo rki = get_rki();
  ObRowDesc row_desc;
  ObRow &row = g_row;
  uint64_t table_id = 1001;

  ObObj rkc[2];
  uint64_t rk1_cid = 16;
  rkc[0].set_int(16);
  uint64_t rk2_cid = 17;
  rkc[1].set_int(17);
  ObRowkey rk;
  rk.assign(rkc, 2);
  uint64_t v1_cid = 101;
  ObObj v1;
  v1.set_varchar(ObString(2, 2, "v1"));

  row_desc.add_column_desc(table_id, rk1_cid);
  row_desc.add_column_desc(table_id, rk2_cid);
  row_desc.add_column_desc(table_id, v1_cid);
  row.set_row_desc(row_desc);
  row.set_cell(table_id, rk1_cid, rkc[0]);
  row.set_cell(table_id, rk2_cid, rkc[1]);
  row.set_cell(table_id, v1_cid, v1);

  ObCellAdaptor iteradp;
  iteradp.set_row(&row, rki.get_size());
  row_desc.reset();

  ObCellInfo *ci = NULL;
  bool irc = false;
  bool irf = false;
  int ret = iteradp.next_cell();
  EXPECT_NE(OB_SUCCESS, ret);
  ret = iteradp.get_cell(&ci, &irc);
  EXPECT_EQ(OB_ITER_END, ret);
  ret = iteradp.is_row_finished(&irf);
  EXPECT_EQ(OB_ITER_END, ret);

  ret = iteradp.next_cell();
  EXPECT_EQ(OB_ITER_END, ret);
}

TEST(TestObIteratorAdapter, adaptor2adaptor_normal_value)
{
  ObRowDesc row_desc;
  row_desc.add_column_desc(1001, 16);
  row_desc.add_column_desc(1001, 17);
  row_desc.add_column_desc(1001, 101);
  row_desc.add_column_desc(1001, 102);
  row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
  ObRowkeyInfo rki = get_rki();

  ObCellIterAdaptor cia;
  ObRowIterAdaptor ria;
  MockIterator mi;
  MockIterator cr;
  ObCellInfo cell;
  ObObj rk_cells[2];

  cell.table_id_ = 1001;
  rk_cells[0].set_int(1);
  rk_cells[1].set_int(10);
  cell.row_key_.assign(rk_cells, 2);
  cell.column_id_ = 16;
  cell.value_.set_int(1);
  mi.add_cell(cell, true, false);

  cell.table_id_ = 1001;
  rk_cells[0].set_int(1);
  rk_cells[1].set_int(10);
  cell.row_key_.assign(rk_cells, 2);
  cell.column_id_ = 17;
  cell.value_.set_int(10);
  mi.add_cell(cell, false, false);

  cell.table_id_ = 1001;
  rk_cells[0].set_int(1);
  rk_cells[1].set_int(10);
  cell.row_key_.assign(rk_cells, 2);
  cell.column_id_ = 101;
  cell.value_.set_varchar(ObString(2, 2, "v1"));
  mi.add_cell(cell, false, false);
  cr.add_cell(cell, true, false);

  cell.table_id_ = 1001;
  rk_cells[0].set_int(1);
  rk_cells[1].set_int(10);
  cell.row_key_.assign(rk_cells, 2);
  cell.column_id_ = 102;
  cell.value_.set_varchar(ObString(2, 2, "v2"));
  mi.add_cell(cell, false, true);
  cr.add_cell(cell, false, true);

  cell.table_id_ = 1001;
  rk_cells[0].set_int(3);
  rk_cells[1].set_int(30);
  cell.row_key_.assign(rk_cells, 2);
  cell.column_id_ = 16;
  cell.value_.set_int(3);
  mi.add_cell(cell, true, false);

  cell.table_id_ = 1001;
  rk_cells[0].set_int(3);
  rk_cells[1].set_int(30);
  cell.row_key_.assign(rk_cells, 2);
  cell.column_id_ = 17;
  cell.value_.set_int(30);
  mi.add_cell(cell, false, false);

  cell.table_id_ = 1001;
  rk_cells[0].set_int(3);
  rk_cells[1].set_int(30);
  cell.row_key_.assign(rk_cells, 2);
  cell.column_id_ = 101;
  cell.value_.set_varchar(ObString(2, 2, "v1"));
  mi.add_cell(cell, false, false);
  cr.add_cell(cell, true, false);

  cell.table_id_ = 1001;
  rk_cells[0].set_int(3);
  rk_cells[1].set_int(30);
  cell.row_key_.assign(rk_cells, 2);
  cell.column_id_ = 102;
  cell.value_.set_varchar(ObString(2, 2, "v2"));
  mi.add_cell(cell, false, true);
  cr.add_cell(cell, false, true);

  ria.set_cell_iter(&mi, row_desc, false);
  cia.set_row_iter(&ria, rki.get_size(), NULL);

  MockIterator res;
  while (OB_SUCCESS == cia.next_cell())
  {
    ObCellInfo *ci = NULL;
    ObCellInfo *nil = NULL;
    bool irc = false;
    bool ifc = false;
    int ret = cia.get_cell(&ci, &irc);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_NE(nil, ci);
    ret = cia.is_row_finished(&ifc);
    EXPECT_EQ(OB_SUCCESS, ret);
    res.add_cell(*ci, irc, ifc);
  }

  ObCellInfo *co = NULL;
  ObCellInfo *cc = NULL;;
  bool irco = false;
  bool ircc = false;
  bool ifco = false;
  bool ifcc = false;
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < 5; i++)
  {
    ret = res.next_cell();
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = res.get_cell(&cc, &ircc);
    EXPECT_EQ(OB_SUCCESS, ret);
    if (ObExtendType == cc->value_.get_type())
    {
      continue;
    }
    ret = res.is_row_finished(&ifcc);
    EXPECT_EQ(OB_SUCCESS, ret);

    ret = cr.next_cell();
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = cr.get_cell(&co, &irco);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = cr.is_row_finished(&ifco);
    EXPECT_EQ(OB_SUCCESS, ret);

    EXPECT_EQ(co->table_id_, cc->table_id_);
    // 没有set rowkey列不能比较
    //EXPECT_EQ(co->row_key_, cc->row_key_);
    EXPECT_EQ(co->column_id_, cc->column_id_);
    EXPECT_TRUE(co->value_ == cc->value_);
  }
  ret = cr.next_cell();
  EXPECT_EQ(OB_ITER_END, ret);
  ret = res.next_cell();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = res.next_cell();
  EXPECT_EQ(OB_ITER_END, ret);
}

TEST(TestObIteratorAdapter, adaptor2adaptor_single_cell)
{
  ObRowDesc row_desc;
  row_desc.add_column_desc(1001, 16);
  row_desc.add_column_desc(1001, 17);
  row_desc.add_column_desc(1001, 101);
  row_desc.add_column_desc(OB_INVALID_ID, OB_ACTION_FLAG_COLUMN_ID);
  ObRowkeyInfo rki = get_rki();

  ObCellIterAdaptor cia;
  ObRowIterAdaptor ria;
  MockIterator mi;
  MockIterator cr;
  ObCellInfo cell;
  ObObj rk_cells[2];

  cell.table_id_ = 1001;
  rk_cells[0].set_int(1);
  rk_cells[1].set_int(10);
  cell.row_key_.assign(rk_cells, 2);
  cell.column_id_ = 101;
  cell.value_.set_varchar(ObString(2, 2, "v1"));
  mi.add_cell(cell, true, true);
  cr.add_cell(cell, true, true);

  cell.table_id_ = 1001;
  rk_cells[0].set_int(3);
  rk_cells[1].set_int(30);
  cell.row_key_.assign(rk_cells, 2);
  cell.column_id_ = 101;
  cell.value_.set_varchar(ObString(2, 2, "v1"));
  mi.add_cell(cell, true, true);
  cr.add_cell(cell, true, true);

  ria.set_cell_iter(&mi, row_desc, false);
  cia.set_row_iter(&ria, rki.get_size(), NULL);

  MockIterator res;
  while (OB_SUCCESS == cia.next_cell())
  {
    ObCellInfo *ci = NULL;
    ObCellInfo *nil = NULL;
    bool irc = false;
    bool ifc = false;
    int ret = cia.get_cell(&ci, &irc);
    EXPECT_EQ(OB_SUCCESS, ret);
    EXPECT_NE(nil, ci);
    ret = cia.is_row_finished(&ifc);
    EXPECT_EQ(OB_SUCCESS, ret);
    res.add_cell(*ci, irc, ifc);
  }

  ObCellInfo *co = NULL;
  ObCellInfo *cc = NULL;;
  bool irco = false;
  bool ircc = false;
  bool ifco = false;
  bool ifcc = false;
  int ret = OB_SUCCESS;
  for (int64_t i = 0; i < 3; i++)
  {
    ret = res.next_cell();
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = res.get_cell(&cc, &ircc);
    EXPECT_EQ(OB_SUCCESS, ret);
    if (ObExtendType == cc->value_.get_type())
    {
      continue;
    }
    ret = res.is_row_finished(&ifcc);
    EXPECT_EQ(OB_SUCCESS, ret);

    ret = cr.next_cell();
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = cr.get_cell(&co, &irco);
    EXPECT_EQ(OB_SUCCESS, ret);
    ret = cr.is_row_finished(&ifco);
    EXPECT_EQ(OB_SUCCESS, ret);

    EXPECT_EQ(co->table_id_, cc->table_id_);
    //EXPECT_EQ(co->row_key_, cc->row_key_);
    EXPECT_EQ(co->column_id_, cc->column_id_);
    EXPECT_TRUE(co->value_ == cc->value_);
  }
  ret = cr.next_cell();
  EXPECT_EQ(OB_ITER_END, ret);
  ret = res.next_cell();
  EXPECT_EQ(OB_SUCCESS, ret);
  ret = res.next_cell();
  EXPECT_EQ(OB_ITER_END, ret);
}

int main(int argc, char **argv)
{
  ob_init_memory_pool();
  testing::InitGoogleTest(&argc,argv);
  int ret = RUN_ALL_TESTS();
  return ret;
}
