#include "ob_sql_list.h"
#include "tblog.h"

int ob_sql_list_init(ObSQLList *list, ObSQLListType type)
{
  int ret = OB_SQL_SUCCESS;
  if (NULL == list)
  {
    TBSYS_LOG(ERROR, "invalide argument list is %p", list);
    ret = OB_SQL_ERROR;
  }
  else
  {
    list->type_ = type;
    list->size_ = 0;
    list->head_ = NULL;
    list->tail_ = NULL;
  }
  return ret;
}

int ob_sql_list_add_before(ObSQLList *list, ObSQLListNode *pos, ObSQLListNode *node)
{
  int ret = OB_SQL_SUCCESS;
  if (NULL == list || NULL == node)
  {
    TBSYS_LOG(ERROR, "invalide argument list is %p, node is %p", list, node);
    ret = OB_SQL_ERROR;
  }
  else
  {
    if (NULL == list->head_ && NULL == list->tail_) //add first node
    {
      node->next_ = NULL;
      node->prev_ = NULL;
      list->head_ = node;
      list->tail_ = node;
      list->size_ = 1;
    }
    else
    {
      node->next_ = pos;
      node->prev_ = pos->prev_;
      if (NULL == pos->prev_) //add befor head node
      {
        list->head_ = node;
      }
      else
      {
        pos->prev_->next_ = pos;
      }
      pos->prev_ = node;
      list->size_++;
    }
  }
  return ret;
}

int ob_sql_list_add_after(ObSQLList *list, ObSQLListNode *pos, ObSQLListNode *node)
{
  int ret = OB_SQL_SUCCESS;
  if (NULL == list || NULL == node)
  {
    TBSYS_LOG(ERROR, "invalide argument list is %p, node is %p", list, node);
    ret = OB_SQL_ERROR;
  }
  else
  {
    if (NULL == list->head_ && NULL == list->tail_) //add first node
    {
      node->next_ = NULL;
      node->prev_ = NULL;
      list->head_ = node;
      list->tail_ = node;
      list->size_ = 1;
    }
    else
    {
      node->prev_ = pos;
      node->next_ = pos->next_;
      if (NULL == pos->next_) //add after tail node
      {
        list->tail_ = node;
      }
      else
      {
        pos->next_->prev_ = pos;
      }
      pos->next_ = node;
      list->size_++;
    }
  }
  return ret;
}

int ob_sql_list_add_head(ObSQLList *list, ObSQLListNode *node)
{
  int ret = OB_SQL_SUCCESS;

  if (NULL == list || NULL == node)
  {
    TBSYS_LOG(ERROR, "invalid argument list is %p, node is %p", list, node);
    ret = OB_SQL_ERROR;
  }
  else
  {
    ret = ob_sql_list_add_before(list, list->head_, node);
  }

  return ret;
}

int ob_sql_list_add_tail(ObSQLList *list, ObSQLListNode *node)
{
  int ret = OB_SQL_SUCCESS;

  if (NULL == list || NULL == node)
  {
    TBSYS_LOG(ERROR, "invalid argument list is %p, node is %p", list, node);
    ret = OB_SQL_ERROR;
  }
  else
  {
    ret = ob_sql_list_add_after(list, list->tail_, node);
  }

  return ret;
}

int ob_sql_list_del(ObSQLList *list, ObSQLListNode *node)
{
  int ret = OB_SQL_SUCCESS;
  if (NULL == list || NULL == node)
  {
    TBSYS_LOG(ERROR, "invalid argument list is %p, node is %p", list, node);
    ret = OB_SQL_ERROR;
  }
  else
  {
    if (NULL == node->prev_ && NULL == node->next_) // the only node in the list
    {
      list->head_ = NULL;
      list->tail_ = NULL;
    }
    else
    {
      if (NULL == node->prev_)
      {
        list->head_ = node->next_;
        node->next_->prev_ = NULL;
      }
      else if (NULL == node->next_)
      {
        list->tail_ = node->prev_;
        node->prev_->next_ = NULL;
      }
      else
      {
        node->next_->prev_ = node->prev_;
        node->prev_->next_ = node->next_;
      }
    }
    list->size_ --;
  }
  return ret;
}

int ob_sql_list_empty(ObSQLList &list)
{
  int ret = 0;
  if (NULL == list.head_ && NULL == list.tail_)
  {
    ret = 1;
  }
  return ret;
}
