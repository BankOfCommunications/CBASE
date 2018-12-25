#include "ob_tablet_location_cache.h"
#include "ob_tablet_location_list.h"

using namespace oceanbase::common;

ObTabletLocationCache::ObTabletLocationCache()
{
  init_ = false;
  cache_timeout_ = DEFAULT_ALIVE_TIMEOUT;
  special_cache_timeout_ = DEFAULT_ALIVE_TIMEOUT;
}

ObTabletLocationCache::~ObTabletLocationCache()
{
}

int ObTabletLocationCache::init(const uint64_t mem_size, const uint64_t count,
    const int64_t timeout, const int64_t special_timeout)
{
  int ret = OB_SUCCESS;
  if (init_)
  {
    TBSYS_LOG(ERROR, "%s", "check cache already inited");
    ret = OB_INNER_STAT_ERROR;
  }
  else if ((timeout <= 0) || (special_timeout <= 0))
  {
    TBSYS_LOG(ERROR, "check cache timeout failed:timeout[%ld], vtimeout[%ld]",
        timeout, special_timeout);
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    cache_timeout_ = timeout;
    special_cache_timeout_ = special_timeout;
    // cache init cache max mem size and init item count
    ret = tablet_cache_.init(mem_size, timeout, static_cast<int32_t>(count));
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "cache init failed:mem[%lu], count[%lu], timeo[%ld], ret[%d]",
          mem_size, count, timeout, ret);
    }
    else
    {
      init_ = true;
    }
  }
  return ret;
}


int ObTabletLocationCache::get(const uint64_t table_id, const ObRowkey & rowkey,
    ObTabletLocationList & location)
{
  int ret = OB_SUCCESS;
  if (!init_)
  {
    TBSYS_LOG(ERROR, "%s", "check init failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else if ((0 == rowkey.length()) || (NULL == rowkey.ptr()))
  {
    TBSYS_LOG(ERROR, "%s", "check rowkey length failed");
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    ObNewRange range;
    range.table_id_ = table_id;
    range.start_key_ = rowkey;
    range.end_key_ = rowkey;
    /*
    int64_t size = range.get_serialize_size();
    char * temp = (char *)ob_tc_malloc(size, ObModIds::OB_MS_LOCATION_CACHE);
    if (NULL == temp)
    {
      TBSYS_LOG(ERROR, "check ob malloc failed:size[%lu], pointer[%p]", size, temp);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      int64_t pos = 0;
      ret = range.serialize(temp, size, pos);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "serialize the search range failed:ret[%d]", ret);
      }
    }
    */

    //if (OB_SUCCESS == ret)
    //{
      //ObString CacheKey;
      //CacheKey.assign(temp, static_cast<int32_t>(size));
      ObCachePair pair;
      //ret = tablet_cache_.get(CacheKey, pair);
      ret = tablet_cache_.get(range, pair);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(DEBUG, "find tablet from cache failed:table_id[%lu]",
            table_id);
      }
      else
      {
        int64_t pos = 0;
        // TODO double check pair.key whether as equal with CacheKey
        ret = location.deserialize(pair.get_value().ptr(), pair.get_value().length(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize location failed:table_id[%lu], length[%ld], "
              "ret[%d]", table_id, rowkey.length(), ret);
        }
      }
    //}
    // destory the temp buffer
   // ob_tc_free(temp);
  }
  return ret;
}


int ObTabletLocationCache::set(const ObNewRange & range, const ObTabletLocationList & location)
{
  int ret = OB_SUCCESS;
  if (!init_)
  {
    TBSYS_LOG(ERROR, "%s", "check init failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ObCachePair pair;
    // malloc the pair mem buffer
    int64_t key_size = range.get_serialize_size();
    int64_t value_size = location.get_serialize_size();
    ret = tablet_cache_.malloc(pair, static_cast<int32_t>(key_size), static_cast<int32_t>(value_size));
    if (OB_SUCCESS != ret)
    {
      TBSYS_LOG(ERROR, "check malloc failed:key_size[%ld], ret[%d]", key_size, ret);
    }

    int64_t pos = 0;
    // key serialize to the pair
    if (OB_SUCCESS == ret)
    {
      ret = range.serialize(pair.get_key().ptr(), key_size, pos);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "serialize range failed:ret[%d]", ret);
      }
    }

    // value serialize to the pair
    if (OB_SUCCESS == ret)
    {
      pos = 0;
      ret = location.serialize(pair.get_value().ptr(), value_size, pos);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "serialize locationlist failed:ret[%d]", ret);
      }
    }

    // delete the old cache item
    if (OB_SUCCESS == ret)
    {
      ret = del(range.table_id_, range.end_key_);
      if ((ret != OB_SUCCESS) && (ret != OB_ENTRY_NOT_EXIST))
      {
        TBSYS_LOG(WARN, "del the old item:table[%lu], ret[%d]", range.table_id_, ret);
      }
      else
      {
        ret = OB_SUCCESS;
      }
    }

    // add new pair and return the old pair for deletion
    if (OB_SUCCESS == ret)
    {
      ObCachePair oldpair;
      ret = tablet_cache_.add(pair, oldpair);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(ERROR, "add the pair failed:ret[%d]", ret);
      }
      else
      {
        TBSYS_LOG(DEBUG, "%s", "set tablet location cache succ");
      }
    }
  }
  return ret;
}

int ObTabletLocationCache::update(const uint64_t table_id, const ObRowkey & rowkey,
    const ObTabletLocationList & location)
{
  int ret = OB_SUCCESS;
  if (!init_)
  {
    TBSYS_LOG(ERROR, "%s", "check init failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else if ((0 == rowkey.length()) || (NULL == rowkey.ptr()))
  {
    TBSYS_LOG(ERROR, "%s", "check rowkey length failed");
    ret = OB_INPUT_PARAM_ERROR;
  }
  else
  {
    ObNewRange range;
    range.table_id_ = table_id;
    range.start_key_ = rowkey;
    range.end_key_ = rowkey;
    /*
    int64_t size = range.get_serialize_size();
    char * temp = (char *)ob_tc_malloc(size, ObModIds::OB_MS_LOCATION_CACHE);
    if (NULL == temp)
    {
      TBSYS_LOG(ERROR, "check ob malloc failed:size[%lu], pointer[%p]", size, temp);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      int64_t pos = 0;
      ret = range.serialize(temp, size, pos);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "serialize the search range failed:ret[%d]", ret);
      }
    }
    */

    //if (OB_SUCCESS == ret)
    //{
      //ObString CacheKey;
      //CacheKey.assign(temp, static_cast<int32_t>(size));
      ObCachePair pair;
      //ret = tablet_cache_.get(CacheKey, pair);
      ret = tablet_cache_.get(range, pair);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(DEBUG, "find tablet from cache failed:table_id[%lu]",
            table_id);
      }
      else
      {
        int64_t pos = 0;
        // TODO double check pair.key whether as equal with CacheKey
        ObObj start_rowkey_obj_array1[OB_MAX_ROWKEY_COLUMN_NUMBER];
        ObObj end_rowkey_obj_array1[OB_MAX_ROWKEY_COLUMN_NUMBER];
        range.start_key_.assign(start_rowkey_obj_array1, OB_MAX_ROWKEY_COLUMN_NUMBER);
        range.end_key_.assign(end_rowkey_obj_array1, OB_MAX_ROWKEY_COLUMN_NUMBER);
        ret = range.deserialize(pair.get_key().ptr(), pair.get_key().length(), pos);
        if (OB_SUCCESS != ret)
        {
          TBSYS_LOG(ERROR, "deserialize tablet range failed:table_id[%lu], ret[%d]",
              table_id, ret);
        }
        else
        {
          ret = set(range, location);
        }
      }
    //}
    //ob_tc_free(temp);
  }
  return ret;
}


int ObTabletLocationCache::del(const uint64_t table_id, const ObRowkey & rowkey)
{
  int ret = OB_SUCCESS;
  if (!init_)
  {
    TBSYS_LOG(ERROR, "%s", "check init failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ObNewRange range;
    range.table_id_ = table_id;
    range.start_key_ = rowkey;
    range.end_key_ = rowkey;
    /*
    int64_t size = range.get_serialize_size();
    char * temp = (char *)ob_tc_malloc(size, ObModIds::OB_MS_LOCATION_CACHE);
    if (NULL == temp)
    {
      TBSYS_LOG(ERROR, "check ob malloc failed:size[%lu], pointer[%p]", size, temp);
      ret = OB_ALLOCATE_MEMORY_FAILED;
    }
    else
    {
      int64_t pos = 0;
      ret = range.serialize(temp, size, pos);
      if (ret != OB_SUCCESS)
      {
        TBSYS_LOG(WARN, "serialize the search range failed:ret[%d]", ret);
      }
    }
    */
    //if (OB_SUCCESS == ret)
    //{
      //ObString CacheKey;
      //CacheKey.assign(temp, static_cast<int32_t>(size));
      ObCachePair pair;
      //ret = tablet_cache_.remove(CacheKey);
      ret = tablet_cache_.remove(range);
      if (OB_SUCCESS != ret)
      {
        TBSYS_LOG(DEBUG, "find this row tablet failed:table_id[%lu]",
            table_id);
      }
      else
      {
        TBSYS_LOG(DEBUG, "%s", "del this location from cache succ");
      }
    //}
    // destory the temp buffer
    //ob_tc_free(temp);
  }
  return ret;
}

uint64_t ObTabletLocationCache::size(void)
{
  return tablet_cache_.get_cache_item_num();
}

int ObTabletLocationCache::clear(void)
{
  int ret = OB_SUCCESS;
  if (!init_)
  {
    TBSYS_LOG(ERROR, "%s", "check init failed");
    ret = OB_INNER_STAT_ERROR;
  }
  else
  {
    ret = tablet_cache_.clear();
  }
  return ret;
}
void ObTabletLocationCache::dump(void)
{
  TBSYS_LOG(INFO, "[dump]tablet location cache size %lu", size());
}


