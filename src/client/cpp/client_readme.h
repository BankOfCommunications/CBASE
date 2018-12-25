/**
 * Oceanbase基础值的类型
 */
enum ObObjType
{
  ObMinType = -1,
  ObNullType,   // 空类型
  ObIntType,
  ObFloatType,
  ObDoubleType,
  ObDateTimeType,
  ObPreciseDateTimeType,
  ObVarcharType,
  ObSeqType,
  ObCreateTimeType,
  ObModifyTimeType,
  ObExtendType,
  //add peiouya [DATE_TIME] 20150906:b
  ObDateType,
  ObTimeType,
  //add 20150906:e
  //add lijianqiang [INT_32] 20150930:b
  ObInt32Type,
  //add 20150930:e
  ObMaxType,
};

/**
 * Oceanbase基础值
 */
class ObObj
{
  public:
    void set_int(const int64_t value);
    void set_datetime(const ObDateTime& value);
    void set_precise_datetime(const ObPreciseDateTime& value);
    //add peiouya [DATE_TIME] 20150906:b
    void set_date(const ObDate& value);
    void set_time(const ObTime& value);
    //add 20150906:e
    void set_varchar(const ObString& value);
    void set_null();
    //add lijianqiang [INT_32] 20150930:b
    void set_int32(const int32_t& value);
    //add 20150930:e
    int get_int(int64_t& value) const;
    int get_datetime(ObDateTime& value) const;
    int get_precise_datetime(ObPreciseDateTime& value) const;
    //add peiouya [DATE_TIME] 20150906:b
    int get_date(ObDate& value) const;
    int get_time(ObTime& value) const;
    //add 20150906:e
    //add lijianqiang [INT_32] 20150930:b
    int get_int32(int32_t& value) const;
    //add 20150930:e
    int get_varchar(ObString& value) const;
    ObObjType get_type(void) const;
};

/**
 * Oceanbase字符串
 */
class ObString 
{
  public:
    ObString();
    ObString(const int32_t size, const int32_t length, char* ptr);
    inline int32_t length() const { return data_length_; }
    inline int32_t size() const { return buffer_size_; }
    inline const char* ptr() const { return ptr_; }
    inline char* ptr() { return ptr_; }
};

/**
 * Oceanbase边界类型
 *   表示前闭后闭, 前开后开, 前闭后开, 前开后闭
 *   表示前是否是最小值, 后是否是最大值
 */
class ObBorderFlag
{
  public:
    void set_inclusive_start();
    void set_inclusive_end();
    void set_min_value();
    void set_max_value();
    void unset_inclusive_start();
    void unset_inclusive_end();
    void unset_min_value();
    void unset_max_value();
};

/**
 * Oceanbase Key Range
 *   StartKey + EndKey + ObBorderFlag
 */
struct ObRange 
{
  ObBorderFlag border_flag_;
  ObString start_key_;
  ObString end_key_;
};

/**
 * Oceanbase 版本Range
 *   StartVersion + EndVersion + ObBorderFlag
 */
struct ObVersionRange
{
  ObBorderFlag border_flag_;
  int64_t start_version_;
  int64_t end_version_;
};

/**
 * Scan 方向
 */
enum Order
{
  ASC = 1,
  DESC
};

/**
 * Scan 条件过滤
 */
enum ObLogicOperator
{
  NIL   = 0, /// invalid op
  LT    = 1, /// less than
  LE    = 2, /// less or equal
  EQ    = 3, /// equal with
  GT    = 4, /// greater than
  GE    = 5, /// greater or equal
  NE    = 6, /// not equal
  LIKE  = 7, /// substring
};

/**
 * Scan 聚合函数
 */
enum ObAggregateFuncType
{
  AGG_FUNC_MIN,
  SUM,
  COUNT,
  MAX,
  MIN,
  LISTAGG,//add gaojt [ListAgg][JHOBv0.1]20150104
  AGG_FUNC_END
};

class ObScanParam
{
  public:
    void set(const uint64_t& table_id, const ObString& table_name, const ObRange& range);
    void set_version_range(const ObVersionRange & range);
    int add_column(const ObString& column_name);
    int add_cond(const ObString & column_name, const ObLogicOperator & cond_op, const ObObj & cond_value);
    int add_orderby_column(const ObString & column_name, Order order = ASC);
    int add_groupby_column(const ObString & column_name);
    int add_aggregate_column(const ObString & org_column_name, const ObString & as_column_name, const ObAggregateFuncType  aggregate_func);
    int set_limit_info(const int64_t offset, const int64_t count);
};

struct ObCellInfo
{
  ObString table_name_;
  uint64_t table_id_;
  ObString row_key_;
  uint64_t column_id_;
  ObString column_name_;
  ObObj value_;
};

class ObGetParam
{
  public:
    void set_version_range(const ObVersionRange & range);
    int add_cell(const ObCellInfo& cell_info);
};

class ObMutator
{
  public:
    int use_ob_sem();
    int use_db_sem();
    int update(const ObString& table_name, const ObString& row_key,
        const ObString& column_name, const ObObj& value);
    int insert(const ObString& table_name, const ObString& row_key,
        const ObString& column_name, const ObObj& value);
    int del_row(const ObString& table_name, const ObString& row_key);
};

class ObScanner
{
  public:
    /**
     * Go to next cell
     * 第一次get_cell之前必须要调用next_cell
     * @retval OB_ITER_END iterate end
     * @retval OB_SUCCESS go to the next cell succ
     */
    int next_cell();
    /**
     * Get current cell
     * 第一次get_cell之前必须要调用next_cell
     */
    int get_cell(ObCellInfo** cell);
    int get_cell(ObCellInfo** cell, bool* is_row_changed);
    /**
     * 重置游标到最开始
     */
    int reset_iter();
    /**
     * @param is_fullfilled 传出参数, 上次请求结果是否全部返回
     * @param fullfilled_item_num 传出参数, 本次请求结果cell个数
     */
    int get_is_req_fullfilled(bool &is_fullfilled, int64_t &fullfilled_item_num) const;
    bool is_empty() const;
    int64_t get_data_version() const
};

enum ObOperationCode
{
  OB_GET = 1,
  OB_SCAN = 2,
  OB_MUTATE = 3
};

struct ObReq          /// OB异步请求描述结构
{
  int32_t opcode;    /// operation code
  int32_t flags;     /// 是否使用下面的事件通知fd
  int32_t resfd;     /// 进行事件通知的fd
  int32_t req_id;
  void *data;        /// 用户自定数据域
  void *arg;         /// 用户自定数据域
  int32_t res;       /// 返回值
  int32_t padding;
  union
  {
    ObGetParam *get_param;
    ObScanParam *scan_param;
    ObMutator *mut;
  };
  ObScanner *scanner;/// 返回结果
};

class ObClient
{
public:
  /**
   * ObClient初始化
   * @param rs_addr Rootserver地址
   * @param rs_port Rootserver端口号
   * @param max_req 最大并发数
   */
  int init(const char* rs_addr, const int rs_port, int max_req = 1);

  /**
   * 执行请求, 同步调用
   * @param req 请求
   * @param timeout 超时(us)
   */
  int exec(ObReq *req, const int64_t timeout);

  /**
   * 提交异步请求
   * @param reqs_num 异步请求个数
   * @param reqs 异步请求数组
   */
  int submit(int reqs_num, ObReq *reqs[]);
  /**
   * 取消异步请求
   * @param reqs_num 异步请求个数
   * @param reqs 异步请求数组
   */
  int cancel(int reqs_num, ObReq *reqs[]);
  /**
   * 获取异步结果
   *   当min_num=0或者timeout=0时, 该函数不会阻塞
   *   当timeout=-1时, 表示等待时间是无限长, 直到满足条件
   * @param min_num 本次获取的最少异步完成事件个数
   * @param min_num 本次获取的最多异步完成事件个数
   * @param timeout 超时(us)
   * @param num 输出参数, 异步返回结果个数
   * @param reqs 输出参数, 异步返回结果数组
   */
  int get_results(int min_num, int max_num, int64_t timeout, int64_t &num, ObReq* reqs[]);

  /**
   * 获取get请求的req
   */
  int acquire_get_req(ObReq* &req);
  /**
   * 获取scan请求的req
   */
  int acquire_scan_req(ObReq* &req);
  /**
   * 获取mutate请求的req
   */
  int acquire_mutate_req(ObReq* &req);
  /**
   * 释放req
   */
  int release_req(ObReq* req);
  
  /**
   * 设置事件通知fd
   */
  void set_resfd(int32_t fd, ObReq* cb);
};

/**
 * Oceanbase错误码
 * 下面是摘抄, 全部错误码见ob_define.h
 */
const int OB_SUCCESS = EXIT_SUCCESS;
const int OB_ERROR = EXIT_FAILURE;
const int OB_ITER_END = -8;
const int OB_RESPONSE_TIME_OUT = -12;
const int OB_EAGAIN = -23;

