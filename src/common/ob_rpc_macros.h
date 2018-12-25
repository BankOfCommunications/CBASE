/*
 * (C) 2007-2010 Alibaba Group Holding Limited.
 *
 * This program is free software; you can redistribute it and/or
 * modify it under the terms of the GNU General Public License 
 * version 2 as published by the Free Software Foundation. 
 *
 *         ob_rpc_macros.h is for what ...
 *
 *  Version: $Id: ob_rpc_macros.h 11/15/2012 10:29:31 AM qushan Exp $
 *
 *  Authors:
 *     qushan < qushan@taobao.com >
 *        - some work details if you want
 */



// -----------------------------------------------------------
// macro definitions


#define DECVAL_1 0
#define DECVAL_2 1
#define DECVAL_3 2
#define DECVAL_4 3
#define DECVAL_5 4
#define DECVAL_6 5
#define DECVAL_7 6
#define DECVAL_8 7
#define DECVAL_9 8
#define DECVAL( n ) DECVAL_##n

#define RPC_FUNC_NAME_JOIN(NUM_ARG, NUM_RESULT) RPC_FUNC_NAME_DO_JOIN(NUM_ARG, NUM_RESULT)
#define RPC_FUNC_NAME_DO_JOIN(NUM_ARG, NUM_RESULT) RPC_FUNC_NAME_DO_JOIN2(NUM_ARG, NUM_RESULT)
#define RPC_FUNC_NAME_DO_JOIN2(NUM_ARG, NUM_RESULT) send_##NUM_ARG##_return_##NUM_RESULT

#define OCEANBASE_JOIN( X, Y ) OCEANBASE_DO_JOIN( X, Y )
#define OCEANBASE_DO_JOIN( X, Y ) OCEANBASE_DO_JOIN2(X,Y)
#define OCEANBASE_DO_JOIN2( X, Y ) X##Y


#define ARG_TN0 
#define ARG_TN1  typename Arg0
#define ARG_TN2  ARG_TN1, typename Arg1
#define ARG_TN3  ARG_TN2, typename Arg2
#define ARG_TN4  ARG_TN3, typename Arg3
#define ARG_TN5  ARG_TN4, typename Arg4
#define ARG_TN6  ARG_TN5, typename Arg5

#define ARG_PN0 
#define ARG_PN1  const Arg0 & arg0
#define ARG_PN2  ARG_PN1, const Arg1 & arg1
#define ARG_PN3  ARG_PN2, const Arg2 & arg2
#define ARG_PN4  ARG_PN3, const Arg3 & arg3
#define ARG_PN5  ARG_PN4, const Arg4 & arg4
#define ARG_PN6  ARG_PN5, const Arg5 & arg5

#define ARG_AN0  
#define ARG_AN1  arg0
#define ARG_AN2  ARG_AN1, arg1
#define ARG_AN3  ARG_AN2, arg2
#define ARG_AN4  ARG_AN3, arg3
#define ARG_AN5  ARG_AN4, arg4
#define ARG_AN6  ARG_AN5, arg5

#define RT_TN0 
#define RT_TN1   typename Result0
#define RT_TN2   RT_TN1, typename Result1
#define RT_TN3   RT_TN2, typename Result2
#define RT_TN4   RT_TN3, typename Result3
#define RT_TN5   RT_TN4, typename Result4
#define RT_TN6   RT_TN5, typename Result5

#define RT_PN0
#define RT_PN1   Result0 & result0
#define RT_PN2   RT_PN1, Result1 & result1
#define RT_PN3   RT_PN2, Result2 & result2
#define RT_PN4   RT_PN3, Result3 & result3
#define RT_PN5   RT_PN4, Result4 & result4
#define RT_PN6   RT_PN5, Result5 & result5

#define RT_AN0   
#define RT_AN1   result0
#define RT_AN2   RT_AN1, result1
#define RT_AN3   RT_AN2, result2
#define RT_AN4   RT_AN3, result3
#define RT_AN5   RT_AN4, result4
#define RT_AN6   RT_AN5, result5

typedef bool (*errno_sensitive_pt)(const int);

extern errno_sensitive_pt &tc_errno_sensitive_func();

extern bool is_errno_sensitive(const int error);

// -----------------------------------------------------------
// serialize_param_* MACROS
#define SERIALIZE_PARAM_DECLARE(NUM_ARG) \
  template <OCEANBASE_JOIN(ARG_TN, NUM_ARG)> \
int OCEANBASE_JOIN(serialize_param_, NUM_ARG)(ObDataBuffer & data_buffer, OCEANBASE_JOIN(ARG_PN, NUM_ARG)) 

#define SERIALIZE_PARAM_DEFINE(NUM_ARG) \
  template <OCEANBASE_JOIN(ARG_TN, NUM_ARG)> \
int OCEANBASE_JOIN(serialize_param_, NUM_ARG)(ObDataBuffer & data_buffer, OCEANBASE_JOIN(ARG_PN, NUM_ARG)) \
{ \
  int ret = OB_SUCCESS; \
  if (OB_SUCCESS != (ret = OCEANBASE_JOIN(serialize_param_, DECVAL(NUM_ARG))( \
          data_buffer, OCEANBASE_JOIN(ARG_AN, DECVAL(NUM_ARG))))) \
  { \
    TBSYS_LOG(WARN, "serialize_param args error. ret:%d, buffer cap:%ld, pos:%ld", \
        ret, data_buffer.get_capacity(), data_buffer.get_position()); \
  } \
  else if (OB_SUCCESS != (ret = serialize_param(data_buffer, OCEANBASE_JOIN(arg, DECVAL(NUM_ARG))))) \
  { \
    TBSYS_LOG(WARN, "serialize_param argn error. ret:%d, buffer cap:%ld, pos:%ld", \
        ret, data_buffer.get_capacity(), data_buffer.get_position()); \
  } \
  return ret; \
}

// -----------------------------------------------------------
// deserialize_result_* MACROS
#define DESERIALIZE_RESULT_DEFINE(NUM_RESULT) \
  template <OCEANBASE_JOIN(RT_TN, NUM_RESULT)> \
int OCEANBASE_JOIN(deserialize_result_, NUM_RESULT)(const ObDataBuffer & data_buffer, \
    int64_t & pos, ObResultCode & rc, OCEANBASE_JOIN(RT_PN, NUM_RESULT)) \
{ \
  int ret = OB_SUCCESS; \
  if (OB_SUCCESS != (ret = OCEANBASE_JOIN(deserialize_result_, DECVAL(NUM_RESULT)) ( \
          data_buffer, pos, rc, OCEANBASE_JOIN(RT_AN, DECVAL(NUM_RESULT))))) \
  { \
    TBSYS_LOG(WARN, "deserialize rets error. ret:%d, buffer length:%ld, pos:%ld", \
        ret, data_buffer.get_position(), pos); \
  } \
  else if (OB_SUCCESS != (ret = deserialize_result(data_buffer, pos, \
          OCEANBASE_JOIN(result, DECVAL(NUM_RESULT))))) \
  { \
    TBSYS_LOG(WARN, "deserialize retn error. ret:%d, buffer length:%ld, pos:%ld", \
        ret, data_buffer.get_position(), pos); \
  } \
  return ret; \
}

// -----------------------------------------------------------
// send_param_* MACROS
#define DO_SEND_PARAM_DECLARE(NUM_ARG, ARG_TNS, ARG_PNS) \
  template <ARG_TNS> \
int OCEANBASE_JOIN(send_param_, NUM_ARG)(ObDataBuffer & data_buffer, \
    const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, ARG_PNS) const

#define SEND_PARAM_DECLARE(NUM_ARG) \
  DO_SEND_PARAM_DECLARE( NUM_ARG,  OCEANBASE_JOIN(ARG_TN, NUM_ARG), OCEANBASE_JOIN(ARG_PN, NUM_ARG))

#define DO_SEND_PARAM_DEFINE(NUM_ARG, ARG_TNS, ARG_PNS, ARG_ANS) \
  template <ARG_TNS> \
int ObRpcStub::OCEANBASE_JOIN(send_param_, NUM_ARG)(ObDataBuffer & data_buffer, \
    const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, ARG_PNS) const \
{ \
  int ret = OB_SUCCESS; \
  if (OB_SUCCESS != (ret = get_rpc_buffer(data_buffer))) \
  { \
    TBSYS_LOG(WARN, "get_rpc_buffer error with rpc call, ret =%d", ret);\
  } \
  else if (OB_SUCCESS != (ret = OCEANBASE_JOIN(serialize_param_, NUM_ARG)(data_buffer, ARG_ANS))) \
  { \
    TBSYS_LOG(WARN, "serialize_param error."); \
  } \
  else if (OB_SUCCESS != (ret = rpc_frame_->send_request( \
          server, pcode, version, timeout, data_buffer))) \
  { \
    TBSYS_LOG(WARN, "send_request error, ret:%d, server=%s, pcode=%d, version=%d, timeout=%ld", \
        ret, to_cstring(server), pcode, version, timeout); \
  } \
  return ret; \
}

#define SEND_PARAM_DEFINE(NUM_ARG) \
  DO_SEND_PARAM_DEFINE( NUM_ARG,  OCEANBASE_JOIN(ARG_TN, NUM_ARG), \
      OCEANBASE_JOIN(ARG_PN, NUM_ARG), OCEANBASE_JOIN(ARG_AN, NUM_ARG))

#define SEND_PARAM_DECLARE_0 \
  int OCEANBASE_JOIN(send_param_, 0)(ObDataBuffer & data_buffer, \
      const ObServer& server, const int64_t timeout, \
      const int32_t pcode, const int32_t version) const

#define SEND_PARAM_DEFINE_0 \
int ObRpcStub::OCEANBASE_JOIN(send_param_, 0)(ObDataBuffer & data_buffer, \
    const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version) const \
{ \
  int ret = OB_SUCCESS; \
  if (OB_SUCCESS != (ret = get_rpc_buffer(data_buffer))) \
  { \
    TBSYS_LOG(WARN, "get_rpc_buffer error with rpc call, ret =%d", ret);\
  } \
  else if (OB_SUCCESS != (ret = rpc_frame_->send_request( \
          server, pcode, version, timeout, data_buffer))) \
  { \
    TBSYS_LOG(WARN, "send_request error, ret=%d, server=%s, pcode=%d, version=%d, timeout=%ld", \
        ret, to_cstring(server), pcode, version, timeout); \
  } \
  return ret; \
}

// -----------------------------------------------------------
// post_request_* MACROS
#define DO_POST_REQUEST_DECLARE(NUM_ARG, ARG_TNS, ARG_PNS) \
  template <ARG_TNS> \
int OCEANBASE_JOIN(post_request_, NUM_ARG)( \
    const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, \
    easy_io_process_pt handler, void* args, ARG_PNS) const

#define POST_REQUEST_DECLARE(NUM_ARG) \
  DO_POST_REQUEST_DECLARE( NUM_ARG,  OCEANBASE_JOIN(ARG_TN, NUM_ARG), OCEANBASE_JOIN(ARG_PN, NUM_ARG))

#define DO_POST_REQUEST_DEFINE(NUM_ARG, ARG_TNS, ARG_PNS, ARG_ANS) \
  template <ARG_TNS> \
int ObRpcStub::OCEANBASE_JOIN(post_request_, NUM_ARG)( \
    const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, \
    easy_io_process_pt handler,  void* args, ARG_PNS) const \
{ \
  int ret = OB_SUCCESS; \
  ObDataBuffer data_buffer; \
  if (OB_SUCCESS != (ret = get_rpc_buffer(data_buffer))) \
  { \
    TBSYS_LOG(WARN, "get_rpc_buffer error with rpc call, ret =%d", ret);\
  } \
  else if (OB_SUCCESS != (ret = OCEANBASE_JOIN(serialize_param_, NUM_ARG)(data_buffer, ARG_ANS))) \
  { \
    TBSYS_LOG(WARN, "serialize_param error."); \
  } \
  else if (OB_SUCCESS != (ret = rpc_frame_->post_request( \
          server, pcode, version, timeout, data_buffer, handler, args))) \
  { \
    TBSYS_LOG(WARN, "send_request error, ret=%d, server=%s, pcode=%d, version=%d, timeout=%ld", \
        ret, to_cstring(server), pcode, version, timeout); \
  } \
  return ret; \
}

#define POST_REQUEST_DEFINE(NUM_ARG) \
  DO_POST_REQUEST_DEFINE( NUM_ARG,  OCEANBASE_JOIN(ARG_TN, NUM_ARG), \
      OCEANBASE_JOIN(ARG_PN, NUM_ARG), OCEANBASE_JOIN(ARG_AN, NUM_ARG))

// -----------------------------------------------------------
// RPC CALL MACROS
#define DO_RPC_CALL_DECLARE_RT_0_NO_RC(NUM_ARG, ARG_TNS, ARG_PNS) \
  template <ARG_TNS> \
int RPC_FUNC_NAME_JOIN(NUM_ARG, 0)(const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, ARG_PNS) const

#define DO_RPC_CALL_DECLARE_RT_0_WITH_RC(NUM_ARG, ARG_TNS, ARG_PNS) \
  template <ARG_TNS> \
int RPC_FUNC_NAME_JOIN(NUM_ARG, 0)(const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, ObResultCode & rc, ARG_PNS) const

#define RPC_CALL_DECLARE_RT_0(NUM_ARG) \
  DO_RPC_CALL_DECLARE_RT_0_NO_RC( NUM_ARG,  OCEANBASE_JOIN(ARG_TN, NUM_ARG), OCEANBASE_JOIN(ARG_PN, NUM_ARG) ); \
  DO_RPC_CALL_DECLARE_RT_0_WITH_RC( NUM_ARG,  OCEANBASE_JOIN(ARG_TN, NUM_ARG), OCEANBASE_JOIN(ARG_PN, NUM_ARG) ) ;


#define DO_RPC_CALL_DECLARE_SND_0_NO_RC(NUM_RESULT, RT_TNS, RT_PNS) \
  template <RT_TNS> \
int RPC_FUNC_NAME_JOIN(0, NUM_RESULT)(const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, RT_PNS) const

#define DO_RPC_CALL_DECLARE_SND_0_WITH_RC(NUM_RESULT, RT_TNS, RT_PNS) \
  template <RT_TNS> \
int RPC_FUNC_NAME_JOIN(0, NUM_RESULT)(const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, ObResultCode & rc, RT_PNS) const

#define RPC_CALL_DECLARE_SND_0(NUM_RESULT) \
  DO_RPC_CALL_DECLARE_SND_0_NO_RC( NUM_RESULT, OCEANBASE_JOIN(RT_TN, NUM_RESULT), OCEANBASE_JOIN(RT_PN, NUM_RESULT) ) ; \
  DO_RPC_CALL_DECLARE_SND_0_WITH_RC( NUM_RESULT, OCEANBASE_JOIN(RT_TN, NUM_RESULT), OCEANBASE_JOIN(RT_PN, NUM_RESULT) ) ;

#define DO_RPC_CALL_DECLARE_NO_RC(NUM_ARG, NUM_RESULT, ARG_TNS, RT_TNS, ARG_PNS, RT_PNS) \
  template <ARG_TNS, RT_TNS> \
int RPC_FUNC_NAME_JOIN(NUM_ARG, NUM_RESULT)(const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, ARG_PNS, RT_PNS) const

#define DO_RPC_CALL_DECLARE_WITH_RC(NUM_ARG, NUM_RESULT, ARG_TNS, RT_TNS, ARG_PNS, RT_PNS) \
  template <ARG_TNS, RT_TNS> \
int RPC_FUNC_NAME_JOIN(NUM_ARG, NUM_RESULT)(const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, ObResultCode &rc, ARG_PNS, RT_PNS) const

#define RPC_CALL_DECLARE_NO_RC(NUM_ARG, NUM_RESULT) \
  DO_RPC_CALL_DECLARE_NO_RC( NUM_ARG, NUM_RESULT, \
      OCEANBASE_JOIN(ARG_TN, NUM_ARG), OCEANBASE_JOIN(RT_TN, NUM_RESULT), \
      OCEANBASE_JOIN(ARG_PN, NUM_ARG), OCEANBASE_JOIN(RT_PN, NUM_RESULT) )

#define RPC_CALL_DECLARE_WITH_RC(NUM_ARG, NUM_RESULT) \
  DO_RPC_CALL_DECLARE_WITH_RC( NUM_ARG, NUM_RESULT, \
      OCEANBASE_JOIN(ARG_TN, NUM_ARG), OCEANBASE_JOIN(RT_TN, NUM_RESULT), \
      OCEANBASE_JOIN(ARG_PN, NUM_ARG), OCEANBASE_JOIN(RT_PN, NUM_RESULT) )

#define RPC_CALL_DECLARE(NUM_ARG, NUM_RESULT) \
  RPC_CALL_DECLARE_NO_RC(NUM_ARG, NUM_RESULT); \
  RPC_CALL_DECLARE_WITH_RC(NUM_ARG, NUM_RESULT); 


#define DO_RPC_CALL_DEFINE_NO_RC(NUM_ARG, NUM_RESULT, ARG_TNS, RT_TNS, ARG_PNS, RT_PNS, ARG_ANS, RT_ANS) \
  template <ARG_TNS, RT_TNS> \
int ObRpcStub::RPC_FUNC_NAME_JOIN(NUM_ARG, NUM_RESULT)(const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, ARG_PNS, RT_PNS) const \
{ \
  int ret = OB_SUCCESS; \
  ObResultCode rc; \
  ObDataBuffer data_buffer; \
  ret = RPC_FUNC_NAME_JOIN(NUM_ARG, NUM_RESULT) (server, timeout, pcode, version, rc, ARG_ANS, RT_ANS); \
  return ret; \
}

#define DO_RPC_CALL_DEFINE_WITH_RC(NUM_ARG, NUM_RESULT, ARG_TNS, RT_TNS, ARG_PNS, RT_PNS, ARG_ANS, RT_ANS) \
  template <ARG_TNS, RT_TNS> \
int ObRpcStub::RPC_FUNC_NAME_JOIN(NUM_ARG, NUM_RESULT)(const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, ObResultCode & rc, ARG_PNS, RT_PNS) const \
{ \
  int ret = OB_SUCCESS; \
  int64_t pos = 0; \
  ObDataBuffer data_buffer; \
  if (OB_SUCCESS != (ret = OCEANBASE_JOIN(send_param_, NUM_ARG)( \
          data_buffer, server, timeout, pcode, version, ARG_ANS))) \
  { \
    TBSYS_LOG(WARN, "send_param error, ret=%d, server=%s, pcode=%d, version=%d, timeout=%ld", \
        ret, to_cstring(server), pcode, version, timeout); \
  } \
  else if (OB_SUCCESS != (ret = OCEANBASE_JOIN(deserialize_result_, NUM_RESULT)( \
          data_buffer, pos, rc, RT_ANS)) \
          && is_errno_sensitive(ret)) \
  { \
    TBSYS_LOG(WARN, "deserialize_result error, ret=%d, server=%s, pcode=%d, version=%d, timeout=%ld", \
        ret, to_cstring(server), pcode, version, timeout); \
  } \
  return ret; \
}

#define RPC_CALL_DEFINE_NO_RC(NUM_ARG, NUM_RESULT) \
  DO_RPC_CALL_DEFINE_NO_RC ( NUM_ARG, NUM_RESULT, \
      OCEANBASE_JOIN(ARG_TN, NUM_ARG), OCEANBASE_JOIN(RT_TN, NUM_RESULT), \
      OCEANBASE_JOIN(ARG_PN, NUM_ARG), OCEANBASE_JOIN(RT_PN, NUM_RESULT), \
      OCEANBASE_JOIN(ARG_AN, NUM_ARG), OCEANBASE_JOIN(RT_AN, NUM_RESULT) )

#define RPC_CALL_DEFINE_WITH_RC(NUM_ARG, NUM_RESULT) \
  DO_RPC_CALL_DEFINE_WITH_RC ( NUM_ARG, NUM_RESULT, \
      OCEANBASE_JOIN(ARG_TN, NUM_ARG), OCEANBASE_JOIN(RT_TN, NUM_RESULT), \
      OCEANBASE_JOIN(ARG_PN, NUM_ARG), OCEANBASE_JOIN(RT_PN, NUM_RESULT), \
      OCEANBASE_JOIN(ARG_AN, NUM_ARG), OCEANBASE_JOIN(RT_AN, NUM_RESULT) )

#define RPC_CALL_DEFINE(NUM_ARG, NUM_RESULT) \
  RPC_CALL_DEFINE_NO_RC(NUM_ARG, NUM_RESULT); \
  RPC_CALL_DEFINE_WITH_RC(NUM_ARG, NUM_RESULT); 


#define DO_RPC_CALL_DEFINE_RT_0_WITH_RC(NUM_ARG, ARG_TNS, ARG_PNS, ARG_ANS) \
  template <ARG_TNS> \
int ObRpcStub::RPC_FUNC_NAME_JOIN(NUM_ARG, 0)(const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, ObResultCode &rc, ARG_PNS) const \
{ \
  int ret = OB_SUCCESS; \
  int64_t pos = 0; \
  ObDataBuffer data_buffer; \
  if (OB_SUCCESS != (ret = OCEANBASE_JOIN(send_param_, NUM_ARG)( \
          data_buffer, server, timeout, pcode, version, ARG_ANS))) \
  { \
  } \
  else if (OB_SUCCESS != (ret = OCEANBASE_JOIN(deserialize_result_, 0)( \
          data_buffer, pos, rc))) \
  { \
  } \
  return ret; \
}

#define DO_RPC_CALL_DEFINE_RT_0_NO_RC(NUM_ARG, ARG_TNS, ARG_PNS, ARG_ANS) \
  template <ARG_TNS> \
int ObRpcStub::RPC_FUNC_NAME_JOIN(NUM_ARG, 0)(const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, ARG_PNS) const \
{ \
  ObResultCode rc; \
  return RPC_FUNC_NAME_JOIN(NUM_ARG, 0)(server, timeout, pcode, version, rc, ARG_ANS); \
}


#define RPC_CALL_DEFINE_RT_0(NUM_ARG) \
  DO_RPC_CALL_DEFINE_RT_0_WITH_RC ( NUM_ARG, \
      OCEANBASE_JOIN(ARG_TN, NUM_ARG),  \
      OCEANBASE_JOIN(ARG_PN, NUM_ARG),  \
      OCEANBASE_JOIN(ARG_AN, NUM_ARG) ); \
  DO_RPC_CALL_DEFINE_RT_0_NO_RC ( NUM_ARG, \
      OCEANBASE_JOIN(ARG_TN, NUM_ARG),  \
      OCEANBASE_JOIN(ARG_PN, NUM_ARG),  \
      OCEANBASE_JOIN(ARG_AN, NUM_ARG) ); 


#define DO_RPC_CALL_DEFINE_SND_0_WITH_RC(NUM_RESULT, RT_TNS, RT_PNS, RT_ANS) \
  template <RT_TNS> \
int ObRpcStub::RPC_FUNC_NAME_JOIN(0, NUM_RESULT)(const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, ObResultCode &rc, RT_PNS) const \
{ \
  int ret = OB_SUCCESS; \
  int64_t pos = 0; \
  ObDataBuffer data_buffer; \
  if (OB_SUCCESS != (ret = OCEANBASE_JOIN(send_param_, 0)( \
          data_buffer, server, timeout, pcode, version))) \
  { \
  } \
  else if (OB_SUCCESS != (ret = OCEANBASE_JOIN(deserialize_result_, NUM_RESULT)( \
          data_buffer, pos, rc, RT_ANS))) \
  { \
  } \
  return ret; \
}

#define DO_RPC_CALL_DEFINE_SND_0_NO_RC(NUM_RESULT, RT_TNS, RT_PNS, RT_ANS) \
  template <RT_TNS> \
int ObRpcStub::RPC_FUNC_NAME_JOIN(0, NUM_RESULT)(const ObServer& server, const int64_t timeout, \
    const int32_t pcode, const int32_t version, RT_PNS) const \
{ \
  ObResultCode rc; \
  return RPC_FUNC_NAME_JOIN(0, NUM_RESULT)(server, timeout, pcode, version, rc, RT_ANS); \
}

#define RPC_CALL_DEFINE_SND_0(NUM_RESULT) \
  DO_RPC_CALL_DEFINE_SND_0_WITH_RC ( NUM_RESULT, \
      OCEANBASE_JOIN(RT_TN, NUM_RESULT), \
      OCEANBASE_JOIN(RT_PN, NUM_RESULT), \
      OCEANBASE_JOIN(RT_AN, NUM_RESULT) ); \
  DO_RPC_CALL_DEFINE_SND_0_NO_RC ( NUM_RESULT, \
      OCEANBASE_JOIN(RT_TN, NUM_RESULT), \
      OCEANBASE_JOIN(RT_PN, NUM_RESULT), \
      OCEANBASE_JOIN(RT_AN, NUM_RESULT) )

