#ifdef __rs_debug__
oceanbase::rootserver::ObRootServer2* __rs = NULL;
//mod zhaoqiong [fixed for Backup]:20150811:b
//oceanbase::common::ObRoleMgr* __role_mgr = NULL;
//oceanbase::common::ObiRole* __obi_role = NULL;
//#define __debug_init__() __rs = &root_server_; __role_mgr = &role_mgr_;  __obi_role = (typeof(__obi_role))&root_server_.get_obi_role();
oceanbase::common::ObRoleMgr* __rs_role_mgr = NULL;
oceanbase::common::ObiRole* __rs_obi_role = NULL;
#define __debug_init__() __rs = &root_server_; __rs_role_mgr = &role_mgr_;  __rs_obi_role = (typeof(__rs_obi_role))&root_server_.get_obi_role();
//mod:e
#endif

#ifdef __ms_debug__
oceanbase::mergeserver::ObMergeServer * __ms = NULL;
oceanbase::common::ObMergerSchemaManager * __ms_schema = NULL;
oceanbase::mergeserver::ObMergeServerService * __ms_service = NULL;
#define __debug_init__() __ms = merge_server_; __ms_schema = schema_mgr_; __ms_service =  this;
#endif

#ifdef __ups_debug__
oceanbase::updateserver::ObUpdateServer* __ups = NULL;
//mod zhaoqiong [fixed for Backup]:20150811:b
//oceanbase::updateserver::ObUpsRoleMgr* __role_mgr = NULL;
//oceanbase::updateserver::ObUpsLogMgr* __log_mgr = NULL;
//oceanbase::common::ObiRole* __obi_role = NULL;
//#define __debug_init__() __ups = this; __role_mgr = &role_mgr_; __obi_role = &obi_role_; __log_mgr = &log_mgr_;
oceanbase::updateserver::ObUpsRoleMgr* __ups_role_mgr = NULL;
oceanbase::updateserver::ObUpsLogMgr* __log_mgr = NULL;
oceanbase::common::ObiRole* __ups_obi_role = NULL;
#define __debug_init__() __ups = this; __ups_role_mgr = &role_mgr_; __ups_obi_role = &obi_role_; __log_mgr = &log_mgr_;
//mod:e
#endif
