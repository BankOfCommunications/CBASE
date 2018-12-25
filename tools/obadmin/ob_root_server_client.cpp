#include "ob_root_server_client.h"

namespace oceanbase {
  namespace tools {
    ObRootServerClient::ObRootServerClient()
      : timeout_(1000000)
    {
    }

    ObRootServerClient::~ObRootServerClient()
    {
    }
    
  } // end of namespace tools
} // end of namespace oceanbase
