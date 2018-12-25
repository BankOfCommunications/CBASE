/*
 * $Header: /net/releng/export/releng/CVS_REPOSITORY/search/simon/client/src/simonSession.h,v 1.4 2006/10/23 19:03:42 dbowen Exp $
 * Object encapsulating run-time data for a particular context while monitoring
 * is enabled on that context
 *
 * Copyright 2006 Yahoo! Inc. All Rights Reserved.
 */

#ifndef _SIMONSESSION_H_
#define _SIMONSESSION_H_

#include "simonUtils.h"
#include "simonClient.h"
#include "simonContext.h"

namespace simon {

class SimonPacketC;  
class CallbackRegistryC;   

class SimonSessionC {
        
  public:

    SimonSessionC(SimonContextC *context,
                  const char *cluster,
                  const char *aggregatorHost,
                  int aggregatorPort,
                  std::string *errMsg);

    ~SimonSessionC();
        

    /* Member functions called by SimonContextC */

    // Returns true iff the session was created successfully.
    bool isValid() {
        return _isValid;
    }

    // Sets the metric data in the blurb with the specified tags
    void setBlurb(ustring &bTags, ustring &bMetrics);

    // Retrieves the metric data from the blurb with the specified tags
    void getBlurb(ustring &bTags, ustring &bMetrics);

    // Clears the stored data for a blurb
    void purgeBlurb(ustring &bTags);

    // Clears stored data for all blurbs of a type
    void purgeAllBlurbsOfType(unsigned char blurbId);
        
    // Set/get config message frequency
    void setConfigFreq(int freq) {
        _client->setConfigFreq(freq);
    }
    int getConfigFreq() {
        return _client->getConfigFreq();
    }

    bool isCallbackThread() {
        return (_client != NULL && _client->isCallbackThread());
    }

    std::string getCluster() {
        return _cluster;
    }

    std::string getAggregatorHost() {
        return _aggregatorHost;
    }

    int getAggregatorPort() {
        return _aggregatorPort;
    }


    /* Member functions called by SimonClientC */

    // Returns config packet
    SimonPacketC* getConfig() {
        return _configPacket;
    }

    // Sends all the blurb data for this context 
    void sendBlurbData();

    // Invoke the callbacks for this context
    void invokeCallbacks();

  private:

    /*
     * Data for a particular blurb type and tag set
     */
    class BlurbDataC : public ustring {
    private:
        int _sendCount;
    public:
        BlurbDataC() : _sendCount(0) {
        }

        BlurbDataC(ustring s) : ustring(s), _sendCount(0) {
        }

//        BlurbDataC& operator=(const BlurbDataC& bd) {
//            if (this != &bd) {
//                ustring::operator=(bd);
//                _sendCount = bd._sendCount;
//            }
//            return *this;
//        }


        void setResendCount(int resendCount) {
            _sendCount = resendCount + 1;
        }

        bool needsSending() {
            if (_sendCount > 0) {
                _sendCount--;
                return true;
            }
            return false;
        }
    }; // class BlurbDataC

    typedef std::map<ustring, BlurbDataC> Str2BlurbDataMapT;
        
    SimonContextC *_context;
    ustring _contextKey;
    SimonPacketC* _configPacket;
    SimonPacketC* _packet;
    SimonClientC* _client;
    Str2BlurbDataMapT _blurbDataMap;
    BasThrMutexT _lock;
    std::string _cluster;
    std::string _aggregatorHost;
    int _aggregatorPort;
    int _resendCount;
    bool _isValid;
};

} // namespace

#endif /*_SIMONSESSION_H_*/
