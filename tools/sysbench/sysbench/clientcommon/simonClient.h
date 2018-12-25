/*
 * $Header: /net/releng/export/releng/CVS_REPOSITORY/search/simon/client/src/simonClient.h,v 1.22 2007/06/27 22:29:25 dbowen Exp $
 * Represents a connection to an aggregation server.
 *
 * Author: dwind
 * Copyright 2006 Yahoo Inc. All Rights Reserved.
 */
#ifndef _SIMONCLIENT_H_
#define _SIMONCLIENT_H_

#include <netinet/in.h>         // for sockaddr_in

#include "simonUtils.h"
#include "simonContext.h"
#include "basthr.h"

namespace simon {

class SimonPacketC;  
class SimonSessionC;   

class SimonClientC {

    /*
     * Determines when config needs to be resent.
     * needConfig returns true with approximate probability 1/_configFreq.
     */
    class ConfigOracleC {
      public:
        ConfigOracleC(int configFreq);
        int getFreq();
        void setFreq(int freq);
        bool needConfig();
        
      private:
        bool _first;
        unsigned int _randomState;
        int _configFreq;
        BasThrMutexT _mutex;

    }; // class ConfigOracleC
        
    /*
     * A simple timer class, whose wait() pauses until the next interval has
     * passed.  Note: Intervals have a fixed period in seconds, and are all 
     * relative to the time of the object's creation- not relative to the time 
     * wait() is called.
     */
    class SimonWaiterC {
      public:
        SimonWaiterC(int secs);
        ~SimonWaiterC();
          
        // returns true if instance was successfully initialized
	// and has not been cancelled
        bool isValid();

        // returns error message if initialization failed
        std::string getErrMsg();

        // wait until end of interval
        void wait();

        // terminate current wait, if any
        void cancel();
                
      private:
        time_t _secs;
        BasThrMutexT _lock;
        struct timespec _wait;
        bool _isValid;
        BasThrCondT *_condP;
        std::string _errMsg;

    }; // class SimonWaiterC


  public:

    static const int kMinConfigFreq= 1; 
    static const int kDefConfigFreq= 100;    //# config is every Nth pkt
    static const int kSendRetries= 3;       //# times _send() should try if fails
        

    // Returns an instance to use, or NULL is host is invalid
    static SimonClientC *getInstance(const char *host, int port, int period,
                                     std::string *errMsg);
    // Must be called when instance is no longer needed
    void releaseInstance();

    void registerSimonSession(ustring &sessionKey, SimonSessionC *simonSession);
    void unregisterSimonSession(ustring &sessionKey);

    // Returns true if the calling thread is the thread created by this client
    bool isCallbackThread();

    /// Adds data to packet, flushing the packet if necessary
    void append(SimonPacketC *packet, const unsigned char *src, int len);
        
    /// Sends packet if it contains data
    /// Returns false if packet contained no data, or send() returned false.
    bool flush(SimonPacketC *packetP);
        

    // accessors:

    void setConfigFreq(int freq) {
        _configOracle.setFreq(freq);
    }
    int getConfigFreq() {
        return _configOracle.getFreq();
    }

    bool isValid() { 
        return _isValid; 
    }
        
  private:

    static const BasThrThreadT kDefaultThreadId = 0xffffffff;

    // static table of SimonClient instances
    static std::map<std::string,SimonClientC*> _sClientMap;
    static BasThrMutexT _sMutex;
        
    /// Private constructor, to enforce singleton per remote host, port
    /// and blurb period.
    SimonClientC(const std::string key, const char *host, int port,
                 int period, std::string *errMsg,
                 int configFreq=kDefConfigFreq);
        
    ~SimonClientC();

    bool _startThread(std::string *errMsg);
    void _stopThread();

    /// Function which is run by spawned thread
    static void* _sWorkerRunLoop(void *arg);
    void _workerRunLoop();
        
    /// Determine if it is time to send a config packet
    bool _needConfig() {
        return _configOracle.needConfig();
    }
        
    bool _send(unsigned char *data, int len);
        
    /// Transmits a UDP packet
    /// Returns false if number of bytes sent does not match that expected.
    bool _send(SimonPacketC *packetP);
        
    std::string _clientKey;             // string of form host:port,period
    int _socket;
    struct sockaddr_in _addr;
    bool _isValid;
    ConfigOracleC _configOracle;
    BasThrThreadT _threadId;
    BasThrMutexT _lock;
    SimonWaiterC _waiter;
    int _refCount;
    volatile bool _isQuitting;
    
    std::map<ustring,SimonSessionC*> _contextMap;

};

} // namespace

#endif /*_SIMONCLIENT_H_*/
