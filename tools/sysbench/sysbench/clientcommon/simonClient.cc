/*
 * $Header: /net/releng/export/releng/CVS_REPOSITORY/search/simon/client/src/simonClient.cc,v 1.26 2007/06/27 23:04:09 dbowen Exp $
 * Code related to a Simon client's connection to an aggregation server.
 *
 * Author: dwind
 * Copyright 2006 Yahoo Inc. All Rights Reserved.
 */

#include <cstdlib>
#include <sys/time.h>

#include "simonClient.h"
#include "simonContext.h"
#include "simonPacket.h"
#include "simonSession.h"
#include "simonUtils.h"

#include "basinaddr.h"
#include "bassockudp.h"

using namespace std;
using namespace simon;

///////////////////////////////////////////////////////////////////////////////
//-------------------------class ConfigOracleC----------------------------------
///////////////////////////////////////////////////////////////////////////////

//-------------------------------------------------------------------------
// ConfigOracleC::ConfigOracleC() constructor
//-------------------------------------------------------------------------
SimonClientC::ConfigOracleC::ConfigOracleC(int configFreq):
        _configFreq(configFreq) 
{
	BasThrMutexInit(&_mutex);
	_first= true;

        // seed the random number generator
        struct timeval timeVal;
        (void) gettimeofday(&timeVal, NULL);
        _randomState = (unsigned int) timeVal.tv_usec;
}


//-------------------------------------------------------------------------
// ConfigOracleC::needConfig()
//
// Returns true with probability 1/_configFreq.  Thread-safe.
//-------------------------------------------------------------------------
bool SimonClientC::ConfigOracleC::needConfig() {

        // Generate a random number in the range 0..RAND_MAX.

	BasThrMutexLock(&_mutex);
	if(_first) {
		_first= false;
		BasThrMutexUnlock(&_mutex);
		return true;
	}
        int rand0 =  rand_r(&_randomState);
        int freq= _configFreq;
	BasThrMutexUnlock(&_mutex);

        // Get random number in range 0..(_configFreq-1) using high-order bits.
        // See "man rand_r" for comment on high-order versus low-order bits.

        int rand = (int) (1.0 * rand0 * freq / (RAND_MAX + 1.0));

        /* Redundant check
        if (rand < 0 || rand >= freq) {
            cerr << "WARNING: invalid random number: " << rand << endl;
        }
        */

	return (rand == 0);
}


//-------------------------------------------------------------------------
// ConfigOracleC::setFreq()
//-------------------------------------------------------------------------
void SimonClientC::ConfigOracleC::setFreq(int freq) {

	BasThrMutexLock(&_mutex);
        _configFreq= freq;
	BasThrMutexUnlock(&_mutex);
}


//-------------------------------------------------------------------------
// ConfigOracleC::getFreq()
//-------------------------------------------------------------------------
int SimonClientC::ConfigOracleC::getFreq() {

	BasThrMutexLock(&_mutex);
        int freq= _configFreq;
	BasThrMutexUnlock(&_mutex);
	return freq;
}


///////////////////////////////////////////////////////////////////////////////
//-------------------------class SimonWaiterC----------------------------------
///////////////////////////////////////////////////////////////////////////////


//-------------------------------------------------------------------------
// SimonWaiterC::SimonWaiterC()
//
// Constructs a waiter that will wait until the end of the next period of
// the specified number of seconds.
//-------------------------------------------------------------------------
SimonClientC::SimonWaiterC::SimonWaiterC(int secs) : 
    _secs(secs),
    _isValid(false),
    _condP(NULL)
{
    BasThrMutexInit(&_lock);
        
    struct timeval   now;   
    int res = gettimeofday(&now, NULL);
    if (res) {
        _errMsg= "SimonWaiterC can't get time: ";
        _errMsg+= strerror(res);
        return;
    }
    _wait.tv_sec = now.tv_sec += _secs;
    _wait.tv_nsec = now.tv_usec * 1000;
                        
    _condP= new BasThrCondT();
    if(BasThrCondInit(_condP) ) {
        _errMsg= "SimonWaiterC can't initialize conditional.";
        return;
    }
    _isValid= true;
}

//-------------------------------------------------------------------------
// SimonWaiterC::~SimonWaiterC()
//-------------------------------------------------------------------------                
SimonClientC::SimonWaiterC::~SimonWaiterC() {
    if (_condP != NULL) {
        BasThrCondDestroy(_condP);
        delete _condP;
    }
    BasThrMutexDestroy(&_lock);
}

//-------------------------------------------------------------------------
// SimonWaiterC::isValid()
//
// Returns true if this instance was successfully initialized 
//-------------------------------------------------------------------------               
bool
SimonClientC::SimonWaiterC::isValid() { 
    return _isValid; 
}

//-------------------------------------------------------------------------
// SimonWaiterC::getErrMsg()
//
// Returns an error message if initialization failed
//-------------------------------------------------------------------------
string 
SimonClientC::SimonWaiterC::getErrMsg() { 
    return _errMsg; 
}

//-------------------------------------------------------------------------
// SimonWaiterC::wait()
//
// Waits until the end of the current period, or until the wait is cancelled.
//
//-------------------------------------------------------------------------
void 
SimonClientC::SimonWaiterC::wait() {
    int res;
    BasThrMutexLock(&_lock);

    do {
        if (!_isValid) {
            if (SIMONDEBUG) {
                cerr << "SimonWaiterC: aborting wait" << endl;
            }
            BasThrMutexUnlock(&_lock);
            return;
        }
        else {
            res = BasThrCondTimedWait(_condP, &_lock, &_wait);
        }
    } 
    while (res == EINTR);
        
    if (res == ETIME) {          // not ETIMEDOUT - see basthr.c
        _wait.tv_sec += _secs;
    }
    else {
        if (SIMONDEBUG) {
            cerr << "SimonWaiterC: wait interrupted" << endl;
        }
    }
    BasThrMutexUnlock(&_lock);
}

//-------------------------------------------------------------------------
// SimonWaiterC::cancel()
//
// Cancels the current wait, if any.
//-------------------------------------------------------------------------
void 
SimonClientC::SimonWaiterC::cancel() {
    BasThrMutexLock(&_lock);
    _isValid = false;
    BasThrCondSignal(_condP);
    BasThrMutexUnlock(&_lock);
}

///////////////////////////////////////////////////////////////////////////////
//-------------------------class SimonClientC----------------------------------
///////////////////////////////////////////////////////////////////////////////

map<string, SimonClientC*> SimonClientC::_sClientMap;
BasThrMutexT SimonClientC::_sMutex = BASTHRMUTEXDEFAULT;

// max size C string to represent a SimonClientC key
static const int MAX_KEY_SIZE = 80;

//-------------------------------------------------------------------------
// SimonClientC::getInstance() 
//
// Finds the client instance for the specified (host,port,period) if it 
// exists, or else creates it.
//-------------------------------------------------------------------------
SimonClientC* 
SimonClientC::getInstance(const char *host, int port, int period, string *errMsg) 
{    
    char clientKey[MAX_KEY_SIZE];
    snprintf(clientKey, MAX_KEY_SIZE, "%s:%d@%d", host, port, period);

    SimonClientC *inst = NULL;
	
    BasThrMutexLock(&_sMutex);
    map<string, SimonClientC*>::iterator iter= _sClientMap.find(clientKey);
    if(iter!=_sClientMap.end() ) {
        inst = iter->second;
        inst->_refCount++;
    }
    else {
        inst= new SimonClientC(clientKey, host, port, period, errMsg);
        if(!inst->isValid() ) {
            delete inst;
            BasThrMutexUnlock(&_sMutex);
            return NULL;
        }
        _sClientMap[clientKey]= inst;
    }
    BasThrMutexUnlock(&_sMutex);

    return inst;	
}

//-------------------------------------------------------------------------
// SimonClientC::releaseInstance()
//
// Called after a SimonSession instance has (successfully) unregistered 
// with this client.
//-------------------------------------------------------------------------
void SimonClientC::releaseInstance() {

    bool noMoreRefs = false;

    BasThrMutexLock(&_sMutex);

    _refCount--;

    if (_refCount <= 0) {

        // remove this instance from the static table of SimonClients
        _sClientMap.erase(_clientKey);

	noMoreRefs = true;

    }
    BasThrMutexUnlock(&_sMutex);

    if (noMoreRefs) {
        _stopThread();
	// Note: stopping thread deletes this instance
	return;
    }

}


//-------------------------------------------------------------------------
// SimonClientC::SimonClientC() constructor
//-------------------------------------------------------------------------
SimonClientC::SimonClientC(const string clientKey, const char *host, int port, 
                           int period, string *errMsg,
                           int configFreq) : 
    _clientKey(clientKey),
    _socket(-1), 
    _isValid(false), 
    _configOracle(configFreq),
    _threadId(kDefaultThreadId),
    _waiter(period),
    _refCount(1),
    _isQuitting(false)
{
    char *hErrMsg = "";

    if(BasInetAddrSet(&_addr, NULL, 0, 0, host, port, &hErrMsg) ) {
        if (errMsg) {
            errMsg->append("gethostbyname failed for host ");
            errMsg->append(host);
            errMsg->append(", ");
            errMsg->append(hErrMsg);
        }
        return;
    }	
    if( (_socket= BasSockUdpAlloc()) <0 ) {
        if (errMsg) {
            errMsg->append("unable to open socket, errno=");
            char tmp[10];
            snprintf(tmp, 10, "%d", errno);
            errMsg->append(tmp);
        }
        return;
    }
    BasThrMutexInit(&_lock);

    if(!_waiter.isValid() ) {
        if (errMsg) {
            errMsg->append(_waiter.getErrMsg());
        }
        return;
    }

    if (!_startThread(errMsg)) {
        return;
    }
    _isValid= true;
}



//-------------------------------------------------------------------------
// SimonClientC::SimonClientC() destructor
//-------------------------------------------------------------------------
SimonClientC::~SimonClientC() 
{
    if (SIMONDEBUG) {
        cerr << "Entering ~SimonClientC" << endl;
    }

    if (_socket > 0) {
        BasSockUdpClose(_socket);
    }

    BasThrMutexDestroy(&_lock);

    if (SIMONDEBUG) {
        cerr << "Exiting ~SimonClientC" << endl;
    }
}


//-------------------------------------------------------------------------
// SimonClientC::registerSimonSession()
//-------------------------------------------------------------------------
void 
SimonClientC::registerSimonSession(ustring &sessionKey, SimonSessionC *simonSession)
{
    BasThrMutexLock(&_lock);

    _contextMap[sessionKey] = simonSession;

    BasThrMutexUnlock(&_lock);

    
}

//-------------------------------------------------------------------------
// SimonClientC::unregisterSimonSession()
//-------------------------------------------------------------------------
void
SimonClientC::unregisterSimonSession(ustring &sessionKey)
{
    BasThrMutexLock(&_lock);

    _contextMap.erase(sessionKey);

    BasThrMutexUnlock(&_lock);
}

//-------------------------------------------------------------------------
// SimonClientC::isCallbackThread()
//
// Returns true if the calling thread is the thread for this client
//-------------------------------------------------------------------------
bool
SimonClientC::isCallbackThread()
{
    return BasThrThreadSelf() == _threadId;
}


typedef void* (*WorkerFunctionP)(void *arg);

//-------------------------------------------------------------------------
// SimonClientC::_startThread()
// Returns true on success.  Appends error message to errMsg on failure.
//-------------------------------------------------------------------------
bool
SimonClientC::_startThread(string *errMsg)
{
    int errorCode;

    WorkerFunctionP fp = &SimonClientC::_sWorkerRunLoop;

    if(SIMONDEBUG) {
        cerr << "Entering SimonClientC::startThread()" << endl;
    }

    errorCode = BasThrThreadCreate(fp, this, &_threadId, 0, 1);

    if (errorCode != 0) {
        char msg[20];
        snprintf(msg, 20, "Error %d", errorCode);
        errMsg->append(msg);
    }
    return (errorCode == 0);

}


//-------------------------------------------------------------------------
// SimonClientC::_stopThread()
//-------------------------------------------------------------------------
void 
SimonClientC::_stopThread() 
{
    if (SIMONDEBUG) {
            cerr << "stopping thread" << endl;
    }

    BasThrMutexLock(&_lock);
    _isQuitting = true;
    _waiter.cancel();
    BasThrMutexUnlock(&_lock);

    // don't do anything here; the stopping thread may have already
    // deleted this instance
}

//-------------------------------------------------------------------------
// SimonClientC::_send()
//-------------------------------------------------------------------------
bool 
SimonClientC::_send(SimonPacketC *packetP) 
{
    unsigned char *data = packetP->getData();
    int len = packetP->getLength();
    bool success= false;

    packetP->incrPacketCounter();

    for(int i=0; !success && (i< kSendRetries); i++) 
        success= BasSockUdpSend(_socket, (const char *) data, 
                                len, &_addr) == len;
    return success;
}


//-------------------------------------------------------------------------
// SimonClientC::flush()
//-------------------------------------------------------------------------
bool 
SimonClientC::flush(SimonPacketC *packetP) 
{
    if(packetP->isEmpty() )
        return false;
    bool success= _send(packetP);
    packetP->reset();
    return success;
}


//-------------------------------------------------------------------------
// SimonClientC::append()
//-------------------------------------------------------------------------
void 
SimonClientC::append(SimonPacketC *packet, const unsigned char *src, 
                     int len) {
    if(packet->append(src, len) )
        return;
    flush(packet);
    packet->append(src, len);
}


//-------------------------------------------------------------------------
// SimonClientC::workerRunLoop()
//-------------------------------------------------------------------------
void*
SimonClientC::_sWorkerRunLoop(void *arg) 
{
    SimonClientC *client  = reinterpret_cast<SimonClientC *>(arg);
    client->_workerRunLoop();
    delete client;
    return NULL;
}

void
SimonClientC::_workerRunLoop()
{
    if(SIMONDEBUG) {
        cerr << "Starting thread run loop" << endl;
    }	

    for (;;) {

        _waiter.wait();

        bool needConfig = _needConfig();

        BasThrMutexLock(&_lock);
		
        if (_isQuitting) {
            break;
        }

        for (map<ustring,SimonSessionC*>::iterator keyIter = _contextMap.begin();
             keyIter != _contextMap.end();
             keyIter++) 
        {
            SimonSessionC* simonSession = keyIter->second;

            simonSession->invokeCallbacks();

            if (needConfig) {
                _send(simonSession->getConfig());
            }
            simonSession->sendBlurbData();
        }
		//move here by wuzhu,20080610
		/*if (_isQuitting) {
            break;
        }*/

        BasThrMutexUnlock(&_lock);
 	
    } // end of for(;;)

    if(SIMONDEBUG) {
        cerr << "Exiting thread run loop" << endl;
    }

}



