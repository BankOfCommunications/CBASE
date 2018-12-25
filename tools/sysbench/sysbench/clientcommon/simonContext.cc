/*
 * $Header: /net/releng/export/releng/CVS_REPOSITORY/search/simon/client/src/simonContext.cc,v 1.23 2007/07/02 22:13:30 dbowen Exp $
 * API for Simon Client Library
 *
 * Copyright 2006 Yahoo! Inc. All Rights Reserved.
 */

#include "simonContext.h"
#include "simonSession.h"
#include "simonPacket.h"
#include "callbackRegistry.h"

static const int kShortBytes= 2;
static const int kFloatBytes=  4;
static const int kLongBytes=  4;

using namespace std;
using namespace simon;

///////////////////////////////////////////////////////////////////////////////
//-------------------------class SimonSigC----------------------------------
///////////////////////////////////////////////////////////////////////////////


//-------------------------------------------------------------------------
// SimonSigC::SimonSigC
//-------------------------------------------------------------------------
SimonContextC::SimonSigC::SimonSigC(const char *name, int version, int md5, int per) :
    _name(name), _version(version), _md5(md5), _period(per)
{
    SimonPacketC::serializeStringToUString(_bytes, _name.c_str());
    unsigned char *src= reinterpret_cast<unsigned char *>(&_version);
    for(int i = 0; i < 4; i++) {
        _bytes += *src++;
    }
    src = reinterpret_cast<unsigned char *>(&_md5);
    for(int i = 0; i < 4; i++) {
        _bytes += *src++;
    }
}


///////////////////////////////////////////////////////////////////////////////
//-------------------------class SimonContextC----------------------------------
///////////////////////////////////////////////////////////////////////////////

// HACK - this was the simplest way I could think of to
// avoid including basthr.h in simonContext.h.  That is
// undesirable, or else I would just have declared _lock
// to be of type BasThrMutexT. Instead we have to dynamically
// allocate the struct and cast it on every usage.
#define LOCK(p)    (reinterpret_cast<BasThrMutexT*>(p))

SimonContextC::SimonContextC() :
    _simonSession(NULL),
    _configFreq(-1),
    _resendCount(DEFAULT_RESEND_COUNT)
{
    _callbackRegistry = new CallbackRegistryC(this);
    _lock = malloc(sizeof(BasThrMutexT));
    BasThrMutexInit(LOCK(_lock));
    _initNode();
}

SimonContextC::~SimonContextC() {
    BasThrMutexDestroy(LOCK(_lock));
    free(_lock);
    delete _callbackRegistry;
}

static const int MAX_HOSTNAME = 256;

void
SimonContextC::_initNode()
{
    char hostname[MAX_HOSTNAME];
    assert(gethostname(hostname, MAX_HOSTNAME) == 0);

    // trim domain to leave unqualified host name
    for (int i=0; i < MAX_HOSTNAME && hostname[i] != '\0'; i++) {
        if (hostname[i] == '.') {
            hostname[i] = '\0';
            break;
        }
    }

    _node= hostname;
}

/**
 * Returns the hostname of the machine that we're running on
 */
string
SimonContextC::_getNode()
{
    return _node;
}

/**
 * Returns the cluster name.
 */
string
SimonContextC::getCluster()
{
    string cluster;
    BasThrMutexLock(LOCK(_lock));
    if(_simonSession) {
        cluster = _simonSession->getCluster();
    }
    BasThrMutexUnlock(LOCK(_lock));
    return cluster;
}

/**
 * Returns the Aggregator hostname.
 */
string
SimonContextC::getHost()
{
    string aggregatorHost;
    BasThrMutexLock(LOCK(_lock));
    if(_simonSession) {
        aggregatorHost = _simonSession->getAggregatorHost();
    }
    BasThrMutexUnlock(LOCK(_lock));
    return aggregatorHost;
}


/**
 * Returns the Aggregator port.
 */
int
SimonContextC::getPort()
{
    int aggregatorPort = -1;
    BasThrMutexLock(LOCK(_lock));
    if(_simonSession) {
        aggregatorPort = _simonSession->getAggregatorPort();
    }
    BasThrMutexUnlock(LOCK(_lock));
    return aggregatorPort;
}



//-------------------------------------------------------------------------
// SimonContextC::stopMonitoring()
//-------------------------------------------------------------------------
bool SimonContextC::stopMonitoring(string *errMsg) {
    SimonSessionC *simonSession;
    bool error = false;

    BasThrMutexLock(LOCK(_lock));

    if (_simonSession == NULL) {
        if (errMsg) {
            errMsg->append("not currently monitoring");
        }
        error = true;
    }
    else if (_simonSession->isCallbackThread()) {
        if (errMsg) {
            errMsg->append("called from callback");
        }
        error = true;
    }
    else {
        simonSession = _simonSession;
        _simonSession = NULL;
    }

    BasThrMutexUnlock(LOCK(_lock));

    if (!error) {
        delete simonSession;
    }
    return !error;
}

//-------------------------------------------------------------------------
// SimonContextC::startMonitoring()
//-------------------------------------------------------------------------
bool
SimonContextC::startMonitoring(const char *cluster,
                               const char *aggregatorHost,
                               int aggregatorPort,
                               string *errMsg)
{
    BasThrMutexLock(LOCK(_lock));
    if(_simonSession != NULL) {
        if(errMsg) {
            errMsg->append("Already monitoring");
        }
        BasThrMutexUnlock(LOCK(_lock));
        return false;
    }
      
    _simonSession = new
        SimonSessionC(this, cluster, aggregatorHost, aggregatorPort, errMsg);

    if (!_simonSession->isValid()) {
        delete _simonSession;
        _simonSession = NULL;
        BasThrMutexUnlock(LOCK(_lock));
        return false;
    }

    if (_configFreq > 0) {
        _simonSession->setConfigFreq(_configFreq);
    }

    BasThrMutexUnlock(LOCK(_lock));
    return true;

}

//-------------------------------------------------------------------------
// SimonContextC::registerUpdater()
//
// Returns an error if updater is already registered
//-------------------------------------------------------------------------
bool SimonContextC::registerUpdater(SimonUpdaterC *updater, string *errMsg) {
        CallbackRegistryC::Callback callback;
        callback.updater = updater;
        return _callbackRegistry->registerCallback(callback, false, errMsg);
}

bool SimonContextC::registerUpdater(SimonUpdaterFuncPT funcP, string *errMsg) {
        CallbackRegistryC::Callback callback;
        callback.funcP = funcP;
        return _callbackRegistry->registerCallback(callback, true, errMsg);
}



//-------------------------------------------------------------------------
// SimonContextC::unregisterUpdater()
//-------------------------------------------------------------------------
bool SimonContextC::unregisterUpdater(SimonUpdaterC *updater, string *errMsg) {
        CallbackRegistryC::Callback callback;
        callback.updater = updater;
        _callbackRegistry->unregisterCallback(callback, false);
        return true;
}

bool SimonContextC::unregisterUpdater(SimonUpdaterFuncPT funcP, string *errMsg) {
        CallbackRegistryC::Callback callback;
        callback.funcP = funcP;
        _callbackRegistry->unregisterCallback(callback, true);
        return true;
}



//-------------------------------------------------------------------------
// SimonContextC::getBlurbMessageName()
//-------------------------------------------------------------------------
string
SimonContextC::getBlurbMessageName()
{
    return getBlurbMessageSig()->getName();
}


//-------------------------------------------------------------------------
// SimonContextC::getBlurb()
//-------------------------------------------------------------------------
void
SimonContextC::getBlurb(ustring &bTags, ustring &bMetrics)
{
    BasThrMutexLock(LOCK(_lock));
    if(_simonSession) {
        _simonSession->getBlurb(bTags, bMetrics);
    }
    BasThrMutexUnlock(LOCK(_lock));
}


//-------------------------------------------------------------------------
// SimonContextC::setBlurb()
//-------------------------------------------------------------------------
void
SimonContextC::setBlurb(ustring &bTags, ustring &bMetrics)
{
    BasThrMutexLock(LOCK(_lock));
    if(_simonSession) {
        _simonSession->setBlurb(bTags, bMetrics);
    }
    BasThrMutexUnlock(LOCK(_lock));
}


//-------------------------------------------------------------------------
// SimonContextC::purgeBlurb()
//-------------------------------------------------------------------------
void
SimonContextC::purgeBlurb(ustring &bTags)
{
    BasThrMutexLock(LOCK(_lock));
    if (_simonSession) {
        _simonSession->purgeBlurb(bTags);
    }
    BasThrMutexUnlock(LOCK(_lock));
}


//-------------------------------------------------------------------------
// SimonContextC::purgeAllBlurbsOfType()
//-------------------------------------------------------------------------
void
SimonContextC::purgeAllBlurbsOfType(unsigned char blurbID)
{
    BasThrMutexLock(LOCK(_lock));
    if (_simonSession) {
        _simonSession->purgeAllBlurbsOfType(blurbID);
    }
    BasThrMutexUnlock(LOCK(_lock));
}


//-------------------------------------------------------------------------
// SimonContextC::setConfigMessageInterval()
//-------------------------------------------------------------------------
void
SimonContextC::setConfigMessageInterval(int interval)
{
    if(interval >= SimonClientC::kMinConfigFreq) {
        _configFreq= interval;
        BasThrMutexLock(LOCK(_lock));
        if (_simonSession) {
            _simonSession->setConfigFreq(interval);
        }
        BasThrMutexUnlock(LOCK(_lock));
    } else {
        cerr << "Error: setConfigMessageInterval argument " << interval <<
            " is less than " << SimonClientC::kMinConfigFreq << endl;
    }
}


//-------------------------------------------------------------------------
// SimonContextC::getConfigMessageInterval()
//-------------------------------------------------------------------------
int
SimonContextC::getConfigMessageInterval()
{
    int messageInterval;
    BasThrMutexLock(LOCK(_lock));
    if(!_simonSession) {
        messageInterval = ( _configFreq < 0 ?
                            SimonClientC::kDefConfigFreq :
                            _configFreq );
    }
    else {
        // note: config freq could have been changed by another
        // SimonContextC instance sharing the same SimonClientC.
        messageInterval = _simonSession->getConfigFreq();
    }
    BasThrMutexUnlock(LOCK(_lock));
    return messageInterval;
}


//-------------------------------------------------------------------------
// public static SimonContextC::getVersion()
//-------------------------------------------------------------------------

#include "version.ic"

const string
SimonContextC::getVersion()
{
    return SIMON_CLIENT_VERSION;
}

///////////////////////////////////////////////////////////////////////////////
//-------------------------class SimonBlurbC----------------------------------
///////////////////////////////////////////////////////////////////////////////


//-------------------------------------------------------------------------
// SimonBlurbC::fetch()
//-------------------------------------------------------------------------
bool SimonBlurbC::fetch() {
        ustring key, value, empty;
        key+= getBlurbCode();
        L_PackData(key, empty, getTagFormat(), getTagOffsets() );
        getContext().getBlurb(key, value);
        if(value.size()==0)
                return false;
              
        L_UnpackData(value, getMetricFormat(), getMetricOffsets() );
        return true;
}


//-------------------------------------------------------------------------
// SimonBlurbC::update()
//-------------------------------------------------------------------------
void SimonBlurbC::update() {
        ustring key, value, prevValue;
        key+= getBlurbCode();
        L_PackData(key, prevValue, getTagFormat(), getTagOffsets() );
      
        SimonContextC &context = getContext();

        // lock context to ensure atomic update
        BasThrMutexLock(LOCK(context._lock));

        SimonSessionC *_simonSession = context._simonSession;
        if (_simonSession) {
            _simonSession->getBlurb(key, prevValue);
            L_PackData(value, prevValue, getMetricFormat(), getMetricOffsets());
            _simonSession->setBlurb(key, value);
        }

        BasThrMutexUnlock(LOCK(context._lock));
}


//-------------------------------------------------------------------------
// SimonBlurbC::purge()
//-------------------------------------------------------------------------
void SimonBlurbC::purge() {
        ustring key, value, empty;
        key+= getBlurbCode();
        L_PackData(key, empty, getTagFormat(), getTagOffsets() );
        getContext().purgeBlurb(key);
}



//-------------------------------------------------------------------------
// protected static SimonBlurbC::purgeAll()
//-------------------------------------------------------------------------
void SimonBlurbC::purgeAll(SimonContextC &simonContext, uint8_t blurbId) {
        simonContext.purgeAllBlurbsOfType(blurbId);
}


//-------------------------------------------------------------------------
// SimonBlurbC::L_PackData()
//-------------------------------------------------------------------------
void
SimonBlurbC::L_PackData(ustring &result, ustring &prevResult,
                        const char *format, const size_t * offsets)
{     
    const char     *fmtPtr;
    const size_t   *offPtr;
    char            typ;

    unsigned char *src= reinterpret_cast<unsigned char *>(this);
    ustring::iterator iter= prevResult.begin();

    for (fmtPtr= format, offPtr=offsets; (typ = *fmtPtr); ++fmtPtr, ++offPtr) {

        switch (typ) {
            case 'b':  {
                unsigned char  cur;
                L_ReadBytes(src+ *offPtr, &cur, 1);
                if (iter!=prevResult.end() ) {
                    unsigned char prev= *iter++;
                    cur+= prev;
                }
                result+= cur;
                break;
            }

            case 'B': {
                unsigned char  cur;
                L_ReadBytes(src+ *offPtr, &cur, 1);
                result+= cur;
                if(iter!=prevResult.end() ) {
                    iter++;
                }
                break;
            }

            case 's': {
                unsigned short  cur;
                L_ReadBytes(src+ *offPtr, &cur, kShortBytes);
                if (iter!=prevResult.end() ) {
                    unsigned short prev;
                    L_ReadBytes(iter, &prev, kShortBytes);
                    cur+= prev;
                }
                L_WriteBytes(&cur, result, kShortBytes);
                break;
            }

            case 'S': {
                unsigned short  cur;
                L_ReadBytes(src+ *offPtr, &cur, kShortBytes);
                L_WriteBytes(&cur, result, kShortBytes);
                for(int i=0; i<kShortBytes && iter!=prevResult.end(); i++) {
                    iter++;
                }
                break;
            }

            case 'l': {
                uint32_t  cur;
                L_ReadBytes(src+ *offPtr, &cur, kLongBytes);
                if (iter!=prevResult.end() ) {
                    uint32_t prev;
                    L_ReadBytes(iter, &prev, kLongBytes);
                    cur+= prev;
                }
                L_WriteBytes(&cur, result, kLongBytes);
                break;
            }

            case 'L':  {
                uint32_t  cur;
                L_ReadBytes(src+ *offPtr, &cur, kLongBytes);
                L_WriteBytes(&cur, result, kLongBytes);
                for (int i=0; i<kLongBytes && iter!=prevResult.end(); i++) {
                    iter++;
                }
                break;
            }

            case 'f': {
                float cur;
                L_ReadBytes(src+ *offPtr, &cur, kFloatBytes);
                if (iter!=prevResult.end() ) {
                    float prev;
                    L_ReadBytes(iter, &prev, kFloatBytes);
                    cur+= prev;
                }
                L_WriteBytes(&cur, result, kFloatBytes);
                break;
            }

            case 'F':  {
                float cur;
                L_ReadBytes(src+ *offPtr, &cur, kFloatBytes);
                L_WriteBytes(&cur, result, kFloatBytes);
                for (int i=0; i<kFloatBytes && iter!=prevResult.end(); i++) {
                    iter++;
                }
                break;
            }

            case 'p':
            case 'P':  {
                string str= *(reinterpret_cast<string*>(src+ *offPtr) );
                SimonPacketC::serializeStringToUString(result, str.c_str() );
                break;
            }

            default:
                break;

        } // end of switch

    } // end of for(formatPtr ... )
}


//-------------------------------------------------------------------------
// SimonBlurbC::L_UnpackData()
//-------------------------------------------------------------------------
void
SimonBlurbC::L_UnpackData(ustring &input, const char *format,  const size_t *offsets)
{     
    const char           *fmtPtr;
    const size_t         *offPtr;
    char                  typ;

    unsigned char *dst= reinterpret_cast<unsigned char *>(this);
    ustring::iterator iter= input.begin();

    for (fmtPtr= format, offPtr=offsets; (typ = *fmtPtr); ++fmtPtr, ++offPtr) {

        switch (typ) {
            case 'b':
            case 'B': {
                L_ReadBytes(iter, dst+ *offPtr, 1);
                break;
            }

            case 's':
            case 'S':  {
                L_ReadBytes(iter, dst+ *offPtr, kShortBytes);
                break;
            }

            case 'l':
            case 'L':  {
                L_ReadBytes(iter, dst+ *offPtr, kLongBytes);
                break;
            }

            case 'f':
            case 'F':  {
                L_ReadBytes(iter, dst+ *offPtr, kFloatBytes);
                break;
            }

            case 'p':
            case 'P':  {
                int  len  = *iter++;
                string sp= *(reinterpret_cast<string*> (dst+ *offPtr) );
                for (int i = 0; i < len; ++i)
                        sp+= *iter++;
                break;
            }

            default:
                break;

        } // end of switch

    } // end of for(fmtPtr...)

}
