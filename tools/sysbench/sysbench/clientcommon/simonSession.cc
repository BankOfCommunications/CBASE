/*
 * $Header: /net/releng/export/releng/CVS_REPOSITORY/search/simon/client/src/simonSession.cc,v 1.4 2006/10/23 19:03:42 dbowen Exp $
 * Object encapsulating run-time data for a particular context while monitoring
 * is enabled on that context
 *
 * Copyright 2006 Yahoo! Inc. All Rights Reserved.
 */

#include <cstdlib>
#include <sys/time.h>

#include "simonClient.h"
#include "simonContext.h"
#include "simonPacket.h"
#include "simonSession.h"
#include "callbackRegistry.h"

using namespace std;
using namespace simon;

//-------------------------------------------------------------------------
// SimonSessionC::SimonSessionC() constructor
//-------------------------------------------------------------------------
SimonSessionC::SimonSessionC(SimonContextC *context,
                               const char *cluster,
                               const char *aggregatorHost,
                               int aggregatorPort,
                               string *errMsg) :
    _context(context),
    _cluster(cluster),
    _aggregatorHost(aggregatorHost),
    _aggregatorPort(aggregatorPort),
    _isValid(false)
{
    BasThrMutexInit(&_lock);

    // Construct the config packet
    string appName = context->getAppName();
    string node = context->_getNode();
    SimonContextC::SimonSigC *bMsgSig = context->getBlurbMessageSig();
    SimonContextC::SimonSigC *reportSigs = context->getReportSigs();
    int numReports = context->getNumReports();
    uint64_t timestamp = context->getTimeStamp();
    ustring &configContents = context->getConfig();

    ustring bMsgSigBytes = bMsgSig->getBytes();

    list<ustring> bMsgSigList;
    bMsgSigList.push_back(bMsgSigBytes);

    list<ustring> reportSigList;
    for (int i = 0; i < numReports; i++, reportSigs++) {
        reportSigList.push_back(reportSigs->getBytes());
    }

    list<ustring> emptyReportSigList;  // intentionally left empty, for data packet

    // construct packet header
    _packet = new SimonPacketC(appName, cluster, node, bMsgSigList, 
                               emptyReportSigList, 0);

    // construct config packet
    _configPacket = new SimonPacketC(appName, cluster, node, bMsgSigList,
                                     reportSigList, timestamp);
    
    if(!_configPacket->append(configContents.c_str(), configContents.size() ) ) {
        delete _configPacket;
        _configPacket = 0;
        if(SIMONDEBUG) {
            cerr << "Config contents are larger than max packet size for app=" 
                 << appName << endl;
        }
        if(errMsg) {
            errMsg->append("Config contents are larger than max packet size");
        }
    }
    else {
        int period = bMsgSig->getPeriod();

        _client = SimonClientC::getInstance(aggregatorHost,
                                            aggregatorPort,
                                            period,
                                            errMsg);

        if (_client) {
            _contextKey += (unsigned char*)(appName.c_str());
            _contextKey += '.';
            _contextKey += bMsgSigBytes;

            _client->registerSimonSession(_contextKey, this);

            _isValid = true;
        }
    }
    _resendCount = context->getResendCount();
}



//-------------------------------------------------------------------------
// SimonSessionC::SimonSessionC() destructor
//-------------------------------------------------------------------------
SimonSessionC::~SimonSessionC() 
{
    if (SIMONDEBUG) {
        cerr << "Entering ~SimonSessionC" << endl;
    }

    if (_client) {
        _client->unregisterSimonSession(_contextKey);
        _client->releaseInstance();
    }

    delete _configPacket;
    delete _packet;

    BasThrMutexDestroy(&_lock);

    if (SIMONDEBUG) {
        cerr << "Exiting ~SimonSessionC" << endl;
    }
}


//-------------------------------------------------------------------------
// Sets the latest values for data in a blurb
//-------------------------------------------------------------------------
void 
SimonSessionC::setBlurb(ustring &bTags, ustring &bMetrics) 
{
    BasThrMutexLock(&_lock);
   
    _blurbDataMap[bTags] = bMetrics;
    _blurbDataMap[bTags].setResendCount(_resendCount);

    BasThrMutexUnlock(&_lock);
}

//-------------------------------------------------------------------------
// Retrieves the latest values for metrics in a blurb.
// bTags contains the packed tag data, and bMetrics is initially empty.
//-------------------------------------------------------------------------
void 
SimonSessionC::getBlurb(ustring &bTags, ustring &bMetrics)
{
    BasThrMutexLock(&_lock);
  	
    Str2BlurbDataMapT::iterator valIter = _blurbDataMap.find(bTags);
    if(valIter != _blurbDataMap.end()) {
        bMetrics = valIter->second;
    }

    BasThrMutexUnlock(&_lock);
}    

//-------------------------------------------------------------------------
// Clears stored data for a blurb
//-------------------------------------------------------------------------
void 
SimonSessionC::purgeBlurb(ustring &bTags) 
{
    BasThrMutexLock(&_lock);
  	
    _blurbDataMap.erase(bTags);

    BasThrMutexUnlock(&_lock);
}    

//-------------------------------------------------------------------------
// Clears stored data for all blurbs of a type
//-------------------------------------------------------------------------
void 
SimonSessionC::purgeAllBlurbsOfType(unsigned char blurbId)
{
    int n = 0;
    BasThrMutexLock(&_lock);
  	
    Str2BlurbDataMapT::iterator spot, next;
    for(spot = _blurbDataMap.begin(); spot != _blurbDataMap.end(); ) {
        next = spot;  
        next++; // in case erase() invalidates spot
        if(spot->first[0] == blurbId) {
            _blurbDataMap.erase(spot);
            n++;
        }
        spot= next;
    }

    BasThrMutexUnlock(&_lock);

    if (SIMONDEBUG) {
        cerr << "Purged " << n << " blurbs" << endl;
    }
}    

//-------------------------------------------------------------------------
// Appends the stored blurb data to the packet
//-------------------------------------------------------------------------
void
SimonSessionC::sendBlurbData()
{
    BasThrMutexLock(&_lock);

    Str2BlurbDataMapT::iterator values = _blurbDataMap.begin();
    for(; values != _blurbDataMap.end(); values++) {
        if (values->second.needsSending()) {
            ustring data = values->first;
            data += values->second;
            _client->append(_packet, data.c_str(), data.size());
        }
    }
    _client->flush(_packet);

    BasThrMutexUnlock(&_lock);
}

void
SimonSessionC::invokeCallbacks() {
    _context->_callbackRegistry->invokeCallbacks();
}
