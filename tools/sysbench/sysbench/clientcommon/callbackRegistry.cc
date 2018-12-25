///////////////////////////////////////////////////////////////////////////////
/// \file
/// Code related to an abstract stats class that the generated code must extend
///
/// Author: dbowen
// Copyright 2006 Yahoo Inc. All Rights Reserved. Confidential.

#include "callbackRegistry.h"
#include "simonContext.h"

using namespace std;
using namespace simon;

///////////////////////////////////////////////////////////////////////////////
//-------------------------class UpdaterFuncC----------------------------------
///////////////////////////////////////////////////////////////////////////////
	

class UpdaterFuncC : public SimonUpdaterC {

private:
        SimonUpdaterFuncPT mFuncP;

public: 
	UpdaterFuncC(SimonUpdaterFuncPT funcP) : mFuncP(funcP) {
	}

	void doUpdates(SimonContextC *simonContext) {
	        mFuncP();
	}

        SimonUpdaterFuncPT getFuncP() {
                return mFuncP;
        }
};

	
///////////////////////////////////////////////////////////////////////////////
//-------------------------class CallbackRegistryC-----------------------------
///////////////////////////////////////////////////////////////////////////////
	
CallbackRegistryC::CallbackRegistryC(SimonContextC *simonContext) :
        mSimonContext(simonContext)
{
        BasThrMutexInit(&mMutex);
}

CallbackRegistryC::~CallbackRegistryC() {

        // delete all callbacks
        list<SimonUpdaterC*>::iterator cbIter= mCallbacks.begin();
        for(; cbIter!=mCallbacks.end(); cbIter++) {
                delete (*cbIter);
        }

        BasThrMutexDestroy(&mMutex);
}

bool CallbackRegistryC::registerCallback(Callback callback, bool isFuncP, 
                                         string *errMsg) 
{
        BasThrMutexLock(&mMutex);
        list<SimonUpdaterC*>::iterator cb=mCallbacks.begin();
        bool found= false;
        for(; !found && cb!=mCallbacks.end(); cb++) {
                found= isMatchingCallback(*cb, callback, isFuncP);
        }
        if(found) {
                if (errMsg) {
                        errMsg->append("Already registered");
                }
        } else {
                SimonUpdaterC *updater;
                if (isFuncP) {
                        updater = new UpdaterFuncC(callback.funcP);
                }
                else {
                        updater = callback.updater;
                }
                mCallbacks.push_back(updater);
        }
        BasThrMutexUnlock(&mMutex);
        return !found;
}


void CallbackRegistryC::unregisterCallback(Callback callback, bool isFuncP) {
        BasThrMutexLock(&mMutex);
        list<SimonUpdaterC*>::iterator cb, next;
        for(cb=mCallbacks.begin(); cb!=mCallbacks.end(); cb=next) {
                next= cb;
                next++;

                if (isMatchingCallback(*cb, callback, isFuncP)) {
                        if (isFuncP) {
			        // delete the UpdaterFuncC object created by registerCallback
			        UpdaterFuncC *obj = dynamic_cast<UpdaterFuncC*>(*cb);
                                delete obj;
                        }
                        mCallbacks.erase(cb);
                        break;
                }
        }  
        BasThrMutexUnlock(&mMutex);
}


// helper function for registering and unregistering callbacks

bool CallbackRegistryC::isMatchingCallback(SimonUpdaterC *updater, Callback callback, 
                                           bool isFuncP) 
{
        bool isMatch;

        if (isFuncP) {
                UpdaterFuncC *obj = dynamic_cast<UpdaterFuncC*>(updater);
                isMatch = (obj != 0 &&
                         obj->getFuncP() == callback.funcP);
        } else {
                isMatch = (updater == callback.updater);
        }
        return isMatch;
}

void CallbackRegistryC::invokeCallbacks() {
        // make a copy of the callbacks list
        BasThrMutexLock(&mMutex);
        list<SimonUpdaterC*> callbacks(mCallbacks);
        BasThrMutexUnlock(&mMutex);

        // call functions registered for callbacks, holding no locks
        list<SimonUpdaterC*>::iterator cbIter= callbacks.begin();
        for(; cbIter!=callbacks.end(); cbIter++) {
                (*cbIter)->doUpdates(mSimonContext);
        }
}
