///////////////////////////////////////////////////////////////////////////////
/// \file
/// Code related to an abstract stats class that the generated code must extend
///
/// Author: dbowen
// Copyright 2006 Yahoo Inc. All Rights Reserved. Confidential.

#ifndef CALLBACK_REGISTRY_H_
#define CALLBACK_REGISTRY_H_

#include <list>
#include "basthr.h"
#include "simonContext.h"

namespace simon {


class CallbackRegistryC {

 public:

        typedef union {
                SimonUpdaterC *updater;
                SimonUpdaterFuncPT funcP;
        } Callback;


        CallbackRegistryC(SimonContextC *simonContext);
	~CallbackRegistryC();

        bool registerCallback(Callback callback,
                              bool isFuncP, 
                              std::string *errMsg=0);

        void unregisterCallback(Callback callback, 
                                 bool isFuncP);

        bool isMatchingCallback(SimonUpdaterC *updater, 
                                Callback callback, 
                                bool isFuncP);

        void invokeCallbacks();

 private:

        SimonContextC *mSimonContext;
        std::list<SimonUpdaterC*> mCallbacks;
        BasThrMutexT mMutex;

};

} // namespace

#endif /*CALLBACK_REGISTRY_H_*/

