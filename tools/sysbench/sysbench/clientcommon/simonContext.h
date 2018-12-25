/*
 * $Header: /net/releng/export/releng/CVS_REPOSITORY/search/simon/client/src/simonContext.h,v 1.28 2006/10/23 19:03:42 dbowen Exp $
 * API for Simon Client Library
 *
 * Author: dwind, dbowen
 * Copyright 2006 Yahoo Inc. All Rights Reserved.
 */

#ifndef SIMONCONTEXT_H_
#define SIMONCONTEXT_H_

#ifndef SIMONDEBUG
#define SIMONDEBUG 0
#endif

#include <string>

namespace simon {

class CallbackRegistryC;
class SimonSessionC;

typedef void (*SimonUpdaterFuncPT)();

typedef std::basic_string<unsigned char> ustring;   // primarily used for sigs


class SimonClientC;
class SimonContextC;

///////////////////////////////////////////////////////////////////////////////
/// Abstract base class for blurbs, atomic chunks of monitoring data.
///////////////////////////////////////////////////////////////////////////////
class SimonBlurbC {
 public:

    /**
     * Fills in the blurb with the current stored metric data. The 
     * caller should set all the tag values in the blurb before calling
     * this function. After the call, the blurb's metric data can be
     * retrieved using the accessor functions.  For counters, this is
     * the current absolute value of the counter, while for gauges it is
     * the most recently set value.
     *
     * Returns true if the blurb data was retrieved successfully, or
     * false if the given tags did not match any stored data - i.e. there
     * has not been an update for this blurb type with this set of tag
     * values.
     */
    bool fetch();

    /**
     * Updates the Simon client library with the contents of the blurb. The
     * caller should set all the tag values and all the metric values in
     * the blurb before calling this function. The blurb contents are not
     * modified by this call, and the blurb object can be discarded or
     * re-used afterwards.
     *
     * Note that in the case of counters, an incremental value has to be
     * specified using the incr<Name> function. If the application is keeping
     * an absolute count, it is necessary to use the fetch() function first,
     * and then use the get<Name> function to get the old value of the 
     * counter and subtract it to get the incremental value.
     *
     * If monitoring is not currently in progress, this function does 
     * nothing.
     */
    void update();

    /**
     * Removes stored data from the client library.  The caller should set
     * all the tag values in the blurb before calling this function.  Any
     * stored data corresponding to that set of tag values is removed.
     */
    void purge();
    

 protected:

    SimonBlurbC() {}
    virtual ~SimonBlurbC() {}

    virtual uint8_t getBlurbCode() = 0;

    // Returns the tag format string
    virtual const char *getTagFormat() = 0;

    // Returns array of offsets of the tag elements in the Blurb struct
    virtual const size_t *getTagOffsets() = 0;

    // Returns the metric format string
    virtual const char *getMetricFormat() = 0;

    // Returns array of offsets of the metric elements in the Blurb struct
    virtual const size_t *getMetricOffsets() = 0;
    
    // Returns pointer to the context associated with the blurb.
    virtual SimonContextC &getContext() = 0;	

    // Removes stored data for the specified blurb type
    static void purgeAll(SimonContextC &simonContext,  uint8_t blurbId);

 private:

    /// Packs args into a buffer, given a format string & an array of offsets.
     /// Format string indicates how to translate args to bytes in the buffer.
     /// Possible format string entries: 
     ///  'B' indicates a single byte, 'b' indicates counter
     ///  'S' indicates a short (2B) value, 's' indicates counter
     ///  'L' indicates a long (4B) value, 'l' indicates counter
     ///  'F' indicates a float (4B) value, 'f' indicates counter
     ///  'P' or 'p' indicates a Pascal-style string (1 byte length before data)
     /// Note: Counters are treated differently than gauges. 
     /// Gauges are represented by uppercase chars, and the value at the arg
     /// offset is always written directly to the output buffer.
     /// Counters are represented by lowercase chars, and the value at the arg
     /// offset is summed with the value in prevResult (if one exists), and the
     /// sum is written to the output buffer.
     /// \param result  The output buffer
     /// \param prevResult A buffer containing previous values for all args
     /// \param format A format string, as defined above
     /// \param offsets Typically the return value of get*Offsets() above
     void L_PackData(ustring &result, ustring &prevResult, const char *format, 
             const size_t * offsets);
	
     /// Unpacks bytes from a buffer to values in a variable list of args.
     /// \param input The input buffer.
     /// \param format A format string.  \see L_PackData()
     /// \param offsets Typically the return value of getMetricOfsets()
     void L_UnpackData(ustring &input, const char *format,  const size_t *offsets);
	
     /// Helper method for L_PackData() & L_UnpackData()
     inline void L_ReadBytes(ustring::iterator &in, void *out, int bytes) {
         unsigned char *ucp= reinterpret_cast<unsigned char *>(out);
         for(int i=0; i<bytes; i++) {
             *ucp++ = *in++;
         }
     }

    inline void L_ReadBytes(void *in, void *out, int bytes) {
        unsigned char *inP=   reinterpret_cast<unsigned char *>(in);
        unsigned char *outP= reinterpret_cast<unsigned char *>(out);
        for(int i=0; i<bytes; i++) {
            *outP++ = *inP++;
        }
     }

     /// Helper method for L_PackData() 
     inline void L_WriteBytes(void *in, ustring &out, int bytes) {
         unsigned char *ucp= reinterpret_cast<unsigned char *>(in);
         for(int i=0; i<bytes; i++) {
             out+= *ucp++;
         }
     }
	
};  // end of class SimonBlurbC




///////////////////////////////////////////////////////////////////////////////
/// Callback class used in registerUpdater
///////////////////////////////////////////////////////////////////////////////    
class SimonUpdaterC {

  public:
    virtual ~SimonUpdaterC() {}
    virtual void doUpdates(SimonContextC *simonContext) = 0;
};	


///////////////////////////////////////////////////////////////////////////////
/// Parent class for statistic contexts, which contain all info from the xml
/// file for an app, and are shared by all blurbs for the app.
/// End users of instances of subclasses of this class are client application
/// threads that wish to monitor statistics.  These end users are not 
/// interested in protocol-specific details such as signatures, so accessors
/// are minimal.
///////////////////////////////////////////////////////////////////////////////
class SimonContextC {

 public:	
    
    /**
     * Initiates the sending of blurb messages to the specified Aggregator
     * server. Once monitoring has been started, it continues until stopped
     * by a call to stopMonitoring(). It should not be started when it is
     * already in progress.
     *
     * The cluster name is used on the Aggregator and SimonWeb servers to 
     * compartmentalize the data. The application developer can choose to 
     * make this fixed for all instances of the appliction, or use it to
     * reflect the organization of those instances into clusters.
     *
     * Returns true if successful.  In the failure case, the errMsg parameter
     * contains a message indicating the reason.
     */
    bool startMonitoring(const char *cluster,
             const char *aggregatorHost,
             int aggregatorPort,
             std::string *errMsg=NULL);

    /**
     * Terminates the sending of blurb messages to an Aggregator server.
     * Once monitoring has been stopped, and can be started again using
     * startMonitoring, possibly to a different Aggregator host.
     */
    bool stopMonitoring(std::string *errMsg = 0);


    /**
     * Registers an Updater object to be called by the client library just
     * before it is to send a blurb message to the Aggregator.
     *
     * Returns true if successful. In the failure case, the errMsg parameter
     * contains a message indicating the reason.
     */
    bool registerUpdater(SimonUpdaterC *updater, std::string *errMsg=NULL);

    /**
     * As above, but takes a pointer to a C function instead of a pointer to
     * an Updater object.
     */
    bool registerUpdater(SimonUpdaterFuncPT funcP, std::string *errMsg=NULL);

    /**
     * Undoes the effect of registerUpdater().
     *
     * Returns true if successful.  In the failure case, the errMsg parameter
     * contains a message indicating the reason.
     */
    bool unregisterUpdater(SimonUpdaterC *updater, std::string *errMsg=NULL);

    bool unregisterUpdater(SimonUpdaterFuncPT funcP, std::string *errMsg=NULL);

    /** 
     * Returns the blurb message name.
     */	
    std::string getBlurbMessageName();

    /**
     * Returns the cluster name.
     */
    std::string getCluster();

    /** 
     * Returns the Aggregator host.
     */
    std::string getHost();

    /**
     * Returns the Aggregator port
     */
    int getPort();

    /** 
     * Causes config packets to be sent with probability 1/n every blurb period.
     */
    void setConfigMessageInterval(int n);

    /**
     * Returns the current config interval
     */
    int getConfigMessageInterval();


    static const int DEFAULT_RESEND_COUNT = 5;

    /**
     * Returns the current "resend count" for this context.  The resend count is
     * the number of times after an update that a blurb is re-sent to the server
     * if it isn't updated again.
     */
    int getResendCount() {
        return _resendCount;
    }

    /**
     * Sets the resend count.  The parameter should be between 0 and 100. This 
     * call will only take effect on the next call to startMonitoring.
     */
    void setResendCount(int resendCount) {
        _resendCount = ( resendCount < 0 ? 0 
                       : resendCount > 100 ? 100
                       : resendCount );
    }

    static const std::string getVersion();

 protected:

    ///////////////////////////////////////////////////////////////////////////////
    /// Represents a "signature" in the Simon protocol, which identifies either
    /// a blurbMessage element or a report element of a Simon config file.
    ///////////////////////////////////////////////////////////////////////////////

     class SimonSigC {

       public:
         /**
          * Constructor.
          *
          * name:    name field of signature
          * version: 4B version info of signature
          * md5:     the least significant 4B of the md5 for the signature
          * per:     the period field of the signature (in seconds) 
          *
          * The period is only used in the case of a blurbMessage signature,
          * not in a report signature. Also, the period is not included in the
          * encoded signature returned by getBytes().
          */
         SimonSigC(const char *name, int version, int md5, int per=0);
		
         // Returns the bytes of this signature, encoded in the Simon protocol
         ustring &getBytes() {  return _bytes; }
		
         // Accessors
         int getPeriod() { return _period; }
         std::string getName() { return _name; }
		
       private:
         std::string _name;
         int _version;
         int _md5;
         int _period;
         ustring _bytes;

     };  // end of class SimonSigC
	
     /// Non-public constructor because subclasses are intended to be singletons
     SimonContextC();
     virtual ~SimonContextC();

     // Returns the BlurbMessage Signature
     virtual SimonSigC *getBlurbMessageSig() = 0;

     // Returns the number of reports (size of the reports array)
     virtual int getNumReports() = 0;

     // Returns the array of Report Signatures
     virtual SimonSigC *getReportSigs() = 0;

     // Returns compressed config file
     virtual ustring& getConfig() = 0;

     // Returns name of application
     virtual std::string getAppName() = 0;

     // Returns timestamp indicating when this file was generated 
     // (milliseconds since the epoch)
     virtual uint64_t getTimeStamp() = 0;

     // Methods that SimonBlurbC needs:
     void getBlurb(ustring &bTags, ustring &bMetrics);
     void setBlurb(ustring &bTags, ustring &bMetrics);

     // delete data associated with a particular blurb type and tag set
     void purgeBlurb(ustring &bTags);

     // delete data associated with a particular blurb type
     void purgeAllBlurbsOfType(uint8_t blurbID);

     // set/get the average number of periods between config messages
     void setConfigFreq(int freq);
     int getConfigFreq();

  private:	

     friend class SimonBlurbC;
     friend class SimonSessionC;
	
     void _initNode();

     // returns node name, i.e. hostname
     std::string _getNode();

     SimonSessionC *_simonSession;
     CallbackRegistryC *_callbackRegistry;
     std::string _node;
     int _configFreq;
     int _resendCount;
     void *_lock;

};  //end of class SimonContextC

}   //namespace

#endif /*SIMONCONTEXT_H_*/
