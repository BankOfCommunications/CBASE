#include "hdfs.h"
#include "hdfsJniHelper.h"

#include "common/ob_define.h"

#define UGI_INFO "hadoop.job.ugi"
#define UGI_DELIMA ","
#define DEFAULT_FS "fs.default.name"

/*  Some frequently used Java paths */
#define HADOOP_CONF     "org/apache/hadoop/conf/Configuration"
#define HADOOP_PATH     "org/apache/hadoop/fs/Path"
#define HADOOP_LOCALFS  "org/apache/hadoop/fs/LocalFileSystem"
#define HADOOP_FS       "org/apache/hadoop/fs/FileSystem"
#define HADOOP_BLK_LOC  "org/apache/hadoop/fs/BlockLocation"
#define HADOOP_DFS      "org/apache/hadoop/hdfs/DistributedFileSystem"
#define HADOOP_ISTRM    "org/apache/hadoop/fs/FSDataInputStream"
#define HADOOP_OSTRM    "org/apache/hadoop/fs/FSDataOutputStream"
#define HADOOP_STAT     "org/apache/hadoop/fs/FileStatus"
#define HADOOP_FSPERM   "org/apache/hadoop/fs/permission/FsPermission"
#define HADOOP_UNIX_USER_GROUP_INFO "org/apache/hadoop/security/UnixUserGroupInformation"
#define HADOOP_USER_GROUP_INFO "org/apache/hadoop/security/UserGroupInformation"
#define JAVA_NET_ISA    "java/net/InetSocketAddress"
#define JAVA_NET_URI    "java/net/URI"
#define JAVA_STRING     "java/lang/String"
#define JAVA_ISTRM      "java/io/InputStream"

#define JAVA_VOID       "V"

/*  Macros for constructing method signatures */
#define JPARAM(X)           "L" X ";"
#define JARRPARAM(X)        "[L" X ";"
#define JMETHOD1(X, R)      "(" X ")" R
#define JMETHOD2(X, Y, R)   "(" X Y ")" R
#define JMETHOD3(X, Y, Z, R)   "(" X Y Z")" R

#define VOID          'V'
#define JOBJECT       'L'
#define JARRAYOBJECT  '['
#define JBOOLEAN      'Z'
#define JBYTE         'B'
#define JCHAR         'C'
#define JSHORT        'S'
#define JINT          'I'
#define JLONG         'J'
#define JFLOAT        'F'
#define JDOUBLE       'D'

static void destroyLocalReference(JNIEnv *env, jobject jObject)
{
  if (jObject)
    (env)->DeleteLocalRef(jObject);
}

jobject constructNewObjectOfClass(JNIEnv *env, Exc *exc, const char *className,
                                  const char *ctorSignature, ...)
{
  va_list args;
  jclass cls;
  jmethodID mid;

  va_start(args, ctorSignature);
  jobject jobj = NULL;
  jthrowable jthr;

  cls = env->FindClass(className);
  if (cls == NULL) {
    fprintf(stderr, "can't find class\n");
    return NULL;
  }

  mid = env->GetMethodID(cls, "<init>", ctorSignature);
  if (mid == NULL) {
    return NULL;
  }

  va_start(args, ctorSignature);
  jobj = (env)->NewObjectV(cls, mid, args);
  va_end(args);

  jthr = (env)->ExceptionOccurred();
  if (jthr != NULL) {
    if (exc != NULL)
      *exc = jthr;
    else
      (env)->ExceptionDescribe();
  }
  return jobj;
}

/**
 * getJNIEnv: A helper function to get the JNIEnv* for the given thread.
 * If no JVM exists, then one will be created. JVM command line arguments
 * are obtained from the LIBHDFS_OPTS environment variable.
 *
 * @param: None.
 * @return The JNIEnv* corresponding to the thread.
 */
JNIEnv* getJNIEnv(void)
{

  const jsize vmBufLength = 1;
  JavaVM* vmBuf[vmBufLength];
  JNIEnv *env;
  jint rv = 0;
  jint noVMs = 0;

  rv = JNI_GetCreatedJavaVMs(&(vmBuf[0]), vmBufLength, &noVMs);
  if (rv != 0) {
    fprintf(stderr, "JNI_GetCreatedJavaVMs failed with error: %d\n", rv);
    return NULL;
  }

  if (noVMs == 0) {
    //Get the environment variables for initializing the JVM
    char *hadoopClassPath = getenv("CLASSPATH");
    if (hadoopClassPath == NULL) {
      fprintf(stderr, "Environment variable CLASSPATH not set!\n");
      return NULL;
    }

    char *hadoopClassPathVMArg = "-Djava.class.path=";
    size_t optHadoopClassPathLen = strlen(hadoopClassPath) +
      strlen(hadoopClassPathVMArg) + 1;
    char *optHadoopClassPath = static_cast<char *>(malloc(sizeof(char)*optHadoopClassPathLen));
    snprintf(optHadoopClassPath, optHadoopClassPathLen,
             "%s%s", hadoopClassPathVMArg, hadoopClassPath);

    int noArgs = 1;
    //determine how many arguments were passed as LIBHDFS_OPTS env var
    char *hadoopJvmArgs = getenv("LIBHDFS_OPTS");
    char jvmArgDelims[] = " ";
    if (hadoopJvmArgs != NULL)  {
      char *result = NULL;
      result = strtok( hadoopJvmArgs, jvmArgDelims );
      while ( result != NULL ) {
        noArgs++;
        result = strtok( NULL, jvmArgDelims);
      }
    }
    JavaVMOption options[noArgs];
    options[0].optionString = optHadoopClassPath;

    //fill in any specified arguments
    if (hadoopJvmArgs != NULL)  {
      char *result = NULL;
      result = strtok( hadoopJvmArgs, jvmArgDelims );
      int argNum = 1;
      for (;argNum < noArgs ; argNum++) {
        options[argNum].optionString = result; //optHadoopArg;
      }
    }

    //Create the VM
    JavaVMInitArgs vm_args;
    JavaVM *vm;
    vm_args.version = JNI_VERSION_1_2;
    vm_args.options = options;
    vm_args.nOptions = noArgs;
    vm_args.ignoreUnrecognized = 1;

    rv = JNI_CreateJavaVM(&vm, (void**)&env, &vm_args);
    if (rv != 0) {
      fprintf(stderr, "Call to JNI_CreateJavaVM failed "
              "with error: %d\n", rv);
      return NULL;
    }

    free(optHadoopClassPath);
  }
  else {
    //Attach this thread to the VM
    JavaVM* vm = vmBuf[0];
    rv = (vm)->AttachCurrentThread((void **)&env, 0);
    if (rv != 0) {
      fprintf(stderr, "Call to AttachCurrentThread "
              "failed with error: %d\n", rv);
      return NULL;
    }
  }

  return env;
}

int invokeMethod(JNIEnv *env, jvalue *retval, Exc *exc,
                 jobject instObj, const char *className,
                 const char *methName, const char *methSignature, ...)
{
  UNUSED(exc);
  jclass cls;
  jmethodID mid;
  va_list args;
  va_start(args, methSignature);

  cls = env->FindClass(className);
  if (cls == NULL) {
    fprintf(stderr, "can't find class, classname=%s\n", className);
    return -1;
  }

  mid = env->GetMethodID(cls, methName, methSignature);
  if (mid == NULL) {
    fprintf(stderr, "can't find method %s, sig=%s\n", methName, methSignature);
    return -1;
  }

  const char *str = methSignature;
  while (*str != ')') str++;
  str++;

  switch (*str) {
   case 'V':
     break;
   case 'L':
     retval->l = env->CallObjectMethodV(instObj, mid, args);
     break;
   default:
     //TODO:add other arch
     break;
  }

  va_end(args);

  return 0;
}

static int parseGroup(char *&user, char *group[], int &group_size, const char *ugi)
{
#define UGI_BUFF_SIZE 4096
  static char ugi_buff[UGI_BUFF_SIZE];

  if (strlen(ugi) > UGI_BUFF_SIZE) {
    fprintf(stderr, "HDFS:parseGroup group name is too len %ld\n", strlen(ugi));
    return -1;
  }

  strcpy(ugi_buff, ugi);
  user = strtok(ugi_buff, UGI_DELIMA);
  if (user == NULL) {
    fprintf(stderr, "no user name in ugi = %s\n", ugi);
    return -1;
  }

  int gidx = 0;
  while (gidx < group_size) {
    char *str = strtok(NULL, UGI_DELIMA);
    if (str == NULL) {
      break;
    }
    group[gidx++] = str;
  }
  group_size = gidx;

  if (group_size == 0) {
    fprintf(stderr, "no group info is specified\n");
    return -1;
  }

  return 0;
}

static int parseHostAndPort(char *&host, unsigned short port, const char *fs_str)
{
  UNUSED(host);
  UNUSED(port);
#define HOST_PORT_BUFF_SIZE 4096
  static char host_port_buff[HOST_PORT_BUFF_SIZE];
  int ret = -1;

  strcpy(host_port_buff, fs_str);
  const char *str = fs_str + strlen(fs_str) - 1;
  while (str > fs_str) {
    if (*str == ':') {                          /* fs format host:port */
      ret = 0;
      break;
    }
    str--;
  }

  return ret;
}

int hdfsGetConf(char *&host, unsigned short &port, char *&user, char *group[], int &group_size)
{
  jobject jConfiguration = NULL;
  JNIEnv *env = 0;
  jthrowable jExc = NULL;
  int ret = 0;

  env = getJNIEnv();
  if (env == NULL) {
    fprintf(stderr, "can't JNI Env\n");
    return -1;
  }

  jConfiguration =
    constructNewObjectOfClass(env, &jExc, HADOOP_CONF, "()V");
  if (jConfiguration == NULL) {
    fprintf(stderr, "Can't construct instance of class "
            "org.apache.hadoop.conf.Configuration\n");
    if (jExc != NULL) {
      env->ExceptionDescribe();
    }

    return -1;
  }

  jvalue jVal;
  jstring ugiStr = (env)->NewStringUTF(UGI_INFO);
  jstring fsStr = env->NewStringUTF(DEFAULT_FS);

  if (invokeMethod(env, &jVal, NULL, jConfiguration, HADOOP_CONF, "get", JMETHOD1(JPARAM(JAVA_STRING), JPARAM(JAVA_STRING)), ugiStr) != 0) {
    //set err return
    ret = -1;

    fprintf(stderr, "can't invoke method get ugi\n");
    if (env->ExceptionOccurred()) {
      env->ExceptionDescribe();
    }
  }

  //parse group and user info
  if (ret == 0) {
    if (jVal.l != NULL) {
      const char *ugi = (env)->GetStringUTFChars((jstring)jVal.l, NULL);

      if (ugi != NULL) {
        fprintf(stdout, "ugi string is %s\n", ugi);

        if (parseGroup(user, group, group_size, ugi)) {
          fprintf(stderr, "can't parse group info\n");
          ret = -1;
        }

        env->ReleaseStringUTFChars((jstring)jVal.l, ugi);
      }
    } else {
      fprintf(stdout, "can't find gui=%s\n", UGI_INFO);
    }
  }

  if (ret == 0) {                               /* get host and port info */
    if (invokeMethod(env, &jVal, NULL, jConfiguration, HADOOP_CONF, "get", JMETHOD1(JPARAM(JAVA_STRING), JPARAM(JAVA_STRING)), fsStr) != 0) {
      ret = -1;

      fprintf(stderr, "can't invoke method get default fs\n");
      if (env->ExceptionOccurred()) {
        env->ExceptionDescribe();
      }
    } else {
      if (jVal.l != NULL) {
        const char *str = (env)->GetStringUTFChars((jstring)jVal.l, NULL);

        if (str != NULL) {
          fprintf(stdout, "fs string is %s\n", str);
          if (parseHostAndPort(host, port, str)) {
            fprintf(stderr, "can't parse host and port info\n");
            ret = -1;
          }

          env->ReleaseStringUTFChars((jstring)jVal.l, str);
        }
      } else {
        fprintf(stdout, "can't find gui=%s\n", UGI_INFO);
      }
    }
  }

  destroyLocalReference(env, ugiStr);
  destroyLocalReference(env, jConfiguration);

  return ret;
}

int main()
{
  int ret = 0;
  char *user = NULL;
  unsigned short port = 0;
  int group_size = 10;
  char *group[group_size];
  char *host = NULL;

  ret = hdfsGetConf(host, port, user, group, group_size);
  if (ret != 0) {
    fprintf(stderr, "can't get conf\n");
    return ret;
  }

  fprintf(stdout, "host = %s, port = %d, user = %s, group_size = %d\n",
          host, port, user, group_size);

  for (int i = 0;i < group_size;i++) {
    fprintf(stdout, "group %d: %s\n", i, group[i]);
  }

  return ret;
}
