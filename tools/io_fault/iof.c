#include <stdint.h>
#include <stdbool.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <limits.h> /* PATH_MAX */
#include <assert.h>

#include <unistd.h>
#include <errno.h>
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/select.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/socket.h>

#include <sys/socket.h>
#include <netinet/in.h>
#include <arpa/inet.h>

const char* _usages = "Usages: ./iof action path probability fake_err extra_arg"
  "Example:\n"
  "\t./iof test\n"
  "\t iof_ctrl=iof.ctrl ./iof set ~/dir probability EAGAIN\n"
  "\t./iof set ~/dir probability EAGAIN\n"
  "\t./iof set ~/dir probability EDELAY 10\n # delay 10us"
  "\t./iof set tcp:8000 probability EDELAY 10\n"
  "\t./iof clear # delete fault rule\n"
  "\t./iof list # list fault rule\n"
  "\tSymbolic Error List:\n"
  "\tEDELAY, EINTR, EIO, EAGAIN, ENOMEM, EACCES, EPERM, EBUSY,EMFILE, ENOSPC, ETIMEDOUT\n";

#define array_len(A) (sizeof(A)/sizeof(A[0]))
#define error(...) fprintf(stderr, __VA_ARGS__);
enum {
  REG_TYPE,
  SOCK_TYPE,
  OTHER_TYPE,
};

struct _iof_t
{
  int64_t seq;
  int64_t count;
  float prob;
  int32_t type;
  dev_t dev_no;
  int32_t port;
  int err;
  int64_t arg;
};
typedef struct _iof_t iof_t;
const int64_t MAX_N_IOF_RULES = 1<<20;
const char* DEFAULT_CTRL_FILE = "iof.ctrl";

struct _iof_queue_t
{
  int64_t front;
  int64_t rear;
  int64_t capacity;
  iof_t* rules;
};

struct name_id_map_t
{
  char* name;
  int id;
};
#define EDELAY 1234
#define ERR_DEF(err) {#err, err}
static struct name_id_map_t err_map[] = {
  {"OK", 0},
  ERR_DEF(EDELAY),
  ERR_DEF(EINTR), ERR_DEF(EIO), ERR_DEF(EAGAIN), ERR_DEF(ENOMEM),
  ERR_DEF(EACCES), ERR_DEF(EPERM), ERR_DEF(EBUSY),ERR_DEF(EMFILE),
  ERR_DEF(ENOSPC), ERR_DEF(ETIMEDOUT),
};
int parse_err(const char* err_str)
{
  int i = 0;
  for(i = 0; i < array_len(err_map); i++)
  {
    if (0 == strcmp(err_map[i].name, err_str))
    {
      return err_map[i].id;
    }
  }
  return atoi(err_str);
}

const char* repr_err(int err)
{
  int i = 0;
  static char buf[256];
  for(i = 0; i < array_len(err_map); i++)
  {
    if (err_map[i].id == err)
    {
      return err_map[i].name;
    }
  }
  snprintf(buf, sizeof(buf), "%d", err);
  buf[sizeof(buf-1)] = 0;
  return buf;
}

static void* file_map(const char* path, size_t len)
{
  char* buf = NULL;
  int fd = -1;
  if ((fd =open(path, O_RDWR|O_CREAT, S_IRWXU)) < 0 || -1 == ftruncate(fd, len))
  {
    perror("file_map:");
    return NULL;
  }
  buf = (char*)mmap(NULL, len, PROT_READ|PROT_WRITE, MAP_SHARED, fd, 0);
  close(fd);
  if (MAP_FAILED == buf)
  {
    perror("file_map:");
    buf = NULL;
  }
  return buf;
}

int get_path_by_fd(char* path, int64_t len, int fd)
{
  int err = 0;
  char proc_path[PATH_MAX];
  int64_t count = 0;
  if (NULL == path || len <= 0 || len < PATH_MAX || fd < 0)
  {
    err = -EINVAL;
  }
  else if (0 != access("/proc/self/fd", X_OK))
  {
    err = errno;
    perror("access:");
  }
  else if (0 >= (count = sprintf(proc_path, "/proc/self/fd/%i/", fd)) || count > len)
  {
    err = -ENOBUFS;
    error("snprintf(): error\n");
  }
  else if (NULL == realpath(proc_path, path))
  {
    err = errno;
    error("realpath(proc_path=%s)=>NULL\n", proc_path);
  }
  return err;
}

dev_t get_dev_no_by_fd(int fd)
{
  int err = 0;
  struct stat _stat;
  dev_t dev_no = 0;
  if (0 != fstat(fd, &_stat))
  {
    err = errno;
    perror("fstat:");
  }
  else
  {
    dev_no = _stat.st_dev;
  }
  return dev_no;
}

dev_t get_dev_no_by_path(const char* path)
{
  int err = 0;
  int fd = -1;
  dev_t dev_no = 0;
  if (0 > (fd = open(path, O_RDONLY)))
  {
    err = errno;
    perror("open:");
  }
  else
  {
    dev_no = get_dev_no_by_fd(fd);
  }
  if (0 == err)
  {
    close(fd);
  }
  return dev_no;
}

int get_port_by_sock(int fd)
{
  int err = 0;
  int port = 0;
  struct sockaddr_in sa;
  socklen_t sa_len = sizeof(sa);
  if (0 != getsockname(fd, (struct sockaddr*)&sa, &sa_len))
  {
    perror("getsockname:");
    err = errno;
  }
  else
  {
    // char* ip = inet_ntoa(sa.sin_addr);
    port = (int)ntohs(sa.sin_port);
  }
  return port;
}

int test()
{
  int err = 0;
  int fd = -1;
  char* path = "/tmp/abc";
  //char real_path[PATH_MAX];
  printf("start test:\n");
  assert((fd =open(path, O_RDWR|O_CREAT, S_IRWXU)) > 0);
  printf("dev_no(path=%s)=%d\n", path, (int)get_dev_no_by_path(path));
  /* assert(0 == get_path_by_fd(real_path, sizeof(real_path), fd)); */
  /* assert(0 == strcmp(real_path, path)); */
  return err;
}

int set_iof(iof_t* iof, iof_t* val)
{
  int64_t old_seq = iof->seq;
  val->seq = old_seq + 1;
  val->count = iof->count;
  return 0 == (old_seq&1) && __sync_bool_compare_and_swap(&iof->seq, old_seq, old_seq+1)
    && memcpy(iof, val, sizeof(*iof)) && __sync_bool_compare_and_swap(&iof->seq, old_seq+1, old_seq+2);
}

bool get_iof(iof_t* iof, iof_t* val)
{
  int64_t old_seq = iof->seq;
  return 0 == (old_seq&1)  && memcpy(val, iof, sizeof(*iof))
    && iof->seq == old_seq;
}

iof_t* get_iof_ctrl()
{
  static iof_t* iof = NULL;
  return iof = (iof? :file_map(getenv("iof_ctrl")?:DEFAULT_CTRL_FILE, sizeof(*iof)));
}

void clean_iof_ctrl() __attribute__ ((constructor));
void clean_iof_ctrl()
{
  iof_t* iof = get_iof_ctrl();
  if (iof)memset(iof, 0, sizeof(iof));
}

const char* iof_type_repr(int type)
{
  static const char* type_repr[] = {"REG", "SOCK", "OTHER",};
  return (type >= 0 && type < array_len(type_repr))? type_repr[type]: "UNKONWN";
}

int clear_iof_rule()
{
  int err = 0;
  iof_t* iof = NULL;
  iof_t val;
  if (NULL == (iof = get_iof_ctrl()))
  {
    err = errno;
  }
  else
  {
    memset(&val, 0, sizeof(val));
    while(!set_iof(iof, &val))
      ;
  }
  return err;
}

int list_iof_rule()
{
  int err = 0;
  int64_t idx = 0;
  iof_t* iof = NULL;
  iof_t val;
  if (NULL == (iof = get_iof_ctrl()))
  {
    err = errno;
  }
  else
  {
    while(!get_iof(iof, &val))
      ;
    printf("Rule[%ld]: seq=%ld, count=%ld, type=%s, dev=%d, port=%d, err=%s, arg=%ld\n",
           idx, val.seq, val.count, iof_type_repr(val.type), (int)val.dev_no, val.port, repr_err(val.err), val.arg);
  }
  return err;
}

#define TCP_PREFIX "tcp:"
int set_iof_rule(const char* path, const char* prob, const char* fake_err, const char* extra_arg)
{
  int err = 0;
  iof_t* iof = NULL;
  iof_t val;
  memset(&val, 0, sizeof(val));
  if (NULL == path || NULL == prob || NULL == fake_err || NULL == extra_arg)
  {
    err = -EINVAL;
    error("set_iof_rule():EINVAL\n");
  }
  else if (NULL == (iof = get_iof_ctrl()))
  {
    err = errno;
  }
  else if (0 == strncmp(TCP_PREFIX, path, strlen(TCP_PREFIX)))
  {
    val.type = SOCK_TYPE;
    val.port = atoi(path + strlen(TCP_PREFIX));
  }
  else
  {
    val.type = REG_TYPE;
    val.dev_no = get_dev_no_by_path(path);
  }
  if (0 == err)
  {
    val.prob = atof(prob);
    val.err = parse_err(fake_err);
    val.arg = atoi(extra_arg);
    while(!set_iof(iof, &val))
      ;
  }
  return err;
}

int iof_hook(int fd)
{
  int err = 0;
  struct stat st;
  iof_t* iof = NULL;
  iof_t val;
  iof_t pat;
  memset(&val, 0, sizeof(val));
  memset(&pat, 0, sizeof(pat));
  if (0 != fstat(fd, &st))
  {}
  else if (NULL == (iof = get_iof_ctrl()))
  {}
  else
  {
    while(!get_iof(iof, &pat))
      ;
     if (S_ISREG(st.st_mode))
     {
       val.type = REG_TYPE;
       val.dev_no = st.st_dev;
     }
     else if (S_ISSOCK(st.st_mode))
     {
       val.type = SOCK_TYPE;
       val.port = get_port_by_sock(fd);
     }
     else
     {
       val.type = OTHER_TYPE;
     }
     if (val.type == pat.type && val.dev_no == pat.dev_no && val.port == pat.port && (random()%1000)/1000.0 < pat.prob)
     {
       (void)__sync_fetch_and_add(&iof->count, 1);
       switch(pat.err)
       {
         case 0:
           break;
         case EDELAY:
           usleep(pat.arg);
           break;
         case ETIMEDOUT:
           usleep(pat.arg);
           err = -pat.err;
           break;
         default:
           err = -pat.err;
           break;
       }
     }
  }
  return err;
}

const char my_interp[] __attribute__((section(".interp")))
    = "/lib64/ld-linux-x86-64.so.2";

int mymain(int argc, char** argv);
extern char** environ;
void mystart()
{
  uint64_t bp = 0;
  asm volatile("\n\tmov %%rbp, %0\n":"=m"(bp));
  environ = (char**)(bp + 24 + 8 * *(int*)(bp + 8));
  exit(mymain(*((int*)(bp+8)), (char**)(bp+16)));
}

int mymain(int argc, char** argv)
{
  int err = 0;
#define getarg(i) argc > i? argv[i]: NULL
  char* func = getarg(1);
  char* path = getarg(2);
  char* prob = getarg(3);
  char* fake_err = getarg(4);
  char* extra_arg = getarg(5);
  if (argc > 6 || NULL == func)
  {
    err = -EINVAL;
  }
  else if (0 == strcmp(func, "test"))
  {
    err = test();
  }
  else if (0 == strcmp(func, "clear"))
  {
    err = clear_iof_rule();
  }
  else if (0 == strcmp(func, "list"))
  {
    err = list_iof_rule();
  }
  else if (0 == strcmp(func, "set"))
  {
    if (NULL == path || NULL == prob || NULL == fake_err)
    {
      err = -EINVAL;
    }
    else
    {
      err = set_iof_rule(path, prob, fake_err, extra_arg?: "0");
    }
  }
  else
  {
    err = -EINVAL;
  }
  if (-EINVAL == err)
  {
    fprintf(stderr, _usages);
  }
  return err;
}
