#define _GNU_SOURCE
#include <dlfcn.h>
#include <errno.h>
#include <limits.h>
#include <stdatomic.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>

static _Atomic unsigned long long skipped;
static int skip_wal(int fd) {
 const char *target = getenv("SLOPDB_TPCC_WAL_FILE");
 if (!target || !*target) return 0;
 char link[64], path[PATH_MAX];
 snprintf(link,sizeof(link),"/proc/self/fd/%d",fd);
 ssize_t n=readlink(link,path,sizeof(path)-1);
 if(n<0) return 0;
 path[n]=0;
 if(strcmp(path,target)!=0) return 0;
 atomic_fetch_add(&skipped,1);
 return 1;
}
int fsync(int fd) {
 if(skip_wal(fd))return 0;
 int (*real_fsync)(int)=dlsym(RTLD_NEXT,"fsync");
 if(!real_fsync){errno=ENOSYS;return -1;}
 return real_fsync(fd);
}
int fdatasync(int fd) {
 if(skip_wal(fd))return 0;
 int (*real_fdatasync)(int)=dlsym(RTLD_NEXT,"fdatasync");
 if(!real_fdatasync){errno=ENOSYS;return -1;}
 return real_fdatasync(fd);
}
unsigned long long slopdb_tpcc_wal_sync_skipped(void){return atomic_load(&skipped);}
