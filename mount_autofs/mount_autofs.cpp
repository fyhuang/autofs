#define FUSE_USE_VERSION 26
#include <fuse.h>

#include <cstdio>
#include <cstdlib>
#include <stdint.h>

#include <unistd.h>
#include <errno.h>
#include <fcntl.h>

#include <limits.h>
#include <syslog.h>
#include <pthread.h>

#include <zmq.h>

#include "logging.h"
#include "network.h"

// FUSE functions
static void *autofs_init(struct fuse_conn_info *conn) {
    // Connect
    connect("tcp://localhost:54321");

    DBPRINTF("Connected!\n");
    return NULL;
}

static int autofs_stat(const char *path, struct stat *stbuf) {
    memset(stbuf, 0, sizeof(struct stat));
    stbuf->st_uid = geteuid();
    stbuf->st_gid = getegid();

    ReqStat req_stat;
    req_stat.set_filepath(path);

    // Send request
    if (send_packet(REQ_STAT, &req_stat) < 0) {
        return -EIO;
    }

    DBPRINTF("stat %s\n", path);

    // Get response
    RespStat sr;
    if (recv_packet(&sr) != ERR_NONE) {
        return -ENOENT;
    }

    time_t ctime_utc = sr.ctime_utc();
    struct tm *tm_loc = localtime(&ctime_utc);
    time_t ctime_local = mktime(tm_loc);
    stbuf->st_ctime = ctime_local;
    stbuf->st_mtime = ctime_local;
    stbuf->st_atime = ctime_local;

    if (sr.ftype() & S_IFDIR) {
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = sr.size();
        stbuf->st_ino = sr.inode();
        DBPRINTF("result dir\n");
        return 0;
    }
    else if (sr.ftype() & S_IFREG) {
        stbuf->st_mode = S_IFREG | 0644;
        stbuf->st_nlink = 1;
        stbuf->st_size = sr.size();
        stbuf->st_ino = sr.inode();

        DBPRINTF("result file, size %lu b\n", sr.size());
        //DBPRINTF("ctime_utc %d, ctime %d, %s", ctime_utc, stbuf->st_ctime, ctime(&ctime_utc));
        return 0;
    }

    DBPRINTF("result NOENT\n");
    return -ENOENT;
}


// FUSE
static int nofs_getattr(const char *path, struct stat *stbuf)
{
    DBPRINTF("nofs_getattr %s\n", path);

    if (strcmp(path, "/") == 0) {
        stbuf->st_uid = geteuid();
        stbuf->st_gid = getegid();
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        return 0;
    }

    return autofs_stat(path, stbuf);
}

static int nofs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                        off_t offset, struct fuse_file_info *fi)
{
    DBPRINTF("nofs_readdir %s\n", path);

    ReqListdir req_listdir;
    req_listdir.set_dirpath(path);
    send_packet(REQ_LISTDIR, &req_listdir);

    RespListdir rl;
    if (recv_packet(&rl) != ERR_NONE) {
        return -ENOENT;
    }

    filler(buf, ".", NULL, 0);
    filler(buf, "..", NULL, 0);

    for (int i = 0; i < rl.entries_size(); i++) {
        DBPRINTF("Reading entry %d\n", i);
        const RespListdir::ListdirEntry &entry = rl.entries(i);
        filler(buf, entry.filename().c_str(), NULL, 0);
    }

    return 0;
}

static int nofs_open(const char *path, struct fuse_file_info *fi)
{
    DBPRINTF("open %s (%X)\n", path, fi->flags);

    // TODO: can the following be taken out?
    struct stat st;
    int err = autofs_stat(path, &st);
    if (err != 0) return err;

    // NOTE: on OSX, fuse4x takes care of checking user permissions
    // TODO: check behavior on Linux
    if ((fi->flags & O_ACCMODE) == O_RDONLY) {
        DBPRINTF("opened read only\n");
    }
    else {
        return -EACCES;
    }

    return 0;
}

static int nofs_read(const char *path, char *buf, size_t size, off_t offset,
                     struct fuse_file_info *fi)
{
    DBPRINTF("read %s\n", path);

    // TODO: can the following be taken out?
    struct stat st;
    int err = autofs_stat(path, &st);
    if (err != 0) return err;

    // TODO (max size is 1 MB)
    if (size > 1024*1024) {
        size = 1024*1024;
    }

    ReqRead req_read;
    req_read.set_filepath(path);
    req_read.set_offset(offset);
    req_read.set_length(size);
    send_packet(REQ_READ, &req_read);

    RespRead rr;
    if (recv_packet(&rr) != ERR_NONE) {
        return -ENOENT;
    }

    const std::string &data = rr.data();
    size_t real_size = data.size();
    memcpy(buf, &data[0], real_size);
    return real_size;
}


static struct fuse_operations nofs_oper;
int main(int argc, char *argv[])
{
    // Setup FUSE pointers
    nofs_oper.init = autofs_init;
    nofs_oper.getattr = nofs_getattr;
    nofs_oper.readdir = nofs_readdir;
    nofs_oper.open = nofs_open;
    nofs_oper.read = nofs_read;

    //openlog("nofs", LOG_CONS, LOG_USER);

    if (argc < 3) {
        printf("not enough arguments!\n");
        exit(1);
    }

    int major, minor, patch;
    zmq_version (&major, &minor, &patch);
    printf ("Current 0MQ version is %d.%d.%d\n", major, minor, patch);

    // Send protocol request
    /*const char *proto_info = "LOCAL";
    send(g_Socket, proto_info, strlen(proto_info), 0);*/

    DBPRINTF("starting fuse\n");

    // TODO: just hardcode the needed arguments to FUSE
    // TODO: pass -s option (single-threaded)
    int fuse_argc = 3;
    char *fuse_argv[] = {argv[0], "-s", argv[2]};
    return fuse_main(fuse_argc, fuse_argv, &nofs_oper, NULL);
}
