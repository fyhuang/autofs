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


#include "logging.h"
#include "network.h"

struct shared_data {
    int sock;
};

shared_data *get_sd() {
    return (shared_data*)fuse_get_context()->private_data;
}

// FUSE functions
static void *autofs_init(struct fuse_conn_info *conn) {
    shared_data *sd = new shared_data();
    sd->sock = connect("localhost:54321");
    if (sd->sock < 0) {
        fprintf(stderr, "Couldn't connect!\n");
        exit(1);
    }

    DBPRINTF("Connected!\n");
    return sd;
}

static void autofs_destroy(void *p_sd) {
    shared_data *sd = (shared_data*)p_sd;
    close(sd->sock);
    delete sd;
}

static int autofs_stat(const char *path, struct stat *stbuf) {
    memset(stbuf, 0, sizeof(struct stat));
    stbuf->st_uid = geteuid();
    stbuf->st_gid = getegid();

    shared_data *sd = get_sd();

    ReqStat req_stat;
    req_stat.set_filepath(path);

    // Send request
    if (send_packet(sd->sock, REQ_STAT, &req_stat) < 0) {
        return -EIO;
    }

    DBPRINTF("stat %s\n", path);

    // Get response
    RespStat sr;
    ErrorCode afs_err = recv_packet(sd->sock, &sr);
    if (afs_err != ERR_NONE) {
        return -ENOENT;
    }

    time_t mtime_utc = sr.mtime_utc();
    struct tm *tm_loc = localtime(&mtime_utc);
    time_t mtime_local = mktime(tm_loc);
    stbuf->st_ctime = mtime_local;
    stbuf->st_mtime = mtime_local;
    stbuf->st_atime = mtime_local;

    if (sr.ftype() & S_IFDIR) {
        stbuf->st_mode = S_IFDIR | sr.perms();
        stbuf->st_nlink = sr.size();
        stbuf->st_ino = sr.inode();
        DBPRINTF("result dir\n");
        return 0;
    }
    else if (sr.ftype() & S_IFREG) {
        stbuf->st_mode = S_IFREG | sr.perms();
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


static int autofs_getattr(const char *path, struct stat *stbuf)
{
    DBPRINTF("autofs_getattr %s\n", path);

    if (strcmp(path, "/") == 0) {
        stbuf->st_uid = geteuid();
        stbuf->st_gid = getegid();
        stbuf->st_mode = S_IFDIR | 0755;
        stbuf->st_nlink = 2;
        return 0;
    }

    return autofs_stat(path, stbuf);
}

static int autofs_readdir(const char *path, void *buf, fuse_fill_dir_t filler,
                        off_t offset, struct fuse_file_info *fi)
{
    DBPRINTF("autofs_readdir %s\n", path);

    shared_data *sd = get_sd();

    ReqListdir req_listdir;
    req_listdir.set_dirpath(path);
    send_packet(sd->sock, REQ_LISTDIR, &req_listdir);

    RespListdir rl;
    if (recv_packet(sd->sock, &rl) != ERR_NONE) {
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

static int autofs_open(const char *path, struct fuse_file_info *fi)
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
        DBPRINTF("opened write\n");
    }

    return 0;
}

static int autofs_read(const char *path, char *buf, size_t size, off_t offset,
                     struct fuse_file_info *fi)
{
    DBPRINTF("read %s\n", path);

    shared_data *sd = get_sd();

    // TODO (max size is 1 MB)
    if (size > 1024*1024*512) {
        fprintf(stderr, "Read is too big!\n");
        exit(1);
    }

    ReqRead req_read;
    req_read.set_filepath(path);
    req_read.set_offset(offset);
    req_read.set_length(size);
    send_packet(sd->sock, REQ_READ, &req_read);

    databuf dbuf;
    if (recv_packet(sd->sock, NULL, &dbuf) != ERR_NONE) {
        return -ENOENT;
    }

    // TODO: keep reading until full
    size_t real_size = dbuf.size();
    memcpy(buf, &dbuf[0], real_size);
    return real_size;
}



static int autofs_write(const char *path, const char *buf, size_t size, off_t off,
        struct fuse_file_info *fi)
{
    DBPRINTF("write(%lu) %s\n", size, path);

    // TODO (max size is 1 MB)
    if (size > 1024*1024*512) {
        fprintf(stderr, "Write is too big!\n");
        exit(1);
    }

    shared_data *sd = get_sd();

    ReqWrite req_write;
    req_write.set_filepath(path);
    req_write.set_offset(off);
    send_packet(sd->sock, REQ_WRITE, &req_write, (uint8_t*)buf, size);

    if (recv_packet(sd->sock, NULL, NULL) != ERR_NONE) {
        return -EIO;
    }
    return size;
}

static int autofs_mknod(const char *path, mode_t mode, dev_t dev)
{
    DBPRINTF("mknod %s\n", path);

    if (!(mode & S_IFREG)) {
        return -EINVAL;
    }

    shared_data *sd = get_sd();

    ReqMknod req_mknod;
    req_mknod.set_filepath(path);
    send_packet(sd->sock, REQ_MKNOD, &req_mknod);

    if (recv_packet(sd->sock, NULL, NULL) != ERR_NONE) {
        return -EIO;
    }
    return 0;
}

static int autofs_truncate(const char *path, off_t new_size)
{
    DBPRINTF("truncate %s\n", path);

    if (new_size < 0) return -EINVAL;
    shared_data *sd = get_sd();

    ReqTruncate req_truncate;
    req_truncate.set_filepath(path);
    req_truncate.set_newlength(new_size);
    send_packet(sd->sock, REQ_TRUNCATE, &req_truncate);

    if (recv_packet(sd->sock, NULL, NULL) != ERR_NONE) {
        return -EIO;
    }
    return 0;
}


static struct fuse_operations autofs_oper;
int main(int argc, char *argv[])
{
    // Setup FUSE pointers
    autofs_oper.init = autofs_init;
    autofs_oper.destroy = autofs_destroy;
    autofs_oper.getattr = autofs_getattr;
    autofs_oper.readdir = autofs_readdir;
    autofs_oper.open = autofs_open;
    autofs_oper.read = autofs_read;

    autofs_oper.write = autofs_write;
    autofs_oper.mknod = autofs_mknod;
    autofs_oper.truncate = autofs_truncate;

    //openlog("nofs", LOG_CONS, LOG_USER);

    if (argc < 3) {
        printf("not enough arguments!\n");
        exit(1);
    }

    // Send protocol request
    /*const char *proto_info = "LOCAL";
    send(g_Socket, proto_info, strlen(proto_info), 0);*/

    DBPRINTF("starting fuse\n");

    // TODO: just hardcode the needed arguments to FUSE
    // TODO: pass -s option (single-threaded)
    int fuse_argc = 6;
    char *fuse_argv[] = {argv[0], "-s", "-d", "-o", "daemon_timeout=5", argv[2]};
    return fuse_main(fuse_argc, fuse_argv, &autofs_oper, NULL);
}
