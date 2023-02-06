使用bosondb专用版本pfs
======================

# 背景

原始的pfs开源版本，要求使用daemon模式，可能存在需要进程间通讯的开销。为了避免这个开销，可以让应用程序直接使用pfs lib
操作卷。当一个bosondb服务器使用pfs lib时，同时也需要允许使用pfs命令行工具，这里存在一个矛盾，因为bosondb已经
mount了pfs卷后pfsdaemon不存在了，为了解决这个问题，还需要把pfsdaemon代码编译进bosondb里。

# 设计

* 运行库分三部分
    * pfs核心库 libpfs.so
    * pfsdaemon核心库 libpfsd_s.so
    * pfsdaemon的客户端库 libpfsd_c.so
* 头文件
    * pfs核心库的api，pfs_api.h
    * pfsdaemon核心库头文件api, pfsd_api.h
    * pfsdaemon客户端api, pfsd_cli.h
    * pfs核心和pfsdaemon的logger输出管理api, pfs_trace_func.h

# 编程指南

* bosondb应该只会使用到libpfsd_s.so 和 libpfs.so

* 为内嵌的pfsdaemon初始化options:
```
    pfsd_option_t opt;
    pfsd_option_init(&opt);
```

pfsd_option具有以下结构:

```
    typedef struct pfsd_option {
        int o_pollers;
        /* Worker threads, same as num of channels */
        int o_workers;
        /* Worker thread usleep interval in us */
        int o_usleep;
        /* pbdname like 1-1 */
        char o_pbdname[64];
        /* shm directory */
        char o_shm_dir[1024];
        /* daemon mode */
        int o_daemon;
        /* auto increase epoch when mount which write mode */
        int o_auto_increase_epoch;
        /* server id, for Postgresql? */
        int o_server_id;
} pfsd_option_t;
```

通常只需要在pfsd_option_init后，设置o_pbdname字段即可，如果需要提升curve pbd epoch，可将o_auto_increase_epoch设置为1。
如果bosondb自己决定mysql整个进程如何切换成daemon模式，则必须将o_daemon设置为0, 如果为1，则pfs自动把进程切成daemon。

* 设置pfs日志输出

  使用 pfs_set_trace_func设置pfs日志输出函数<br>
  如果不设置，缺省输出到stderr

* 启动内嵌的pfsdaemon

  * 使用 pfsd_start 函数启动

  ```
  if  (pfsd_start(&opt))
    return FAILURE;
  ```

* 如果后来想关闭pfsdaemon

  可以使用pfsd_stop函数先通知daemon， 然后使用pfsd_wait_stop等待。

* 启动完pfsdaemon后，如何使用pfs核心api

  - mount pfs卷

  ```
  int     pfs_mount_acquire(const char *cluster, const char *pbdname,             
                int host_id, int flags);

  ```

  读写模式应该使用的flags:<br>
  MNTFLG_RD|MNTFLG_WR|MNTFLG_LOG|MNTFLG<br>
  只读模式使用的flags:<br>
  MNTFLG_RD|MNTFLG_LOG|MNTFLG<br>
  函数返回0表示成功。

  - 卸载卷

  ```
    int     pfs_mount_release(const char *pbdname, int host_id);
  ```

  - 读写卷

  使用pfs_api.h中的其他api读写pfs文件系统。
  

