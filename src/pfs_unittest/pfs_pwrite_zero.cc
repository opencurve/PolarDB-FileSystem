#include <err.h>
#include <iostream>
#include <string>
#include <sys/types.h>
#include <unistd.h>
#include <fcntl.h>
#include <chrono>
#include <rte_malloc.h>
#include <rte_cycles.h>
#include <sys/time.h>
#include <sys/resource.h>

#include <gflags/gflags.h>
#include <glog/logging.h>
#include <gtest/gtest.h>
#include "pfs_spdk.h"
#include "pfs_spdk_api.h"
#include "pfs_api.h"
#include "pfs_option.h"

using namespace std;

DEFINE_string(cluster, "", "cluster name");
DEFINE_int32(host_id, 1, "hosit id");
DEFINE_string(pbd_name, "", "pbdname name");                       
DEFINE_string(spdk_nvme_controller, "", "nvme controller");                       
DEFINE_int32(pbd_cpu_id, -1, "pbd cpu id");

extern int                                                                             
pfs_spdk_dev_io_get_cpu_stats(const char *devname,                              
        uint64_t * busy_tsc, uint64_t *idle_tsc);

int main(int argc, char **argv)
{
	gflags::ParseCommandLineFlags(&argc, &argv, true);

	int hostid = FLAGS_host_id;
	string cluster = FLAGS_cluster;
	string pbdname = FLAGS_pbd_name;

	pfs_option_set("spdk_nvme_controller", FLAGS_spdk_nvme_controller.c_str());

	if (cluster.empty()) {
		std::cout << "cluster is empty";
		return 1;
	}
	if (pbdname.empty()) {
		std::cout << "pbd_name is empty";
		return 1;
	}
	if (FLAGS_pbd_cpu_id != -1) {
		pfs_spdk_set_devcpu_bind(FLAGS_pbd_name.c_str(), FLAGS_pbd_cpu_id);
	}

	if (pfs_spdk_setup()) {
		std::cerr << "can not init spdk";
		return 1;
	}

	int flags = MNTFLG_TOOL | MNTFLG_RDWR | MNTFLG_LOG;
	int rc = pfs_mount(cluster.c_str(), pbdname.c_str(), hostid, flags);
	if (rc < 0) {                                                              
        	cout << "pfs_mount failed." << endl;                                    
		return 1;
    	} 
	cout << "Setup successful" << endl;       	

	std::string path;
	path = "/" + pbdname + "/zero_write_file";
	int fd = pfs_open(path.c_str(), O_RDWR|O_CREAT, 0600);
	if (fd < 0) {
		cout << "can not open file\n";
		return 1;
	}

#define DO(func, ...) 			\
	{				\
	struct timeval tv1, tv2; \
	struct rusage ru1, ru2; \
    std::cout << "testing " << #func << "\n"; \
    getrusage(RUSAGE_THREAD, &ru1); \
	auto start = std::chrono::steady_clock::now(); \
	for (int j = 0; j < loops; ++j) { \
		rc = pfs_lseek(fd, 0, SEEK_SET); \
		if (rc == -1) { \
			err(1, "pfs_lseek"); \
		} \
		\
		for (int i = 0; i < blocks; ++i) { \
			rc = func(__VA_ARGS__); \
			if (rc != buf_sz) { \
				err(1, #func); \
			} \
		} \
	} \
	auto end = std::chrono::steady_clock::now(); \
        getrusage(RUSAGE_THREAD, &ru2); \
	timersub(&ru2.ru_utime, &ru1.ru_utime, &tv1); \
	timersub(&ru2.ru_stime, &ru1.ru_stime, &tv2); \
	timeradd(&tv1, &tv2, &tv1); \
	std::chrono::duration<double> elapsed_seconds = end-start; \
	std::cout << #func << " elapsed time: " << elapsed_seconds.count() << "s"; \
	std::cout << ", used cpu time:" << tv1.tv_sec + double(tv1.tv_usec)/1000000 << "s\n"; \
	}

	std::cout << "initializing...\n";
	rc = pfs_ftruncate(fd, 0);
	if (rc) {
		err(1, "pfs_ftruncat");
	}
	
	const int buf_sz = 16384;
	const int blocks = 10000;
	const int loops = 20;
	char buf[buf_sz];
	memset(buf, 0, sizeof(buf));
	for (int i = 0; i < blocks; ++i) {
		rc = pfs_write(fd, buf, buf_sz);
		if (rc != sizeof(buf)) {
			err(1, "pfs_write");
		}
	}
	std::cout << "initialized.\n";

	char *dma_buf = (char *)rte_malloc("", buf_sz+3584, 4096);
	memset(dma_buf+3584, 0, sizeof(buf_sz));

	DO(pfs_write, fd, buf, buf_sz);
	DO(pfs_write_dma, fd, dma_buf+3584, buf_sz);
	DO(pfs_write_zero, fd, buf_sz);

	rte_free(dma_buf);
#ifndef TEST_SUSPEND
	pfs_umount(pbdname.c_str());
	pfs_spdk_cleanup();
#else
	pfs_spdk_suspend();
#endif
	return 0;
}
