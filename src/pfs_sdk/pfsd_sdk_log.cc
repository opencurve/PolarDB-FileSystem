#include "pfsd_sdk_log.h"

#include <errno.h>
#include <stdarg.h>
#include <stdio.h>
#include <time.h>
#include <unistd.h>
#include <sys/time.h>

static const char mon_name[][4] = {
    "Jan", "Feb", "Mar", "Apr", "May", "Jun",
    "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"
};

static void
default_log(const char *filename, const char *func, int line,
	int priority, const char *fmt, va_list ap);

static pfsd_log_func_t log_func = &default_log;

static inline int                                                             
LogFormatTime(char* buf, size_t bufsize)                                       
{                                                                               
	struct timeval tv;
	struct tm tm;

	gettimeofday(&tv, NULL);
	localtime_r(&tv.tv_sec, &tm);
	int len = snprintf(buf, bufsize, "%.3s%3d %.2d:%.2d:%.2d.%06ld ",
		mon_name[tm.tm_mon], tm.tm_mday,
		tm.tm_hour, tm.tm_min, tm.tm_sec, tv.tv_usec);

	return len;
}                                                                               
                                                                                
static void
default_log(const char *filename, const char *func, int line,
	int priority, const char *fmt, va_list ap)
{
	char buf[128];
	int len;

	len = LogFormatTime(buf, sizeof(buf));
	flockfile(stderr);
	fprintf(stderr, "[PFSD_SDK INF %.*s][%d]%s %d: ",
		(len > 0 ? len - 1 : 0), buf, getpid(), func, line);
	vfprintf(stderr, fmt, ap);
	fprintf(stderr, "\n");
	funlockfile(stderr);
}

void
pfsd_sdk_set_log_func(pfsd_log_func_t f)
{
	log_func = f;
}

void
pfsd_sdk_log(const char *filename, const char *func, int line,
        int priority, const char *fmt, ...)
{
	int error_save = errno;
	va_list ap;

	va_start(ap, fmt);
	log_func(filename, func, line, priority, fmt, ap);
	va_end(ap);
	errno = error_save;
}

