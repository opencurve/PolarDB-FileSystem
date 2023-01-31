#ifndef _PFSD_LOG_H_
#define _PFSD_LOG_H_

#include <errno.h>

#ifdef PFSD_SERVER 
#include "../pfs_core/pfs_trace.h"

#define pfsd_debug(...)  pfs_dbgtrace(__VA_ARGS__)
#define pfsd_info(...)	 pfs_itrace(__VA_ARGS__)
#define pfsd_warn(...)	 pfs_wtrace(__VA_ARGS__)
#define pfsd_error(...)  pfs_etrace(__VA_ARGS__)
#define pfsd_fatal(...)  pfs_fatal(__VA_ARGS__)

#else

#include "pfsd_sdk_log.h"

#ifdef NDEBUG

#define pfsd_debug(...) 		\
	if (false)			\
	    pfsd_sdk_log(__FILE__,  	\
		         __func__,  	\
		         __LINE__, PFSD_SDK_INFO, __VA_ARGS__)

#else

#define pfsd_debug(...) 		\
	pfsd_sdk_log(__FILE__,  	\
		     __func__,  	\
		     __LINE__, PFSD_SDK_INFO, __VA_ARGS__)

#endif // NDEBUG

#define pfsd_info(...) 			\
	pfsd_sdk_log(__FILE__,		\
		     __func__,		\
		     __LINE__, PFSD_SDK_INFO, __VA_ARGS__)


#define pfsd_warn(...)			\
	pfsd_sdk_log(__FILE__,		\
		     __func__,		\
		     __LINE__, PFSD_SDK_WARNING, __VA_ARGS__)

#define pfsd_error(...)			\
	pfsd_sdk_log(__FILE__,		\
		     __func__,		\
		     __LINE__, PFSD_SDK_ERROR, __VA_ARGS__)

#define pfsd_fatal(...)			\
	pfsd_sdk_log(__FILE__,	 	\
		     __func__,		\
		     __LINE__, PFSD_SDK_FATAL, __VA_ARGS__)

#endif //PFSD_SERVER
#endif

