#ifndef PFSD_H
#define PFSD_H

#ifdef __cplusplus
extern "C" {
#endif

int pfsd_start(struct pfsd_option *opt);
int pfsd_stop(void);
int pfsd_is_started(void);
int pfsd_wait_stop(void);

#ifdef __cplusplus
}
#endif

#endif
