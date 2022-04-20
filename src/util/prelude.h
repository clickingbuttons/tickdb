#pragma once
#ifndef _GNU_SOURCE
#define _GNU_SOURCE
#endif

#include <errno.h>

static char TDB_ERR[8096];

#define TDB_DEBUG true
#define TDB_LINENO true
#define TDB_SETERR(...) snprintf(TDB_ERR, sizeof(TDB_ERR), __VA_ARGS__)
#define TDB_PRINT_LINENO()                                                     \
	if (TDB_LINENO)                                                            \
	fprintf(stderr, "%s:%d ", __FILE__, __LINE__)
#define TDB_ERRF(...)                                                          \
	{                                                                          \
		TDB_SETERR(__VA_ARGS__);                                               \
		TDB_PRINT_LINENO();                                                    \
		if (TDB_DEBUG)                                                         \
			fprintf(stderr, "%s\n", TDB_ERR);                                  \
	}
#define TDB_ERRF_SYS(...)                                                      \
	{                                                                          \
		TDB_SETERR(__VA_ARGS__);                                               \
		TDB_PRINT_LINENO();                                                    \
		if (TDB_DEBUG)                                                         \
			fprintf(stderr, "%s: %s\n", TDB_ERR, strerror(errno));             \
	}
//#define TDB_CHECK(err) if (err != 0) fprintf(stderr, "%s\n", TDB_ERR)
#define API __attribute__((__visibility__("default")))
