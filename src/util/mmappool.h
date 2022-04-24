#pragma once

#include "platform.h"

typedef struct pool {
	mmaped_file file;
	u64 used;
} pool;

static i32 pool_init(pool* p, u64 size, const char* path) {
	memset(p, 0, sizeof(mmaped_file));
	if (mmaped_file_open(&p->file, path))
		return 1;
	if (mmaped_file_resize(&p->file, size))
		return 2;

	return 0;
}

static void* pool_get(pool* p, u64 size) {
	u64 free = p->file.size - p->used;
	if (size > free) {
		mmaped_file_resize(&p->file, p->file.size * 2);
	}
	void* res = p->file.data + p->used;
	p->used += size;

	return res;
}

static i32 pool_close(pool* p) {
	p->used = 0;
	return mmaped_file_close(&p->file);
}
