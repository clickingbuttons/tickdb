#pragma once

#include "inttypes.h"
#include "prelude.h"
#include "string.h"

#include <fcntl.h>		  // open
#include <linux/limits.h> // PATH_MAX
#include <sys/mman.h>	  // mmap, munmap, mremap
#include <sys/stat.h>	  // mkdir
#include <unistd.h>		  // ftruncate

static i32 mkdirp(const char* path) {
	string builder = string_empty;
	int last_dir = 0;
	for (int i = 0;; i++) {
		if (path[i] == '/') {
			string_catn(&builder, path + last_dir, i - last_dir);
			if (mkdir(sdata(builder), S_IRWXU | S_IRWXG | S_IRWXO)) {
				if (errno != EEXIST) {
					TDB_ERRF_SYS("mkdirp: %s", sdata(builder));
					return 1;
				}
			}
			last_dir = i;
		} else if (path[i] == '\0') {
			break;
		}
	}
	string_free(&builder);
	return 0;
}

typedef struct mmaped_file {
	string path;
	i32 fd;
	i64 size;
	char* data;
} mmaped_file;

static i32 mmaped_file_open(mmaped_file* res, const char* path) {
	memset(res, 0, sizeof(mmaped_file));
	res->path = string_init(path);
	if (mkdirp(path))
		return 1;
	res->fd = open(path, O_RDWR);
	if (res->fd == -1 && errno == ENOENT)
		res->fd = open(path, O_CREAT | O_RDWR, S_IRWXU);
	if (res->fd == -1) {
		TDB_ERRF_SYS("open %s", path);
		return 2;
	}

	return 0;
}

static i32 mmaped_file_resize(mmaped_file* m, u64 newsize) {
	if (ftruncate(m->fd, newsize) != 0) {
		TDB_ERRF_SYS("ftruncate %s (fd %d) %lu", sdata(m->path), m->fd,
					 newsize);
		return 1;
	}

	if (m->data == NULL) {
		m->data = (char*)mmap(NULL, newsize, PROT_READ | PROT_WRITE, MAP_SHARED,
							  m->fd, 0);
	} else {
		m->data = (char*)mremap(m->data, m->size, newsize, MREMAP_MAYMOVE);
	}
	if (m->data == MAP_FAILED) {
		TDB_ERRF_SYS("mmap %s", sdata(m->path));
		return 2;
	}
	m->size = newsize;

	return 0;
}

static i32 mmaped_file_close(mmaped_file* m) {
	string_free(&m->path);
	if (m->data != NULL) {
		if (msync(m->data, m->size, MS_SYNC)) {
			TDB_ERRF_SYS("msync %s", sdata(m->path));
			return 1;
		}
		if (munmap(m->data, m->size)) {
			TDB_ERRF_SYS("munmap %s", sdata(m->path));
			return 1;
		}
	}

	return 0;
}
