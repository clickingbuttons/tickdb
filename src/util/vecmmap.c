#include "vecmmap.h"

#include <errno.h>
#include <fcntl.h>		  // open
#include <linux/limits.h> // PATH_MAX
#include <sys/mman.h>	  // mmap, munmap, mremap
#include <sys/stat.h>	  // mkdir
#include <unistd.h>		  // ftruncate

i32 mkdirp(const char* path) {
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

i32 vec_mmap_unmap(vec_mmap* v) {
	if (v->data != NULL && munmap(v->data, v->size)) {
		TDB_ERRF_SYS("munmap %s", sdata(v->path));
		return 1;
	}
	v->data = NULL;

	return 0;
}

i32 vec_mmap_grow(vec_mmap* v) {
	i64 fsize = v->size * 2;
	if (ftruncate(v->fd, fsize) != 0) {
		TDB_ERRF_SYS("ftruncate %s", sdata(v->path));
		return 1;
	}

	if (v->data == NULL)
		v->data = (char*)mmap(NULL, fsize, PROT_READ | PROT_WRITE, MAP_PRIVATE,
							  v->fd, 0);
	else
		v->data =
		 (char*)mremap(v->data, v->size, fsize, MREMAP_MAYMOVE);
	if (v->data == MAP_FAILED) {
		TDB_ERRF_SYS("mmap %s", sdata(v->path));
		return 1;
	}
	v->size *= 2;

	return 0;
}

i32 vec_mmap_open(vec_mmap* v, const char* path, i64 size) {
	v->path = string_init(path);
	v->size = size;
	if (string_len(&v->path) > PATH_MAX) {
		TDB_ERRF("file %s is longer than PATH_MAX of %d\n", sdata(v->path),
				 PATH_MAX);
		return 1;
	}

	if (mkdirp(sdata(v->path)))
		return 1;

	i32 fd = open(sdata(v->path), O_RDWR);
	if (fd == -1 && errno == ENOENT)
		fd = open(sdata(v->path), O_CREAT | O_RDWR, S_IRWXU);
	if (fd == -1) {
		TDB_ERRF("open %s", sdata(v->path));
		return 1;
	}
	v->fd = fd;
	if (vec_mmap_grow(v))
		return 1;

	// TODO: sym column support
	return 0;
}

i32 vec_mmap_close(vec_mmap* v) {
	if (vec_mmap_unmap(v))
		return 1;
	if (v->fd && close(v->fd)) {
		TDB_ERRF_SYS("close %s %d", sdata(v->path), v->fd);
		return 1;
	}
  v->fd = -1;
	string_free(&v->path);

	return 0;
}
