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
  if (msync(v->data, v->size, MS_SYNC)) {
		TDB_ERRF_SYS("msync %s", sdata(v->path));
		return 1;
  }
	if (v->data != NULL && munmap(v->data, v->size)) {
		TDB_ERRF_SYS("munmap %s", sdata(v->path));
		return 2;
	}
	v->data = NULL;

	return 0;
}

i32 vec_mmap_resize(vec_mmap* v, i64 newsize) {
	if (ftruncate(v->fd, newsize) != 0) {
		TDB_ERRF_SYS("ftruncate %s", sdata(v->path));
		return 1;
	}

	if (v->data == NULL) {
		v->data = (char*)mmap(NULL, newsize, PROT_READ | PROT_WRITE, MAP_SHARED,
							  v->fd, 0);
  } else {
		v->data =
		 (char*)mremap(v->data, v->size, newsize, MREMAP_MAYMOVE);
  }
	if (v->data == MAP_FAILED) {
		TDB_ERRF_SYS("mmap %s", sdata(v->path));
		return 1;
	}
	v->size = newsize;

	return 0;
}

i32 vec_mmap_open(vec_mmap* v, const char* path, i64 size) {
	v->path = string_init(path);
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
	if (vec_mmap_resize(v, size))
		return 1;

	// TODO: sym column support
	return 0;
}

i32 vec_mmap_close(vec_mmap* v) {
	if (vec_mmap_unmap(v))
		return 1;
	if (v->fd && close(v->fd)) {
		TDB_ERRF_SYS("close %s %d", sdata(v->path), v->fd);
		return 2;
	}
  v->fd = -1;
	string_free(&v->path);

	return 0;
}

#ifdef TEST_MMAP_VEC
#include <stdio.h>
int main(void) {
	vec_mmap_i8 v = {0};
  vec_mmap_init(v, "asdfasdf.file");
	vec_mmap_push(v, 1);
	i8 a = 2;
	vec_mmap_push_ptr(&v, &a);
	for (i8 i = 3; i < 10; i++) {
		vec_mmap_push(v, i);
	}
	for_each(i, v) { printf("%d\n", *i); }
  vec_mmap_free(v);
}
#endif

