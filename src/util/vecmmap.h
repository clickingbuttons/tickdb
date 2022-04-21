#include "prelude.h"
#include "platform.h"
#include "string.h"

#define VEC_MMAP_DEFAULT_CAPACITY 8

#define vec_mmap_t(T)                                                               \
	struct {                                                                   \
    mmaped_file file; \
    i64 cap; \
    i64 len; \
		T* data;                                                               \
	}

// vec_mmap_i64 new_blocks = { 0 };
typedef vec_mmap_t(i8)  vec_mmap_i8;
typedef vec_mmap_t(i16) vec_mmap_i16;
typedef vec_mmap_t(i32) vec_mmap_i32;
typedef vec_mmap_t(i64) vec_mmap_i64;
typedef vec_mmap_t(u8)  vec_mmap_u8;
typedef vec_mmap_t(u16) vec_mmap_u16;
typedef vec_mmap_t(u32) vec_mmap_u32;
typedef vec_mmap_t(u64) vec_mmap_u64;
typedef vec_mmap_t(f32) vec_mmap_f32;
typedef vec_mmap_t(f64) vec_mmap_f64;

#define vec_mmap_push_ptr(v, ptr)                                                   \
	{                                                                          \
		if ((v)->len + 1 > (v)->cap) {                                         \
      vec_mmap_resize(&(v)->file, (v)->file.size * 2);                                \
      (v)->data = (v)->file.data; \
      (v)->cap = (v)->file.size / sizeof(*(v)->data); \
		}                                                                      \
		memcpy((v)->data + (v)->len, ptr, sizeof(*(v)->data));                 \
		(v)->len += 1;                                                         \
	}

#define vec_mmap_push(v, val) \
	{                                                                          \
		typeof(*((v).data)) tmp = (val);                                       \
		vec_mmap_push_ptr(&(v), &tmp);                                              \
	}

#define vec_mmap_free(v)                                                            \
	{                                                                          \
    vec_mmap_close(&(v).file); \
		(v).data = NULL;                                                      \
	}

// see vec.h
#define for_each(i, c)                                                         \
	for (typeof((c).data) i = (c).data; i < (c).data + (c).len; i++)

#define vec_mmap_init(v, fname) { \
  (v).len = 0; \
  (v).cap = VEC_MMAP_DEFAULT_CAPACITY; \
  vec_mmap_open(&(v).file, fname, VEC_MMAP_DEFAULT_CAPACITY * sizeof(*(v).data)); \
  (v).data = (v).file.data; \
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
