#pragma once

#include "inttypes.h"
#include <stdlib.h>
#include <string.h>

#define VEC_DEFAULT_CAPACITY 8

#define vec_t(T)                                                               \
  struct {                                                                     \
    T* data;                                                                   \
    size_t len;                                                                \
    size_t cap;                                                                \
  }
typedef vec_t(i8) vec_i8;
typedef vec_t(i16) vec_i16;
typedef vec_t(i32) vec_i32;
typedef vec_t(i64) vec_i64;
typedef vec_t(u8) vec_u8;
typedef vec_t(u16) vec_u16;
typedef vec_t(u32) vec_u32;
typedef vec_t(u64) vec_u64;
typedef vec_t(f32) vec_f32;
typedef vec_t(f64) vec_f64;

// vec_i64 new_blocks = { 0 };
#define vec_resize(v, nmemb)                                                   \
  {                                                                            \
    (v)->data = realloc((v)->data, nmemb * sizeof(*(v)->data));                \
    (v)->cap = nmemb;                                                          \
  }

#define vec_push_ptr(v, ptr)                                                   \
  {                                                                            \
    if ((v)->len + 1 > (v)->cap) {                                             \
      if ((v)->cap == 0) {                                                       \
        vec_resize((v), VEC_DEFAULT_CAPACITY);                                 \
			} else {                                                                     \
        vec_resize((v), (v)->cap * 2);                                         \
			} \
    }                                                                          \
    typeof((v)->data) dest = (v)->data + (v)->len;                             \
    memcpy(dest, ptr, sizeof(*(v)->data));                                     \
    (v)->len += 1;                                                             \
  }

#define vec_push(v, val) vec_push_ptr(v, &val);

#define vec_free(v)                                                            \
  {                                                                            \
    free((v)->data);                                                           \
    (v)->data = NULL;                                                          \
  }
