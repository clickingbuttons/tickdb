#pragma once

#include <stdlib.h>
#include <string.h>

#define VEC_DEFAULT_CAPACITY 8

typedef struct vec {
  size_t stride;
  size_t capacity;
  size_t size;
  char* data;
} vec;

#define vec_init(type) _vec_init(sizeof(type))

static vec _vec_init(size_t stride) {
  vec res = {
    .stride = stride,
    .capacity = VEC_DEFAULT_CAPACITY,
    .data = (char*)malloc(VEC_DEFAULT_CAPACITY * stride),
  };
  return res;
}

static void vec_resize(vec* v, size_t nmemb) {
  v->data = (char*)realloc(v->data, nmemb * v->stride);
  v->capacity = nmemb;
}

#define vec_push(v, val) ({ \
  typeof(val) copy = val;  \
  _vec_push(v, &copy);     \
})

static void* _vec_push(vec* v, void* value_ptr) {
  if (v->size + 1 > v->capacity) {
    vec_resize(v, v->capacity * 2);
  }

  void* dest = v->data + (v->size * v->stride);
  memcpy(dest, value_ptr, v->stride);
  v->size += 1;

  return dest;
}

static void vec_free(vec* v) {
  free(v->data);
  v->data = NULL;
}

