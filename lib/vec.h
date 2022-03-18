#pragma once

#include <stdlib.h>
#include <string.h>

typedef struct vec {
  size_t size; // stride
  size_t capacity;
  size_t nmemb;
  void* data;
} vec;

#define vec_init(type) _vec_init(sizeof(type))

static vec _vec_init(size_t size) {
  const size_t default_capacity = 8;
  vec res = {
    .size = size,
    .capacity = default_capacity,
    .nmemb = 0,
    .data = malloc(default_capacity * size),
  };
  return res;
}

static void vec_resize(vec* v, size_t nmemb) {
  v->capacity = nmemb;
  v->data = reallocarray(v->data, nmemb, v->size);
}

#define vec_push(v, val) { \
  typeof(val) copy = val;  \
  _vec_push(v, &copy);     \
}

static void _vec_push(vec *v, void* value_ptr) {
  if (v->nmemb + 1 > v->capacity) {
    vec_resize(v, v->capacity * 2);
  }

  memcpy((char*)v->data + (v->nmemb * v->size), value_ptr, v->size);
  v->nmemb += 1;
}

static void vec_free(vec* v) {
  free(v->data);
  v->data = NULL;
}

