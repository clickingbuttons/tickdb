#pragma once
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// Inspired by gcc sso_stringing
// https://www.youtube.com/watch?v=kPR8h4-qZdk

typedef struct string {
  char* data;
  uint32_t size;

  union {
    uint32_t capacity;
    char     small[16];
  };
} string;

// http://locklessinc.com/articles/next_pow2/
static inline uint32_t next_pow2(uint32_t x) {
  return 1 << (32 - __builtin_clz (x - 1));
}

static string string_initn(char* stringing, uint32_t size) {
  string res = {
    .size = size
  };

  if (res.size > 15) {
    res.capacity = next_pow2(res.size);
    res.data = (char*) malloc(res.capacity);
    memcpy(res.data, stringing, res.size);
  } else {
    memcpy(res.small, stringing, res.size);
    res.data = res.small;
  }

  return res;
}

static string string_init(char* stringing) {
  uint32_t size = strlen(stringing);
  return string_initn(stringing, size);
}

static void string_free(string* stringing) {
  if (stringing->data != stringing->small) {
    free(stringing->data);
  }
}

