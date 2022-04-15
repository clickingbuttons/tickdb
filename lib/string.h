#pragma once
#include "inttypes.h"
#include "wyhash.h"
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Thanks izabera: https://github.com/izabera/s/blob/master/LICENSE

typedef union string {
	// allow strings up to 15 bytes to stay on the stack
	// use the last byte as a null terminator and to store flags
	// much like fbstring
	char _data[16];

	struct {
		u8 filler1[15],
		 // how many free bytes in this stack allocated string
		 // same idea as fbstring
		 space_left : 4,
		 // if it's on heap, this is set to 1
		 is_pointer : 1, flag1 : 1, flag2 : 1, flag3 : 1;
	};

	// heap allocated
	struct {
		char* ptr;
		// supports strings up to 2^54 -1 bytes
		size_t _size : 54,
		 // capacity is always a power of 2 -1
		 capacity : 6;
		// the last 4 bits are important flags
	};
} string;

static inline int ilog2(int n) { return 32 - __builtin_clz(n) - 1; }

static string string_initn(char* s, size_t size) {
	string res = {0};

	if (size > 15) {
		res.capacity = ilog2(size) + 1;
		res._size = size;
		res.is_pointer = 1;
		res.ptr = (char*)malloc((size_t)1 << res.capacity);
		memcpy(res.ptr, s, size);
		res.ptr[size] = '\0';
	} else {
		memcpy(res._data, s, size);
		res.space_left = 15 - size;
	}

	return res;
}

static string string_init(char* s) { return string_initn(s, strlen(s)); }

static void string_free(string* s) {
	if (s->is_pointer)
		free(s->ptr);
}

static char* string_data(const string* s) {
	return s->is_pointer ? s->ptr : (char*)s->_data;
}

// Because typing "&" is annoying
#define sdata(s) string_data(&s)

static size_t string_size(const string* s) {
	return s->is_pointer ? s->_size : 15 - s->space_left;
}

static size_t string_capacity(const string* s) {
	return s->is_pointer ? ((size_t)1 << s->capacity) - 1 : 15;
}

static void string_grow(string* s, size_t size) {
	if (size <= string_capacity(s))
		return;
	size_t capacity = 1 << (ilog2(size) + 1);
	if (s->is_pointer)
		s->ptr = (char*)realloc(s->ptr, capacity);
	else {
		char buf[16];
		memcpy(buf, s->_data, 16);
		s->ptr = (char*)malloc(capacity);
		memcpy(s->ptr, buf, 16);
	}
	s->is_pointer = 1;
	s->capacity = capacity;
}

static void string_catn(string* a, const char* b, size_t sizeb) {
	size_t sizea = string_size(a);
	if (sizea + sizeb > 15) {
		if (string_capacity(a) < sizea + sizeb + 1)
			string_grow(a, sizea + sizeb + 1);
		memcpy(a->ptr + sizea, b, sizeb + 1);
		a->_size = sizea + sizeb;
		a->ptr[a->_size] = '\0';
	} else {
		memcpy(a->_data + sizea, b, sizeb);
		a->space_left = 15 - (sizea + sizeb);
	}
}

static void string_cat(string* a, const string* b) {
	string_catn(a, string_data(b), string_size(b));
}

static void string_catc(string* a, const char* b) {
	string_catn(a, b, strlen(b));
}

static int string_cmp(const string* a, const string* b) {
	size_t asize = string_size(a);
	size_t bsize = string_size(b);
	if (asize == bsize)
		return memcmp(string_data(a), string_data(b), asize);

	return asize - bsize;
}

static u64 string_hash(const string* s) {
	u64 res = wyhash(string_data(s), string_size(s));
	return res;
}

static bool string_equals(const string* s1, const string* s2) {
	if (string_data(s1) == NULL && string_data(s2) != NULL ||
		string_data(s1) != NULL && string_data(s2) == NULL)
		return false;
	return string_cmp(s1, s2) == 0;
}

#ifdef TEST_STRING
#include <unistd.h>
int main(void) {
	char buffer[10];
	read(STDIN_FILENO, buffer, 10);
	string a = string_init(buffer);
	read(STDIN_FILENO, buffer, 10);
	string b = string_init(buffer);
	printf("%d\n", string_cmp(&a, &b) == 0);
}
#endif
