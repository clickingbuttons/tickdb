#pragma once
#include "inttypes.h"
#include "wyhash.h"
#include <stdarg.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

// Thanks izabera: https://github.com/izabera/s/blob/master/LICENSE

// allow strings up to 15 bytes to stay on the stack
typedef union string {
	char _data[16];

	struct {
		u8 filler1[15], space_left : 4,
		 // use the last byte as a null terminator and to store flags
		 is_pointer : 1, flag1 : 1, flag2 : 1, flag3 : 1;
	};

	// heap allocated
	struct {
		char* ptr;
		// supports strings up to 2^54 -1 bytes
		size_t _size : 54,
		 // capacity is always a power of 2 -1
		 capacity : 6;
		// leave last 4 bits alone
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

#define string_empty                                                           \
	(string) { .space_left = 15 }

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

static size_t string_len(const string* s) {
	return s->is_pointer ? s->_size : 15 - s->space_left;
}

static size_t string_cap(const string* s) {
	return s->is_pointer ? ((size_t)1 << s->capacity) - 1 : 15;
}

static void string_grow(string* s, size_t len) {
	if (len <= string_cap(s))
		return;
	len = ilog2(len) + 1;
	if (s->is_pointer)
		s->ptr = (char*)realloc(s->ptr, (size_t)1 << len);
	else {
		char buf[16];
		memcpy(buf, s->_data, 16);
		s->ptr = (char*)malloc((size_t)1 << len);
		memcpy(s->ptr, buf, 16);
	}
	s->is_pointer = 1;
	s->capacity = len;
}

static void string_catn(string* a, const char* b, size_t sizeb) {
	size_t sizea = string_len(a);
	if (sizea + sizeb > 15) {
		if (string_cap(a) < sizea + sizeb + 1)
			string_grow(a, sizea + sizeb + 1);
		memcpy(a->ptr + sizea, b, sizeb);
		a->_size = sizea + sizeb;
		a->ptr[a->_size] = '\0';
	} else {
		memcpy(a->_data + sizea, b, sizeb);
		a->space_left = 15 - (sizea + sizeb);
	}
}

static void string_cat(string* a, string* b) {
	string_catn(a, string_data(b), string_len(b));
}

static void string_catc(string* a, const char* b) {
	string_catn(a, b, strlen(b));
}

static int string_cmp(const string* a, const string* b) {
	size_t asize = string_len(a);
	size_t bsize = string_len(b);
	if (asize == bsize)
		return memcmp(string_data(a), string_data(b), asize);

	return asize - bsize;
}

static u64 string_hash(const string* s) {
	u64 res = wyhash(string_data(s), string_len(s));
	return res;
}

static bool string_equals(const string* s1, const string* s2) {
	if (string_data(s1) == NULL && string_data(s2) != NULL ||
		string_data(s1) != NULL && string_data(s2) == NULL)
		return false;
	return string_cmp(s1, s2) == 0;
}

// %p = *string
__attribute__((format(printf, 2, 3))) static void
string_printf(string* dest, const char* format, ...) {
	*dest = string_empty;
	va_list argp;
	va_start(argp, format);
	while (*format) {
		if (*format == '%') {
			format++;
			switch (*format) {
			case '%':
				string_catn(dest, "%", 1);
				break;
			case 's': {
				const char* s = va_arg(argp, char*);
				string_catc(dest, s);
				break;
			}
			case 'p': {
				string* s = va_arg(argp, string*);
				string_cat(dest, s);
				break;
			}
			}
		} else {
			string_catn(dest, format, 1);
		}
		format++;
	}
	va_end(argp);
}

// this leaks if the string is too long but it's very handy for short strings
// "" causes a compile time error if x is not a string literal or too long
// _Static_assert is a declaration, not an expression.  fizzie came up with this
// hack
#define string_tmp(x)                                                          \
	({                                                                         \
		(void)((struct {                                                       \
			_Static_assert(sizeof x <= 16, "it's too big");                    \
			int dummy;                                                         \
		}){1});                                                                \
		string tmp = string_init(x);                                           \
		&tmp;                                                                  \
	})

#ifdef TEST_STRING
#include <unistd.h>
int main(void) {
	// char buffer[10];
	// read(STDIN_FILENO, buffer, 10);
	// string a = string_init(buffer);
	// read(STDIN_FILENO, buffer, 10);
	// string b = string_init(buffer);
	// printf("%d\n", string_cmp(&a, &b));

	string a = string_empty;
	string_catn(&a, "asdf", 4);
	const char* b = "ts_participant";
	string_catc(&a, b);

	string c = string_init("hsad j sldfk jjk j jdj asdfs ");
	string_cat(&a, &c);
	string d = string_init("ff");
	string_cat(&a, &d);

	printf("%s\n", sdata(a));
}
#endif
