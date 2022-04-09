#pragma once

#define _POSIX_C_SOURCE 200809L
#include <stdbool.h>
#include <stdint.h>

typedef int8_t i8;
typedef uint8_t u8;
typedef uint16_t u16;
typedef uint32_t u32;
typedef uint64_t u64;

typedef int8_t i8;
typedef int16_t i16;
typedef signed int i32;
typedef int64_t i64;

typedef float f32;
typedef double f64;

// Ensure all types are of the correct size.
_Static_assert(sizeof(u8) == 1, "Expected u8 to be 1 byte.");
_Static_assert(sizeof(u16) == 2, "Expected u16 to be 2 bytes.");
_Static_assert(sizeof(u32) == 4, "Expected u32 to be 4 bytes.");
_Static_assert(sizeof(u64) == 8, "Expected u64 to be 8 bytes.");
_Static_assert(sizeof(i8) == 1, "Expected i8 to be 1 byte.");
_Static_assert(sizeof(i16) == 2, "Expected i16 to be 2 bytes.");
_Static_assert(sizeof(i32) == 4, "Expected i32 to be 4 bytes.");
_Static_assert(sizeof(i64) == 8, "Expected i64 to be 8 bytes.");
_Static_assert(sizeof(f32) == 4, "Expected f32 to be 4 bytes.");
_Static_assert(sizeof(f64) == 8, "Expected f64 to be 8 bytes.");
_Static_assert(sizeof(bool) == 1, "Expected bool to be 1 bytes.");

#define GIBIBYTES(amount) amount * 1024 * 1024 * 1024
#define MEBIBYTES(amount) amount * 1024 * 1024
#define KIBIBYTES(amount) amount * 1024

#define GIGABYTES(amount) amount * 1000 * 1000 * 1000
#define MEGABYTES(amount) amount * 1000 * 1000
#define KILOBYTES(amount) amount * 1000
