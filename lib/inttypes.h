#pragma once

#define _POSIX_C_SOURCE 200809L
#include <stdbool.h>
#include <stdint.h>

_Static_assert(sizeof(bool) == 1, "Expected bool to be 1 bytes.");

#define GIBIBYTES(amount) amount * 1024 * 1024 * 1024
#define MEBIBYTES(amount) amount * 1024 * 1024
#define KIBIBYTES(amount) amount * 1024

#define GIGABYTES(amount) amount * 1000 * 1000 * 1000
#define MEGABYTES(amount) amount * 1000 * 1000
#define KILOBYTES(amount) amount * 1000

