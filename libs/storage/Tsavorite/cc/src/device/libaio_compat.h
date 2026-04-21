// Copyright (c) Microsoft Corporation.
// Licensed under the MIT license.

// Force specific versioned libaio symbols at link time.
//
// Background: libaio.so exports multiple versions of the core AIO entry points:
//   LIBAIO_0.1 - original syscall-only ABI
//   LIBAIO_0.4 - adds a userspace ring-buffer fast path for io_getevents
//   LIBAIO_0.5 / LIBAIO_0.6 - later additions
//
// On older distros libaio-dev shipped a library that marked LIBAIO_0.4 as the
// default version, so linking `-laio` recorded `io_getevents@LIBAIO_0.4` etc.
// automatically. Starting with the t64 ABI transition (Debian 13 / Ubuntu
// 24.04+), libaio1t64 no longer advertises a default version for these
// symbols, and libaio.h has no `.symver` redirects for x86_64, so a fresh
// link produces UNVERSIONED references. At runtime the dynamic linker then
// picks the first matching definition - LIBAIO_0.1, whose `io_getevents`
// always goes through the syscall and blocks until `min_nr` events arrive.
// That caused Garnet's NativeStorageDevice probe/TryComplete paths to hang
// indefinitely on an empty context when they expected the LIBAIO_0.4 fast
// path to return 0 immediately.
//
// To keep rebuilds correct regardless of which distro they happen on, we
// pin the exact versions we want:
//   io_setup    @LIBAIO_0.4
//   io_destroy  @LIBAIO_0.4
//   io_getevents@LIBAIO_0.4
//   io_submit   @LIBAIO_0.1
// These match the versions that the original (pre-t64) build produced and
// that Garnet has shipped against for years.
//
// The mechanism: declare aliases via `.symver`, then #define the unadorned
// names to route to those aliases. This has no runtime cost; the resulting
// binary simply records versioned UND references.
//
// IMPORTANT: Include this header before any use of io_setup / io_destroy /
// io_getevents / io_submit. (file_linux.h already includes <libaio.h> then
// this header, so including file_linux.h is sufficient.)

#pragma once

#ifdef __linux__

#include <libaio.h>

__asm__(".symver io_setup_0_4, io_setup@LIBAIO_0.4");
__asm__(".symver io_destroy_0_4, io_destroy@LIBAIO_0.4");
__asm__(".symver io_getevents_0_4, io_getevents@LIBAIO_0.4");
__asm__(".symver io_submit_0_1, io_submit@LIBAIO_0.1");

extern "C" {
int io_setup_0_4(int maxevents, io_context_t* ctxp);
int io_destroy_0_4(io_context_t ctx);
int io_getevents_0_4(io_context_t ctx, long min_nr, long nr,
                     struct io_event* events, struct timespec* timeout);
int io_submit_0_1(io_context_t ctx, long nr, struct iocb** iocbs);
}

#define io_setup    io_setup_0_4
#define io_destroy  io_destroy_0_4
#define io_getevents io_getevents_0_4
#define io_submit   io_submit_0_1

#endif // __linux__
