#include <spdk/stdinc.h>

#include <spdk/env.h>
#include <spdk/log.h>
#include <spdk/nvme.h>
#include <spdk/nvme_zns.h>
#include <spdk/string.h>
#include <spdk/vmd.h>

#define EXPORTED_SYMBOL __attribute__((visibility("default")))
#define MAX_DEVICE_NAME_LEN

typedef void (*AsyncIOCallback)(void *context, int32_t result,
                                uint32_t bytes_transferred);

struct spdk_ns_entry {
    struct spdk_nvme_ctrlr *ctrlr;
    struct spdk_nvme_ns *ns;
    int32_t nsid;
};

struct spdk_device {
    struct spdk_ns_entry *ns_entry;
    struct spdk_nvme_qpair *qpair;
};

EXPORTED_SYMBOL int32_t spdk_device_init();

EXPORTED_SYMBOL struct spdk_device *spdk_device_create(int nsid);
// TODO: chyin add destroy function.

EXPORTED_SYMBOL int32_t spdk_device_read_async(struct spdk_device *device,
                                               uint64_t source, void *dest,
                                               uint32_t length,
                                               AsyncIOCallback callback,
                                               void *callback_context);

EXPORTED_SYMBOL int32_t spdk_device_write_async(struct spdk_device *device,
                                                const void *source,
                                                uint64_t dest, uint32_t length,
                                                AsyncIOCallback callback,
                                                void *callback_context);

EXPORTED_SYMBOL bool spdk_device_try_complete();

EXPORTED_SYMBOL uint64_t
spdk_device_get_segment_size(struct spdk_device *device, uint64_t segment);

EXPORTED_SYMBOL int32_t spdk_device_poll();

EXPORTED_SYMBOL void
spdk_device_remove_segment_before(struct spdk_device *device,
                                  uint64_t target_segment);
