#include <spdk/nvme.h>

#define EXPORTED_SYMBOL __attribute__((visibility("default")))

typedef void (*AsyncIOCallback)(void *context, int result,
                                size_t bytes_transferred);

EXPORTED_SYMBOL spdk_device_read_async(uint64_t source, void *dest,
                                       uint32_t length,
                                       AsyncIOCallback callback, void *context);

EXPORTED_SYMBOL spdk_device_write_async(const void *source, uint64_t dest,
                                        uint32_t length,
                                        AsyncIOCallback callback,
                                        void *context);

EXPORTED_SYMBOL spdk_device_try_complete();

EXPORTED_SYMBOL spdk_device_get_segment_size(uint64_t segment);

EXPORTED_SYMBOL spdk_device_poll(int timeout_secs);

EXPORTED_SYMBOL spdk_device_remove_segment_before(uint64_t target_segment);
