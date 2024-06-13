#include "spdk_device.h"

#include <error.h>
#include <threads.h>
#include <time.h>

#define DEVICE_MAX_NUM 16
#define NS_MAX_NUM 8
#define SIZE_4K (4 * 1024)
#define SIZE_1G (1 * 1024 * 1024 * 1024)
#define SECTOR_SIZE SIZE_4K
#define IO_BATCH_NUM 8
#define POLL_TIME 64

static bool initted = false;
static pthread_mutex_t module_init_lock = PTHREAD_MUTEX_INITIALIZER;
static bool attach_error = 0;

static int32_t ns_num = 0;
static struct spdk_ns_entry g_spdk_ns_list[NS_MAX_NUM];

static int32_t device_num = 0;
static struct spdk_device g_spdk_device_list[DEVICE_MAX_NUM];
static pthread_mutex_t device_init_lock = PTHREAD_MUTEX_INITIALIZER;

struct internal_io_context {
    struct spdk_device *device;
    void *buffer;
    int32_t io_length;
    void *read_dest;
    AsyncIOCallback complete_callback;
    void *complete_callback_context;
};

static int32_t register_ns(struct spdk_nvme_ctrlr *ctrlr,
                           struct spdk_nvme_ns *ns, int nsid)
{
    struct spdk_ns_entry *entry;

    if (ns_num >= NS_MAX_NUM) {
        fprintf(stderr, "Too many NS, the max number is:%d\n", NS_MAX_NUM);
        return EOVERFLOW;
    }

    if (!spdk_nvme_ns_is_active(ns)) {
        return 0;
    }

    entry = &g_spdk_ns_list[ns_num++];
    entry->ctrlr = ctrlr;
    entry->ns = ns;
    entry->nsid = nsid;
    return 0;
}

static bool probe_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                     struct spdk_nvme_ctrlr_opts *opts)
{
    return true;
}

static void attach_cb(void *cb_ctx, const struct spdk_nvme_transport_id *trid,
                      struct spdk_nvme_ctrlr *ctrlr,
                      const struct spdk_nvme_ctrlr_opts *opts)
{
    int nsid;
    struct spdk_nvme_ns *ns;
    if (attach_error != 0) {
        return;
    }

    for (nsid = spdk_nvme_ctrlr_get_first_active_ns(ctrlr); nsid != 0;
         nsid = spdk_nvme_ctrlr_get_next_active_ns(ctrlr, nsid)) {
        ns = spdk_nvme_ctrlr_get_ns(ctrlr, nsid);
        if (ns == NULL) {
            continue;
        }
        attach_error = register_ns(ctrlr, ns, nsid);
    }
}

static void cleanup()
{
    struct spdk_nvme_detach_ctx *detach_ctx = NULL;

    for (int i = device_num; i >= 0; i--) {
        spdk_nvme_ctrlr_free_io_qpair(g_spdk_device_list[i].qpair);
        device_num--;
    }
    for (int i = ns_num; i >= 0; i--) {
        spdk_nvme_detach_async(g_spdk_ns_list[i].ctrlr, &detach_ctx);
        ns_num--;
    }

    if (detach_ctx) {
        spdk_nvme_detach_poll(detach_ctx);
    }
}

int spdk_device_init()
{
    int rc = 0;
    pthread_mutex_lock(&module_init_lock);
    if (initted) {
        goto exit;
    }
    struct spdk_env_opts opts;
    spdk_env_opts_init(&opts);
    opts.name = "garnet_spdk_device";
    rc = spdk_env_init(&opts);
    if (rc != 0) {
        fprintf(stderr, "ERROR: Unable to initialize SPDK env error:%d.\n", rc);
        goto exit;
    }

    rc = spdk_nvme_probe(NULL, NULL, probe_cb, attach_cb, NULL);
    if (rc != 0) {
        fprintf(stderr, "ERROR: spdk_nvme_probe() failed.\n");
        goto exit;
    }
    if (attach_error != 0) {
        fprintf(stderr, "ERROR: some ns register failed with error code: %d\n.",
                errno);
        rc = attach_error;
        goto exit;
    }

    if (ns_num == 0) {
        fprintf(stderr, "ERROR: no NVMe namespace found.\n");
        rc = 1;
        goto exit;
    }
    initted = true;
exit:
    if (rc == 0) {
        initted = true;
    } else {
        cleanup();
    }
    pthread_mutex_unlock(&module_init_lock);
    return rc;
}

static struct spdk_ns_entry *ns_lookup(int32_t nsid)
{
    struct spdk_ns_entry *ns_entry = NULL;
    for (int i = 0; i < ns_num; i++) {
        if (g_spdk_ns_list[i].nsid == nsid) {
            ns_entry = &g_spdk_ns_list[i];
            break;
        }
    }
    return ns_entry;
}

struct spdk_device *spdk_device_create(int nsid)
{
    struct spdk_device *device = NULL;
    struct spdk_ns_entry *ns_entry = ns_lookup(nsid);

    if (ns_entry == NULL) {
        return NULL;
    }

    pthread_mutex_lock(&device_init_lock);
    device = &g_spdk_device_list[device_num++];
    device->ns_entry = ns_entry;
    device->qpair = spdk_nvme_ctrlr_alloc_io_qpair(ns_entry->ctrlr, NULL, 0);
    if (device->qpair == NULL) {
        fprintf(stderr, "ERROR: spdk_nvme_ctrlr_alloc_io_qpair() failed\n");
        device = NULL;
        device_num--;
    }
    pthread_mutex_unlock(&device_init_lock);

    return device;
}

static void io_complete(void *arg, const struct spdk_nvme_cpl *completion)
{
    struct internal_io_context *io_context = (struct internal_io_context *)arg;
    int16_t rc = 0;
    uint32_t bytes_transferred = 0;
    AsyncIOCallback callback = io_context->complete_callback;
    void *callback_context = io_context->complete_callback_context;

    if (spdk_nvme_cpl_is_error(completion)) {
        spdk_nvme_qpair_print_completion(io_context->device->qpair,
                                         (struct spdk_nvme_cpl *)completion);
        fprintf(stderr, "I/O error status: %s\n",
                spdk_nvme_cpl_get_status_string(&completion->status));
        rc = completion->status_raw;
    } else {
        if (io_context->read_dest != NULL) {
            memcpy(io_context->read_dest, io_context->buffer,
                   io_context->io_length);
        }
        bytes_transferred = io_context->io_length;
    }

    spdk_free(io_context->buffer);
    free(io_context);
    callback(callback_context, (int32_t)rc, bytes_transferred);
}

static struct internal_io_context *
get_internal_io_context(struct spdk_device *device, uint32_t io_length,
                        void *read_dest, AsyncIOCallback callback,
                        void *context)
{
    struct internal_io_context *io_context = NULL;

    io_context = malloc(sizeof(struct internal_io_context));
    if (io_context == NULL) {
        goto exit;
    }
    io_context->buffer = spdk_malloc(io_length, SIZE_4K, NULL,
                                     SPDK_ENV_SOCKET_ID_ANY, SPDK_MALLOC_DMA);
    if (io_context->buffer == NULL) {
        free(io_context);
        io_context = NULL;
        goto exit;
    }
    io_context->io_length = io_length;
    io_context->read_dest = read_dest;
    io_context->complete_callback = callback;
    io_context->complete_callback_context = context;
    io_context->device = device;

exit:
    return io_context;
}

int32_t spdk_device_read_async(struct spdk_device *device, uint64_t source,
                               void *dest, uint32_t length,
                               AsyncIOCallback callback, void *callback_context)
{
    struct internal_io_context *io_context = NULL;
    int rc = 0;

    if (length % SECTOR_SIZE != 0) {
        fprintf(stderr,
                "ERROR: io size %d is not sector align, sector size is %d.\n",
                length, SECTOR_SIZE);
        rc = EINVAL;
        goto exit;
    }

    io_context = get_internal_io_context(device, length, dest, callback,
                                         callback_context);
    if (io_context == NULL) {
        rc = ENOMEM;
        goto exit;
    }

    rc = spdk_nvme_ns_cmd_read(device->ns_entry->ns, device->qpair,
                               io_context->buffer, source / SECTOR_SIZE,
                               length / SECTOR_SIZE, io_complete,
                               (void *)io_context, 0);
    if (rc != 0) {
        fprintf(stderr, "ERROR: starting read I/O failed with %d.\n", rc);
    }

exit:
    if (rc != 0) {
        if (io_context != NULL) {
            spdk_free(io_context->buffer);
            free(io_context);
        }
    }

    return rc;
}

int32_t spdk_device_write_async(struct spdk_device *device, const void *source,
                                uint64_t dest, uint32_t length,
                                AsyncIOCallback callback,
                                void *callback_context)
{
    struct internal_io_context *io_context = NULL;
    int rc = 0;

    if (length % SECTOR_SIZE != 0) {
        fprintf(stderr,
                "ERROR: io size %d is not sector align, sector size is %d.\n",
                length, SECTOR_SIZE);
        rc = EINVAL;
        goto exit;
    }

    io_context = get_internal_io_context(device, length, NULL, callback,
                                         callback_context);
    if (io_context == NULL) {
        rc = ENOMEM;
        goto exit;
    }
    memcpy(io_context->buffer, source, length);

    rc = spdk_nvme_ns_cmd_write(device->ns_entry->ns, device->qpair,
                                io_context->buffer, dest / SECTOR_SIZE,
                                length / SECTOR_SIZE, io_complete,
                                (void *)io_context, 0);
    if (rc != 0) {
        fprintf(stderr, "ERROR: starting write I/O failed with %d.\n", rc);
    }

exit:
    if (rc != 0) {
        if (io_context != NULL) {
            spdk_free(io_context->buffer);
            free(io_context);
        }
    }

    return rc;
}

bool spdk_device_try_complete() {}

uint64_t spdk_device_get_segment_size(struct spdk_device *device,
                                      uint64_t segment)
{
    return (uint32_t)100 * SIZE_1G;
}

int32_t spdk_device_poll(uint32_t timeout)
{
    static int qp_pointer = 0;
    int n = 0;
    int t = 0;
    struct spdk_device *device = NULL;

    clock_t start = 0, diff = 0;
    start = clock();

    while (true) {
        device = &g_spdk_device_list[qp_pointer];
        int complete_io_num =
            spdk_nvme_qpair_process_completions(device->qpair, IO_BATCH_NUM);
        if (complete_io_num > 0) {
            start = clock();
            n += complete_io_num;
        }

        qp_pointer += 1;
        qp_pointer %= device_num;

        if (n >= IO_BATCH_NUM) {
            break;
        }
        diff = clock() - start;
        if (diff * 1000 / CLOCKS_PER_SEC >= timeout) {
            break;
        }
    }
    return n;
}

void spdk_device_remove_segment_before(struct spdk_device *device,
                                       uint64_t target_segment)
{
    return;
}
