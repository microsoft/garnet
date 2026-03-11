// simdjson_native.h — C API for simdjson On-Demand, designed for P/Invoke from .NET
#pragma once
#include <stdint.h>
#include <stddef.h>

#ifdef _WIN32
  #define SJAPI __declspec(dllexport)
#else
  #define SJAPI __attribute__((visibility("default")))
#endif

#ifdef __cplusplus
extern "C" {
#endif

typedef struct sj_parser_s* sj_parser;

typedef enum {
    SJ_NONE = 0,
    SJ_NUM  = 1,
    SJ_STR  = 2,
    SJ_BOOL = 3,
    SJ_NULL_TYPE = 4,
} sj_type;

// Blittable result struct (32 bytes)
typedef struct {
    int32_t    type;          //  4 bytes
    int32_t    _pad;          //  4 bytes (alignment)
    double     num_value;     //  8 bytes
    const char* str_ptr;      //  8 bytes (into parser's internal buffer, valid until next call)
    uint32_t   str_len;       //  4 bytes
    uint32_t   _pad2;         //  4 bytes
} sj_field_result;

SJAPI sj_parser sj_parser_create(size_t max_capacity);
SJAPI void      sj_parser_destroy(sj_parser p);

SJAPI sj_field_result sj_extract_field(
    sj_parser       parser,
    const uint8_t*  json_buf,
    size_t          json_len,
    size_t          json_capacity,
    const char*     field_name,
    size_t          field_name_len
);

SJAPI int32_t sj_extract_fields(
    sj_parser             parser,
    const uint8_t*        json_buf,
    size_t                json_len,
    size_t                json_capacity,
    const char* const*    field_names,
    const size_t*         field_lens,
    size_t                field_count,
    sj_field_result*      results
);

SJAPI const char* sj_simdjson_version(void);
SJAPI const char* sj_active_implementation(void);

#ifdef __cplusplus
}
#endif
