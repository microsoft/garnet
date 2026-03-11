// simdjson_native.cpp — C API implementation using simdjson On-Demand
#include "simdjson.h"
#include "simdjson_native.h"
#include <cstring>
#include <new>

using namespace simdjson;

struct sj_parser_s {
    ondemand::parser parser;
    // Scratch buffer for string copies that need to outlive the document
    char str_scratch[4096];
    size_t str_scratch_used;
};

static sj_field_result make_none() {
    sj_field_result r{};
    r.type = SJ_NONE;
    return r;
}

static sj_field_result make_num(double v) {
    sj_field_result r{};
    r.type = SJ_NUM;
    r.num_value = v;
    return r;
}

static sj_field_result make_str(sj_parser_s* p, std::string_view sv) {
    sj_field_result r{};
    r.type = SJ_STR;
    // Copy into scratch buffer so pointer is stable until next call
    if (p->str_scratch_used + sv.size() + 1 <= sizeof(p->str_scratch)) {
        char* dst = p->str_scratch + p->str_scratch_used;
        memcpy(dst, sv.data(), sv.size());
        dst[sv.size()] = '\0';
        r.str_ptr = dst;
        r.str_len = (uint32_t)sv.size();
        p->str_scratch_used += sv.size() + 1;
    }
    return r;
}

static sj_field_result make_bool(bool v) {
    sj_field_result r{};
    r.type = SJ_NUM; // match ExprToken convention: bool → Num 0/1
    r.num_value = v ? 1.0 : 0.0;
    return r;
}

static sj_field_result make_null() {
    sj_field_result r{};
    r.type = SJ_NULL_TYPE;
    return r;
}

static sj_field_result extract_value(sj_parser_s* p, ondemand::value val) {
    ondemand::json_type jt;
    if (val.type().get(jt)) return make_none();

    switch (jt) {
        case ondemand::json_type::number: {
            double d;
            if (!val.get_double().get(d)) return make_num(d);
            break;
        }
        case ondemand::json_type::string: {
            std::string_view sv;
            if (!val.get_string().get(sv)) return make_str(p, sv);
            break;
        }
        case ondemand::json_type::boolean: {
            bool b;
            if (!val.get_bool().get(b)) return make_bool(b);
            break;
        }
        case ondemand::json_type::null: {
            bool is_null;
            if (!val.is_null().get(is_null) && is_null) return make_null();
            break;
        }
        default: break;
    }
    return make_none();
}

extern "C" {

SJAPI sj_parser sj_parser_create(size_t max_capacity) {
    (void)max_capacity;
    auto* p = new(std::nothrow) sj_parser_s();
    if (p) p->str_scratch_used = 0;
    return p;
}

SJAPI void sj_parser_destroy(sj_parser p) {
    delete p;
}

SJAPI sj_field_result sj_extract_field(
    sj_parser parser, const uint8_t* json_buf, size_t json_len,
    size_t json_capacity, const char* field_name, size_t field_name_len)
{
    if (!parser) return make_none();
    parser->str_scratch_used = 0;

    padded_string_view json((const char*)json_buf, json_len, json_capacity);
    ondemand::document doc;
    if (parser->parser.iterate(json).get(doc)) return make_none();

    ondemand::object obj;
    if (doc.get_object().get(obj)) return make_none();

    std::string_view key(field_name, field_name_len);
    ondemand::value val;
    if (obj.find_field(key).get(val)) return make_none();

    return extract_value(parser, val);
}

SJAPI int32_t sj_extract_fields(
    sj_parser parser, const uint8_t* json_buf, size_t json_len,
    size_t json_capacity, const char* const* field_names,
    const size_t* field_lens, size_t field_count, sj_field_result* results)
{
    if (!parser || !results) return 0;
    parser->str_scratch_used = 0;

    for (size_t i = 0; i < field_count; i++)
        results[i] = make_none();

    padded_string_view json((const char*)json_buf, json_len, json_capacity);
    ondemand::document doc;
    if (parser->parser.iterate(json).get(doc)) return 0;

    ondemand::object obj;
    if (doc.get_object().get(obj)) return 0;

    int32_t found = 0;

    // Single forward pass over all fields
    for (auto field : obj) {
        std::string_view key;
        if (field.unescaped_key().get(key)) continue;

        int match_idx = -1;
        for (size_t i = 0; i < field_count; i++) {
            if (results[i].type != SJ_NONE) continue;
            if (key.size() == field_lens[i] &&
                memcmp(key.data(), field_names[i], key.size()) == 0) {
                match_idx = (int)i;
                break;
            }
        }

        if (match_idx < 0) continue;

        ondemand::value val;
        if (field.value().get(val)) continue;
        results[match_idx] = extract_value(parser, val);
        if (results[match_idx].type != SJ_NONE) found++;
        if (found == (int32_t)field_count) break;
    }
    return found;
}

SJAPI const char* sj_simdjson_version(void) {
    return SIMDJSON_VERSION;
}

SJAPI const char* sj_active_implementation(void) {
    static char buf[64];
    auto name = simdjson::get_active_implementation()->name();
    size_t len = name.size() < 63 ? name.size() : 63;
    memcpy(buf, name.data(), len);
    buf[len] = '\0';
    return buf;
}

} // extern "C"
