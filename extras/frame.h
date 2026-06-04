#pragma once
#include <stdint.h>
struct __attribute__((packed)) Header
{
    uint16_t magic;
    uint8_t version;
    uint8_t flags;
    uint8_t type;
    uint32_t length;
    uint8_t reserved;
};

struct BufferView{
    const uint8_t* data;
    uint32_t len;
};

struct Frame{
    Header header;
    BufferView payload;
};