// Minimal Parquet writer: one row group, PLAIN encoding, uncompressed, data page v1.
#pragma once
#include <cstdint>
#include <cstring>
#include <string>
#include <vector>

struct TBuf {
  std::vector<uint8_t> b;
  void byte(uint8_t x) { b.push_back(x); }
  void varint(uint64_t v) { while (v >= 0x80) { b.push_back((uint8_t)(v | 0x80)); v >>= 7; } b.push_back((uint8_t)v); }
  void zz(int64_t v) { varint(((uint64_t)v << 1) ^ (uint64_t)(v >> 63)); }
  void raw(const void* p, size_t n) { const uint8_t* q = (const uint8_t*)p; b.insert(b.end(), q, q + n); }
};

// Thrift compact protocol writer with field-id tracking.
struct TW {
  TBuf& o;
  std::vector<int> stack;
  int last = 0;
  explicit TW(TBuf& b) : o(b) {}
  void fh(int id, int type) {
    int d = id - last;
    if (d > 0 && d <= 15) o.byte((uint8_t)((d << 4) | type));
    else { o.byte((uint8_t)type); o.zz(id); }
    last = id;
  }
  void i32(int id, int32_t v) { fh(id, 5); o.zz(v); }
  void i64(int id, int64_t v) { fh(id, 6); o.zz(v); }
  void str(int id, const std::string& s) { fh(id, 8); o.varint(s.size()); o.raw(s.data(), s.size()); }
  void beginStruct(int id) { fh(id, 12); stack.push_back(last); last = 0; }
  void endStruct() { o.byte(0); last = stack.back(); stack.pop_back(); }
  void beginList(int id, int elemType, size_t n) {
    fh(id, 9);
    if (n < 15) o.byte((uint8_t)((n << 4) | elemType));
    else { o.byte((uint8_t)(0xF0 | elemType)); o.varint(n); }
  }
  // list element struct
  void beginElem() { stack.push_back(last); last = 0; }
  void endElem() { o.byte(0); last = stack.back(); stack.pop_back(); }
  void elemI32(int32_t v) { o.zz(v); }
  void elemStr(const std::string& s) { o.varint(s.size()); o.raw(s.data(), s.size()); }
};

enum PType { P_INT32 = 1, P_INT64 = 2, P_BYTE_ARRAY = 6 };
enum PKind { K_STRING, K_DATE, K_INT32, K_INT64 };

struct PColumn {
  std::string name;
  PKind kind;
  bool optional = false;
  TBuf data;            // encoded values (PLAIN)
  std::vector<uint8_t> defs;  // def levels if optional (0/1 per row)
  int64_t nvals = 0;    // number of rows
  void addStr(const char* p, uint32_t n) { data.raw(&n, 4); data.raw(p, n); nvals++; if (optional) defs.push_back(1); }
  void addNull() { nvals++; defs.push_back(0); }
  void addI32(int32_t v) { data.raw(&v, 4); nvals++; if (optional) defs.push_back(1); }
  void addI64(int64_t v) { data.raw(&v, 8); nvals++; if (optional) defs.push_back(1); }
  int ptype() const { return kind == K_STRING ? P_BYTE_ARRAY : (kind == K_INT64 ? P_INT64 : P_INT32); }
};

inline std::vector<uint8_t> parquet_file(std::vector<PColumn>& cols, int64_t nrows) {
  TBuf f;
  f.raw("PAR1", 4);
  struct Meta { int64_t off, size; };
  std::vector<Meta> metas;
  for (auto& c : cols) {
    TBuf page;
    if (c.optional) {
      // RLE/bit-packed hybrid, bit width 1, bit-packed groups of 8.
      TBuf lv;
      size_t groups = (c.defs.size() + 7) / 8;
      lv.varint((groups << 1) | 1);
      for (size_t g = 0; g < groups; g++) {
        uint8_t x = 0;
        for (int k = 0; k < 8; k++) { size_t i = g * 8 + k; if (i < c.defs.size() && c.defs[i]) x |= (uint8_t)(1 << k); }
        lv.byte(x);
      }
      uint32_t len = (uint32_t)lv.b.size();
      page.raw(&len, 4);
      page.raw(lv.b.data(), lv.b.size());
    }
    page.raw(c.data.b.data(), c.data.b.size());
    TBuf hdr;
    {
      TW w(hdr);
      w.i32(1, 0);  // DATA_PAGE
      w.i32(2, (int32_t)page.b.size());
      w.i32(3, (int32_t)page.b.size());
      w.beginStruct(5);
      w.i32(1, (int32_t)c.nvals);
      w.i32(2, 0);  // PLAIN
      w.i32(3, 3);  // RLE
      w.i32(4, 3);  // RLE
      w.endStruct();
      hdr.byte(0);
    }
    int64_t off = (int64_t)f.b.size();
    f.raw(hdr.b.data(), hdr.b.size());
    f.raw(page.b.data(), page.b.size());
    metas.push_back({off, (int64_t)(hdr.b.size() + page.b.size())});
    std::vector<uint8_t>().swap(c.data.b);
  }
  TBuf m;
  {
    TW w(m);
    w.i32(1, 1);
    w.beginList(2, 12, cols.size() + 1);
    w.beginElem();
    w.str(4, "schema");
    w.i32(5, (int32_t)cols.size());
    w.endElem();
    for (auto& c : cols) {
      w.beginElem();
      w.i32(1, c.ptype());
      w.i32(3, c.optional ? 1 : 0);
      w.str(4, c.name);
      if (c.kind == K_STRING) {
        w.i32(6, 0);  // UTF8
        w.beginStruct(10); w.beginStruct(1); w.endStruct(); w.endStruct();
      } else if (c.kind == K_DATE) {
        w.i32(6, 6);  // DATE
        w.beginStruct(10); w.beginStruct(6); w.endStruct(); w.endStruct();
      }
      w.endElem();
    }
    w.i64(3, nrows);
    w.beginList(4, 12, 1);
    w.beginElem();
    w.beginList(1, 12, cols.size());
    int64_t total = 0;
    for (size_t i = 0; i < cols.size(); i++) {
      auto& c = cols[i];
      w.beginElem();
      w.i64(2, metas[i].off);
      w.beginStruct(3);
      w.i32(1, c.ptype());
      w.beginList(2, 5, c.optional ? 2 : 1);
      w.elemI32(0);
      if (c.optional) w.elemI32(3);
      w.beginList(3, 8, 1);
      w.elemStr(c.name);
      w.i32(4, 0);
      w.i64(5, c.nvals);
      w.i64(6, metas[i].size);
      w.i64(7, metas[i].size);
      w.i64(9, metas[i].off);
      w.endStruct();
      w.endElem();
      total += metas[i].size;
    }
    w.i64(2, total);
    w.i64(3, nrows);
    w.endElem();
    m.byte(0);
  }
  f.raw(m.b.data(), m.b.size());
  uint32_t ml = (uint32_t)m.b.size();
  f.raw(&ml, 4);
  f.raw("PAR1", 4);
  return std::move(f.b);
}
