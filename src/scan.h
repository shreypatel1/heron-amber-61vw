// SIMD line/field scanner and fast field decoders.
#pragma once
#include <immintrin.h>
#include <cstdint>
#include <cstring>

typedef uint64_t u64; typedef uint32_t u32; typedef int64_t i64; typedef uint8_t u8;

static inline u64 ld64(const char* p) { u64 v; memcpy(&v, p, 8); return v; }
static inline u32 ld32(const char* p) { u32 v; memcpy(&v, p, 4); return v; }

struct Field { const char* p; u32 n; };

// Scans structural characters 64 bytes at a time.
struct Scanner {
  const char* base;   // start of current 64-byte block
  u64 sep;            // remaining separators (',' or '\n') in block, as bits
  u64 nl;             // newline bits
  u64 q;              // remaining quote bits
  inline void load(const char* b) {
    base = b;
    __m512i v = _mm512_loadu_si512((const void*)b);
    u64 c = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8(','));
    nl = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8('\n'));
    q = _mm512_cmpeq_epi8_mask(v, _mm512_set1_epi8('"'));
    sep = c | nl;
  }
  inline void start(const char* p) { load(p); }
  // Next separator. Returns its pointer; sets isNL and adds quotes passed to qc.
  inline const char* next(bool& isNL, u32& qc) {
    while (!sep) {
      qc += (u32)_mm_popcnt_u64(q);
      load(base + 64);
    }
    u64 low = sep & (0 - sep);            // lowest set bit
    u64 below = low - 1;
    qc += (u32)_mm_popcnt_u64(q & below);
    q &= ~(below | low);
    isNL = (nl & low) != 0;
    sep ^= low;
    return base + __builtin_ctzll(low);
  }
  // skip to just after the next newline; returns pointer after it
  inline const char* skip_line() {
    for (;;) {
      u64 m = nl & sep;
      if (m) {
        u64 low = m & (0 - m);
        u64 upto = (low - 1) | low;
        sep &= ~upto; q &= ~upto; nl &= ~upto;
        return base + __builtin_ctzll(low) + 1;
      }
      load(base + 64);
    }
  }
};

// Parse one line of exactly N fields starting at p (scanner positioned at p).
// Returns pointer to next line start; ok=false if malformed.
template <int N>
static inline const char* scan_line(Scanner& s, const char* p, Field* f, bool& ok) {
  ok = true;
  const char* fs = p;
  for (int k = 0; k < N; k++) {
    bool isNL; u32 qc = 0;
    const char* e = s.next(isNL, qc);
    u32 len = (u32)(e - fs);
    if (qc) {
      if (qc == 2 && len >= 2 && fs[0] == '"' && e[-1] == '"') { f[k].p = fs + 1; f[k].n = len - 2; }
      else ok = false;
    } else { f[k].p = fs; f[k].n = len; }
    if (isNL) {
      if (k != N - 1) ok = false;
      return e + 1;
    }
    if (k == N - 1) { ok = false; return s.skip_line(); }
    fs = e + 1;
  }
  return nullptr;  // unreachable
}

// ---- codes: <=9 ASCII chars (1..127) -> order-preserving 63-bit code; else 0 (caller falls back)
static inline u64 code9(const char* p, u32 n, bool& fits) {
  fits = false;
  if (n == 0 || n > 9) return 0;
  u64 a = ld64(p);
  u64 b = 0;
  if (n < 8) a &= (1ULL << (8 * n)) - 1;
  else if (n == 9) b = (u8)p[8];
  u64 lm = n >= 8 ? ~0ULL : ((1ULL << (8 * n)) - 1);
  if (a & 0x8080808080808080ULL) return 0;
  if (b & 0x80) return 0;
  u64 z = (a - 0x0101010101010101ULL) & ~a & 0x8080808080808080ULL & lm;
  if (z) return 0;
  if (n == 9 && b == 0) return 0;
  fits = true;
  return (_pext_u64(__builtin_bswap64(a), 0x7f7f7f7f7f7f7f7fULL) << 7) | b;
}

// ---- digits
static inline bool all_digits8(u64 x) {  // all 8 bytes '0'..'9'
  return (((x + 0x4646464646464646ULL) | (x - 0x3030303030303030ULL)) & 0x8080808080808080ULL) == 0;
}
// value of 8 digit bytes (already validated), first char in lowest byte
static inline u64 swar8(u64 x) {
  x -= 0x3030303030303030ULL;
  x = (x * 10) + (x >> 8);
  x = (((x & 0x000000FF000000FFULL) * (100 + (1000000ULL << 32))) +
       (((x >> 16) & 0x000000FF000000FFULL) * (1 + (10000ULL << 32)))) >> 32;
  return x;
}
// parse up to 8 digits at p (n in 1..8); returns false if non-digit
static inline bool digits_upto8(const char* p, u32 n, u64& v) {
  u64 x = ld64(p);
  u32 sh = 8 * (8 - n);
  x <<= sh;                                   // digits to high bytes
  x |= sh ? (0x3030303030303030ULL >> (64 - sh)) : 0;  // pad low bytes with '0'
  if (!all_digits8(x)) return false;
  v = swar8(x);
  return true;
}
// general digit string -> value; returns false if any non-digit or n==0. over=true if > INT64_MAX
static inline bool parse_digits(const char* p, u32 n, u64& v, bool& over) {
  over = false;
  if (n == 0) return false;
  if (n <= 8) return digits_upto8(p, n, v);
  if (n <= 16) {
    u64 hi, lo;
    if (!digits_upto8(p, n - 8, hi)) return false;
    if (!digits_upto8(p + n - 8, 8, lo)) return false;
    v = hi * 100000000ULL + lo;
    return true;
  }
  v = 0;
  for (u32 i = 0; i < n; i++) {
    unsigned c = (unsigned)(p[i] - '0');
    if (c >= 10) return false;
    if (!over) {
      if (v > (u64)(INT64_MAX - c) / 10) over = true;
      else v = v * 10 + c;
    }
  }
  return true;
}

// ---- timestamps
static const i64 BADTS = INT64_MIN;
static int32_t MONTH_BASE[400 * 12];  // days since epoch of 1st of month for years 1900..2299
static inline i64 days_from_civil_slow(i64 y, i64 m, i64 d) {
  y -= m <= 2;
  i64 era = (y >= 0 ? y : y - 399) / 400;
  i64 yoe = y - era * 400;
  i64 doy = (153 * (m + (m > 2 ? -3 : 9)) + 2) / 5 + d - 1;
  i64 doe = yoe * 365 + yoe / 4 - yoe / 100 + doy;
  return era * 146097 + doe - 719468;
}
static inline bool is_leap(i64 y) { return y % 4 == 0 && (y % 100 != 0 || y % 400 == 0); }
static u8 DIMTAB[400 * 12];
static inline void init_time_tables() {
  static const int D[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
  for (int y = 1900; y < 2300; y++)
    for (int m = 1; m <= 12; m++) {
      MONTH_BASE[(y - 1900) * 12 + m - 1] = (int32_t)days_from_civil_slow(y, m, 1);
      DIMTAB[(y - 1900) * 12 + m - 1] = (u8)(D[m - 1] + (m == 2 && is_leap(y)));
    }
}

static constexpr u64 pat_mask(const char* s, char kind) {  // kind 'd': digit positions; 's': separator positions
  u64 m = 0;
  for (int i = 0; i < 8; i++) {
    bool isd = s[i] == 'd';
    if ((kind == 'd') == isd) m |= 0xFFULL << (8 * i);
  }
  return m;
}
static constexpr u64 pat_val(const char* s) {
  u64 m = 0;
  for (int i = 0; i < 8; i++) if (s[i] != 'd') m |= (u64)(u8)s[i] << (8 * i);
  return m;
}
static inline bool digits_masked(u64 w, u64 dm) {
  u64 x = (w & dm) | (0x3030303030303030ULL & ~dm);
  return all_digits8(x);
}
static inline u64 pairs(u64 w, u64 dm) {  // byte i of result = 10*d_i + d_{i+1}
  u64 x = ((w & dm) | (0x3030303030303030ULL & ~dm)) - 0x3030303030303030ULL;
  return x * 10 + (x >> 8);
}

// Parse a timestamp field. Returns ms since epoch, BADTS if malformed; INT64_MAX if F3 overflows.
static inline i64 parse_ts(const char* p, u32 n) {
  if (n == 20 || n == 25) {
    constexpr const char* P0 = "dddd-dd-";
    constexpr const char* P1 = "ddTdd:dd";
    u64 w0 = ld64(p), w1 = ld64(p + 8);
    constexpr u64 D0 = pat_mask(P0, 'd'), S0 = pat_mask(P0, 's'), V0 = pat_val(P0);
    constexpr u64 D1 = pat_mask(P1, 'd'), S1 = pat_mask(P1, 's'), V1 = pat_val(P1);
    if ((w0 & S0) == V0) {
      if ((w1 & S1) != V1 || !digits_masked(w0, D0) || !digits_masked(w1, D1)) return BADTS;
      u64 t0 = pairs(w0, D0), t1 = pairs(w1, D1);
      int y = (int)(t0 & 0xFF) * 100 + (int)((t0 >> 16) & 0xFF);
      int mo = (int)((t0 >> 40) & 0xFF);
      int d = (int)(t1 & 0xFF), h = (int)((t1 >> 24) & 0xFF), mi = (int)((t1 >> 48) & 0xFF);
      int s;
      i64 off = 0;
      if (n == 20) {
        u32 c = ld32(p + 16);  // ":SSZ"
        u32 cs = c & 0xFF0000FFu;
        if (cs != (((u32)'Z' << 24) | ':')) return BADTS;
        u32 s1 = ((c >> 8) & 0xFF) - '0', s2 = ((c >> 16) & 0xFF) - '0';
        if (s1 > 9 || s2 > 9) return BADTS;
        s = (int)(s1 * 10 + s2);
      } else {
        // bytes 16..24 = ":SS+HH:MM"
        constexpr const char* P2 = "dd+dd:dd";
        constexpr u64 D2 = pat_mask(P2, 'd');
        if (p[16] != ':') return BADTS;
        u64 w2 = ld64(p + 17);
        char sg = (char)((w2 >> 16) & 0xFF);
        if (sg != '+' && sg != '-') return BADTS;
        if (((w2 >> 40) & 0xFF) != ':') return BADTS;
        if (!digits_masked(w2, D2)) return BADTS;
        u64 t2 = pairs(w2, D2);
        s = (int)(t2 & 0xFF);
        int oh = (int)((t2 >> 24) & 0xFF), om = (int)((t2 >> 48) & 0xFF);
        if (oh > 23 || om > 59) return BADTS;
        off = (i64)(oh * 60 + om) * 60000;
        if (sg == '-') off = -off;
      }
      if (mo < 1 || mo > 12 || h > 23 || mi > 59 || s > 59 || d < 1) return BADTS;
      i64 days;
      if (y >= 1900 && y < 2300) {
        int ix = (y - 1900) * 12 + mo - 1;
        if (d > DIMTAB[ix]) return BADTS;
        days = MONTH_BASE[ix] + d - 1;
      } else {
        static const int D[12] = {31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
        int dm = D[mo - 1] + (mo == 2 && is_leap(y));
        if (d > dm) return BADTS;
        days = days_from_civil_slow(y, mo, d);
      }
      return ((days * 24 + h) * 60 + mi) * 60000LL + s * 1000LL - off;
    }
  }
  u64 v; bool over;
  if (!parse_digits(p, n, v, over)) return BADTS;
  return over ? INT64_MAX : (i64)v;
}

// ---- whole-line scanner: one line of up to 127 bytes per call, branch-light.
typedef unsigned __int128 u128;
static inline int ctz128(u128 x) { u64 lo = (u64)x; return lo ? __builtin_ctzll(lo) : 64 + __builtin_ctzll((u64)(x >> 64)); }
static inline int pop128(u128 x) { return (int)(_mm_popcnt_u64((u64)x) + _mm_popcnt_u64((u64)(x >> 64))); }

// Scalar fallback for long lines: [p, e) is the line.
template <int N>
static inline bool split_scalar(const char* p, const char* e, Field* f) {
  if (p == e) return false;
  int k = 0;
  const char* s = p;
  for (;;) {
    const char* c = (const char*)memchr(s, ',', e - s);
    const char* fe = c ? c : e;
    if (k >= N) return false;
    size_t len = fe - s;
    const char* q = (const char*)memchr(s, '"', len);
    if (q) {
      if (q != s || len < 2 || s[len - 1] != '"' || memchr(s + 1, '"', len - 2)) return false;
      f[k].p = s + 1; f[k].n = (u32)(len - 2);
    } else { f[k].p = s; f[k].n = (u32)len; }
    k++;
    if (!c) break;
    s = c + 1;
  }
  return k == N;
}

// Parses the line at p (needs 128 readable bytes). Sets *next to the next line start.
template <int N>
static inline bool line128(const char* p, Field* f, const char** next) {
  __m512i a = _mm512_loadu_si512((const void*)p);
  __m512i b = _mm512_loadu_si512((const void*)(p + 64));
  const __m512i NLc = _mm512_set1_epi8('\n'), CMc = _mm512_set1_epi8(','), QTc = _mm512_set1_epi8('"');
  u64 nl0 = _mm512_cmpeq_epi8_mask(a, NLc), nl1 = _mm512_cmpeq_epi8_mask(b, NLc);
  if (__builtin_expect((nl0 | nl1) == 0, 0)) {
    const char* e = (const char*)rawmemchr(p, '\n');
    *next = e + 1;
    return split_scalar<N>(p, e, f);
  }
  // L = first newline; masks restricted to [0, L)
  u64 m0, m1;
  int L;
  if (nl0) { L = __builtin_ctzll(nl0); m0 = (nl0 - 1) & ~nl0; m1 = 0; }
  else { L = 64 + __builtin_ctzll(nl1); m0 = ~0ULL; m1 = (nl1 - 1) & ~nl1; }
  *next = p + L + 1;
  u64 c0 = _mm512_cmpeq_epi8_mask(a, CMc) & m0, c1 = _mm512_cmpeq_epi8_mask(b, CMc) & m1;
  u64 q0 = _mm512_cmpeq_epi8_mask(a, QTc) & m0, q1 = _mm512_cmpeq_epi8_mask(b, QTc) & m1;
  int n0 = (int)_mm_popcnt_u64(c0);
  if (n0 + (int)_mm_popcnt_u64(c1) != N - 1) return false;
  int pos[N + 16];
  pos[0] = -1;
  {
    int* o = pos + 1;
    #pragma GCC unroll 8
    for (int k = 0; k < (N < 8 ? N - 1 : 8); k++) { o[k] = __builtin_ctzll(c0); c0 &= c0 - 1; }
    o += n0;
    #pragma GCC unroll 8
    for (int k = 0; k < (N < 8 ? N - 1 : 8); k++) { o[k] = 64 + __builtin_ctzll(c1); c1 &= c1 - 1; }
  }
  pos[N] = L;
  int qtot = 0;
  int bad = 0;
  #pragma GCC unroll 8
  for (int k = 0; k < N; k++) {
    int s = pos[k] + 1, len = pos[k + 1] - s;
    int open = (len > 0) & (p[s] == '"');
    int close = (p[pos[k + 1] - 1] == '"');
    bad |= open & ((len < 2) | !close);
    qtot += 2 * open;
    f[k].p = p + s + open; f[k].n = (u32)(len - 2 * open);
  }
  bad |= ((int)_mm_popcnt_u64(q0) + (int)_mm_popcnt_u64(q1)) != qtot;
  return !bad;
}
