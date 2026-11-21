// Shopping Sessions: fast exact pipeline.
#include <algorithm>
#include <atomic>
#include <chrono>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <dirent.h>
#include <fcntl.h>
#include <mutex>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <unordered_map>
#include <vector>
#include "parquet.h"
#include "scan.h"

using namespace std;

static const i64 DAYMS = 86400000LL;
static double T0;
static double now() { return chrono::duration<double>(chrono::steady_clock::now().time_since_epoch()).count(); }
static bool VERBOSE = false;
static void tick(const char* what) { if (VERBOSE) fprintf(stderr, "%8.3f  %s\n", now() - T0, what); }

// ---------------------------------------------------------------- strings -> codes
// Strings of 1..9 ASCII chars (1..127) pack into an order-preserving 63-bit code (code9).
// Everything else is interned: code = FALLBACK | index.
static const u64 FALLBACK = 1ULL << 63;
struct Interner {
  mutex mu;
  unordered_map<string, u32> map;
  vector<string> strs;
  u64 get(const char* p, size_t n) {
    lock_guard<mutex> g(mu);
    auto it = map.find(string(p, n));
    if (it != map.end()) return FALLBACK | it->second;
    u32 id = (u32)strs.size();
    strs.emplace_back(p, n);
    map.emplace(strs.back(), id);
    return FALLBACK | id;
  }
} INTERN;
static inline u64 encode(const char* p, size_t n) {
  bool fits;
  u64 c = code9(p, (u32)n, fits);
  if (__builtin_expect(fits, 1)) return c;
  return INTERN.get(p, n);
}
static string decode(u64 code) {
  if (code & FALLBACK) { lock_guard<mutex> g(INTERN.mu); return INTERN.strs[(u32)code]; }
  string s;
  for (int i = 0; i < 8; i++) {
    char c = (char)((code >> (7 + 7 * (7 - i))) & 0x7f);
    if (!c) return s;
    s.push_back(c);
  }
  char c = (char)(code & 0x7f);
  if (c) s.push_back(c);
  return s;
}
static inline bool code_less(u64 a, u64 b) {
  if (!((a | b) & FALLBACK)) return a < b;
  if (a == b) return false;
  return decode(a) < decode(b);
}

// ---------------------------------------------------------------- files
struct File { const char* p; size_t n; };
struct Chunk { int kind; const char* p; const char* end; const char* safeEnd; const char* fileEnd; bool first; };

static vector<string> list_csv(const string& dir) {
  vector<string> out;
  DIR* d = opendir(dir.c_str());
  if (!d) return out;
  while (dirent* e = readdir(d)) {
    string nm = e->d_name;
    if (nm.size() >= 4 && nm.compare(nm.size() - 4, 4, ".csv") == 0) out.push_back(dir + "/" + nm);
  }
  closedir(d);
  sort(out.begin(), out.end());
  return out;
}
static File map_file(const string& path) {
  int fd = open(path.c_str(), O_RDONLY);
  if (fd < 0) { perror(path.c_str()); exit(2); }
  struct stat st; fstat(fd, &st);
  File f{nullptr, (size_t)st.st_size};
  if (f.n) {
    // reserve zeroed slack after the file so SIMD loads may run past EOF
    size_t pg = 4096, len = (f.n + pg - 1) / pg * pg + 2 * pg;
    void* r = mmap(nullptr, len, PROT_READ, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    void* m = mmap(r, f.n, PROT_READ, MAP_PRIVATE | MAP_FIXED | MAP_POPULATE, fd, 0);
    if (m == MAP_FAILED || r == MAP_FAILED) { perror("mmap"); exit(2); }
    f.p = (const char*)m;
  }
  close(fd);
  return f;
}
static string read_small(const string& path) {
  File f = map_file(path);
  return string(f.p ? f.p : "", f.n);
}

// Split a line [p,e) into exactly n fields with quote handling. Returns false if malformed.
static inline bool split_line(const char* p, const char* e, int n, Field* f) {
  if (p == e) return false;
  int k = 0;
  const char* s = p;
  for (;;) {
    const char* c = (const char*)memchr(s, ',', e - s);
    const char* fe = c ? c : e;
    if (k >= n) return false;
    // validate quotes
    size_t len = fe - s;
    const char* q = (const char*)memchr(s, '"', len);
    if (q) {
      if (q != s || len < 2 || s[len - 1] != '"' || memchr(s + 1, '"', len - 2)) return false;
      f[k].p = s + 1; f[k].n = (u32)(len - 2);
    } else {
      f[k].p = s; f[k].n = (u32)len;
    }
    k++;
    if (!c) break;
    s = c + 1;
  }
  return k == n;
}

// iterate lines of a chunk with the SIMD scanner: fn(Field*) for each well-formed line of N fields
template <int N, class F>
static inline void for_lines(const Chunk& ch, F fn) {
  const char* fe = ch.fileEnd;
  // unterminated last line of the file (only when the file has a newline, i.e. a header line)
  if (ch.safeEnd < fe && ch.p <= ch.safeEnd && ch.safeEnd < ch.end && ch.safeEnd[-1] == '\n') {
    size_t n = fe - ch.safeEnd;
    vector<char> buf(n + 1 + 256, 0);
    memcpy(buf.data(), ch.safeEnd, n);
    buf[n] = '\n';
    Field f[N]; const char* nx;
    if (line128<N>(buf.data(), f, &nx)) fn(f);
  }
  const char* p = ch.p;
  if (ch.first || p[-1] != '\n') {
    const char* nl = (const char*)memchr(p, '\n', fe - p);
    if (!nl) return;
    p = nl + 1;
  }
  const char* lim = ch.end < ch.safeEnd ? ch.end : ch.safeEnd;
  Field f[N];
  while (p < lim) {
    if (line128<N>(p, f, &p)) fn(f);
  }
}

// ---------------------------------------------------------------- global state
static i64 WS, WE, CWS;     // window start/end, click window start
static i64 DAY0;            // floor(WS / DAY)
static int NDAYS;
// products
static vector<u8> PROD_CAT_DENSE;   // index by number for "p<num>" canonical ids; 255 = absent
static unordered_map<string, u8> PROD_CAT_MAP;  // other product ids
static vector<string> CATS;         // category names sorted byte-wise; includes "unknown"
static u8 CAT_UNKNOWN;
// campaigns
static vector<u32> CAMP_DENSE;      // "c_<num>" -> campaign index+1 (0 = absent)
static unordered_map<string, u32> CAMP_MAP;
static vector<string> CAMP_ID, CAMP_CH;   // campaign index -> id, channel
static u32 NCAMP;                   // index NCAMP = organic

static inline bool canon_num(const char* p, size_t n, u64& v) {
  if (n == 0 || n > 9 || (p[0] == '0')) return false;
  v = 0;
  for (size_t i = 0; i < n; i++) { unsigned c = (unsigned)(p[i] - '0'); if (c >= 10) return false; v = v * 10 + c; }
  return true;
}
static inline u8 product_cat(const char* p, size_t n) {
  u64 v;
  if (n >= 2 && p[0] == 'p' && canon_num(p + 1, n - 1, v)) {
    if (v < PROD_CAT_DENSE.size()) { u8 c = PROD_CAT_DENSE[v]; return c == 255 ? CAT_UNKNOWN : c; }
    return CAT_UNKNOWN;
  }
  if (PROD_CAT_MAP.empty()) return CAT_UNKNOWN;
  auto it = PROD_CAT_MAP.find(string(p, n));
  return it == PROD_CAT_MAP.end() ? CAT_UNKNOWN : it->second;
}
static inline int campaign_idx(const char* p, size_t n) {
  u64 v;
  if (n >= 3 && p[0] == 'c' && p[1] == '_' && canon_num(p + 2, n - 2, v)) {
    if (v < CAMP_DENSE.size()) return (int)CAMP_DENSE[v] - 1;
    return -1;
  }
  if (CAMP_MAP.empty()) return -1;
  auto it = CAMP_MAP.find(string(p, n));
  return it == CAMP_MAP.end() ? -1 : (int)it->second;
}

// ---------------------------------------------------------------- records & blocks
#pragma pack(push, 1)
struct EvRec { u64 key; u64 user; u32 ts; u32 ing; u32 pc; };   // ts == NOTS: copy does not survive
#pragma pack(pop)
static const u32 NOTS = 0xFFFFFFFFu;
struct PEv { u64 person; u32 ts; u32 pc; };
struct ClRec { u64 user; u64 code; u32 ts; u32 camp; };

static inline u64 mix(u64 x) {
  x ^= x >> 33; x *= 0xff51afd7ed558ccdULL; x ^= x >> 33; x *= 0xc4ceb9fe1a85ec53ULL; x ^= x >> 33;
  return x;
}

static const size_t BLK = 16384;
struct Block { Block* next; u32 n; u32 pad; char data[BLK - 16]; };
struct Arena {
  char* base = nullptr; size_t used = 0, cap = 0;
  vector<Block*> freeList;
  void init(size_t c) {
    cap = c;
    base = (char*)mmap(nullptr, cap, PROT_READ | PROT_WRITE, MAP_PRIVATE | MAP_ANONYMOUS | MAP_NORESERVE, -1, 0);
    if (base == MAP_FAILED) { perror("arena"); exit(2); }
    madvise(base, cap, MADV_HUGEPAGE);
  }
  Block* get() {
    if (!freeList.empty()) { Block* b = freeList.back(); freeList.pop_back(); return b; }
    if (used + BLK > cap) { fprintf(stderr, "arena full\n"); exit(2); }
    Block* b = (Block*)(base + used); used += BLK; return b;
  }
};
template <class T>
struct BList {
  Block* first = nullptr; Block* last = nullptr;
  T* wp = nullptr; T* we = nullptr;
  static constexpr u32 CAP = (u32)((BLK - 16) / sizeof(T));
  inline void push(const T& r, Arena& a) {
    if (__builtin_expect(wp == we, 0)) {
      if (last) last->n = CAP;
      Block* b = a.get(); b->next = nullptr; b->n = 0;
      if (last) last->next = b; else first = b;
      last = b;
      wp = (T*)b->data; we = wp + CAP;
    }
    memcpy(wp, &r, sizeof(T)); wp++;
  }
  void seal() { if (last) last->n = (u32)(wp - (T*)last->data); }
  template <class F> void each(F fn) const {
    for (Block* b = first; b; b = b->next) { const T* p = (const T*)b->data; for (u32 i = 0; i < b->n; i++) fn(p[i]); }
  }
  size_t size() const { size_t n = 0; for (Block* b = first; b; b = b->next) n += b->n; return n; }
};

int main() {
  T0 = now();
  VERBOSE = getenv("SS_VERBOSE") != nullptr;
  init_time_tables();
  const char* dd = getenv("DATA_DIR"); string DATA = dd ? dd : "/data";
  const char* od = getenv("OUTPUT_DIR"); string OUT = od ? od : "/output";
  int NT = 8;
  if (const char* e = getenv("NCPU")) NT = max(1, atoi(e));
  if (const char* e = getenv("SS_THREADS")) NT = max(1, atoi(e));

  // ---- manifest
  {
    string m = read_small(DATA + "/manifest.json");
    auto get = [&](const char* key) -> i64 {
      size_t k = m.find(key);
      size_t q1 = m.find('"', m.find(':', k) + 1);
      size_t q2 = m.find('"', q1 + 1);
      string v = m.substr(q1 + 1, q2 - q1 - 1);
      v.append(64, '\0');
      return parse_ts(v.data(), (u32)(q2 - q1 - 1));
    };
    WS = get("\"window_start\"");
    WE = get("\"window_end\"");
    CWS = WS - 7 * DAYMS;
    DAY0 = WS >= 0 ? WS / DAYMS : -((-WS + DAYMS - 1) / DAYMS);
    i64 lastDay = (WE - 1) >= 0 ? (WE - 1) / DAYMS : -((-(WE - 1) + DAYMS - 1) / DAYMS);
    NDAYS = (int)(lastDay - DAY0 + 1);
    if (WE - CWS > (i64)UINT32_MAX - 2) { fprintf(stderr, "window too long\n"); return 3; }
  }
  const i64 IBASE = (WS >= 0 ? WS / 1000 : -((-WS + 999) / 1000)) - (1LL << 31);
  // ---- products
  {
    string s = read_small(DATA + "/products.csv");
    vector<pair<string, string>> rows;
    const char* p = s.data(); const char* e = p + s.size();
    const char* nl = (const char*)memchr(p, '\n', e - p);
    p = nl ? nl + 1 : e;
    while (p < e) {
      const char* q = (const char*)memchr(p, '\n', e - p);
      const char* le = q ? q : e;
      Field f[2];
      if (split_line(p, le, 2, f)) rows.push_back({string(f[0].p, f[0].n), string(f[1].p, f[1].n)});
      p = le + 1;
    }
    vector<string> cats;
    for (auto& r : rows) cats.push_back(r.second);
    cats.push_back("unknown");
    sort(cats.begin(), cats.end());
    cats.erase(unique(cats.begin(), cats.end()), cats.end());
    if (cats.size() > 255) { fprintf(stderr, "too many categories\n"); return 3; }
    CATS = cats;
    unordered_map<string, u8> ci;
    for (size_t i = 0; i < cats.size(); i++) ci[cats[i]] = (u8)i;
    CAT_UNKNOWN = ci["unknown"];
    for (auto& r : rows) {
      u64 v;
      u8 c = ci[r.second];
      if (r.first.size() >= 2 && r.first[0] == 'p' && canon_num(r.first.data() + 1, r.first.size() - 1, v) && v < 50000000) {
        if (v >= PROD_CAT_DENSE.size()) PROD_CAT_DENSE.resize(v + 1, 255);
        PROD_CAT_DENSE[v] = c;
      } else {
        PROD_CAT_MAP[r.first] = c;
      }
    }
  }
  // ---- campaigns
  {
    string s = read_small(DATA + "/campaigns.csv");
    const char* p = s.data(); const char* e = p + s.size();
    const char* nl = (const char*)memchr(p, '\n', e - p);
    p = nl ? nl + 1 : e;
    while (p < e) {
      const char* q = (const char*)memchr(p, '\n', e - p);
      const char* le = q ? q : e;
      Field f[2];
      if (split_line(p, le, 2, f)) {
        string id(f[0].p, f[0].n);
        u32 idx = (u32)CAMP_ID.size();
        CAMP_ID.push_back(id);
        CAMP_CH.push_back(string(f[1].p, f[1].n));
        u64 v;
        if (id.size() >= 3 && id[0] == 'c' && id[1] == '_' && canon_num(id.data() + 2, id.size() - 2, v) && v < 50000000) {
          if (v >= CAMP_DENSE.size()) CAMP_DENSE.resize(v + 1, 0);
          CAMP_DENSE[v] = idx + 1;
        } else {
          CAMP_MAP[id] = idx;
        }
      }
      p = le + 1;
    }
    NCAMP = (u32)CAMP_ID.size();
  }
  tick("lookups");

  // ---- map files, make chunks
  vector<Chunk> chunks;
  size_t evBytes = 0, clBytes = 0;
  const size_t CH = 2 << 20;
  auto add_dir = [&](const string& dir, int kind) {
    for (auto& path : list_csv(dir)) {
      File f = map_file(path);
      if (!f.n) continue;
      if (kind == 1) evBytes += f.n;
      if (kind == 2) clBytes += f.n;
      const char* lastNL = (const char*)memrchr(f.p, '\n', f.n);
      const char* safeEnd = lastNL ? lastNL + 1 : f.p;
      for (size_t o = 0; o < f.n; o += CH) {
        size_t e = min(f.n, o + CH);
        chunks.push_back({kind, f.p + o, f.p + e, safeEnd, f.p + f.n, o == 0});
      }
    }
  };
  add_dir(DATA + "/links", 0);
  size_t nLinkChunks = chunks.size();
  add_dir(DATA + "/clicks", 2);
  add_dir(DATA + "/events", 1);
  tick("mapped");

  size_t estEvents = evBytes / 60 + 1024;
  int EB = 64;
  while ((size_t)EB * 16384 < estEvents && EB < 65536) EB *= 2;
  const int EBSH = 64 - __builtin_ctz(EB);
  int PB = 64;
  while ((size_t)PB * 16384 < estEvents && PB < 65536) PB *= 2;
  const int PBSH = 64 - __builtin_ctz(PB);

  vector<Arena> arena(NT);
  size_t perThread = (evBytes + clBytes) / NT * 2 + (256ULL << 20) + (size_t)(EB + PB) * BLK * 2;
  for (int t = 0; t < NT; t++) arena[t].init(perThread);
  vector<vector<BList<EvRec>>> evb(NT, vector<BList<EvRec>>(EB));
  vector<BList<ClRec>> clicks(NT);
  vector<vector<pair<u64, u64>>> links(NT);

  // union-find result: linked code -> person code
  struct PEnt { u64 key, val; };
  struct PMap {
    PEnt* e = nullptr; u64 mask = 0; vector<PEnt> store;
    inline u64 slot(u64 k) const { return mix(k) & mask; }
    inline u64 get(u64 k) const {
      if (!mask) return k;
      u64 h = slot(k);
      for (;;) {
        const PEnt& x = e[h];
        if (x.key == k) return x.val;
        if (x.key == ~0ULL) return k;
        h = (h + 1) & mask;
      }
    }
  } pmap;
  atomic<size_t> linkLeft{nLinkChunks};

  auto build_pmap = [&]() {
    size_t n = 0;
    for (auto& v : links) n += v.size();
    if (!n) return;
    u64 cap = 16; while (cap < n * 4 + 16) cap <<= 1;
    u64 mask = cap - 1;
    vector<PEnt> tab(cap, PEnt{~0ULL, 0});
    vector<u64> codes; codes.reserve(n * 2);
    vector<u32> parent; parent.reserve(n * 2);
    // tab.val temporarily holds dense id
    auto id_of = [&](u64 k) -> u32 {
      u64 h = mix(k) & mask;
      for (;;) {
        if (tab[h].key == k) return (u32)tab[h].val;
        if (tab[h].key == ~0ULL) { tab[h].key = k; u32 i = (u32)codes.size(); tab[h].val = i; codes.push_back(k); parent.push_back(i); return i; }
        h = (h + 1) & mask;
      }
    };
    auto find = [&](u32 x) { while (parent[x] != x) { parent[x] = parent[parent[x]]; x = parent[x]; } return x; };
    for (auto& v : links)
      for (auto& pr : v) {
        u32 a = find(id_of(pr.first)), b = find(id_of(pr.second));
        if (a != b) {
          if (code_less(codes[a], codes[b])) parent[b] = a; else parent[a] = b;
        }
      }
    for (u64 h = 0; h < cap; h++) if (tab[h].key != ~0ULL) tab[h].val = codes[find((u32)tab[h].val)];
    pmap.store = move(tab);
    pmap.e = pmap.store.data();
    pmap.mask = mask;
    tick("union-find done");
  };

  // ---- phase A: parse
  atomic<size_t> next{0};
  auto worker = [&](int t) {
    auto& eb = evb[t];
    auto& cl = clicks[t];
    auto& lk = links[t];
    Arena& ar = arena[t];
    size_t i;
    while ((i = next.fetch_add(1)) < chunks.size()) {
      const Chunk& ch = chunks[i];
      if (ch.kind == 0) {
        for_lines<2>(ch, [&](Field* f) {
          if (!f[0].n || !f[1].n) return;
          lk.push_back({encode(f[0].p, f[0].n), encode(f[1].p, f[1].n)});
        });
        if (linkLeft.fetch_sub(1) == 1) build_pmap();
      } else if (ch.kind == 2) {
        for_lines<4>(ch, [&](Field* f) {
          if (!f[0].n || !f[1].n) return;
          i64 ts = parse_ts(f[3].p, f[3].n);
          if (ts == BADTS || ts < CWS || ts >= WE) return;
          int c = campaign_idx(f[2].p, f[2].n);
          if (c < 0) return;
          cl.push(ClRec{encode(f[1].p, f[1].n), encode(f[0].p, f[0].n), (u32)(ts - CWS), (u32)c}, ar);
        });
      } else {
        for_lines<7>(ch, [&](Field* f) {
          if (!f[0].n) return;
          EvRec r;
          r.key = encode(f[0].p, f[0].n);
          i64 ing = parse_ts(f[3].p, f[3].n);
          if (ing == BADTS || ing == INT64_MAX) r.ing = 0xFFFFFFFFu;
          else {
            i64 s = (ing >= 0 ? ing / 1000 : -((-ing + 999) / 1000)) - IBASE;
            r.ing = s < 0 ? 0 : (s > 0xFFFFFFFELL ? 0xFFFFFFFEu : (u32)s);
          }
          r.ts = NOTS; r.user = 0; r.pc = 0;
          bool ok = ing != BADTS && f[1].n && f[5].n;
          i64 ts = 0;
          u32 price = 0;
          if (ok) {
            ts = parse_ts(f[2].p, f[2].n);
            ok = ts != BADTS && ts >= WS && ts < WE;
          }
          if (ok) {
            const char* tp = f[4].p; u32 tn = f[4].n;
            if (tn == 4) {
              u32 w = ld32(tp);
              ok = w == 0x77656976u /*view*/ || w == 0x74726163u /*cart*/;
            } else if (tn == 8 && ld64(tp) == 0x6573616863727570ULL /*purchase*/) {
              u64 v; bool over;
              if (!parse_digits(f[6].p, f[6].n, v, over) || over || v == 0 || v > 10000000) ok = false;
              else price = (u32)v;
            } else ok = false;
          }
          if (ok && ing - ts > DAYMS) ok = false;
          if (ok) {
            r.pc = price | ((u32)product_cat(f[5].p, f[5].n) << 24);
            r.user = encode(f[1].p, f[1].n);
            r.ts = (u32)(ts - WS);
          }
          eb[mix(r.key) >> EBSH].push(r, ar);
        });
      }
    }
    for (auto& b : eb) b.seal();
    cl.seal();
  };
  {
    vector<thread> th;
    for (int t = 0; t < NT; t++) th.emplace_back(worker, t);
    for (auto& x : th) x.join();
  }
  tick("parsed");
  if (getenv("SS_STOP")) return 0;

  // ---- phase B: dedup events per key bucket, map to person, scatter by person hash
  vector<vector<BList<PEv>>> pev(NT, vector<BList<PEv>>(PB));
  vector<vector<BList<ClRec>>> pcl(NT, vector<BList<ClRec>>(PB));
  {
    atomic<int> nb{0};
    atomic<int> nc{0};
    auto w = [&](int t) {
      auto& out = pev[t];
      Arena& ar = arena[t];
      vector<u64> tkey;
      vector<const EvRec*> tidx;
      vector<const EvRec*> win;
      int b;
      while ((b = nb.fetch_add(1)) < EB) {
        size_t n = 0;
        for (int s = 0; s < NT; s++) n += evb[s][b].size();
        u64 cap = 16; while (cap < n * 2) cap <<= 1;
        u64 mask = cap - 1;
        tkey.assign(cap, ~0ULL);
        tidx.assign(cap, nullptr);
        for (int s = 0; s < NT; s++) {
          for (Block* bl = evb[s][b].first; bl; bl = bl->next) {
            const EvRec* p = (const EvRec*)bl->data;
            for (u32 i = 0; i < bl->n; i++) {
              const EvRec* r = p + i;
              u64 k = r->key;
              u64 h = (k * 0x9E3779B97F4A7C15ULL) >> 20 & mask;
              for (;;) {
                u64 kk = tkey[h];
                if (kk == ~0ULL) { tkey[h] = k; tidx[h] = r; break; }
                if (kk == k) { if (r->ing < tidx[h]->ing) tidx[h] = r; break; }
                h = (h + 1) & mask;
              }
            }
          }
        }
        win.clear();
        for (u64 h = 0; h < cap; h++) { const EvRec* r = tidx[h]; if (r && r->ts != NOTS) win.push_back(r); }
        // person lookups with prefetch
        const size_t PF = 16;
        for (size_t i = 0; i < win.size(); i++) {
          if (i + PF < win.size() && pmap.mask) __builtin_prefetch(&pmap.e[pmap.slot(win[i + PF]->user)]);
          const EvRec* r = win[i];
          u64 person = pmap.get(r->user);
          out[mix(person) >> PBSH].push(PEv{person, r->ts, r->pc}, ar);
        }
        for (int s = 0; s < NT; s++) {
          for (Block* bl = evb[s][b].first; bl;) { Block* nx = bl->next; ar.freeList.push_back(bl); bl = nx; }
          evb[s][b].first = nullptr;
        }
      }
      int s;
      while ((s = nc.fetch_add(1)) < NT) {
        clicks[s].each([&](const ClRec& c) {
          u64 person = pmap.get(c.user);
          ClRec r = c; r.user = person;
          pcl[t][mix(person) >> PBSH].push(r, ar);
        });
      }
      for (auto& x : out) x.seal();
      for (auto& x : pcl[t]) x.seal();
    };
    vector<thread> th;
    for (int t = 0; t < NT; t++) th.emplace_back(w, t);
    for (auto& x : th) x.join();
  }
  tick("dedup+scatter");

  // ---- phase C: per person bucket: group, sort, sessionize, attribute
  struct URow { u64 person; int32_t day; u32 sessions; u32 events; i64 revenue; int top; };
  struct CAgg { i64 sessions, purch, revenue, assisted; };
  vector<vector<URow>> urows(NT);
  vector<vector<CAgg>> cagg(NT);
  {
    atomic<int> nb{0};
    auto w = [&](int t) {
      auto& ur = urows[t];
      auto& ca = cagg[t];
      ca.assign((size_t)(NCAMP + 1) * NDAYS, CAgg{0, 0, 0, 0});
      vector<u64> hkey; vector<u32> hval;
      vector<u64> pcode;           // local id -> person
      vector<u32> cnt, off, ccnt, coff;
      vector<u64> evs;             // (ts << 32 | pc) grouped by person
      struct LC { u32 ts; u32 camp; u64 code; };
      vector<LC> cls;
      vector<u32> lid;             // local id per event in arrival order
      vector<i64> catsum(CATS.size(), 0);
      vector<u32> active(NCAMP + 1, 0);
      vector<u32> activeList;
      int b;
      while ((b = nb.fetch_add(1)) < PB) {
        size_t n = 0;
        for (int s = 0; s < NT; s++) n += pev[s][b].size();
        if (!n) continue;
        u64 cap = 16; while (cap < n * 2) cap <<= 1;
        u64 mask = cap - 1;
        hkey.assign(cap, ~0ULL); hval.resize(cap);
        pcode.clear(); cnt.clear(); lid.resize(n);
        size_t k = 0;
        for (int s = 0; s < NT; s++)
          pev[s][b].each([&](const PEv& e) {
            u64 h = (e.person * 0x9E3779B97F4A7C15ULL) >> 20 & mask;
            u32 id;
            for (;;) {
              if (hkey[h] == e.person) { id = hval[h]; break; }
              if (hkey[h] == ~0ULL) { hkey[h] = e.person; id = hval[h] = (u32)pcode.size(); pcode.push_back(e.person); cnt.push_back(0); break; }
              h = (h + 1) & mask;
            }
            cnt[id]++;
            lid[k++] = id;
          });
        size_t np = pcode.size();
        off.resize(np + 1);
        off[0] = 0;
        for (size_t i = 0; i < np; i++) off[i + 1] = off[i] + cnt[i];
        evs.resize(n);
        for (size_t i = 0; i < np; i++) cnt[i] = off[i];
        k = 0;
        for (int s = 0; s < NT; s++)
          pev[s][b].each([&](const PEv& e) { evs[cnt[lid[k++]]++] = ((u64)e.ts << 32) | e.pc; });
        // clicks: only those of persons with events
        ccnt.assign(np, 0);
        size_t nc = 0;
        for (int s = 0; s < NT; s++) nc += pcl[s][b].size();
        lid.resize(max(n, nc));
        k = 0;
        for (int s = 0; s < NT; s++)
          pcl[s][b].each([&](const ClRec& c) {
            u64 h = (c.user * 0x9E3779B97F4A7C15ULL) >> 20 & mask;
            u32 id = ~0u;
            for (;;) {
              if (hkey[h] == c.user) { id = hval[h]; break; }
              if (hkey[h] == ~0ULL) break;
              h = (h + 1) & mask;
            }
            lid[k++] = id;
            if (id != ~0u) ccnt[id]++;
          });
        coff.resize(np + 1);
        coff[0] = 0;
        for (size_t i = 0; i < np; i++) coff[i + 1] = coff[i] + ccnt[i];
        cls.resize(coff[np]);
        for (size_t i = 0; i < np; i++) ccnt[i] = coff[i];
        k = 0;
        for (int s = 0; s < NT; s++)
          pcl[s][b].each([&](const ClRec& c) { u32 id = lid[k++]; if (id != ~0u) cls[ccnt[id]++] = LC{c.ts, c.camp, c.code}; });

        for (size_t pi = 0; pi < np; pi++) {
          u64 person = pcode[pi];
          u64* e0 = evs.data() + off[pi];
          u64* e1 = evs.data() + off[pi + 1];
          if (e1 - e0 <= 32) {
            for (u64* a = e0 + 1; a < e1; a++) { u64 v = *a; u64* q = a; while (q > e0 && (q[-1] >> 32) > (v >> 32)) { *q = q[-1]; q--; } *q = v; }
          } else sort(e0, e1, [](u64 a, u64 b) { return (a >> 32) < (b >> 32); });
          LC* c0 = cls.data() + coff[pi];
          LC* c1 = cls.data() + coff[pi + 1];
          sort(c0, c1, [](const LC& a, const LC& b) { return a.ts != b.ts ? a.ts < b.ts : code_less(a.code, b.code); });
          LC* lo = c0; LC* hi = c0;
          activeList.clear();
          int curDay = INT32_MIN;
          URow row{};
          auto flush = [&]() {
            if (curDay == INT32_MIN) return;
            int top = -1; i64 best = 0;
            for (size_t c = 0; c < catsum.size(); c++) { if (catsum[c] > best) { best = catsum[c]; top = (int)c; } catsum[c] = 0; }
            row.top = top;
            ur.push_back(row);
          };
          u64* q = e0;
          while (q < e1) {
            u32 st = (u32)(*q >> 32);
            u64* m = q + 1;
            u32 prev = st;
            while (m < e1 && (u32)(*m >> 32) - prev <= 1800000) { prev = (u32)(*m >> 32); m++; }
            i64 startAbs = WS + st;
            i64 dayAbs = startAbs >= 0 ? startAbs / DAYMS : -((-startAbs + DAYMS - 1) / DAYMS);
            int day = (int)(dayAbs - DAY0);
            if (day != curDay) {
              flush();
              curDay = day;
              row = URow{person, (int32_t)(DAY0 + day), 0, 0, 0, -1};
            }
            i64 rev = 0; bool purch = false;
            for (u64* x = q; x < m; x++) {
              u32 pc = (u32)*x;
              u32 price = pc & 0xFFFFFF;
              if (price) { purch = true; rev += price; catsum[pc >> 24] += price; }
            }
            row.sessions++; row.events += (u32)(m - q); row.revenue += rev;
            u32 sc = (u32)(startAbs - CWS);
            while (hi < c1 && hi->ts <= sc) { u32 c = hi->camp; if (active[c]++ == 0) activeList.push_back(c); hi++; }
            u32 lowTs = sc - (u32)(7 * DAYMS);
            while (lo < hi && lo->ts < lowTs) { active[lo->camp]--; lo++; }
            u32 camp = NCAMP;
            if (lo < hi) {
              LC* best = hi - 1;
              while (best > lo && best[-1].ts == hi[-1].ts) best--;
              camp = best->camp;
            }
            CAgg& a = ca[(size_t)camp * NDAYS + day];
            a.sessions++; a.purch += purch; a.revenue += rev;
            size_t w2 = 0;
            for (size_t z = 0; z < activeList.size(); z++) {
              u32 c = activeList[z];
              if (active[c]) { ca[(size_t)c * NDAYS + day].assisted++; activeList[w2++] = c; }
            }
            activeList.resize(w2);
            q = m;
          }
          flush();
          for (LC* x = lo; x < hi; x++) active[x->camp] = 0;
        }
      }
    };
    vector<thread> th;
    for (int t = 0; t < NT; t++) th.emplace_back(w, t);
    for (auto& x : th) x.join();
  }
  tick("sessions");

  // ---- phase D: write outputs
  mkdir((OUT + "/user_daily").c_str(), 0777);
  mkdir((OUT + "/campaign_daily").c_str(), 0777);
  auto write_file = [&](const string& path, const vector<uint8_t>& bytes) {
    int fd = open(path.c_str(), O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (fd < 0) { perror(path.c_str()); exit(2); }
    size_t o = 0;
    while (o < bytes.size()) { ssize_t w = write(fd, bytes.data() + o, bytes.size() - o); if (w <= 0) { perror("write"); exit(2); } o += w; }
    close(fd);
  };
  {
    vector<thread> th;
    for (int t = 0; t < NT; t++) th.emplace_back([&, t] {
      auto& ur = urows[t];
      vector<PColumn> cols(6);
      cols[0].name = "user_id"; cols[0].kind = K_STRING;
      cols[1].name = "day"; cols[1].kind = K_DATE;
      cols[2].name = "sessions"; cols[2].kind = K_INT32;
      cols[3].name = "events"; cols[3].kind = K_INT32;
      cols[4].name = "revenue_cents"; cols[4].kind = K_INT64;
      cols[5].name = "top_category"; cols[5].kind = K_STRING; cols[5].optional = true;
      for (auto& r : ur) {
        string s = decode(r.person);
        cols[0].addStr(s.data(), (u32)s.size());
        cols[1].addI32(r.day);
        cols[2].addI32((int32_t)r.sessions);
        cols[3].addI32((int32_t)r.events);
        cols[4].addI64(r.revenue);
        if (r.top < 0) cols[5].addNull(); else cols[5].addStr(CATS[r.top].data(), (u32)CATS[r.top].size());
      }
      auto bytes = parquet_file(cols, (int64_t)ur.size());
      write_file(OUT + "/user_daily/part-" + to_string(t) + ".parquet", bytes);
    });
    vector<PColumn> cols(7);
    cols[0].name = "campaign_id"; cols[0].kind = K_STRING;
    cols[1].name = "channel"; cols[1].kind = K_STRING;
    cols[2].name = "day"; cols[2].kind = K_DATE;
    cols[3].name = "sessions"; cols[3].kind = K_INT64;
    cols[4].name = "purchasing_sessions"; cols[4].kind = K_INT64;
    cols[5].name = "revenue_cents"; cols[5].kind = K_INT64;
    cols[6].name = "assisted_sessions"; cols[6].kind = K_INT64;
    int64_t n = 0;
    for (u32 c = 0; c <= NCAMP; c++)
      for (int d = 0; d < NDAYS; d++) {
        CAgg a{0, 0, 0, 0};
        for (int t = 0; t < NT; t++) { auto& x = cagg[t][(size_t)c * NDAYS + d]; a.sessions += x.sessions; a.purch += x.purch; a.revenue += x.revenue; a.assisted += x.assisted; }
        if (!a.sessions && !a.assisted) continue;
        const string id = c == NCAMP ? string("organic") : CAMP_ID[c];
        const string ch = c == NCAMP ? string("organic") : CAMP_CH[c];
        cols[0].addStr(id.data(), (u32)id.size());
        cols[1].addStr(ch.data(), (u32)ch.size());
        cols[2].addI32((int32_t)(DAY0 + d));
        cols[3].addI64(a.sessions); cols[4].addI64(a.purch); cols[5].addI64(a.revenue); cols[6].addI64(a.assisted);
        n++;
      }
    auto bytes = parquet_file(cols, n);
    write_file(OUT + "/campaign_daily/part-0.parquet", bytes);
    for (auto& x : th) x.join();
  }
  tick("written");
  return 0;
}
