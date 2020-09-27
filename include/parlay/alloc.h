#ifndef PARLAY_ALLOC_H
#define PARLAY_ALLOC_H

#include <cstdlib>

#include <algorithm>
#include <atomic>
#include <iostream>
#include <new>
#include <optional>
#include <stdexcept>
#include <utility>
#include <vector>

#include "parallel.h"
#include "utilities.h"

#include "internal/concurrent_stack.h"
#include "internal/memory_size.h"
#include "internal/block_allocator.h"

#ifdef DECHECK
#include <decheck/alloc.h>
#endif

namespace parlay {

// ****************************************
//    pool_allocator
// ****************************************

// Allocates headerless blocks from pools of different sizes.
// A vector of pool sizes is given to the constructor.
// Sizes must be at least 8, and must increase.
// For pools of small blocks (below large_threshold) each thread keeps a
//   thread local list of elements from each pool using the
//   block_allocator.
// For pools of large blocks there is only one shared pool for each.
struct pool_allocator {

private:
  static const size_t large_align = 64;
  static const size_t large_threshold = (1 << 20);
  size_t num_buckets;
  size_t num_small;
  size_t max_small;
  size_t max_size;
  std::atomic<long> large_allocated{0};

  concurrent_stack<void*>* large_buckets;
  struct block_allocator *small_allocators;
  std::vector<size_t> sizes;

  void* allocate_large(size_t n) {

    size_t bucket = num_small;
    size_t alloc_size;

    if (n <= max_size) {
      while (n > sizes[bucket]) bucket++;
      std::optional<void*> r = large_buckets[bucket-num_small].pop();
      if (r) return *r;
      alloc_size = sizes[bucket];
    } else alloc_size = n;

    // Alloc size must be a multiple of the alignment
    // Round up to the next multiple.
    if (alloc_size % large_align != 0) {
      alloc_size += (large_align - (alloc_size % large_align));
    }

    void* a = (void*) ::operator new(alloc_size, std::align_val_t{large_align});
    if (a == nullptr) throw std::bad_alloc();
    
    large_allocated += n;
    return a;
  }

  void deallocate_large(void* ptr, size_t n) {
    if (n > max_size) { 
      ::operator delete(ptr, std::align_val_t{large_align});
      large_allocated -= n;
    } else {
      size_t bucket = num_small;
      while (n > sizes[bucket]) bucket++;
      large_buckets[bucket-num_small].push(ptr);
    }
  }

  const size_t small_alloc_block_size = (1 << 20);
  
  pool_allocator(const pool_allocator&) = delete;
  pool_allocator(pool_allocator&&) = delete;
  pool_allocator& operator=(const pool_allocator&) = delete;
  pool_allocator& operator=(pool_allocator&&) = delete;

public:
  ~pool_allocator() {
    for (size_t i=0; i < num_small; i++)
      small_allocators[i].~block_allocator();
    ::operator delete(small_allocators, std::align_val_t{alignof(block_allocator)});
    clear();
    delete[] large_buckets;
  }

  pool_allocator() {}

  explicit pool_allocator(std::vector<size_t> const &sizes) : sizes(sizes) {
    num_buckets = sizes.size();
    max_size = sizes[num_buckets-1];
    num_small = 0;
    while (num_small < num_buckets && sizes[num_small] < large_threshold)
      num_small++;
    max_small = (num_small > 0) ? sizes[num_small - 1] : 0;

    large_buckets = new concurrent_stack<void*>[num_buckets-num_small];

    small_allocators = (struct block_allocator*)
      ::operator new(num_buckets * sizeof(struct block_allocator), std::align_val_t{alignof(block_allocator)} );

    size_t prev_bucket_size = 0;
  
    for (size_t i = 0; i < num_small; i++) {
      size_t bucket_size = sizes[i];
      if (bucket_size < 8)
        throw std::invalid_argument("for small_allocator, bucket sizes must be at least 8");
      if (!(bucket_size > prev_bucket_size))
        throw std::invalid_argument("for small_allocator, bucket sizes must increase");
      prev_bucket_size = bucket_size;
      new (static_cast<void*>(std::addressof(small_allocators[i]))) 
      block_allocator(bucket_size, 0, small_alloc_block_size - 64); 
    }
  }

  void* allocate(size_t n) {
    if (n > max_small) return allocate_large(n);
    size_t bucket = 0;
    while (n > sizes[bucket]) bucket++;
    return small_allocators[bucket].alloc();
  }

  void deallocate(void* ptr, size_t n) {
    if (n > max_small) deallocate_large(ptr, n);
    else {
      size_t bucket = 0;
      while (n > sizes[bucket]) bucket++;
      small_allocators[bucket].free(ptr);
    }
  }

  // allocate, touch, and free to make sure space for small blocks is paged in
  void reserve(size_t bytes) {
    size_t bc = bytes/small_alloc_block_size;
    std::vector<void*> h(bc);
    parallel_for(0, bc, [&] (size_t i) {
      h[i] = allocate(small_alloc_block_size);
    }, 1);
    parallel_for(0, bc, [&] (size_t i) {
      for (size_t j=0; j < small_alloc_block_size; j += (1 << 12))
        ((char*) h[i])[j] = 0;
    }, 1);
    for (size_t i=0; i < bc; i++)
      deallocate(h[i], small_alloc_block_size);
  }

  void print_stats() {
    size_t total_a = 0;
    size_t total_u = 0;
    for (size_t i = 0; i < num_small; i++) {
      size_t bucket_size = sizes[i];
      size_t allocated = small_allocators[i].num_allocated_blocks();
      size_t used = small_allocators[i].num_used_blocks();
      total_a += allocated * bucket_size;
      total_u += used * bucket_size;
      std::cout << "size = " << bucket_size << ", allocated = " << allocated
           << ", used = " << used << std::endl;
    }
    std::cout << "Large allocated = " << large_allocated << std::endl;
    std::cout << "Total bytes allocated = " << total_a + large_allocated << std::endl;
    std::cout << "Total bytes used = " << total_u << std::endl;
  }

  void clear() {
    for (size_t i = num_small; i < num_buckets; i++) {
      std::optional<void*> r = large_buckets[i-num_small].pop();
      while (r) {
        large_allocated -= sizes[i];
        ::operator delete(*r, std::align_val_t{large_align});
        r = large_buckets[i-num_small].pop();
      }
    }
  }
};

// ****************************************
//    default_allocator (uses powers of two as pool sizes)
// ****************************************

// these are bucket sizes used by the default allocator.
inline std::vector<size_t> default_sizes() {
  size_t log_min_size = 4;
  size_t log_max_size = parlay::log2_up(getMemorySize()/64);

  std::vector<size_t> sizes;
  for (size_t i = log_min_size; i <= log_max_size; i++)
    sizes.push_back(size_t{1} << i);
  return sizes;
}



namespace internal {
  
extern inline pool_allocator& get_default_allocator() {
  static pool_allocator default_allocator(default_sizes());
  return default_allocator;
}

}  // namespace internal


// ****************************************
// Following Matches the c++ Allocator specification (minimally)
// https://en.cppreference.com/w/cpp/named_req/Allocator
// Can therefore be used for containers, e.g.:
//    std::vector<int, parlay::allocator<int>>
// ****************************************

template <typename T>
struct allocator {
  using value_type = T;
  T* allocate(size_t n) {
    T *ptr = internal::get_default_allocator().allocate(n * sizeof(T));
#ifdef DECHECK
    decheck::internal::decheck_alloc(ptr);
#endif
    return ptr;
  }
  void deallocate(T* ptr, size_t n) {
#ifdef DECHECK
    decheck::internal::decheck_dealloc(ptr);
#endif
    internal::get_default_allocator().deallocate((void*) ptr, n * sizeof(T));
  }

  allocator() = default;
  template <class U> constexpr allocator(const allocator<U>&) {}
};

template <class T, class U>
bool operator==(const allocator<T>&, const allocator<U>&) { return true; }
template <class T, class U>
bool operator!=(const allocator<T>&, const allocator<U>&) { return false; }

// ****************************************
// Static allocator for single items of a given type, e.g.
//   using long_allocator = type_allocator<long>;
//   long* foo = long_allocator::alloc();
//   *foo = (long) 23;
//   long_allocator::free(foo);
// Uses block allocator, and is headerless  
// ****************************************

template <typename T>
class type_allocator {
public:
  static constexpr size_t default_alloc_size = 0;
  static block_allocator allocator;
  static const bool initialized{true};

  static T* alloc() { 
    T *ptr = (T *) allocator.alloc();
#ifdef DECHECK
    decheck::internal::decheck_alloc(ptr);
#endif
    return ptr;
  }

  static void free(T* ptr) {
#ifdef DECHECK
    decheck::internal::decheck_dealloc(ptr);
#endif
    allocator.free((void*) ptr);
  }

  // for backward compatibility
  //static void init(size_t _alloc_size = 0, size_t _list_size=0) {};
  static void init(size_t, size_t) {};
  static void init() {};
  static void reserve(size_t n = default_alloc_size) {
    allocator.reserve(n);
  }
  static void finish() {allocator.clear();
  }
  static size_t block_size () {return allocator.block_size();}
  static size_t num_allocated_blocks() {return allocator.num_allocated_blocks();}
  static size_t num_used_blocks() {return allocator.num_used_blocks();}
  static size_t num_used_bytes() {return num_used_blocks() * block_size();}
  static void print_stats() {allocator.print_stats();}
};

template<typename T>
block_allocator type_allocator<T>::allocator = block_allocator(sizeof(T));

}  // namespace parlay

#endif  // PARLAY_ALLOC_H
