// Lock free, linearizable implementation of a concurrent stack
// supporting:
//    push
//    pop
//    size
// Works for elements of any type T
// It requires memory proportional to the largest it has been
// This can be cleared, but only when noone else is using it.
// Requires 128-bit-compare-and-swap
// Counter could overflow "in theory", but would require over 500 years even
// if updated every nanosecond (and must be updated sequentially)

#ifndef PARLAY_CONCURRENT_STACK_H_
#define PARLAY_CONCURRENT_STACK_H_

#include <cstdint>
#include <cstdio>

#include <atomic>
#include <iostream>
#include <optional>

#include "../experimental/atomic.h"

#include "../utilities.h"

namespace parlay {

template <typename T>
class concurrent_stack {
  struct Node {
    T value;
    Node* next;
    size_t length;
  };

  class alignas(64) prim_concurrent_stack {
    struct nodeAndCounter {
      Node* node;
      uint64_t counter;
      nodeAndCounter() = default;
      nodeAndCounter(Node* _node, uint64_t _counter) : node(_node), counter(_counter) { }
    };

    nodeAndCounter head;

    size_t length(Node* n) {
      if (n == nullptr)
        return 0;
      else
        return n->length;
    }

   public:
    prim_concurrent_stack() : head(nullptr, 0) {

    }

    size_t size() { return length(head.node); }

    void push(Node* newNode) {
      nodeAndCounter oldHead, newHead;
      do {
        oldHead = head;
        
        newNode->next = oldHead.node;
        newNode->length = length(oldHead.node) + 1;

        newHead.node = newNode;
        newHead.counter = oldHead.counter + 1;
      } while (!experimental::atomic_compare_and_swap_16(&head, oldHead, newHead));
    }
    
    Node* pop() {
      Node* result;
      nodeAndCounter oldHead, newHead;
      do {
        oldHead = head;
        result = oldHead.node;
        if (result == nullptr) return result;
        newHead.node = result->next;
        newHead.counter = oldHead.counter + 1;
      } while (!experimental::atomic_compare_and_swap_16(&head, oldHead, newHead));

      return result;
    }
  };

  prim_concurrent_stack a;
  prim_concurrent_stack b;

 public:
  size_t size() { return a.size(); }

  void push(T v) {
    Node* x = b.pop();
    if (!x) x = (Node*) ::operator new(sizeof(Node));
    x->value = v;
    a.push(x);
  }

  std::optional<T> pop() {
    Node* x = a.pop();
    if (!x) return {};
    T r = x->value;
    b.push(x);
    return {r};
  }

  // assumes no push or pop in progress
  void clear() {
    Node* x;
    while ((x = a.pop())) ::operator delete(x);
    while ((x = b.pop())) ::operator delete(x);
  }

  concurrent_stack() {}
  ~concurrent_stack() { clear(); }
};

}  // namespace parlay

#endif  // PARLAY_CONCURRENT_STACK_H_
