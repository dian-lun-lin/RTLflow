#pragma once


namespace taro { // begin of namespace taro ===================================

enum TaskPriority : unsigned {
  HIGH = 0,
  LOW = 1,
  MAX = 2
};

constexpr auto TARO_MAX_PRIORITY = static_cast<unsigned>(TaskPriority::MAX);

// ==========================================================================
//
// Definition of class WorkStealingQueue
//
// ==========================================================================

template <typename T>
class WorkStealingQueue {

  public:
    
    explicit WorkStealingQueue(int64_t capacity = 512);

    ~WorkStealingQueue();
    
    bool empty() const noexcept;
    bool empty(unsigned p) const noexcept;
    
    size_t size() const noexcept;
    size_t size(unsigned p) const noexcept;

    int64_t capacity() const noexcept;
    int64_t capacity(unsigned p) const noexcept;
    
    template <typename O>
    void push(O&& item, unsigned p);
    
    std::optional<T> pop();
    std::optional<T> pop(unsigned p);
    
    std::optional<T> steal();
    std::optional<T> steal(unsigned p);

  private:

    struct Array {

      int64_t C;
      int64_t M;
      std::atomic<T>* S;

      explicit Array(int64_t c) : 
        C {c},
        M {c-1},
        S {new std::atomic<T>[static_cast<size_t>(C)]} {
      }

      ~Array() {
        delete [] S;
      }

      int64_t capacity() const noexcept {
        return C;
      }
      
      template <typename O>
      void push(int64_t i, O&& o) noexcept {
        S[i & M].store(std::forward<O>(o), std::memory_order_relaxed);
      }

      T pop(int64_t i) noexcept {
        return S[i & M].load(std::memory_order_relaxed);
      }

      Array* resize(int64_t b, int64_t t) {
        Array* ptr = new Array {2*C};
        for(int64_t i=t; i!=b; ++i) {
          ptr->push(i, pop(i));
        }
        return ptr;
      }

    };

    std::array<std::atomic<int64_t>, static_cast<size_t>(TARO_MAX_PRIORITY)> _top;
    std::array<std::atomic<int64_t>, static_cast<size_t>(TARO_MAX_PRIORITY)> _bottom;
    std::array<std::atomic<Array*>, static_cast<size_t>(TARO_MAX_PRIORITY)> _array;
    std::array<std::vector<Array*>, static_cast<size_t>(TARO_MAX_PRIORITY)> _garbage;

};

// ==========================================================================
//
// Definition of class WorkStealingQueue
//
// ==========================================================================

// Constructor
template <typename T>
WorkStealingQueue<T>::WorkStealingQueue(int64_t c) {
  assert(c && (!(c & (c-1))));

  // duff's device unrolling
  switch(0) {
    case 0: 
      _top[0].store(0, std::memory_order_relaxed);
      _bottom[0].store(0, std::memory_order_relaxed);
      _array[0].store(new Array{c}, std::memory_order_relaxed);
      _garbage[0].reserve(32);
    case 1:
      _top[1].store(0, std::memory_order_relaxed);
      _bottom[1].store(0, std::memory_order_relaxed);
      _array[1].store(new Array{c}, std::memory_order_relaxed);
      _garbage[1].reserve(32);
  }
}

// Destructor
template <typename T>
WorkStealingQueue<T>::~WorkStealingQueue() {
  // duff's device unrolling
  switch(0) {
    case 0: 
      for(auto a : _garbage[0]) {
        delete a;
      }
      delete _array[0].load();
    case 1: 
      for(auto a : _garbage[1]) {
        delete a;
      }
      delete _array[1].load();
  }
}
  
// Function: empty
template <typename T>
bool WorkStealingQueue<T>::empty() const noexcept {
  // duff's device unrolling
  switch(0) {
    case 0: 
      if(!empty(0)) { return false; }
    case 1: 
      if(!empty(1)) { return false; }
  }
  return true;
}

// Function: empty
template <typename T>
bool WorkStealingQueue<T>::empty(unsigned p) const noexcept {
  int64_t b = _bottom[p].load(std::memory_order_relaxed);
  int64_t t = _top[p].load(std::memory_order_relaxed);
  return (b <= t);
}

// Function: size
template <typename T>
size_t WorkStealingQueue<T>::size() const noexcept {
  size_t s{0};
  // duff's device unrolling
  switch(0) {
    case 0: 
      s += size(0);
    case 1: 
      s += size(1);
  }
  return s;
}

// Function: size
template <typename T>
size_t WorkStealingQueue<T>::size(unsigned p) const noexcept {
  int64_t b = _bottom[p].load(std::memory_order_relaxed);
  int64_t t = _top[p].load(std::memory_order_relaxed);
  return static_cast<size_t>(b >= t ? b - t : 0);
}

// Function: push
template <typename T>
template <typename O>
void WorkStealingQueue<T>::push(O&& o, unsigned p) {
  int64_t b = _bottom[p].load(std::memory_order_relaxed);
  int64_t t = _top[p].load(std::memory_order_acquire);
  Array* a = _array[p].load(std::memory_order_relaxed);

  // queue is full
  if(a->capacity() - 1 < (b - t)) {
    Array* tmp = a->resize(b, t);
    _garbage[p].push_back(a);
    std::swap(a, tmp);
    _array[p].store(a, std::memory_order_relaxed);
  }

  a->push(b, std::forward<O>(o));
  std::atomic_thread_fence(std::memory_order_release);
  _bottom[p].store(b + 1, std::memory_order_relaxed);
}

// Function: pop
// pop from HIGH to LOW
// HIGH: 0
// LOW: 1
template <typename T>
std::optional<T> WorkStealingQueue<T>::pop() {
  // duff's device unrolling
  switch(0) {
    case 0: 
      if(auto s = pop(0); s) {
        return s;
      }
    case 1: 
      if(auto s = pop(1); s) {
        return s;
      }
  }
  return std::nullopt;
}

// Function: pop
template <typename T>
std::optional<T> WorkStealingQueue<T>::pop(unsigned p) {
  int64_t b = _bottom[p].load(std::memory_order_relaxed) - 1;
  Array* a = _array[p].load(std::memory_order_relaxed);
  _bottom[p].store(b, std::memory_order_relaxed);
  std::atomic_thread_fence(std::memory_order_seq_cst);
  int64_t t = _top[p].load(std::memory_order_relaxed);

  std::optional<T> item;

  if(t <= b) {
    item = a->pop(b);
    if(t == b) {
      // the last item just got stolen
      if(!_top[p].compare_exchange_strong(t, t+1, 
                                       std::memory_order_seq_cst, 
                                       std::memory_order_relaxed)) {
        item = std::nullopt;
      }
      _bottom[p].store(b + 1, std::memory_order_relaxed);
    }
  }
  else {
    _bottom[p].store(b + 1, std::memory_order_relaxed);
  }

  return item;
}

// Function: steal
// steal task from LOW to HIGH
// HIGH: 0
// LOW: 1
template <typename T>
std::optional<T> WorkStealingQueue<T>::steal() {
  // duff's device unrolling
  switch(1) {
    case 1: 
      if(auto s = steal(1); s) {
        return s;
      }
    case 0: 
      if(auto s = steal(0); s) {
        return s;
      }
  }
  return std::nullopt;
}

// Function: steal
template <typename T>
std::optional<T> WorkStealingQueue<T>::steal(unsigned p) {
  int64_t t = _top[p].load(std::memory_order_acquire);
  std::atomic_thread_fence(std::memory_order_seq_cst);
  int64_t b = _bottom[p].load(std::memory_order_acquire);
  
  std::optional<T> item;

  if(t < b) {
    Array* a = _array[p].load(std::memory_order_consume);
    item = a->pop(t);
    if(!_top[p].compare_exchange_strong(t, t+1,
                                     std::memory_order_seq_cst,
                                     std::memory_order_relaxed)) {
      return std::nullopt;
    }
  }

  return item;
}

// Function: capacity
template <typename T>
int64_t WorkStealingQueue<T>::capacity() const noexcept {
  size_t c{0};
  // duff's device unrolling
  switch(0) {
    case 0:
      c += capacity(0);
    case 1:
      c += capacity(1);
  }
  return c;
}

// Function: capacity
template <typename T>
int64_t WorkStealingQueue<T>::capacity(unsigned p) const noexcept {
  return _array[p].load(std::memory_order_relaxed)->capacity();
}

} // end of namespace taro ==============================================
