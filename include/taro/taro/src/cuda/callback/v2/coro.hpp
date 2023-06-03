#pragma once

namespace taro { // begin of namespace taro ===================================

// ==========================================================================
//
// Decalartion of class Coro
//
// ==========================================================================

struct Coro { // Coroutine needs to be struct

  friend class TaroCBV2;
  friend class Task;
  friend void CUDART_CB _cuda_stream_callback_v2(void* void_args);

  public:

    struct promise_type {

      size_t _id;
      bool _final{false};
      Coro get_return_object() { return Coro{this}; }

      std::suspend_always initial_suspend() noexcept { return {}; } // suspend a coroutine now and schedule it after
      std::suspend_always final_suspend() noexcept { _final = true; return {}; } // suspend to decrement dependencies for a task graph
                                                                  // otherwise we don't know whether a coroutine is finished
      void unhandled_exception() {}
      void return_void() noexcept {}
    };

    // coroutine should not be copied
    explicit Coro(promise_type* p);
    ~Coro();
    Coro(const Coro&) = delete;
    Coro(Coro&& rhs);
    Coro& operator=(const Coro&&) = delete;
    Coro& operator=(Coro&& rhs);

  private:

    void _resume();
    bool _done();

    std::coroutine_handle<promise_type> _coro_handle;

    // TODO: move constructor may have issue?
    std::mutex _mtx;
};

// ==========================================================================
//
// Definition of class Coro
//
// ==========================================================================

Coro::Coro(promise_type* p): _coro_handle{std::coroutine_handle<promise_type>::from_promise(*p)} {
}

Coro::Coro(Coro&& rhs): _coro_handle{std::exchange(rhs._coro_handle, nullptr)} {
}

Coro& Coro::operator=(Coro&& rhs) {
  _coro_handle = std::exchange(rhs._coro_handle, nullptr);
  return *this;
}

Coro::~Coro() { 
  if(_coro_handle) { 
    _coro_handle.destroy(); 
  }
}

void Coro::_resume() {
  _coro_handle.resume();
}

bool Coro::_done() {
  return _coro_handle.done();
}

} // end of namespace taro ==============================================
