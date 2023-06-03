#pragma once

namespace taro { // begin of namespace taro ===================================

// ==========================================================================
//
// Decalartion of class Coro
//
// ==========================================================================

struct Coro { // Coroutine needs to be struct

  friend class TaroCBV4;
  friend class Task;

  public:

    struct promise_type {

      size_t _id;
      Coro get_return_object() { return Coro{this}; }

      std::suspend_always initial_suspend() noexcept { return {}; } // suspend a coroutine now and schedule it after
      auto final_suspend() noexcept { 
        struct final_awaiter: std::suspend_always {
          void await_suspend(std::coroutine_handle<Coro::promise_type> ch) {
            size_t tid = ch.promise().id;
            Task* tp = Coro::taro._tasks[tid].get();
            Worker& worker = Coro::taro._this_worker();
            
            Task* nextp{nullptr};
            for(auto succp: tp->_succs) {
              if(succp->_join_counter.fetch_sub(1) == 1) {
                if(nextp == nullptr) {
                  nextp = succp;
                  continue;
                }
                Coro::taro._enqueue(worker, succp);
                Coro::taro._notify(worker);
              }
            }

            if(Coro::taro._finished.fetch_add(1) + 1 == Coro::taro._tasks.size()) {
              Coro::taro._request_stop();
            }

            if(nextp) {
              return nextp->coro._coro_handle;
            }
            return std::noop_coroutine();
          }
        };
        return final_awaitable{}; 
      } 
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

    static void set_scheduler(TaroCBV4& taro) {
      _taro = taro;
    }

  private:

    void _resume();
    bool _done();

    std::coroutine_handle<promise_type> _coro_handle;

    static TaroCBV4& _taro;

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
