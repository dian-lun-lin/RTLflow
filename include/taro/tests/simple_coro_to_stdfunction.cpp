#include <coroutine>
#include <functional>
#include <iostream>

struct Coro {
  struct promise_type {
    Coro get_return_object() { return Coro{this}; }
    std::suspend_always initial_suspend(int a) noexcept { return {}; }
    std::suspend_always final_suspend() noexcept { return {}; } 
                                                               
    void unhandled_exception() {}

    auto await_transform(int a) noexcept {
      struct awaiter: public std::suspend_always {
        explicit awaiter() noexcept  {}
        void await_suspend(std::coroutine_handle<>) const noexcept {}
      };

      return awaiter{};
    }

    void return_void() noexcept {}
  };

  Coro(promise_type* p): _coro_handle{std::coroutine_handle<promise_type>::from_promise(*p)} {}

  void resume() { _coro_handle.resume(); }
  bool done() { return _coro_handle.done(); }

  private:

    std::coroutine_handle<promise_type> _coro_handle;
};

Coro task() {
  std::cout << "task11\n";
  co_await 2;
  std::cout << "task12\n";
}

int main() {
  std::function<Coro()> func1([]() -> Coro {
    std::cout << "task21\n";
    co_await 1;
    std::cout << "task22\n";
  });

  std::function<Coro()> func2(task);

  Coro coro1 = func1();
  Coro coro2 = func1();
  coro1.resume();
  coro2.resume();
}
