#include <coroutine>
#include <list>
#include <numeric>
#include "threadpool.hpp"

struct Task {

  struct promise_type {
    std::suspend_never initial_suspend() noexcept { return {}; }
    std::suspend_never final_suspend() noexcept { return {}; }

    // ignore
    Task get_return_object() { return Task{}; }
    void unhandled_exception() {}
  };
};


class Scheduler {

  std::list<std::coroutine_handle<>> _tasks;
  ThreadPool _threadpool;


  public: 

    Scheduler(size_t num_threads): _threadpool{num_threads} {}
    ~Scheduler() = default;
    Scheduler(const Scheduler&) = delete;
    Scheduler(Scheduler&&) = delete;
    Scheduler& operator = (const Scheduler&) = delete;
    Scheduler& operator = (Scheduler&&) = delete;
    

    void schedule() {
      while(!_tasks.empty()) {
        std::future<void> fu;
        auto task = _tasks.front();
        _tasks.pop_front();

        if(!task.done()) { 
          fu = _threadpool.insert([=]() { task.resume(); }); 
          fu.get();
        }
      }
      _threadpool.shutdown();
    }

    auto suspend() {
      struct Awaiter: std::suspend_always {
        Scheduler& scheduler;
        Awaiter(Scheduler& sched): scheduler{sched} {}
        void await_suspend(std::coroutine_handle<> coro) {
          scheduler._tasks.push_back(coro);
        }
      };

      return Awaiter{*this};
    }
};



