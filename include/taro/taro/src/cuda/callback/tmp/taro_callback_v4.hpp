#pragma once

#include <taro/declarations.h>
#include "../../utility/utility.hpp"
#include "worker.hpp"
#include "task.hpp"
#include "coro.hpp"

namespace taro { // begin of namespace taro ===================================

class TaroCBV4;

// TODO: memory_order
// TODO: can we assign a kenel mutex for each stream
  
// work-stealing alrotihm using C++20 synchronization primitives
// callback tries enque tasks back to original worker to minimize conext switch
// we don't use notifier as it cannot wake up a specific waiting thread
// we use C++ binary semaphore for each thread instead
//
// C++20 synchronization primitives we use:
// jthread
// C++ binary semaphore
// latch
// atomic.wait()
//
// As suggested by CUDA doc, we use cudaLaunchHostFunc rather than cudaStreamAddCallback
// cudaStreamAddcallback
// cudaStream is handled by Taro
// work-stealing approach
// TODO: check memory order
//
// ==========================================================================
//
// Declaration of class TaroCBV4
//
// ==========================================================================
//


class TaroCBV4 {

  //friend void CUDART_CB _cuda_stream_callback_v1(cudaStream_t st, cudaError_t stat, void* void_args);
  friend void CUDART_CB _cuda_stream_callback_v4(void* void_args);
  friend struct Coro;

  struct cudaStream {
    cudaStream_t st;
    size_t id;
  };

  struct cudaCallbackData {
    TaroCBV4* taro;
    Coro::promise_type* prom;
    Worker* worker;
    size_t stream_id;
  };


  public:

    TaroCBV4(size_t num_threads, size_t num_streams);

    ~TaroCBV4();

    template <typename C, std::enable_if_t<is_static_task_v<C>, void>* = nullptr>
    TaskHandle emplace(C&&);

    template <typename C, std::enable_if_t<is_coro_task_v<C>, void>* = nullptr>
    TaskHandle emplace(C&&);

    //auto suspend();

    template <typename C, std::enable_if_t<is_kernel_v<C>, void>* = nullptr>
    auto cuda_suspend(C&&);

    void schedule();

    void wait();

    bool is_DAG() const;


  private:

    void _process(Worker& worker, Task* tp);

    void _enqueue(Worker& worker, Task* tp, TaskPriority p = TaskPriority::LOW);

    Worker* _this_worker();

    void _exploit_task(Worker& worker);
    bool _explore_task(Worker& worker, const std::stop_token& stop);

    void _request_stop();

    bool _is_DAG(
      Task* tp,
      std::vector<bool>& visited,
      std::vector<bool>& in_recursion
    ) const;

    void _notify(Worker& worker);

    std::vector<std::jthread> _threads;
    std::vector<Worker> _workers;
    std::vector<cudaStream> _streams;
    std::vector<std::unique_ptr<Task>> _tasks;
    std::unordered_map<std::thread::id, size_t> _wids;

    std::vector<size_t> _in_stream_tasks;
    std::mutex _stream_mtx;
    std::mutex _kernel_mtx;

    std::atomic<size_t> _finished{0};

    // _pending_tasks uses int64_t as its type
    // consider this situation:
    //
    // _pending_tasks = 0
    // worker._que.push(task) 
    // thread A steal this task (_pending_tasks.fetch_sub(1));
    // _pending_task.fetch_add(1);
    // if we use size_t, we will get issue as size_t cannot be negative
    std::atomic<int64_t> _pending_tasks{0}; 
    std::atomic<size_t> _cb_cnt{0};
    size_t _cnt{0};
    size_t _MAX_STEALS;
};

// ==========================================================================
//
// callback
//
// ==========================================================================

// cuda callback
void CUDART_CB _cuda_stream_callback_v4(void* void_args) {

  // unpack
  auto* data = (TaroCBV4::cudaCallbackData*) void_args;
  auto* taro = data->taro;
  auto* prom = data->prom;
  auto* worker = data->worker;
  size_t stream_id = data->stream_id;
  
  {
    std::scoped_lock lock(taro->_stream_mtx);
    --taro->_in_stream_tasks[stream_id];
  }


  {
    // high priortiy queue is owned by the callback function
    // Due to CUDA runtime, we cannot guarntee whether the cuda callback function is called sequentially
    // we need a lock to atomically enqueue the task
    // note that there is no lock in enqueue functions
    
    std::scoped_lock lock(worker->_mtx);
    taro->_enqueue(*worker, taro->_tasks[prom->_id].get(), TaskPriority::HIGH);
  }

  //worker->_status.store(Worker::STAT::SIGNALED);
  if(worker->_status.exchange(Worker::STAT::SIGNALED) == Worker::STAT::SLEEP) {
    worker->_status.notify_one();
  }

  taro->_cb_cnt.fetch_sub(1);

}

// ==========================================================================
//
// Definition of class TaroCBV4
//
// ==========================================================================

TaroCBV4::TaroCBV4(size_t num_threads, size_t num_streams): 
  _workers{num_threads}, 
  _MAX_STEALS{(num_threads + 1) << 1},
  _threads{num_threads}
{

  //cudaSetDeviceFlags(cudaDeviceScheduleBlockingSync);

  // GPU streams
  _in_stream_tasks.resize(num_streams, 0);
  _streams.resize(num_streams);
  for(size_t i = 0; i < num_streams; ++i) {
    _streams[i].id = i;
    cudaStreamCreateWithFlags(&(_streams[i].st), cudaStreamNonBlocking);
  }

  // TODO num_threads has warning (invalid conversion: unsigned long to long)
  std::latch initial_done{int(_workers.size())};
  std::mutex wmtx;

  for(size_t id = 0; id < num_threads; ++id) {
    auto* worker = &_workers[id];
    worker->_id = id;

    _threads[id] = std::jthread([this, id, worker, &initial_done, &wmtx](const std::stop_token &stop) {

      worker->_thread = &_threads[id];

      {
        std::scoped_lock lock(wmtx);
        _wids[std::this_thread::get_id()] = worker->_id;
      }
      initial_done.count_down();

      // begin of task scheduling ===================================================
      worker->_status.wait(Worker::STAT::SLEEP);
      //worker._status.wait(Worker::STAT::SLEEP);

      do {
        worker->_status.store(Worker::STAT::BUSY);

        do {  
          _exploit_task(*worker);

          if(!_explore_task(*worker, stop)) {
            return; // stop
          }
        } while(_pending_tasks.load() > 0);

        // false means a task is enqueued by callback
        if(worker->_status.exchange(Worker::STAT::SLEEP) == Worker::STAT::BUSY) {
          worker->_status.wait(Worker::STAT::SLEEP);
        }
      } while(!stop.stop_requested());

      // end of task scheduling =====================================================
      
    });

  }

  initial_done.wait();
}

void TaroCBV4::_exploit_task(Worker& worker) {

  _exploit_task_high:
    while(auto task = worker._que.steal(TaskPriority::HIGH)) {
      _pending_tasks.fetch_sub(1);
      _process(worker, task.value());
    }

  //_exploit_task_low:
  while(auto task = worker._que.pop(TaskPriority::LOW)) {
    _pending_tasks.fetch_sub(1);
    _process(worker, task.value());
    if(!worker._que.empty(TaskPriority::HIGH)) {
      goto _exploit_task_high;
    }
  }
}

bool TaroCBV4::_explore_task(Worker& worker, const std::stop_token& stop) {

  size_t num_steals{0};
  size_t num_yields{0};


  do {

    // TODO: difference between round robin and random?
    for(size_t i = 1; i < _workers.size(); ++i) {
      size_t idx = (worker._id + i) % _workers.size();
      auto opt = _workers[idx]._que.steal(); // steal task from priority LOW to priority HIGH
      if(opt) {
        _pending_tasks.fetch_sub(1);
        _process(worker, opt.value());
        _notify(worker);
        return true;
      }

      if(num_steals++ > _MAX_STEALS) {
        std::this_thread::yield();
        if(num_yields++ > 100) {
          return true;
        }
      }
      
    }

    if(!worker._que.empty(TaskPriority::HIGH)) {
      return true;
    }
  } while(!stop.stop_requested());

  return false; // stop
}

auto TaroCBV4::_sym_schedule(Worker& worker) {
  Task* task{nullptr};

  do {  
    task = _sym_exploit_task(*worker);

    if(task == nullptr) {
      task = _sym_explore_task(*worker);
    }
  } while(_pending_tasks.load() > 0 && task == nullptr);

  if(task != nullptr) {
    return task.coro._coro_handle;
  }

  return std::noop_coroutine();
}

Task* TaroCBV4::_sym_exploit_task(Worker& worker) {

  std::optional<Task*> task;

  if(task = worker._que.steal(TaskPriority::HIGH)) {
    _pending_tasks.fetch_sub(1);
    return task.value();
  }

  if(task = worker._que.pop(TaskPriority::LOW)) {
    _pending_tasks.fetch_sub(1);
    return task.value;
  }

  return nullptr;
}

Task* TaroCBV4::_sym_explore_task(Worker& worker, const std::stop_token& stop) {

  size_t num_steals{0};
  size_t num_yields{0};


  do {

    // TODO: difference between round robin and random?
    for(size_t i = 1; i < _workers.size(); ++i) {
      size_t idx = (worker._id + i) % _workers.size();
      auto opt = _workers[idx]._que.steal(); // steal task from priority LOW to priority HIGH
      if(opt) {
        _pending_tasks.fetch_sub(1);
        return opt.value();
      }

      if(num_steals++ > _MAX_STEALS) {
        std::this_thread::yield();
        if(num_yields++ > 100) {
          return nullptr;
        }
      }
      
    }

    //if(!worker._que.empty(TaskPriority::HIGH)) {
      //return true;
    //}
  } while(true);

  return nullptr;
}


TaroCBV4::~TaroCBV4() {
  for(auto& st: _streams) {
    cudaStreamDestroy(st.st);
  }
}

void TaroCBV4::wait() {
  for(auto& t: _threads) {
    t.join();
  }
  
  while(_cb_cnt != 0) {}

}

void TaroCBV4::schedule() {

  std::vector<Task*> srcs;
  for(auto& t: _tasks) {
    if(t->_join_counter.load() == 0) {
      srcs.push_back(t.get());
    }
  }

  // enqueue tasks before wake up workers to avoid data racing (i.e., worker._que.push())
  for(auto src: srcs) {
    auto& worker = _workers[_cnt++ % _workers.size()];
    _enqueue(worker, src);
  }

  for(size_t i = 0; i < std::min(srcs.size(), _workers.size()); ++i) {
    _workers[i]._status.store(Worker::STAT::SIGNALED);
    _workers[i]._status.notify_one();
  }
}

template <typename C, std::enable_if_t<is_kernel_v<C>, void>*>
auto TaroCBV4::cuda_suspend(C&& c) {

  struct awaitable: std::suspend_always {
    std::function<void(cudaStream_t)> kernel;
    cudaCallbackData data;

    explicit awaitable(TaroCBV4* taro, C&& c): kernel{std::forward<C>(c)} {
      data.taro = taro; 
    }
    auto await_suspend(std::coroutine_handle<Coro::promise_type> coro_handle) {

      // choose the best stream id
      size_t stream_id;
      {
        std::scoped_lock lock(data.taro->_stream_mtx);
        stream_id = std::distance(
          data.taro->_in_stream_tasks.begin(), 
          std::min_element(data.taro->_in_stream_tasks.begin(), data.taro->_in_stream_tasks.end())
        );
        ++data.taro->_in_stream_tasks[stream_id];
      }

      // set callback data
      data.prom = &(coro_handle.promise());
      data.stream_id = stream_id;
      data.worker = data.taro->_this_worker();


      // enqueue the kernel to the stream
      data.taro->_cb_cnt.fetch_add(1);
      {
        std::scoped_lock lock(data.taro->_kernel_mtx);
        kernel(data.taro->_streams[stream_id].st);
        cudaLaunchHostFunc(data.taro->_streams[stream_id].st, _cuda_stream_callback_v4, (void*)&data);
      }

      do {

        auto* task = data.taro->_sym_schedule();
        if(task != nullptr) {
          return task.coro._coro_handle;
        }

        // false means a task is enqueued by callback
        if(worker->_status.exchange(Worker::STAT::SLEEP) == Worker::STAT::BUSY) {
          worker->_status.wait(Worker::STAT::SLEEP);
        }
        worker->_status.store(Worker::STAT::BUSY);
      } while(!stop.stop_requested());
    }
    
  };

  return awaitable{this, std::forward<C>(c)};
}

template <typename C, std::enable_if_t<is_static_task_v<C>, void>*>
TaskHandle TaroCBV4::emplace(C&& c) {
  auto t = std::make_unique<Task>(_tasks.size(), std::in_place_type_t<Task::StaticTask>{}, std::forward<C>(c));
  _tasks.emplace_back(std::move(t));
  return TaskHandle{_tasks.back().get()};
}

template <typename C, std::enable_if_t<is_coro_task_v<C>, void>*>
TaskHandle TaroCBV4::emplace(C&& c) {
  auto t = std::make_unique<Task>(_tasks.size(), std::in_place_type_t<Task::CoroTask>{}, std::forward<C>(c));
  std::get<Task::CoroTask>(t->_handle).coro._coro_handle.promise()._id = _tasks.size();
  _tasks.emplace_back(std::move(t));
  return TaskHandle{_tasks.back().get()};
}

bool TaroCBV4::is_DAG() const {
  std::stack<Task*> dfs;
  std::vector<bool> visited(_tasks.size(), false);
  std::vector<bool> in_recursion(_tasks.size(), false);

  for(auto& t: _tasks) {
    if(!_is_DAG(t.get(), visited, in_recursion)) {
      return false;
    }
  }

  return true;
}

void TaroCBV4::_enqueue(Worker& worker, Task* tp, TaskPriority p) {
  worker._que.push(tp, p);
  _pending_tasks.fetch_add(1); // make sure to add pending tasks after push
}

// this enqueue is only used by main thread
//void TaroCBV4::_enqueue(Task* tp, TaskPriority p) {
  //auto& worker = _workers[_cnt++ % _threads.size()];
  //worker._que.push(tp, p);
  //_pending_tasks.fetch_add(1, std::memory_order_relaxed);
//}



void TaroCBV4::_notify(Worker& worker) {
  size_t cnt{1};
  do {
    unsigned tmp = Worker::STAT::SLEEP;
    size_t idx = (worker._id + cnt++) % _workers.size();
    if(_workers[idx]._status.compare_exchange_weak(tmp, Worker::STAT::SIGNALED)) {
      // TODO: not sure if success can be memory_order_relaxed
      _workers[idx]._status.notify_one();
      return;
    }
  } while(cnt < _workers.size());

  // everyone seems to be busy
  // but busy workers may turn to sleep after we check the status
  // the worker need to wake up in case everyone go to sleep
  worker._status.store(Worker::STAT::SIGNALED);
}

void TaroCBV4::_process(Worker& worker, Task* tp) {
  auto* coro_t = std::get_if<Task::CoroTask>(&tp->_handle);
  coro_t->resume();
}

auto TaroCBV4::_sym_process(Worker& worker, Task* tp) {
  auto* coro_t = std::get_if<Task::CoroTask>(&tp->_handle);
  return coro_t->coro->_coro_handle;
}

void TaroCBV4::_request_stop() {
  for(auto& w: _workers) {
    w._thread->request_stop();
    w._status.store(Worker::STAT::SIGNALED);
    w._status.notify_one();
  }
}

Worker* TaroCBV4::_this_worker() {
  auto it = _wids.find(std::this_thread::get_id());
  return (it == _wids.end()) ? nullptr : &_workers[it->second];
}

bool TaroCBV4::_is_DAG(
  Task* tp,
  std::vector<bool>& visited,
  std::vector<bool>& in_recursion
) const {
  if(!visited[tp->_id]) {
    visited[tp->_id] = true;
    in_recursion[tp->_id] = true;

    for(auto succp: tp->_succs) {
      if(!visited[succp->_id]) {
        if(!_is_DAG(succp, visited, in_recursion)) {
          return false;
        }
      }
      else if(in_recursion[succp->_id]) {
        return false;
      }
    }
  }

  in_recursion[tp->_id] = false;

  return true;
}


} // end of namespace taro ==============================================
