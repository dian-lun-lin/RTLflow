#pragma once

#include <taro/declarations.h>
#include <taskflow/notifier.hpp>
#include <taskflow/wsq.hpp>
#include "../../utility/utility.hpp"
#include "worker.hpp"
#include "task.hpp"
#include "coro.hpp"

namespace taro { // begin of namespace taro ===================================

class TaroCBV2;
  
// As suggested by CUDA doc, we use cudaLaunchHostFunc rather than cudaStreamAddCallback
// cudaStreamAddcallback
// cudaStream is handled by Taro
// work-stealing approach
//
// "stream-stealing" approach
// keep creating cuda stream if none of exitisng streams is available
//
// ==========================================================================
//
// Declaration of class TaroCBV2
//
// ==========================================================================
//


class TaroCBV2 {

  //friend void CUDART_CB _cuda_stream_callback_v2(cudaStream_t st, cudaError_t stat, void* void_args);
  friend void CUDART_CB _cuda_stream_callback_v2(void* void_args);

  struct cudaCallbackData {
    TaroCBV2* taro{nullptr};
    Coro::promise_type* prom{nullptr};
    cudaStream_t stream{nullptr};
    Task* callback_task{nullptr};
  };


  public:

    // num_streams here does not mean anything
    // this arg is for ease of benchmarking
    TaroCBV2(size_t num_threads, size_t num_streams = 0);

    ~TaroCBV2();

    template <typename C, std::enable_if_t<is_static_task_v<C>, void>* = nullptr>
    TaskHandle emplace(C&&);

    template <typename C, std::enable_if_t<is_coro_task_v<C>, void>* = nullptr>
    TaskHandle emplace(C&&);

    auto suspend();

    template <typename C, std::enable_if_t<is_kernel_v<C>, void>* = nullptr>
    auto cuda_suspend(C&&);

    void schedule();

    void wait();

    bool is_DAG() const;


  private:

    void _process(Worker& worker, Task* tp);

    void _enqueue(Worker& worker, const std::vector<Task*>& tps);
    void _enqueue(Worker& worker, Task* tp);
    void _enqueue(Task* tp);
    void _enqueue(const std::vector<Task*>& tps);

    void _invoke_coro_task(Worker& worker, Task* tp);
    void _invoke_static_task(Worker& worker, Task* tp);
    void _invoke_inner_task(Worker& worker, Task* tp);

    Worker* _this_worker();

    void _exploit_task(Worker& worker);
    Task* _explore_task(Worker& worker);
    bool _wait_for_task(Worker& worker);


    bool _is_DAG(
      Task* tp,
      std::vector<bool>& visited,
      std::vector<bool>& in_recursion
    ) const;

    std::vector<std::thread> _threads;
    std::vector<Worker> _workers;
    std::vector<std::unique_ptr<Task>> _tasks;
    std::unordered_map<std::thread::id, size_t> _wids;

    WorkStealingQueue<Task*> _que;

    std::mutex _qmtx;
    std::mutex _stream_mtx;

    Notifier _notifier;
    std::atomic<bool> _stop{false};
    std::atomic<size_t> _finished{0};
    std::atomic<size_t> _cbcnt{0};
    size_t _MAX_STEALS;

};

// ==========================================================================
//
// callback
//
// ==========================================================================

// cuda callback
void CUDART_CB _cuda_stream_callback_v2(void* void_args) {
  auto* data = (TaroCBV2::cudaCallbackData*) void_args;
  auto* callback_task = data->callback_task;
  auto* taro = data->taro;

  // after enqueue, taro may finish the task and be destructed
  // in that case _notifier is destructed before we call notify here
  // we need to count callback times
  taro->_enqueue(callback_task);
  taro->_notifier.notify(false);
  taro->_cbcnt.fetch_sub(1);
}

// ==========================================================================
//
// Definition of class TaroCBV2
//
// ==========================================================================

TaroCBV2::TaroCBV2(size_t num_threads, size_t num_streams): 
  _workers{num_threads}, 
  _notifier{num_threads}, 
  _MAX_STEALS{(num_threads + 1) << 1},
  _threads{num_threads}
{

  std::mutex wmtx;
  std::condition_variable wcv;

  // CPU threads
  size_t cnt{0};
  for(size_t id = 0; id < num_threads; ++id) {
    auto& worker = _workers[id];
    worker._id = id;
    worker._vtm = id;
    worker._waiter = &_notifier._waiters[id];

    _threads[id] = std::thread([this, id, num_threads, &worker, &cnt, &wmtx, &wcv]() {

      worker._thread = &_threads[id];

      cudaStream_t stream;
      cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking);
      worker._sque.push(stream);

      {
        std::scoped_lock lock(wmtx);
        _wids[std::this_thread::get_id()] = worker._id;
        if(cnt++; cnt == num_threads) {
          wcv.notify_one();
        }
      }

      // TODO: must use 1 instead of !done
      // TODO: before we call schedule, we know there's no task in the queue
      // can we not enter into scheduling loop until we call schedule?
      while(1) {
        _exploit_task(worker);

        if(!_wait_for_task(worker)) {
          break;
        }
      }
    });

  }

  std::unique_lock<std::mutex> lock(wmtx);
  wcv.wait(lock, [&](){ return cnt == num_threads; });
}

// get a task from worker's own queue
void TaroCBV2::_exploit_task(Worker& worker) {
  while(auto task = worker._que.pop()) {
    _process(worker, task.value());
  }
}

// try to steal
Task* TaroCBV2::_explore_task(Worker& worker) {

  size_t num_steals{0};
  size_t num_yields{0};
  std::uniform_int_distribution<size_t> rdvtm(0, _workers.size() - 1);

  Task* task{nullptr};

  do {
    auto opt = ((worker._id == worker._vtm) ? _que.steal() : _workers[worker._vtm]._que.steal());

    if(opt) {
      task = opt.value();
      _process(worker, task);
      break;
    }

    if(num_steals++ > _MAX_STEALS) {
      std::this_thread::yield();
      if(num_yields++ > 100) {
        break;
      }
    }
    worker._vtm = rdvtm(worker._rdgen);
  } while(!_stop);

  return task;
}

bool TaroCBV2::_wait_for_task(Worker& worker) {

  Task* task{nullptr};
  explore_task:
    task = _explore_task(worker);

  // TODO: why do we need to wake up another worker to avoid starvation?
  // I thought std::this_thread::yield() already did that
  if(task) {
    _notifier.notify(false);
    return true;
  }
  
  // ======= 2PC guard =======
  _notifier.prepare_wait(worker._waiter);
  
  if(!_que.empty()) {
    _notifier.cancel_wait(worker._waiter);
    worker._vtm = worker._id; 
    goto explore_task;
  }

  if(_stop) {
    _notifier.cancel_wait(worker._waiter);
    _notifier.notify(true);
    return false;
  }

  // TODO: why do we need to use index-based scan to avoid data race?
  for(size_t vtm = 0; vtm < _workers.size(); ++vtm) {
    if(!_workers[vtm]._que.empty()) {
      _notifier.cancel_wait(worker._waiter);
      worker._vtm = vtm;
      goto explore_task;
    }
  }

  _notifier.commit_wait(worker._waiter);

  goto explore_task;
}


TaroCBV2::~TaroCBV2() {
  for(auto& w: _workers) {
    while(!w._sque.empty()) {
      checkCudaError(cudaStreamDestroy(w._sque.pop().value()));
    }
  }
}

void TaroCBV2::wait() {
  for(auto& t: _threads) {
    t.join();
  }
  //std::unique_lock lock(_nmtx);
  while(_cbcnt.load() != 0) {}
}

void TaroCBV2::schedule() {

  std::vector<Task*> srcs;
  for(auto& t: _tasks) {
    if(t->_join_counter.load() == 0) {
      srcs.push_back(t.get());
    }
  }

  _enqueue(srcs);
  _notifier.notify(srcs.size());
}

template <typename C, std::enable_if_t<is_kernel_v<C>, void>*>
auto TaroCBV2::cuda_suspend(C&& c) {

  struct awaiter: std::suspend_always {
    std::function<void(cudaStream_t)> kernel;
    cudaCallbackData data;
    Task callback_task;
    std::atomic<bool> sync{false};
    awaiter(const awaiter& ) {} // compiler bug?

    explicit awaiter(TaroCBV2* taro, C&& c): kernel{std::forward<C>(c)} {
      data.taro = taro; 
    }
    void await_suspend(std::coroutine_handle<Coro::promise_type> coro_handle) {
      _set_callback(coro_handle);

      // enqueue the kernel to the stream
      kernel(data.stream);
      data.taro->_cbcnt.fetch_add(1);
      cudaLaunchHostFunc(data.stream, _cuda_stream_callback_v2, (void*)&data);
    }

    private:

      cudaStream_t _get_stream() {

        auto* taro = data.taro;
        Worker& worker = *(taro->_this_worker());

        // get an empty stream from worker's own sque
        auto opt = worker._sque.pop();
        if(opt) {
          return opt.value();
        }

        // try to steal an empty stream from other workers
        cudaStream_t stream{nullptr};
        size_t num_steals{0};
        size_t num_yields{0};
        std::uniform_int_distribution<size_t> rdvtm(0, taro->_workers.size() - 1);

        do {
          size_t vtm = rdvtm(worker._rdgen);

          if(worker._id == vtm) { continue; }

          auto opt = taro->_workers[vtm]._sque.steal();

          if(opt) {
            stream = opt.value();
            break;
          }

          if(num_steals++ > taro->_MAX_STEALS) {
            std::this_thread::yield();
            if(num_yields++ > 10) {
              break;
            }
          }
        } while(!taro->_stop);

        // if we really cannot get an empty stream, create a new one
        if(stream == nullptr) {
          cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking);
        }

        return stream;
      }

      void _set_callback(std::coroutine_handle<Coro::promise_type> coro_handle) {
        data.stream = _get_stream();
        data.prom = &(coro_handle.promise());

        callback_task = Task(0, std::in_place_type_t<Task::InnerTask>{}, 
          [coro_handle, this](Worker& worker) mutable {
            cudaStream_t stream;
            size_t id;
            TaroCBV2* taro{nullptr};

            stream = data.stream;
            id = data.prom->_id;
            taro = data.taro;

            worker._sque.push(stream);
            Task* tp = taro->_tasks[id].get();

            // when enqueued coroutine get executed
            // this awaiter will be destryed due to coro.resume()
            // in this case, the callback_task is destroyed
            // however, the callback thread may still in callback function (e.g., calling notify())
            // which results in data race
            //
            // hence, we need to finish the callback_task before we execute this enqueued coroutine
            auto* coro_t = std::get_if<Task::CoroTask>(&tp->_handle);
            std::scoped_lock lock(coro_t->coro._mtx);
            taro->_enqueue(worker, tp);
            taro->_notifier.notify(false);
          }
        );

        data.callback_task = &callback_task;
      }
  };

  return awaiter{this, std::forward<C>(c)};
}

auto TaroCBV2::suspend() {
  struct awaiter: std::suspend_always {
    TaroCBV2* _taro;
    explicit awaiter(TaroCBV2* taro) noexcept : _taro{taro} {}
    void await_suspend(std::coroutine_handle<Coro::promise_type> coro_handle) const noexcept {
      auto id = coro_handle.promise()._id;
      _taro->_enqueue(*(_taro->_this_worker()), _taro->_tasks[id].get());
      _taro->_notifier.notify(false);
    }
  };

  return awaiter{this};
}

template <typename C, std::enable_if_t<is_static_task_v<C>, void>*>
TaskHandle TaroCBV2::emplace(C&& c) {
  auto t = std::make_unique<Task>(_tasks.size(), std::in_place_type_t<Task::StaticTask>{}, std::forward<C>(c));
  _tasks.emplace_back(std::move(t));
  return TaskHandle{_tasks.back().get()};
}

template <typename C, std::enable_if_t<is_coro_task_v<C>, void>*>
TaskHandle TaroCBV2::emplace(C&& c) {
  auto t = std::make_unique<Task>(_tasks.size(), std::in_place_type_t<Task::CoroTask>{}, std::forward<C>(c));
  std::get<Task::CoroTask>(t->_handle).coro._coro_handle.promise()._id = _tasks.size();
  _tasks.emplace_back(std::move(t));
  return TaskHandle{_tasks.back().get()};
}

bool TaroCBV2::is_DAG() const {
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

void TaroCBV2::_enqueue(Worker& worker, Task* tp) {
  worker._que.push(tp);
}

void TaroCBV2::_enqueue(Worker& worker, const std::vector<Task*>& tps) {
  for(auto* tp: tps) {
    worker._que.push(tp);
  }
}

void TaroCBV2::_enqueue(Task* tp) {
  {
    std::scoped_lock lock(_qmtx);
    _que.push(tp);
  }
}

void TaroCBV2::_enqueue(const std::vector<Task*>& tps) {
  {
    std::scoped_lock lock(_qmtx);
    for(auto* tp: tps) {
      _que.push(tp);
    }
  }
}

void TaroCBV2::_process(Worker& worker, Task* tp) {

  switch(tp->_handle.index()) {
    case Task::STATICTASK: {
      _invoke_static_task(worker, tp);
    }
    break;

    case Task::COROTASK: {
      _invoke_coro_task(worker, tp);
    }
    break;

    case Task::INNERTASK: {
      _invoke_inner_task(worker, tp);
    }
    break;

    default:
      assert(false);
  }
}

void TaroCBV2::_invoke_static_task(Worker& worker, Task* tp) {
  std::get_if<Task::StaticTask>(&tp->_handle)->work();
  for(auto succp: tp->_succs) {
    if(succp->_join_counter.fetch_sub(1) == 1) {
      _enqueue(worker, succp);
      _notifier.notify(false);
    }
  }

  if(_finished.fetch_add(1) + 1 == _tasks.size()) {
    _stop = true;
    _notifier.notify(true);
  }
}

void TaroCBV2::_invoke_coro_task(Worker& worker, Task* tp) {
  auto* coro_t = std::get_if<Task::CoroTask>(&tp->_handle);
  // when this thread (i.e., t1) calls cuda_suspend and insert a callback to CUDA runtime
  // the CUDA runtime may finish cuda kernel very fast and 
  // use its own CPU thread to call the callback to enque the coroutine back
  // then, another thread (i.e., t2) may get this coroutine and performs resume()
  // However, t1 may still in resume()
  // which in turn causing data race
  // That is, two same coroutines are executed in parallel by t1 and t2
  // hence we use lock in each coro to check if a coroutine is in busy used 
  // final has similar issue as well
  bool final{false};
  {
    std::scoped_lock lock(coro_t->coro._mtx);
    coro_t->resume();
    final = coro_t->coro._coro_handle.promise()._final;
  }

  if(final) {
    for(auto succp: tp->_succs) {
      if(succp->_join_counter.fetch_sub(1) == 1) {
        _enqueue(worker, succp);
        _notifier.notify(false);
      }
    }

    if(_finished.fetch_add(1) + 1 == _tasks.size()) {
      // TODO: we need to check if there's no callback
      _stop = true;
      _notifier.notify(true);
    }
  }
}

void TaroCBV2::_invoke_inner_task(Worker& worker, Task* tp) {
  std::get_if<Task::InnerTask>(&tp->_handle)->work(worker);
}

Worker* TaroCBV2::_this_worker() {
  auto it = _wids.find(std::this_thread::get_id());
  return (it == _wids.end()) ? nullptr : &_workers[it->second];
}

bool TaroCBV2::_is_DAG(
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
