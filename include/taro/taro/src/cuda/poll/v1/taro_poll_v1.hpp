#pragma once

#include <taro/declarations.h>
#include <taskflow/notifier.hpp>
#include <taskflow/wsq.hpp>
#include "../../utility/utility.hpp"
#include "worker.hpp"
#include "task.hpp"
#include "coro.hpp"

namespace taro { // begin of namespace taro ===================================

class TaroPV1;
  
// cudaStream is handled by Taro
// work-stealing approach
// polling approach
//
// ==========================================================================
//
// Declaration of class TaroPV1
//
// ==========================================================================
//


class TaroPV1 {


  public:

    TaroPV1(size_t num_threads, size_t num_streams);

    ~TaroPV1();

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
    //void _cuda_enqueue(Task* tp);

    void _invoke_coro_task(Worker& worker, Task* tp);
    void _invoke_static_task(Worker& worker, Task* tp);
    void _handle_cuda_task(Worker& worker, Task* tp);

    Worker* _this_worker();

    cudaWorker* _cuda_find_worker(Worker& worker);
    void _cuda_update_status(Worker& worker);
    bool _cuda_commit_task(Worker& worker);
    bool _cuda_all_available(Worker& worker);
    //cudaWorker* _cuda_explore_task(Worker& worker);
    

    void _exploit_task(Worker& worker);
    Task* _explore_task(Worker& worker);
    bool _wait_for_task(Worker& worker);

    bool _is_DAG(
      Task* tp,
      std::vector<bool>& visited,
      std::vector<bool>& in_recursion
    ) const;

    size_t _num_streams;
    std::vector<std::thread> _threads;
    std::vector<Worker> _workers;
    std::vector<std::unique_ptr<Task>> _tasks;
    std::unordered_map<std::thread::id, size_t> _wids;

    WorkStealingQueue<Task*> _que;
    //WorkStealingQueue<Task*> _gque;

    std::mutex _qmtx;
    std::mutex _gqmtx;

    Notifier _notifier;
    std::atomic<bool> _stop{false};
    std::atomic<size_t> _finished{0};
    size_t _MAX_STEALS;
    size_t _CUDA_MAX_STEALS;
};

// ==========================================================================
//
// Definition of class TaroPV1
//
// ==========================================================================

TaroPV1::TaroPV1(size_t num_threads, size_t num_streams): 
  _workers{num_threads}, 
  _num_streams{num_streams},
  _notifier{num_threads}, 
  _MAX_STEALS{(num_threads + 1) << 1},
  _CUDA_MAX_STEALS{(num_threads + 1) << 1}, 
  _threads{num_threads}
{

  std::mutex wmtx;
  std::condition_variable wcv;
  //CUcontext ctx;
  //cuCtxCreate(&ctx, CU_CTX_SCHED_YIELD, 0);

  // CPU threads
  _threads.reserve(num_threads);
  size_t cnt{0};
  for(size_t id = 0; id < num_threads; ++id) {
    auto& worker = _workers[id];
    worker._id = id;
    worker._vtm = id;
    worker._waiter = &_notifier._waiters[id];

    // evenly distribute cuda workers to workers
    worker._gws.resize((num_streams - id + num_threads - 1) / num_threads);

    _threads[id] = std::thread([this, id, num_threads, num_streams, &worker, &cnt, &wmtx, &wcv]() {
      //cuCtxSetCurrent(ctx);

      worker._thread = &_threads[id];

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

//cudaWorker* TaroPV1::_cuda_explore_task(Worker& worker) {
  //auto* gw = _cuda_poll(worker);
  //if(gw != nullptr) {
    //auto opt = _gque.steal();
    //if(opt) {
      //auto* cuda_task = opt.value();
      //_process(worker, cuda_task);
    //}
  //}

  ////auto opt = _gque.steal();
  ////if(opt) {
    ////cuda_task = opt.value();
    ////_process(worker, cuda_task);
    //////cuda_task(*gw);
  ////}
  //return gw;
//}

void TaroPV1::_cuda_update_status(Worker& worker) {
  for(auto& gw: worker._gws) {
    if(cudaStreamQuery(gw.stream) != cudaErrorNotReady && gw.status == -1)  {
      gw.status = 0;
    }
  }
}

// return true if there exits a cuda worker whose status changes from 0 to 1
bool TaroPV1::_cuda_commit_task(Worker& worker) {
  bool res{false};
  for(auto& gw: worker._gws) {
    if(gw.status == 0) {
      assert(gw.cur_task != nullptr);
      // commit task to queue
      // previous coro finished, we need to enqueue the coro back
      auto* tmp = gw.cur_task;
      gw.cur_task = nullptr;
      gw.status = 1;
      _enqueue(worker, tmp);
      _notifier.notify(false);
      res = true;
    }
  }
  return res;
}

cudaWorker* TaroPV1::_cuda_find_worker(Worker& worker) {
  for(auto& gw: worker._gws) {
    if(gw.status == 1) {
      return &gw;
    }
  }
  return nullptr;
}

// get a task from worker's own queue
void TaroPV1::_exploit_task(Worker& worker) {
  while(auto task = worker._que.pop()) {
    _process(worker, task.value());
  }
}

// try to steal
Task* TaroPV1::_explore_task(Worker& worker) {

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

bool TaroPV1::_cuda_all_available(Worker& worker) {
  auto git = std::find_if(
    worker._gws.begin(), worker._gws.end(), 
    [](const cudaWorker& gw) { return gw.status != 1; }
  );
  return git == worker._gws.end() ? true : false;
}

bool TaroPV1::_wait_for_task(Worker& worker) {

  Task* task{nullptr};


  explore_task:
    task = _explore_task(worker);
    

  // TODO: why do we need to wake up another worker to avoid starvation?
  // I thought std::this_thread::yield() already did that
  if(task != nullptr) {
    _notifier.notify(false);
    return true;
  }

  cuda_update:
    _cuda_update_status(worker);
    if(_cuda_commit_task(worker)) {
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

  for(size_t vtm = 0; vtm < _workers.size(); ++vtm) {
    if(!_workers[vtm]._que.empty()) {
      _notifier.cancel_wait(worker._waiter);
      worker._vtm = vtm;
      goto explore_task;
    }
  }

  // a coroutine is enqueued to local queue if there exists one cuda_task that is committed
  // return true and go back to explore
  _cuda_update_status(worker);
  if(_cuda_commit_task(worker)) {
    _notifier.cancel_wait(worker._waiter);
    return true;
  }

  // if a cuda worker is not available (i.e., status != 1), we need to keep polling
  if(!_cuda_all_available(worker)) {
    _notifier.cancel_wait(worker._waiter);
    goto cuda_update;
  }

  _notifier.commit_wait(worker._waiter);
  goto explore_task;
}


TaroPV1::~TaroPV1() {
}

void TaroPV1::wait() {
  for(auto& t: _threads) {
    t.join();
  }
}

void TaroPV1::schedule() {

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
auto TaroPV1::cuda_suspend(C&& c) {

  struct awaiter: std::suspend_always {
    std::function<void(cudaStream_t)> kernel;
    TaroPV1& cf;
    Task cuda_task;

    explicit awaiter(TaroPV1* cf, C&& c): cf{*cf}, kernel{std::forward<C>(c)} {}

    void await_suspend(std::coroutine_handle<Coro::promise_type> coro_handle) {
      cuda_task = Task(0, std::in_place_type_t<Task::cudaTask>{}, [coro_handle, this](cudaWorker& gw) mutable {
        // update cur_task
        auto id = coro_handle.promise()._id;
        gw.cur_task = cf._tasks[id].get();
        gw.status = -1;
        kernel(gw.stream);
      });

      cf._enqueue(*(cf._this_worker()), &cuda_task);
      cf._notifier.notify(false);
    }
  };
  return awaiter{this, std::forward<C>(c)};
}

////void TaroPV1::_cuda_update(Worker& worker) {
////}

//void TaroPV1::_cuda_exploit_task(Worker& worker, cudaWorker& gw, bool& assigned) {
  //if(!assigned) {
    //if(!worker._gque.empty()) {
      //// exploit
      //// get a new cuda task from queue
      //auto opt = worker._gque.pop();
      //auto cuda_task = *(opt.value());
      
      //cuda_task(gw);
      //assigned = true;
    //}
  //}
//}

//// try to steal from other workers
//void TaroPV1::_cuda_explore_task(Worker& worker, cudaWorker& gw, bool& assigned) {
  //if(!assigned) {
    //size_t num_steals{0};
    //size_t num_yields{0};
    //std::uniform_int_distribution<size_t> rdvtm(0, _workers.size() - 1);
    //while(true) {
      //size_t vtm = rdvtm(worker._rdgen);
      //if(worker._id != vtm) {
        //auto opt = _workers[vtm]._gque.steal();
        //if(opt) {
          //auto cuda_task = *(opt.value());
          //cuda_task(gw);
          //assigned = true;
          //break;
        //}
      //}
      //if(num_steals++ > _CUDA_MAX_STEALS) {
        //std::this_thread::yield();
        //if(num_yields++ > 100) {
          //break;
        //}
      //}
    //}
  //}
//}

auto TaroPV1::suspend() {
  struct awaiter: std::suspend_always {
    TaroPV1* _cf;
    explicit awaiter(TaroPV1* cf) noexcept : _cf{cf} {}
    void await_suspend(std::coroutine_handle<Coro::promise_type> coro_handle) const noexcept {
      auto id = coro_handle.promise()._id;
      _cf->_enqueue(*(_cf->_this_worker()), _cf->_tasks[id].get());
      _cf->_notifier.notify(false);
    }
  };

  return awaiter{this};
}

template <typename C, std::enable_if_t<is_static_task_v<C>, void>*>
TaskHandle TaroPV1::emplace(C&& c) {
  auto t = std::make_unique<Task>(_tasks.size(), std::in_place_type_t<Task::StaticTask>{}, std::forward<C>(c));
  _tasks.emplace_back(std::move(t));
  return TaskHandle{_tasks.back().get()};
}

template <typename C, std::enable_if_t<is_coro_task_v<C>, void>*>
TaskHandle TaroPV1::emplace(C&& c) {
  auto t = std::make_unique<Task>(_tasks.size(), std::in_place_type_t<Task::CoroTask>{}, std::forward<C>(c));
  std::get<Task::CoroTask>(t->_handle).coro._coro_handle.promise()._id = _tasks.size();
  _tasks.emplace_back(std::move(t));
  return TaskHandle{_tasks.back().get()};
}

bool TaroPV1::is_DAG() const {
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

void TaroPV1::_enqueue(Worker& worker, Task* tp) {
  worker._que.push(tp);
}

void TaroPV1::_enqueue(Worker& worker, const std::vector<Task*>& tps) {
  for(auto* tp: tps) {
    worker._que.push(tp);
  }
}

void TaroPV1::_enqueue(Task* tp) {
  {
    std::scoped_lock lock(_qmtx);
    _que.push(tp);
  }
}

void TaroPV1::_enqueue(const std::vector<Task*>& tps) {
  {
    std::scoped_lock lock(_qmtx);
    for(auto* tp: tps) {
      _que.push(tp);
    }
  }
}

//void TaroPV1::_cuda_enqueue(Task* tp) {
  //{
    //std::scoped_lock lock(_gqmtx);
    //_gque.push(tp);
  //}
//}

void TaroPV1::_process(Worker& worker, Task* tp) {

  switch(tp->_handle.index()) {
    case Task::STATICTASK: {
      _invoke_static_task(worker, tp);
    }
    break;

    case Task::COROTASK: {
      _invoke_coro_task(worker, tp);
    }
    break;

    case Task::CUDATASK: {
      _handle_cuda_task(worker, tp);
    }
    break;

    default:
      assert(false);

  }
}

void TaroPV1::_invoke_static_task(Worker& worker, Task* tp) {
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

void TaroPV1::_invoke_coro_task(Worker& worker, Task* tp) {
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

void TaroPV1::_handle_cuda_task(Worker& worker, Task* tp) {
  _cuda_update_status(worker);
  _cuda_commit_task(worker);
  auto* gw = _cuda_find_worker(worker);

  if(gw != nullptr) {
    std::get_if<Task::cudaTask>(&tp->_handle)->work(*gw);
  }
  else {
    // push back to global _que
    // TODO: may lead to serious serialization
    // is there any better way? i.g., decentralized que?
    _enqueue(tp);
    _notifier.notify(false);
  }
}

Worker* TaroPV1::_this_worker() {
  auto it = _wids.find(std::this_thread::get_id());
  return (it == _wids.end()) ? nullptr : &_workers[it->second];
}

bool TaroPV1::_is_DAG(
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
