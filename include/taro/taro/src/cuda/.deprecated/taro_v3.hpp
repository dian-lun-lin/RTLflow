#pragma once

#include "../../declarations.h"
#include "../coro.hpp"
#include "../task.hpp"
#include "utility.hpp"

namespace taro { // begin of namespace taro ===================================

// cudaStreamAddcallback
// cudaStream is handled by Taro

class TaroV3;

  
// ==========================================================================
//
// Declaration of class TaroV3
//
// ==========================================================================
//


class TaroV3 {

  friend void CUDART_CB _cuda_stream_callback_v3(cudaStream_t st, cudaError_t stat, void* void_args);

  struct cudaStream {
    cudaStream_t st;
    size_t id;
  };

  struct cudaCallbackData {
    TaroV3* cf;
    Coro::promise_type* prom;
    size_t stream_id;
    //size_t num_kernels;
  };



  public:

    TaroV3(size_t num_threads, size_t num_streams);

    ~TaroV3();

    template <typename C, std::enable_if_t<is_static_task_v<C>, void>* = nullptr>
    TaskHandle emplace(C&&);

    template <typename C, std::enable_if_t<is_coro_task_v<C>, void>* = nullptr>
    TaskHandle emplace(C&&);

    auto suspend();

    template <typename C, std::enable_if_t<is_kernel_v<C>, void>* = nullptr>
    auto cuda_suspend(C&&);

    void schedule();

    void wait();

    bool is_DAG();


  private:

    void _process(Task* tp);
    void _enqueue(Task* tp);


    void _invoke_coro_task(Task* tp);
    void _invoke_static_task(Task* tp);


    bool _is_DAG(
      Task* tp,
      std::vector<bool>& visited,
      std::vector<bool>& in_recursion
    );

    std::vector<std::thread> _workers;
    std::vector<cudaStream> _streams;
    std::vector<size_t> _in_stream_tasks;

    std::vector<std::unique_ptr<Task>> _tasks;
    std::queue<Task*> _queue;
    // std::vector<bool> has specilized implementation
    // concurrently writes to std::vector<bool> is never OK
    // 0: false
    // 1: true
    std::vector<int> _callbacks;

    std::mutex _mtx;
    std::mutex _stream_mtx;
    std::mutex _kernel_mtx;
    std::condition_variable _cv;
    std::atomic<bool> _stop{false};
    std::atomic<size_t> _finished{0};
};


// ==========================================================================
//
// callback
//
// ==========================================================================

void CUDART_CB _cuda_stream_callback_v3(cudaStream_t st, cudaError_t stat, void* void_args) {
  checkCudaError(stat);

  // unpack
  auto* data = (TaroV3::cudaCallbackData*) void_args;
  auto* cf = data->cf;
  auto* prom = data->prom;
  auto stream_id = data->stream_id;

  {
    std::scoped_lock(cf->_stream_mtx);
    --cf->_in_stream_tasks[stream_id];
  }

  cf->_callbacks[prom->_id] = 0;
  cf->_enqueue(cf->_tasks[prom->_id].get());
}
// ==========================================================================
//
// Definition of class TaroV3
//
// ==========================================================================

template <typename C, std::enable_if_t<is_kernel_v<C>, void>*>
auto TaroV3::cuda_suspend(C&& c) {

  struct awaiter: std::suspend_always {
    std::function<void(cudaStream_t)> kernel;
    cudaCallbackData data;

    explicit awaiter(TaroV3* cf, C&& c): kernel{std::forward<C>(c)} {
      data.cf = cf; 
    }
    void await_suspend(std::coroutine_handle<Coro::promise_type> coro_handle) {

      // choose the best stream id
      size_t stream_id;
      {
        std::scoped_lock lock(data.cf->_stream_mtx);
        stream_id = std::distance(
          data.cf->_in_stream_tasks.begin(), 
          std::min_element(data.cf->_in_stream_tasks.begin(), data.cf->_in_stream_tasks.end())
        );
        ++data.cf->_in_stream_tasks[stream_id];
      }

      // set callback data
      data.prom = &(coro_handle.promise());
      data.stream_id = stream_id;
      data.cf->_callbacks[data.prom->_id] = 1;


      // enqueue the kernel to the stream
      {
        std::scoped_lock lock(data.cf->_kernel_mtx);
        kernel(data.cf->_streams[stream_id].st);
        cudaStreamAddCallback(data.cf->_streams[stream_id].st, _cuda_stream_callback_v3, (void*)&data, 0);
      }

    }
    
  };

  return awaiter{this, std::forward<C>(c)};
}

auto TaroV3::suspend() {  // value from co_await
  struct awaiter: public std::suspend_always { // definition of awaitable for co_await
    explicit awaiter() noexcept {}
    void await_suspend(std::coroutine_handle<Coro::promise_type>) const noexcept {
      // TODO: add CPU callback?
    }
  };

  return awaiter{};
}

TaroV3::TaroV3(size_t num_threads, size_t num_streams) {

  // CPU workers
  _workers.reserve(num_threads);

  for(size_t t = 0; t < num_threads; ++t) {
    _workers.emplace_back([this, t]() {
        while(true) {
          Task* tp{nullptr};
          {
            std::unique_lock<std::mutex> lock(_mtx);
            _cv.wait(lock, [this]{ return _stop || (!_queue.empty()); });
            if(_stop) {
              return;
            }

            tp = _queue.front();
            _queue.pop();
          }
          if(tp) {
            _process(tp);
          }
        }
      }
    );
  }

  // GPU streams
  _in_stream_tasks.resize(num_streams, 0);
  _streams.reserve(num_streams);
  for(size_t i = 0; i < num_streams; ++i) {
    _streams[i].id = i;
    cudaStreamCreate(&(_streams[i].st));
  }
}

TaroV3::~TaroV3() {
  for(auto& w: _workers) {
    w.join();
  } 

  for(auto& st: _streams) {
    cudaStreamDestroy(st.st);
  }
}

void TaroV3::wait() {
  for(auto& w: _workers) {
    w.join();
  } 
  _workers.clear();
}

template <typename C, std::enable_if_t<is_static_task_v<C>, void>*>
TaskHandle TaroV3::emplace(C&& c) {
  auto t = std::make_unique<Task>(_tasks.size(), std::in_place_type_t<Task::StaticTask>{}, std::forward<C>(c));
  _tasks.emplace_back(std::move(t));
  return TaskHandle{_tasks.back().get()};
}

template <typename C, std::enable_if_t<is_coro_task_v<C>, void>*>
TaskHandle TaroV3::emplace(C&& c) {
  auto t = std::make_unique<Task>(_tasks.size(), std::in_place_type_t<Task::CoroTask>{}, std::forward<C>(c));
  std::get<Task::CoroTask>(t->_handle).coro._coro_handle.promise()._id = _tasks.size();
  _tasks.emplace_back(std::move(t));
  return TaskHandle{_tasks.back().get()};
}

void TaroV3::schedule() {

  _callbacks.resize(_tasks.size(), 0);

  std::vector<Task*> srcs;
  for(auto& t: _tasks) {
    if(t->_join_counter.load() == 0) {
      srcs.push_back(t.get());
    }
  }

  for(auto tp: srcs) {
    _enqueue(tp);
  }
}


bool TaroV3::is_DAG() {
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

bool TaroV3::_is_DAG(
  Task* tp,
  std::vector<bool>& visited,
  std::vector<bool>& in_recursion
) {
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

void TaroV3::_invoke_static_task(Task* tp) {
  std::get_if<Task::StaticTask>(&tp->_handle)->work();
  for(auto succp: tp->_succs) {
    if(succp->_join_counter.fetch_sub(1) == 1) {
      _enqueue(succp);
    }
  }

  if(_finished.fetch_add(1) + 1 == _tasks.size()) {
    {
      std::scoped_lock lock(_mtx);
      _stop = true;
      _cv.notify_all();
    }
  }
}

void TaroV3::_invoke_coro_task(Task* tp) {
  auto* coro = std::get_if<Task::CoroTask>(&tp->_handle);
  if(!coro->done()) {
    coro->resume();
    if(_callbacks[coro->coro._coro_handle.promise()._id] == 0) {
      _enqueue(tp);
    }
  }
  else {
    for(auto succp: tp->_succs) {
      if(succp->_join_counter.fetch_sub(1) == 1) {
        _enqueue(succp);
      }
    }

    if(_finished.fetch_add(1) + 1 == _tasks.size()) {
      {
        std::scoped_lock lock(_mtx);
        _stop = true;
        _cv.notify_all();
      }
    }
  }
}

void TaroV3::_process(Task* tp) {

  switch(tp->_handle.index()) {
    case Task::STATICTASK: {
      _invoke_static_task(tp);
    }
    break;

    case Task::COROTASK: {
      _invoke_coro_task(tp);
    }
    break;
  }
}

void TaroV3::_enqueue(Task* tp) {
  {
    std::unique_lock<std::mutex> lock(_mtx);
    _queue.push(tp);
  }
  _cv.notify_one();
}

} // end of namespace taro ==============================================
