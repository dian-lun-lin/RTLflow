#pragma once

namespace taro { // begin of namespace taro ===================================

class TaroCBTaskflowRuntime;
  
// directly use taskflow to schedule
//
// "stream-stealing" approach
// As suggested by CUDA doc, we use cudaLaunchHostFunc rather than cudaStreamAddCallback
// cudaStreamAddcallback
// cudaStream is handled by Taro
// if none of existing streams is available, enqueue the task back to master que
//
// ==========================================================================
//
// Declaration of class TaroCBTaskflowRuntime
//
// ==========================================================================
//


class TaroCBTaskflowRuntime {

  friend void CUDART_CB _cuda_stream_callback_taskflow_runtime(void* void_args);

  struct cudaCallbackData {
    TaroCBTaskflowRuntime* taro{nullptr};
    Coro::promise_type* prom{nullptr};
    cudaStream_t stream{nullptr};
  };


  public:

    // num_streams here does not mean anything
    // this arg is for ease of benchmarking
    TaroCBTaskflowRuntime(size_t num_threads, size_t num_streams = 0);

    ~TaroCBTaskflowRuntime();

    template <typename C, std::enable_if_t<is_static_task_v<C>, void>* = nullptr>
    TaskHandle emplace(C&&);

    template <typename C, std::enable_if_t<is_coro_task_v<C>, void>* = nullptr>
    TaskHandle emplace(C&&);

    auto suspend();

    template <typename C, std::enable_if_t<is_kernel_v<C>, void>* = nullptr>
    auto cuda_suspend(C&&);

    void schedule();

    bool is_DAG() const;

    void wait();


  private:

    void _process(tf::Runtime& rt, const std::vector<Task*>& tps);

    void _construct_taskflow_task();

    void _construct_coro_task(Task* tp);
    
    void _construct_static_task(Task* tp);

    void _enqueue(Task* tp);
    void _enqueue(const std::vector<Task*>& tps);

    Worker* _this_worker();

    bool _is_DAG(
      Task* tp,
      std::vector<bool>& visited,
      std::vector<bool>& in_recursion
    ) const;

    std::vector<std::unique_ptr<Task>> _tasks;

    std::vector<cudaStream_t> _streams;
    std::vector<size_t> _in_stream_tasks;

    // TODO: may change to priority queue
    std::queue<Task*> _que;

    std::mutex _qmtx;
    std::mutex _stream_mtx;
    std::mutex _kernel_mtx;
    std::condition_variable _cv;

    std::atomic<bool> _stop{false};
    std::atomic<size_t> _finished{0};
    std::atomic<size_t> _cbcnt{0};

    tf::Taskflow _taskflow;
    tf::Executor _executor;
    std::vector<tf::Task> _tftasks;
};

// ==========================================================================
//
// callback
//
// ==========================================================================

// cuda callback
void CUDART_CB _cuda_stream_callback_taskflow_runtime(void* void_args) {
  auto* data = (TaroCBTaskflowRuntime::cudaCallbackData*) void_args;

  auto* taro = data->taro;
  auto* prom = data->prom;
  // after enqueue, cf may finish the task and be destructed
  // in that case _cv is destructed before we call notify in the callback
  // use cbcnt to check if number of callback is zero
  taro->_enqueue(taro->_tasks[prom->_id].get());
  // std::scope_lock lock(taro->_qmtx); // TODO: do we need this lock?
  taro->_cbcnt.fetch_sub(1);
  taro->_cv.notify_one();
}

// ==========================================================================
//
// Definition of class TaroCBTaskflowRuntime
//
// ==========================================================================

TaroCBTaskflowRuntime::TaroCBTaskflowRuntime(size_t num_threads, size_t num_streams): 
  _executor{num_threads},
  _streams{num_streams},
  _in_stream_tasks{num_streams, 0}
{
  for(auto& stream: _streams) {
    cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking);
  }
}

TaroCBTaskflowRuntime::~TaroCBTaskflowRuntime() {
  for(auto& stream: _streams) {
    checkCudaError(cudaStreamDestroy(stream));
  }
}

void TaroCBTaskflowRuntime::wait() {
}

void TaroCBTaskflowRuntime::_construct_taskflow_task() {

  _tftasks.resize(_tasks.size());

  for(auto& t: _tasks) {
    Task* tp = t.get();
    switch(tp->_handle.index()) {
      case Task::STATICTASK: {
        _construct_static_task(tp);
      }
      break;

      case Task::COROTASK: {
        _construct_coro_task(tp);
      }
      break;

      default:
        assert(false);
    }
  }
}

void TaroCBTaskflowRuntime::_construct_static_task(Task* tp) {

  _tftasks[tp->_id]  = _taskflow.emplace([tp, this]() {
    std::get_if<Task::StaticTask>(&tp->_handle)->work();
    for(auto succp: tp->_succs) {
      if(succp->_join_counter.fetch_sub(1) == 1) {
        _enqueue(succp);
        _cv.notify_one();
      }
      if(_finished.fetch_add(1) + 1 == _tasks.size()) {
        std::scoped_lock lock(_qmtx);
        _stop = true;
        _cv.notify_one();
      }
    }
  });

}
void TaroCBTaskflowRuntime::_construct_coro_task(Task* tp) {

  _tftasks[tp->_id] = _taskflow.emplace([tp, this]() {
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
          _enqueue(succp);
          _cv.notify_one();
        }
      }
      if(_finished.fetch_add(1) + 1 == _tasks.size()) {
        std::scoped_lock lock(_qmtx);
        _stop = true;
        _cv.notify_one();
      }
    }
  });
}

void TaroCBTaskflowRuntime::schedule() {

  _construct_taskflow_task();

  std::vector<Task*> srcs;
  for(auto& t: _tasks) {
    if(t->_join_counter.load() == 0) {
      srcs.push_back(t.get());
    }
  }
  _enqueue(srcs);

  tf::Task start_t = _taskflow.emplace([]() {
    return 0;
  });

  tf::Task runtime_t = _taskflow.emplace([this](tf::Runtime& rt) {
    std::cerr << "runtime\n";
    while(true) {
      std::vector<Task*> tps;
      {
        std::unique_lock lock(_qmtx);
        _cv.wait(lock, [this]{ return (_stop && _cbcnt == 0) || !_que.empty(); });
        if(_stop && _cbcnt == 0) {
          return;
        }
        while(!_que.empty()) {
          tps.emplace_back(_que.front());
          _que.pop();
        }
      }

      if(!tps.empty()) {
        _process(rt, tps);
      }
    }
    std::cerr << "finish\n";
  });

  start_t.precede(runtime_t);

  for(auto& t: _tftasks) {
    t.succeed(start_t);
  }

  _executor.run(_taskflow).wait();
}

template <typename C, std::enable_if_t<is_kernel_v<C>, void>*>
auto TaroCBTaskflowRuntime::cuda_suspend(C&& c) {

  struct awaiter: std::suspend_always {
    std::function<void(cudaStream_t)> kernel;
    cudaCallbackData data;

    explicit awaiter(TaroCBTaskflowRuntime* taro, C&& c): kernel{std::forward<C>(c)} {
      data.taro = taro; 
    }
    void await_suspend(std::coroutine_handle<Coro::promise_type> coro_handle) {

      // set callback data
      data.stream = _get_stream();
      data.prom = &(coro_handle.promise());

      // enqueue the kernel to the stream
      data.taro->_cbcnt.fetch_add(1);
      {
        std::scoped_lock lock(data.taro->_kernel_mtx);
        kernel(data.stream);
        cudaLaunchHostFunc(data.stream, _cuda_stream_callback_taskflow_runtime, (void*)&data);
      }
    }

    private:

      cudaStream_t _get_stream() {

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

        return data.taro->_streams[stream_id];
      }
  };

  return awaiter{this, std::forward<C>(c)};
}

auto TaroCBTaskflowRuntime::suspend() {
  struct awaiter: std::suspend_always {
    TaroCBTaskflowRuntime* _taro;
    explicit awaiter(TaroCBTaskflowRuntime* taro) noexcept : _taro{taro} {}
    void await_suspend(std::coroutine_handle<Coro::promise_type> coro_handle) const noexcept {
      auto id = coro_handle.promise()._id;
      _taro->_enqueue(_taro->_tasks[id].get());
      _taro->_cv.notify_one();
    }
  };

  return awaiter{this};
}

template <typename C, std::enable_if_t<is_static_task_v<C>, void>*>
TaskHandle TaroCBTaskflowRuntime::emplace(C&& c) {
  auto t = std::make_unique<Task>(_tasks.size(), std::in_place_type_t<Task::StaticTask>{}, std::forward<C>(c));
  _tasks.emplace_back(std::move(t));
  return TaskHandle{_tasks.back().get()};
}

template <typename C, std::enable_if_t<is_coro_task_v<C>, void>*>
TaskHandle TaroCBTaskflowRuntime::emplace(C&& c) {
  auto t = std::make_unique<Task>(_tasks.size(), std::in_place_type_t<Task::CoroTask>{}, std::forward<C>(c));
  std::get<Task::CoroTask>(t->_handle).coro._coro_handle.promise()._id = _tasks.size();
  _tasks.emplace_back(std::move(t));
  return TaskHandle{_tasks.back().get()};
}

void TaroCBTaskflowRuntime::_enqueue(Task* tp) {
  {
    std::scoped_lock lock(_qmtx);
    _que.push(tp);
  }
}

void TaroCBTaskflowRuntime::_enqueue(const std::vector<Task*>& tps) {
  {
    std::scoped_lock lock(_qmtx);
    for(auto* tp: tps) {
      _que.push(tp);
    }
  }
}

void TaroCBTaskflowRuntime::_process(tf::Runtime& rt, const std::vector<Task*>& tps) {
  for(auto& tp: tps) {
    std::cerr << "scheduling...\n";
    rt.schedule(_tftasks[tp->_id]);
    std::cerr << "finish...\n";
  }
}

bool TaroCBTaskflowRuntime::is_DAG() const {
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

bool TaroCBTaskflowRuntime::_is_DAG(
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
