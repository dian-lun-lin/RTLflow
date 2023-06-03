#pragma once

#include <chrono>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>
#include <latch>

#include <boost/assert.hpp>
#include <boost/fiber/all.hpp>
#include <boost/fiber/detail/thread_barrier.hpp>
#include <boost/fiber/cuda/waitfor.hpp>


class FiberTask;
class FiberTaskHandle;
class FiberTaskScheduler;

// ==========================================================================
//
// Decalartion of class FiberTask
//
// ==========================================================================

class FiberTask {

  
  friend class FiberTaskScheduler;
  friend class FiberTaskHandle;

  public:

    template <typename C>
    explicit FiberTask(FiberTaskScheduler& sched, C&& c);

    ~FiberTask() = default;

  private:

    void _precede(FiberTask* tp);
    std::atomic<int> _join_counter{0};

    std::vector<FiberTask*> _preds;
    std::vector<FiberTask*> _succs;
    size_t _id;
    std::function<void()> _work; 


    FiberTaskScheduler& _sched;
};

// ==========================================================================
//
// Declaration of class FiberTaskHandle
//
// ==========================================================================

class FiberTaskHandle {

  friend class FiberTaskScheduler;

  public:

    FiberTaskHandle();
    explicit FiberTaskHandle(FiberTask* tp);
    FiberTaskHandle(FiberTaskHandle&&) = default;
    FiberTaskHandle(const FiberTaskHandle&) = default;
    FiberTaskHandle& operator = (const FiberTaskHandle&) = default;
    FiberTaskHandle& operator = (FiberTaskHandle&&) = default;
    ~FiberTaskHandle() = default;    

    FiberTaskHandle& precede(FiberTaskHandle fth);

    FiberTaskHandle& succeed(FiberTaskHandle fth);

  private:

    FiberTask* _tp;
};

// ==========================================================================
//
// Declaration of class FiberTaskScheduler
//
// ==========================================================================

class FiberTaskScheduler {

  friend class FiberTask;

  public:

    FiberTaskScheduler(size_t num_threads, size_t num_streams);
    ~FiberTaskScheduler();
  
    template <typename C>
    FiberTaskHandle emplace(C&& c);

    void schedule();
    void wait();

  private:

  size_t _get_stream();
  std::vector<FiberTask*> _tasks;

  std::vector<cudaStream_t> _streams;
  boost::fibers::mutex _stream_mtx;
  size_t _num_streams;
  std::vector<size_t> _in_stream_tasks;

  std::atomic<size_t> _finished{0};
  std::vector<std::thread> _threads;
  size_t _num_threads;
  boost::fibers::condition_variable_any _cv;
  std::atomic<bool> _stop{false};
  std::mutex _mtx;
  
};

// ==========================================================================
//
// Definition of class FiberTask
//
// ==========================================================================

void FiberTask::_precede(FiberTask* tp) {
  _succs.push_back(tp);
  tp->_preds.push_back(this);
  tp->_join_counter.fetch_add(1, std::memory_order_relaxed);
}

template <typename C>
FiberTask::FiberTask(FiberTaskScheduler& sched, C&& c): _sched{sched} { 
  _work = [this, c=std::forward<C>(c)]() {
    size_t stream_id = _sched._get_stream();
    c(_sched._streams[stream_id]);

    {
      _sched._stream_mtx.lock();
      --_sched._in_stream_tasks[stream_id];
      _sched._stream_mtx.unlock();
    }

    for(auto succp: _succs) {
      if(succp->_join_counter.fetch_sub(1) == 1) {
        boost::fibers::fiber([succp](){
          succp->_work();
        }).detach();
      }
    }

    if(_sched._finished.fetch_add(1) + 1 == _sched._tasks.size()) {
      _sched._stop = true;
      _sched._cv.notify_all();
    }
  };
}

// ==========================================================================
//
// Definition of class FiberTaskHandle
//
// ==========================================================================

FiberTaskHandle::FiberTaskHandle(): _tp{nullptr} {
}

FiberTaskHandle::FiberTaskHandle(FiberTask* tp): _tp{tp} {
}

FiberTaskHandle& FiberTaskHandle::precede(FiberTaskHandle fth) {
  _tp->_precede(fth._tp);
  return *this;
}

FiberTaskHandle& FiberTaskHandle::succeed(FiberTaskHandle fth) {
  fth._tp->_precede(_tp);
  return *this;
}



// ==========================================================================
//
// Definition of class FiberTaskScheduler
//
// ==========================================================================

FiberTaskScheduler::FiberTaskScheduler(size_t num_threads, size_t num_streams): 
  _num_threads{num_threads}, _num_streams{num_streams} {
  _threads.reserve(_num_threads);
  _in_stream_tasks.resize(_num_streams, 0);
  _streams.resize(_num_streams);

  for(auto& st: _streams) {
    cudaStreamCreateWithFlags(&st, cudaStreamNonBlocking);
    //cudaStreamCreate(&st);
  }
}

FiberTaskScheduler::~FiberTaskScheduler() {
  for(auto task: _tasks) {
    delete task;
  }

  for(auto st: _streams) {
    cudaStreamDestroy(st);
  }
}

template <typename C>
FiberTaskHandle FiberTaskScheduler::emplace(C&& c) {
  _tasks.emplace_back(new FiberTask(*this, std::forward<C>(c)));
  _tasks.back()->_id = _tasks.size() - 1;
  return FiberTaskHandle{_tasks.back()};
}

void FiberTaskScheduler::schedule() {


  boost::fibers::barrier b{_num_threads};

  for(size_t i = 0; i < _num_threads - 1; ++i) {
    _threads.emplace_back([this, &b](){
      boost::fibers::use_scheduling_algorithm<boost::fibers::algo::work_stealing>(_num_threads); 
      b.wait();

      std::unique_lock<std::mutex> lock(_mtx);
      _cv.wait(lock, [this](){ return _stop.load(); } ); 
      BOOST_ASSERT(_stop.load());
    });
  }

  boost::fibers::use_scheduling_algorithm<boost::fibers::algo::work_stealing>(_num_threads);
  b.wait();
  for(auto task: _tasks) {
    if(task->_join_counter.load(std::memory_order_relaxed) == 0) {
      boost::fibers::fiber([this, task](){
        task->_work();
      }).detach();
    }
  }

}

void FiberTaskScheduler::wait() {
  {
    std::unique_lock<std::mutex> lock(_mtx);
    _cv.wait(lock, [this](){ return _stop.load(); } ); 
  }
  BOOST_ASSERT(_stop.load());

  for(auto& t: _threads) {
    t.join();
  }
}

size_t FiberTaskScheduler::_get_stream() {
  // choose the stream with the least number of enqueued tasks
  size_t stream_id;
  {
    _stream_mtx.lock();
    stream_id = std::distance(
      _in_stream_tasks.begin(), 
      std::max_element(_in_stream_tasks.begin(), _in_stream_tasks.end())
    );
    ++_in_stream_tasks[stream_id];
    _stream_mtx.unlock();
  }
  return stream_id;
}
