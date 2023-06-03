#include <chrono>
#include <condition_variable>
#include <deque>
#include <iostream>
#include <mutex>
#include <string>
#include <thread>

#include <boost/assert.hpp>
#include <boost/fiber/all.hpp>
#include <boost/fiber/detail/thread_barrier.hpp>
#include <boost/fiber/cuda/waitfor.hpp>

__global__
void kernel( int size, int * a, int * b, int * c) {
    int idx = threadIdx.x + blockIdx.x * blockDim.x;
    if ( idx < size) {
        int idx1 = (idx + 1) % 256;
        int idx2 = (idx + 2) % 256;
        float as = (a[idx] + a[idx1] + a[idx2]) / 3.0f;
        float bs = (b[idx] + b[idx1] + b[idx2]) / 3.0f;
        c[idx] = (as + bs) / 2;
    }
}

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

    FiberTaskScheduler(size_t num_threads);
  
    void shutdown();

    template <typename C>
    FiberTaskHandle emplace(C&& c);

    void schedule();

  private:

  std::vector<FiberTask*> _tasks;
  std::atomic<size_t> _finished{0};
  std::vector<std::thread> _threads;
  size_t _num_threads;
  boost::fibers::condition_variable_any _cv;
  std::atomic<bool> _stop{false};
  std::mutex _mtx;
  
};

//static std::size_t fiber_count{ 1 };
//static std::mutex mtx_count{};
//static boost::fibers::condition_variable_any cnd_count{};
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
    c();

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

FiberTaskScheduler::FiberTaskScheduler(size_t num_threads): _num_threads{num_threads} {
  _threads.reserve(num_threads);
}

template <typename C>
FiberTaskHandle FiberTaskScheduler::emplace(C&& c) {
  _tasks.emplace_back(new FiberTask(*this, std::forward<C>(c)));
  _tasks.back()->_id = _tasks.size();
  return FiberTaskHandle{_tasks.back()};
}

void FiberTaskScheduler::schedule() {

  for(auto task: _tasks) {
    if(task->_join_counter.load(std::memory_order_relaxed) == 0) {
      boost::fibers::fiber([this, task](){
        task->_work();
      }).detach();
      std::cerr << "source node\n";
    }
  }

  std::cerr << "number of threads: " << _num_threads << "\n";

  for(size_t i = 0; i < _num_threads - 1; ++i) {
    _threads.emplace_back([this](){
      boost::fibers::use_scheduling_algorithm<boost::fibers::algo::work_stealing>(_num_threads); 

      std::unique_lock<std::mutex> lock(_mtx);
      _cv.wait(lock, [this](){ return _stop.load(); } ); 
      BOOST_ASSERT(_stop.load());
    });
  }

  boost::fibers::use_scheduling_algorithm< boost::fibers::algo::work_stealing >(_num_threads);
}

void FiberTaskScheduler::shutdown() {
  {
    std::unique_lock<std::mutex> lock(_mtx);
    _cv.wait(lock, [this](){ return _stop.load(); } ); 
  }

  for(auto& t: _threads) {
    t.join();
  }
}


/*****************************************************************************
 *   main()
 *****************************************************************************/
int main(int argc, char *argv[]) {

  FiberTaskScheduler ft_sched(4);

  auto ft_a = ft_sched.emplace([]() {
    std::cerr << "task a\n";
    std::cerr << "thread id: " << std::this_thread::get_id() << "\n";
  });

  auto ft_b = ft_sched.emplace([]() {
    std::cerr << "task b\n";
    std::cerr << "thread id: " << std::this_thread::get_id() << "\n";
     cudaStream_t stream;
    cudaStreamCreate( & stream);
    int size = 1024 * 1024;
    int full_size = 20 * size;
    int * host_a, * host_b, * host_c;
    cudaHostAlloc( & host_a, full_size * sizeof( int), cudaHostAllocDefault);
    cudaHostAlloc( & host_b, full_size * sizeof( int), cudaHostAllocDefault);
    cudaHostAlloc( & host_c, full_size * sizeof( int), cudaHostAllocDefault);
    int * dev_a, * dev_b, * dev_c;
    cudaMalloc( & dev_a, size * sizeof( int) );
    cudaMalloc( & dev_b, size * sizeof( int) );
    cudaMalloc( & dev_c, size * sizeof( int) );
    std::minstd_rand generator;
    std::uniform_int_distribution<> distribution(1, 6);
    for ( int i = 0; i < full_size; ++i) {
        host_a[i] = distribution( generator);
        host_b[i] = distribution( generator);
    }
    for ( int i = 0; i < full_size; i += size) {
        cudaMemcpyAsync( dev_a, host_a + i, size * sizeof( int), cudaMemcpyHostToDevice, stream);
        cudaMemcpyAsync( dev_b, host_b + i, size * sizeof( int), cudaMemcpyHostToDevice, stream);
        kernel<<< size / 256, 256, 0, stream >>>( size, dev_a, dev_b, dev_c);
        cudaMemcpyAsync( host_c + i, dev_c, size * sizeof( int), cudaMemcpyDeviceToHost, stream);
    }
    std::cerr << "f1 suspend" << std::endl;
    auto result = boost::fibers::cuda::waitfor_all( stream); // suspend fiber till CUDA stream has finished
    BOOST_ASSERT( stream == std::get< 0 >( result) );
    BOOST_ASSERT( cudaSuccess == std::get< 1 >( result) );
    std::cerr << "f1: GPU computation finished" << std::endl;
    cudaFreeHost( host_a);
    cudaFreeHost( host_b);
    cudaFreeHost( host_c);
    cudaFree( dev_a);
    cudaFree( dev_b);
    cudaFree( dev_c);
    cudaStreamDestroy( stream);
  });

  auto ft_c = ft_sched.emplace([]() {
    std::cerr << "task c\n";
    std::cerr << "thread id: " << std::this_thread::get_id() << "\n";
  });

  auto ft_d = ft_sched.emplace([]() {
    std::cerr << "task d\n";
    std::cerr << "thread id: " << std::this_thread::get_id() << "\n";
  });

  ft_a.precede(ft_b);
  ft_a.precede(ft_c);
  ft_b.precede(ft_d);
  ft_c.precede(ft_d);

  ft_sched.schedule();
  ft_sched.shutdown();

  std::cout << "done.\n";
}
