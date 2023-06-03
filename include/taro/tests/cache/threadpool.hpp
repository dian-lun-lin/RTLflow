#include <iostream>
#include <chrono>
#include <vector>
#include <future>
#include <thread>
#include <queue>
#include <functional>
#include <mutex>
#include <condition_variable>
#include <type_traits>

template <typename T>
struct MoC {

  MoC(T&& rhs) : object(std::move(rhs)) {}
  MoC(const MoC& other) : object(std::move(other.object)) {}

  T& get() { return object; }

  mutable T object;
};

class Worker {

  friend class ThreadPool;

  size_t _id;
  std::mutex _mtx;
  std::condition_variable _cv;
  std::queue< std::function<void()> > _que;
  bool _stop{false};
};

// ----------------------------------------------------------------------------
// Class definition for ThreadPool
// ----------------------------------------------------------------------------

class ThreadPool {

  public:
    
    // constructor tasks a unsigned integer representing the number of
    // workers you need
    ThreadPool(size_t N): _workers{N} {


      for(size_t i=0; i<N; i++) {
        Worker& worker = _workers[i];
        worker._id = i;

        _threads.emplace_back([this, &worker](){
          // keep doing my job until the main thread sends a stop signal
          while(!worker._stop) {
            std::function<void()> task;
            // my job is to iteratively grab a task from the queue
            {
              std::unique_lock lock(worker._mtx);
              while(worker._que.empty() && !worker._stop) {
                worker._cv.wait(lock);
              }
              if(!worker._que.empty()) {
                task = worker._que.front();
                worker._que.pop();
              }
            }
            // and run the task...
            if(task) {
              task();
            }
          }
        });
      }
    }

    // destructor will release all threading resources by joining all of them
    ~ThreadPool() {
      // I need to join the threads to release their resources
      for(auto& t : _threads) {
        t.join();
      }
    }

    // shutdown the threadpool
    void shutdown() {
      for(auto& worker: _workers) {
        std::scoped_lock lock(worker._mtx);
        worker._stop = true;
        worker._cv.notify_one();
      }
    }

    // insert a task "callable object" into the threadpool
    template <typename C>
    auto insert(C&& task) {

      std::promise<void> promise;
      auto fu = promise.get_future();

      size_t id = _cnt++ % _workers.size();
      Worker& worker = _workers[id];

      {
        std::scoped_lock lock(worker._mtx);
        worker._que.push(
          [moc=MoC{std::move(promise)}, task=std::forward<C>(task)] () mutable {
            task();
            moc.object.set_value();
          }
        );
      }

      worker._cv.notify_one();
      return fu;
    }
    

  private:

    std::vector<Worker> _workers;
    std::vector<std::thread> _threads;
    size_t _cnt{0};
};
