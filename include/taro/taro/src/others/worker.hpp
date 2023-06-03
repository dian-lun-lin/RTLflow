#pragma once

namespace taro { // begin of namespace taro ===================================


class cudaWorker {
  friend class Worker;
  friend class TaroPV1;
  friend class TaroPV2;

  cudaStream_t stream;
  Task* cur_task;
  // -1: busy
  // 0: (busy -> available) => need to commit
  // 1:  available
  int status{1};

  public:

    cudaWorker(): cur_task{nullptr} {
      //cudaSetDeviceFlags(cudaDeviceScheduleBlockingSync);
      cudaStreamCreateWithFlags(&stream, cudaStreamNonBlocking);
    }

    ~cudaWorker() {
      cudaStreamSynchronize(stream);
      cudaStreamDestroy(stream);
    }

    //cudaWorker(const cudaWorker&) = delete;
    //cudaWorker(cudaWorker&&) = delete;
    //cudaWorker& operator = (cudaWorker&&) = delete;
    //cudaWorker& operator = (const cudaWorker&) = delete;
};

// ==========================================================================
//
// Declaration/Definition of class Worker
//
// ==========================================================================

class Worker {

  friend class TaroCBV1;
  friend class TaroCBV2;
  friend class TaroCBV3;
  friend class TaroPV1;
  friend class TaroPV2;

  friend void CUDART_CB _cuda_stream_callback_v3(void* void_args);

  // for TaroCBV3
  enum class Stage: unsigned {
    ACTIVE,
    SLEEP
  };

  public:

    size_t get_id() const { return _id; }

  private:

    WorkStealingQueue<Task*> _que;
    
    // for TaroPV1
    std::vector<cudaWorker> _gws;

    // for TaroCBV2
    WorkStealingQueue<cudaStream_t> _sque;

    Notifier::Waiter* _waiter;
    std::thread* _thread;
    size_t _id;
    size_t _vtm;
    std::default_random_engine _rdgen{std::random_device{}()};

    // for TaroCBV3
    Stage _stage;
    std::mutex _mtx;
    std::binary_semaphore _wait_task{0};

    // for TaroCBV3
    // this function updates stage in the following order:
    // EXPLOIT -> EXPLORE -> SLEEP
    //void _update_stage() { 
      //std::scoped_lock lock(_mtx);
      //switch(worker->_stage) {
        //case Worker::Stage::EXPLOIT_HIGH:
          //_stage = Worker::Stage::EXPLOIT_LOW; 
          //break;
        //case Worker::Stage::EXPLOIT_LOW:
          //_stage = Worker::Stage::EXPLORE; 
          //break;
        //case Worker::Stage::EXPLORE:
          //_stage = Worker::Stage::SLEEP; 
          //break;
        //case Worker::Stage::SLEEP:
          //_stage = Worker::Stage::EXPLOIT_HIGH; 
          //break;
      //}
    //}

};

} // end of namespace taro ==============================================
