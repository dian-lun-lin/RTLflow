#pragma once
#include "wsq.hpp"

namespace taro { // begin of namespace taro ===================================

// ==========================================================================
//
// Declaration/Definition of class Worker
//
// ==========================================================================

class Worker {

  friend class TaroCBV4;

  friend void CUDART_CB _cuda_stream_callback_v4(void* void_args);

  public:

    size_t get_id() const { return _id; }

  private:

    WorkStealingQueue<Task*> _que;
    
    std::jthread* _thread;
    size_t _id;
    std::mutex _mtx;

    enum STAT: unsigned {
      SLEEP,
      BUSY,
      SIGNALED // 
    };
    std::atomic<unsigned> _status{STAT::SLEEP};

};

} // end of namespace taro ==============================================
