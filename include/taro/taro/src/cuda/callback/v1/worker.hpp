#pragma once

namespace taro { // begin of namespace taro ===================================

// ==========================================================================
//
// Declaration/Definition of class Worker
//
// ==========================================================================

class Worker {

  friend class TaroCBV1;

  public:

    size_t get_id() const { return _id; }

  private:

    WorkStealingQueue<Task*> _que;
    Notifier::Waiter* _waiter;
    std::thread* _thread;
    size_t _id;
    size_t _vtm;
    std::default_random_engine _rdgen{std::random_device{}()};
};

} // end of namespace taro ==============================================
