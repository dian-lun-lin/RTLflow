#pragma once

#include "coro.hpp"

namespace taro { // begin of namespace taro ===================================

// ==========================================================================
//
// Task Traits
//
// ==========================================================================

class Worker;

// ==========================================================================
//
// Decalartion of class Task
//
// Task stores a coroutine and handles dependencies of the task graph
// ==========================================================================

template <typename C>
class Task {

  friend class TaroCBV4;
  friend class TaskHandle;


  public:

    Task(size_t id, C&& c);

    void resume() {
      coro._resume();
    }

    bool done() {
      return coro._done();
    }

  private:

    void _precede(Task* task);
    std::vector<Task*> _succs;
    std::vector<Task*> _preds;
    std::atomic<int> _join_counter{0};

    size_t _id;
    std::function<taro::Coro()> work;
    Coro coro;
};

// ==========================================================================
//
// Definition of class Task
//
// ==========================================================================

Task::Task(size_t id, C&& c):_id{id}, work{std::forward<C>(c)}, coro{work()} {
}

void Task::_precede(Task* tp) {
  _succs.push_back(tp);
  tp->_preds.push_back(this);
  tp->_join_counter.fetch_add(1, std::memory_order_relaxed);
}

// ==========================================================================
//
// Decalartion of class TaskHandle
//
// ==========================================================================

class TaskHandle {

  public:

    TaskHandle();
    explicit TaskHandle(Task* tp);
    TaskHandle(TaskHandle&&) = default;
    TaskHandle(const TaskHandle&) = default;
    TaskHandle& operator = (const TaskHandle&) = default;
    TaskHandle& operator = (TaskHandle&&) = default;
    ~TaskHandle() = default;    

    TaskHandle& precede(TaskHandle ch);

    TaskHandle& succeed(TaskHandle ch);

  private:

    Task* _tp;
};

// ==========================================================================
//
// Definition of class TaskHandle
//
// ==========================================================================
//
TaskHandle::TaskHandle(): _tp{nullptr} {
}

TaskHandle::TaskHandle(Task* tp): _tp{tp} {
}

TaskHandle& TaskHandle::precede(TaskHandle ch) {
  _tp->_precede(ch._tp);
  return *this;
}

TaskHandle& TaskHandle::succeed(TaskHandle ch) {
  ch._tp->_precede(_tp);
  return *this;
}


} // end of namespace taro ==============================================
