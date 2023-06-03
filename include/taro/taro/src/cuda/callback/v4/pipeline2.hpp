
#pragma once
#include "taro_callback_v4.hpp"

namespace taro { // begin of namespace taro ===================================


class Pipeline {

  struct Token {
    std::atomic<size_t> cnt{0};
  };

  public:

    Pipeline(TaroCBV4& taro, size_t num_pipes, size_t num_lines, size_t num_tokens);

    template <typename C, std::enable_if_t<is_coro_task_v<C>, void>* = nullptr>
    void set_pipe(size_t pid, C&& c);

    size_t num_tokens() const;
    size_t token(size_t pid) const;
    size_t fetch_token(size_t pid);

    void stop();
    auto step();

  private:

    TaroCBV4& _taro;
    size_t _num_pipes;
    size_t _num_lines;
    size_t _num_tokens;
    std::vector<std::vector<TaskHandle>> _pipes;
    std::vector<size_t> _ids;
    std::vector<Token> _tokens;
    std::vector<bool> _setted;

    std::unordered_map<size_t, std::pair<size_t, size_t>> _task_pipe;
};

Pipeline::Pipeline(TaroCBV4& taro, size_t num_pipes, size_t num_lines, size_t num_tokens): 
  _taro{taro}, _num_pipes{num_pipes}, _num_lines{num_lines}, _num_tokens{num_tokens},
  _tokens(num_pipes), _pipes(num_pipes), _setted(num_pipes, false)
{
  for(auto& p: _pipes) {
    p.resize(_num_lines);
  }
}

size_t Pipeline::token(size_t pid) const {
  return _tokens[pid].cnt;
}

size_t Pipeline::num_tokens() const {
  return _num_tokens;
}
size_t Pipeline::fetch_token(size_t pid) {
  return _tokens[pid].cnt.fetch_add(1);
}

void Pipeline::stop() {
  Worker* worker   = _taro._this_worker();
  auto& pipe_line = _task_pipe[worker->_work_on_task_id];
  if(pipe_line.first + 1 != _num_pipes) {
    auto* next = _pipes[pipe_line.first + 1][pipe_line.second]._tp;
    _taro._enqueue(*worker, next, TaskPriority::LOW);
  }
}

auto Pipeline::step() {
  struct awaiter: std::suspend_always {
    Pipeline& _pipeline;
    explicit awaiter(Pipeline& pipeline): _pipeline{pipeline} {}
    
    std::coroutine_handle<> await_suspend(std::coroutine_handle<> cur_handle) {
      // resume will destroy the awaiter
      // before enqueue(i.e. resume), we need to get all data first
      auto& pl         = _pipeline;
      Worker* worker   = pl._taro._this_worker();

      auto& pipe_line = pl._task_pipe[worker->_work_on_task_id];
      Task* next;
      if(pipe_line.first + 1 == pl._num_pipes) {
        next = pl._pipes[0][pipe_line.second]._tp;
        pl._taro._enqueue(*worker, next, TaskPriority::LOW);
        return std::noop_coroutine();
      }
      next = pl._pipes[(pipe_line.first + 1) % pl._num_pipes][pipe_line.second]._tp;
      auto next_handle = std::get_if<Task::CoroTask>(&next->_handle)->coro._coro_handle;
      worker->_work_on_task_id = next->_id;
      return next_handle;
    }
  };

  return awaiter{*this};
}

template <typename C, std::enable_if_t<is_coro_task_v<C>, void>*>
void Pipeline::set_pipe(size_t pid, C&& c) {
  assert(!_setted[pid]);
  for(size_t l = 0; l < _num_lines; ++l) {
    _pipes[pid][l] = _taro.emplace(std::forward<C>(c));
    _task_pipe.insert({_pipes[pid][l].id(), {pid, l}});
    if(pid != 0) {
      _pipes[pid][l]._tp->_wait_first = true;
    }
  }

  _setted[pid] = true;
}

Pipeline pipeline(TaroCBV4& taro, size_t num_pipes, size_t num_lines, size_t num_tokens) {
  return Pipeline{taro, num_pipes, num_lines, num_tokens};
}

} // end of namespace taro ==============================================
