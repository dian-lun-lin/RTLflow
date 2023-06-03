#pragma once
#include "taro_callback_v4.hpp"

namespace taro { // begin of namespace taro ===================================

class Pipeline {

  public:

    Pipeline(TaroCBV4& taro, size_t num_pipes, size_t num_lines, size_t num_tokens);

    template <typename C, std::enable_if_t<is_coro_task_v<C>, void>* = nullptr>
    void set_pipe(size_t pid, C&& c);

    size_t num_tokens() const;
    size_t token();
    size_t fetch_token();
    void stop();
    auto step();

  private:

    TaroCBV4& _taro;
    size_t _num_pipes;
    size_t _num_lines;
    size_t _num_tokens;
    std::atomic<size_t> _cur_token{0};
    std::vector<std::vector<TaskHandle>> _pipes;
    std::vector<size_t> _ids;
    std::vector<size_t> _tokens;
    std::vector<bool> _setted;
    std::unordered_map<size_t, std::pair<size_t, size_t>> _task_pipe;
};

Pipeline::Pipeline(TaroCBV4& taro, size_t num_pipes, size_t num_lines, size_t num_tokens): 
  _taro{taro}, _num_pipes{num_pipes}, _num_lines{num_lines}, _num_tokens{num_tokens},
  _tokens(num_lines, 0), _pipes(num_pipes), _setted(num_pipes, false)
{
  for(auto& p: _pipes) {
    p.resize(_num_lines);
  }
}

size_t Pipeline::num_tokens() const {
  return _num_tokens;
}

size_t Pipeline::token() {
  Worker* worker   = _taro._this_worker();
  auto& pipe_line = _task_pipe[worker->_work_on_task_id];
  return _tokens[pipe_line.second];
}


size_t Pipeline::fetch_token() {
  Worker* worker   = _taro._this_worker();
  auto& pipe_line = _task_pipe[worker->_work_on_task_id];
  assert(pipe_line.first == 0);
  _tokens[pipe_line.second] = _cur_token.fetch_add(1);
  return _tokens[pipe_line.second];
}

void Pipeline::stop() {
  Worker* worker   = _taro._this_worker();
  auto& pipe_line = _task_pipe[worker->_work_on_task_id];
  assert(pipe_line.first == 0);
  for(size_t p = 0; p < _num_pipes; ++p) {
    _taro._done(_pipes[p][pipe_line.second].id());
  }
}

auto Pipeline::step() {
  struct awaiter: std::suspend_always {
    Pipeline& _pipeline;
    explicit awaiter(Pipeline& pipeline): _pipeline{pipeline} { }
    
    std::coroutine_handle<> await_suspend(std::coroutine_handle<>) {
      Worker* worker   = _pipeline._taro._this_worker();
      auto& pipe_line = _pipeline._task_pipe[worker->_work_on_task_id];
      auto* next = _pipeline._pipes[(pipe_line.first + 1) % _pipeline._num_pipes][pipe_line.second]._tp;
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
