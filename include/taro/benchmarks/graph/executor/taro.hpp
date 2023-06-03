#pragma once

#include "executor.hpp"

template <typename TARO>
class TaroGraphExecutor: public GraphExecutor {

  public:
    TaroGraphExecutor(Graph& graph, int dev_id, size_t num_threads, size_t num_streams = 0);

    std::pair<double, double> run_loop(int cpu_time, int gpu_time) final;
    std::pair<double, double> run_data(int data_size) final;

  private:

    TARO _taro;
};

template <typename TARO>
TaroGraphExecutor<TARO>::TaroGraphExecutor(Graph& graph, int dev_id, size_t num_threads, size_t num_streams):
  GraphExecutor{graph, dev_id, num_threads, num_streams}, _taro{num_threads, num_streams} {
}

template <typename TARO>
std::pair<double, double> TaroGraphExecutor<TARO>::run_loop(int cpu_time, int gpu_time) {
  auto constr_tic = std::chrono::steady_clock::now();

  size_t cnt{0};

  std::vector<std::vector<taro::TaskHandle>> tasks;
  tasks.resize(_g.get_graph().size());
  for(size_t l = 0; l < _g.get_graph().size(); ++l) {
    tasks[l].resize((_g.get_graph())[l].size());
    for(size_t i = 0; i < (_g.get_graph())[l].size(); ++i) {

      // GPU computing
      tasks[l][i] = _taro.emplace([this, cpu_time, gpu_time]() mutable -> taro::Coro {
        cpu_loop(cpu_time);
        co_await _taro.cuda_suspend([gpu_time](cudaStream_t st) mutable {
          cuda_loop<<<8, 256, 0, st>>>(gpu_time);
        });
      });
        
      ++cnt;
    }
  }

  //connection
  for(size_t l = 0; l < _g.get_graph().size() - 1; ++l) {
    for(size_t i = 0; i < (_g.get_graph())[l].size(); ++i) {
      for(auto&& out_node: _g.at(l, i).out_nodes) {
        tasks[l][i].precede(tasks[l + 1][out_node]);
        //tasks[l][i][3].precede(tasks[l + 1][out_node][1]);
        //tasks[l][i][3].precede(tasks[l + 1][out_node][2]);
      }
    }
  }
  auto constr_toc = std::chrono::steady_clock::now();

  auto exec_tic = std::chrono::steady_clock::now();

  _taro.schedule();
  _taro.wait();

  auto exec_toc = std::chrono::steady_clock::now();

  //assert(_g.traversed());

  auto constr_dur = std::chrono::duration_cast<std::chrono::milliseconds>(constr_toc - constr_tic).count();

  auto exec_dur = std::chrono::duration_cast<std::chrono::milliseconds>(exec_toc - exec_tic).count();

  return {constr_dur, exec_dur};
}

template <typename TARO>
std::pair<double, double> TaroGraphExecutor<TARO>::run_data(int data_size) {
  auto constr_tic = std::chrono::steady_clock::now();

  size_t cnt{0};

  std::vector<std::vector<int>> cdata(_g.num_nodes());
  for(auto& d: cdata) {
    d.resize(data_size, 0);
  }
  std::vector<int*> gdata(_g.num_nodes());
  std::vector<std::vector<taro::TaskHandle>> tasks;
  tasks.resize(_g.get_graph().size());

  for(size_t l = 0; l < _g.get_graph().size(); ++l) {
    tasks[l].resize((_g.get_graph())[l].size());
    for(size_t i = 0; i < (_g.get_graph())[l].size(); ++i) {

      // GPU computing
      tasks[l][i] = _taro.emplace([this, &cdata, &gdata, data_size, cnt]() mutable -> taro::Coro {
        int k = 0;
        while(k++ < 1000) {
          for(auto& d: cdata[cnt]) { 
            ++d; 
          }
          co_await _taro.cuda_suspend([&cdata, &gdata, data_size, cnt](cudaStream_t st) mutable {
            cudaMallocAsync(&gdata[cnt], sizeof(int) * data_size, st);
            cudaMemcpyAsync(gdata[cnt], cdata[cnt].data(),  sizeof(int) * data_size, cudaMemcpyHostToDevice, st);
            cuda_assign<<<16, 512, 0, st>>>(gdata[cnt], data_size);
            cudaMemcpyAsync(gdata[cnt], cdata[cnt].data(), sizeof(int) * data_size, cudaMemcpyDeviceToHost, st);
            cudaFreeAsync(gdata[cnt], st);
          });
        }
      });
        
      ++cnt;
    }
  }

  //connection
  for(size_t l = 0; l < _g.get_graph().size() - 1; ++l) {
    for(size_t i = 0; i < (_g.get_graph())[l].size(); ++i) {
      for(auto&& out_node: _g.at(l, i).out_nodes) {
        tasks[l][i].precede(tasks[l + 1][out_node]);
        //tasks[l][i][3].precede(tasks[l + 1][out_node][1]);
        //tasks[l][i][3].precede(tasks[l + 1][out_node][2]);
      }
    }
  }
  auto constr_toc = std::chrono::steady_clock::now();

  auto exec_tic = std::chrono::steady_clock::now();

  _taro.schedule();
  _taro.wait();

  auto exec_toc = std::chrono::steady_clock::now();

  //assert(_g.traversed());

  auto constr_dur = std::chrono::duration_cast<std::chrono::milliseconds>(constr_toc - constr_tic).count();

  auto exec_dur = std::chrono::duration_cast<std::chrono::milliseconds>(exec_toc - exec_tic).count();

  return {constr_dur, exec_dur};
}

