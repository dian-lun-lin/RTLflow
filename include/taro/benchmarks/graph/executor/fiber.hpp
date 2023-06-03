#pragma once

#include "executor.hpp"
#include "../../boost_fiber/fiber.hpp"

//#include <thrust/reduce.h>
//#include <thrust/sort.h>
//#include <thrust/functional.h>
//#include <thrust/random.h>
//

class FiberGraphExecutor: public GraphExecutor {

  public:
  
    FiberGraphExecutor(Graph& graph, int dev_id, size_t num_threads, size_t num_streams);

    std::pair<double, double> run_loop(int cpu_time, int gpu_time) final;
    std::pair<double, double> run_data(int data_size) final;

  private:
    
    FiberTaskScheduler _ft_sched;
};

FiberGraphExecutor::FiberGraphExecutor(Graph& graph, int dev_id, size_t num_threads, size_t num_streams): 
  GraphExecutor{graph, dev_id, num_threads, num_streams}, _ft_sched{num_threads, num_streams} {
}

std::pair<double, double> FiberGraphExecutor::run_loop(int cpu_time, int gpu_time) {
  auto constr_tic = std::chrono::steady_clock::now();


  size_t cnt{0};

  std::vector<std::vector<FiberTaskHandle>> tasks;
  tasks.resize(_g.get_graph().size());
  for(size_t l = 0; l < _g.get_graph().size(); ++l) {
    tasks[l].resize((_g.get_graph())[l].size());
    for(size_t i = 0; i < (_g.get_graph())[l].size(); ++i) {

      // GPU computing
      tasks[l][i] = _ft_sched.emplace([this, cpu_time, gpu_time](cudaStream_t st)  {
        cpu_loop(cpu_time);
        cuda_loop<<<8, 256, 0, st>>>(gpu_time);
        boost::fibers::cuda::waitfor_all(st);
      });
        
      ++cnt;
    }
  }

  //connection
  for(size_t l = 0; l < _g.get_graph().size() - 1; ++l) {
    for(size_t i = 0; i < (_g.get_graph())[l].size(); ++i) {
      for(auto&& out_node: _g.at(l, i).out_nodes) {
        tasks[l][i].precede(tasks[l + 1][out_node]);
      }
    }
  }
  auto constr_toc = std::chrono::steady_clock::now();

  auto exec_tic = std::chrono::steady_clock::now();

  _ft_sched.schedule();
  _ft_sched.wait();

  auto exec_toc = std::chrono::steady_clock::now();

  //assert(_g.traversed());

  auto constr_dur = std::chrono::duration_cast<std::chrono::milliseconds>(constr_toc - constr_tic).count();

  auto exec_dur = std::chrono::duration_cast<std::chrono::milliseconds>(exec_toc - exec_tic).count();

  return {constr_dur, exec_dur};
}

std::pair<double, double> FiberGraphExecutor::run_data(int data_size) {
  auto constr_tic = std::chrono::steady_clock::now();

  size_t cnt{0};

  std::vector<std::vector<int>> cdata(_g.num_nodes());
  for(auto& d: cdata) {
    d.resize(data_size, 0);
  }
  std::vector<int*> gdata(_g.num_nodes());
  std::vector<std::vector<FiberTaskHandle>> tasks;
  tasks.resize(_g.get_graph().size());

  for(size_t l = 0; l < _g.get_graph().size(); ++l) {
    tasks[l].resize((_g.get_graph())[l].size());
    for(size_t i = 0; i < (_g.get_graph())[l].size(); ++i) {

      // GPU computing
      tasks[l][i] = _ft_sched.emplace([this, &cdata, &gdata, cnt, data_size](cudaStream_t st)  {
        int k = 0;
        while(k++ < 1000) {
          for(auto& d: cdata[cnt]) { 
            ++d; 
          }
          cudaMallocAsync(&gdata[cnt], sizeof(int) * data_size, st);
          cudaMemcpyAsync(gdata[cnt], cdata[cnt].data(),  sizeof(int) * data_size, cudaMemcpyHostToDevice, st);
          cuda_assign<<<16, 512, 0, st>>>(gdata[cnt], data_size);
          cudaMemcpyAsync(gdata[cnt], cdata[cnt].data(), sizeof(int) * data_size, cudaMemcpyDeviceToHost, st);
          cudaFreeAsync(gdata[cnt], st);
          boost::fibers::cuda::waitfor_all(st);
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
      }
    }
  }
  auto constr_toc = std::chrono::steady_clock::now();

  auto exec_tic = std::chrono::steady_clock::now();

  _ft_sched.schedule();
  _ft_sched.wait();

  auto exec_toc = std::chrono::steady_clock::now();

  //assert(_g.traversed());

  auto constr_dur = std::chrono::duration_cast<std::chrono::milliseconds>(constr_toc - constr_tic).count();

  auto exec_dur = std::chrono::duration_cast<std::chrono::milliseconds>(exec_toc - exec_tic).count();

  return {constr_dur, exec_dur};
}

