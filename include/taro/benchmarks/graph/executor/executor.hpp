#pragma once

#include "../graph.hpp"
#include <utility>
//#include <thrust/reduce.h>
//#include <thrust/sort.h>
//#include <thrust/functional.h>
//#include <thrust/random.h>
//

class GraphExecutor {

  public:
  
    GraphExecutor(Graph& graph, int dev_id, size_t num_threads, size_t num_streams = 0);

    template <typename ...Args>
    std::pair<double, double> run(const std::string& benchmark, const std::vector<int>& benchmark_args);
    virtual std::pair<double, double> run_loop(int cpu_time, int gpu_time) = 0;
    virtual std::pair<double, double> run_data(int data_size) = 0;

  protected:
    
    int _dev_id;

    Graph& _g;

    size_t _num_threads;
    size_t _num_streams;
};

GraphExecutor::GraphExecutor(Graph& graph, int dev_id, size_t num_threads, size_t num_streams): 
  _g{graph}, _dev_id{dev_id}, _num_threads{num_threads}, _num_streams{num_streams} {
}

template <typename ...Args>
std::pair<double, double> GraphExecutor::run(const std::string& benchmark, const std::vector<int>& benchmark_args) {

  std::pair<double, double> timer;
  if(benchmark == "loop") {
    assert(benchmark_args.size() == 2);
    timer = run_loop(benchmark_args[0], benchmark_args[1]);
  }
  else if(benchmark == "data") {
    assert(benchmark_args.size() == 1);
    timer = run_data(benchmark_args[0]);
  }
  else {
    assert(false);
  }

  return timer;
}
