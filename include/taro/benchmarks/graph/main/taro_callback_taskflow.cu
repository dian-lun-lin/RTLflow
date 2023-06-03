#include <taro.hpp>

#include "configure.hpp"
#include <taro/src/cuda/callback/taskflow/taro_callback_taskflow.hpp>
#include "../executor/taro.hpp"

#include <iostream>

int main(int argc, char* argv[]) {

  Configure cfg(argc, argv);

  if(cfg.status != 0) {
    return cfg.status;
  }

  std::pair<double, double> time_pair;

  TaroGraphExecutor<taro::TaroCBTaskflow> executor(*cfg.g_ptr, 0, cfg.num_threads, cfg.num_streams); 
  time_pair = executor.run(cfg.benchmark, cfg.benchmark_args);
  
  std::cout << "Construction time: " 
            << time_pair.first
            << " ms\n"
            << "Execution time: "
            << time_pair.second
            << " ms\n";
}

