#pragma once

#include <vector>
#include <iostream>
#include <string>
#include <cstring>
#include <algorithm>
#include <random>


#include "base/graph_base.hpp"
#include "tree/tree.hpp"
#include "random_DAG/random_DAG.hpp"
#include "extreme_graph/parallel_graph.hpp"
#include "extreme_graph/serial_graph.hpp"
#include "map_reduce/diamond.hpp"
#include "wavefront/wavefront_graph.hpp"

#include <taskflow/taskflow/cuda/algorithm/reduce.hpp>
#include <taro/src/cuda/algorithm.hpp>

#include <chrono>
#include <cassert>

// GPU count kernel
__global__ void cuda_assign(
  int* data,
  size_t N
) {

  unsigned gid = blockDim.x * blockIdx.x + threadIdx.x;

  if(gid < N) { 
    data[gid] += 1;
  }
}

// GPU sleep kernel
__global__ void cuda_loop(
  size_t ms
) {
  long long sleep_cycles = 1350000 * ms; // TODO: 1350MHZ is for 2080ti. change it to clock for arbitrary GPU 
  long long start = clock64();
  long long cycles_elapsed;
  do { 
    cycles_elapsed = clock64() - start; 
  } while (cycles_elapsed < sleep_cycles);
}

// CPU task
void cpu_loop(
size_t ms) {
  auto start = std::chrono::steady_clock::now();
  int a = 1;
  int b = a * 10 % 7;
  while(b != 0)
  {
    a = b * 10;
    b = a % 7;
    if(std::chrono::steady_clock::now() - start > std::chrono::milliseconds(ms)) 
      break;
  }
}






