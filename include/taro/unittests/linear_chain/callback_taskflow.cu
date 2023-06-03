#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>
#include <taro.hpp>
#include <taro/src/cuda/callback/taskflow/taro_callback_taskflow.hpp>
#include <taro/src/cuda/algorithm.hpp>
#include <vector>
#include <algorithm>
#include <numeric>

// --------------------------------------------------------
// Testcase:: Linear chain
// --------------------------------------------------------

// o - o - o - o

template <typename T>
__global__
void count(T* count) {
  ++(*count);
}

void linear_chain_cbtaskflow(size_t num_tasks, size_t num_threads, size_t num_streams) {
  int* counter;
  cudaMallocManaged(&counter, sizeof(int));

  taro::TaroCBTaskflow taro{num_threads, num_streams};
  std::vector<taro::TaskHandle> _tasks(num_tasks);

  for(size_t t = 0; t < num_tasks; ++t) {
    _tasks[t] = taro.emplace([t, counter, &taro]() -> taro::Coro {
      REQUIRE(*counter == t); 

      co_await taro.cuda_suspend([counter](cudaStream_t st) {
        count<<<8, 32, 0, st>>>(counter);
      });

      REQUIRE(*counter == t + 1); 
    });
  }

  for(size_t t = 0; t < num_tasks - 1; ++t) {
    _tasks[t].precede(_tasks[t + 1]);
  }

  REQUIRE(taro.is_DAG());
  taro.schedule();
}

TEST_CASE("linear_chain_cbtaskflow.1thread.1stream" * doctest::timeout(300)) {
  linear_chain_cbtaskflow(1, 1, 1);
}

TEST_CASE("linear_chain_cbtaskflow.2thread.2stream" * doctest::timeout(300)) {
  linear_chain_cbtaskflow(99, 2, 2);
}

TEST_CASE("linear_chain_cbtaskflow.3thread.4stream" * doctest::timeout(300)) {
  linear_chain_cbtaskflow(712, 3, 4);
}

TEST_CASE("linear_chain_cbtaskflow.4thread.8stream" * doctest::timeout(300)) {
  linear_chain_cbtaskflow(443, 4, 8);
}

TEST_CASE("linear_chain_cbtaskflow.5thread.2stream" * doctest::timeout(300)) {
  linear_chain_cbtaskflow(1111, 5, 2);
}

TEST_CASE("linear_chain_cbtaskflow.6thread.3stream" * doctest::timeout(300)) {
  linear_chain_cbtaskflow(2, 6, 3);
}

TEST_CASE("linear_chain_cbtaskflow.7thread.1stream" * doctest::timeout(300)) {
  linear_chain_cbtaskflow(5, 7, 1);
}

TEST_CASE("linear_chain_cbtaskflow.8threads" * doctest::timeout(300)) {
  linear_chain_cbtaskflow(9211, 8, 9);
}

