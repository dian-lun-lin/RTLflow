#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>
#include <taro.hpp>
#include <taro/src/cuda/callback/v2/taro_callback_v2.hpp>
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

void linear_chain_cbv2(size_t num_tasks, size_t num_threads) {
  int* counter;
  cudaMallocManaged(&counter, sizeof(int));

  taro::TaroCBV2 taro{num_threads};
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
  taro.wait(); 
}

TEST_CASE("linear_chain_cbv2.1thread" * doctest::timeout(300)) {
  linear_chain_cbv2(1, 1);
}

TEST_CASE("linear_chain_cbv2.2thread" * doctest::timeout(300)) {
  linear_chain_cbv2(99, 2);
}

TEST_CASE("linear_chain_cbv2.3thread" * doctest::timeout(300)) {
  linear_chain_cbv2(712, 3);
}

TEST_CASE("linear_chain_cbv2.4thread" * doctest::timeout(300)) {
  linear_chain_cbv2(443, 4);
}

TEST_CASE("linear_chain_cbv2.5thread" * doctest::timeout(300)) {
  linear_chain_cbv2(1111, 5);
}

TEST_CASE("linear_chain_cbv2.6thread" * doctest::timeout(300)) {
  linear_chain_cbv2(2, 6);
}

TEST_CASE("linear_chain_cbv2.7thread" * doctest::timeout(300)) {
  linear_chain_cbv2(5, 7);
}

TEST_CASE("linear_chain_cbv2.8thread" * doctest::timeout(300)) {
  linear_chain_cbv2(9211, 8);
}

