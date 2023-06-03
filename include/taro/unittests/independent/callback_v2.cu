#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>
#include <taro.hpp>
#include <taro/src/cuda/callback/v2/taro_callback_v2.hpp>
#include <taro/src/cuda/algorithm.hpp>
#include <vector>
#include <algorithm>
#include <numeric>


//// --------------------------------------------------------
//// Testcase:: Independent
//// --------------------------------------------------------
void independent_cbv2(size_t num_threads, size_t num_tasks) {
  taro::TaroCBV2 taro{num_threads};

  std::vector<taro::TaskHandle> tasks(num_tasks);

  int* a;
  int* b; 
  int* c;
  size_t M{10};
  size_t K{10};
  size_t N{10};
  size_t BLOCK_SIZE = 32;
  dim3 dim_grid((N - 1) / BLOCK_SIZE + 1, (N - 1) / BLOCK_SIZE + 1, 1);
  dim3 dim_block(BLOCK_SIZE, BLOCK_SIZE, 1);

  cudaMallocManaged(&a, M * K * sizeof(int));
  cudaMallocManaged(&b, K * N * sizeof(int));
  cudaMallocManaged(&c, M * N * num_tasks * sizeof(int));
  for(size_t i = 0; i < M * K; ++i) {
    a[i] = M + K;
  }
  for(size_t i = 0; i < K * N; ++i) {
    b[i] = K + N;
  }

  for(size_t i = 0; i < num_tasks; ++i) {
    tasks[i] = taro.emplace([&taro, i, a, b, c, M, K, N, dim_grid, dim_block]() -> taro::Coro {
      co_await taro.cuda_suspend([a, b, c, i, M, K, N, dim_grid, dim_block](cudaStream_t st) {
        taro::cuda_matmul<<<dim_grid, dim_block, 0, st>>>(a, b, c + i * M * N, M, K, N);
      });

      for(size_t k = 0; k < M * N; ++k) {
        REQUIRE(c[k + i * M * N] == (int)(M + K) * (K + N) * K);
      }

      co_return;
    });
  }

  REQUIRE(taro.is_DAG());
  taro.schedule();
  taro.wait();

  
  REQUIRE(cudaFree(a) == cudaSuccess);
  REQUIRE(cudaFree(b) == cudaSuccess);
  REQUIRE(cudaFree(c) == cudaSuccess);

}

TEST_CASE("independent.cbv2.1thread.1task" * doctest::timeout(300)) {
  independent_cbv2(1, 1);
}

TEST_CASE("independent.cbv2.2thread.3task" * doctest::timeout(300)) {
  independent_cbv2(2, 3);
}

TEST_CASE("independent.cbv2.2thread.18task" * doctest::timeout(300)) {
  independent_cbv2(2, 18);
}

TEST_CASE("independent.cbv2.2thread.18task" * doctest::timeout(300)) {
  independent_cbv2(2, 18);
}

TEST_CASE("independent.cbv2.3thread.2task" * doctest::timeout(300)) {
  independent_cbv2(3, 2);
}

TEST_CASE("independent.cbv2.3thread.4task" * doctest::timeout(300)) {
  independent_cbv2(3, 4);
}

TEST_CASE("independent.cbv2.3thread.18task" * doctest::timeout(300)) {
  independent_cbv2(3, 18);
}

TEST_CASE("independent.cbv2.4thread.1task" * doctest::timeout(300)) {
  independent_cbv2(4, 1);
}

TEST_CASE("independent.cbv2.4thread.11task" * doctest::timeout(300)) {
  independent_cbv2(4, 11);
}

TEST_CASE("independent.cbv2.4thread.38task" * doctest::timeout(300)) {
  independent_cbv2(4, 38);
}

TEST_CASE("independent.cbv2.4threadm.123task" * doctest::timeout(300)) {
  independent_cbv2(4, 123);
}
