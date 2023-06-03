#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>
#include <taro.hpp>
#include <taro/src/cuda/callback/v4/taro_callback_v4.hpp>
#include <taro/src/cuda/algorithm.hpp>
#include <vector>
#include <algorithm>
#include <numeric>


//// --------------------------------------------------------
//// Testcase:: Independent
//// --------------------------------------------------------


void independent_cbv4(size_t num_threads, size_t num_streams, size_t num_tasks) {
  taro::TaroCBV4 taro{num_threads, num_streams};

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

TEST_CASE("independent.cbv4.1thread.1stream.1task" * doctest::timeout(300)) {
  independent_cbv4(1, 1, 1);
}

TEST_CASE("independent.cbv4.2thread.1stream.3task" * doctest::timeout(300)) {
  independent_cbv4(2, 1, 3);
}

TEST_CASE("independent.cbv4.2thread.2stream.18task" * doctest::timeout(300)) {
  independent_cbv4(2, 2, 18);
}

TEST_CASE("independent.cbv4.2thread.3stream.18task" * doctest::timeout(300)) {
  independent_cbv4(2, 3, 18);
}

TEST_CASE("independent.cbv4.3thread.1stream.2task" * doctest::timeout(300)) {
  independent_cbv4(3, 1, 2);
}

TEST_CASE("independent.cbv4.3thread.2stream.4task" * doctest::timeout(300)) {
  independent_cbv4(3, 2, 4);
}

TEST_CASE("independent.cbv4.3thread.3stream.18task" * doctest::timeout(300)) {
  independent_cbv4(3, 3, 18);
}

TEST_CASE("independent.cbv4.4thread.1stream.1task" * doctest::timeout(300)) {
  independent_cbv4(4, 1, 1);
}

TEST_CASE("independent.cbv4.4thread.2stream.11task" * doctest::timeout(300)) {
  independent_cbv4(4, 2, 11);
}

TEST_CASE("independent.cbv4.4thread.8stream.38task" * doctest::timeout(300)) {
  independent_cbv4(4, 8, 38);
}

TEST_CASE("independent.cbv4.4thread.15stream.123task" * doctest::timeout(300)) {
  independent_cbv4(4, 15, 123);
}
