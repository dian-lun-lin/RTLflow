#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>
#include "../benchmarks/boost_fiber/fiber.hpp"
#include <taro/src/cuda/algorithm.hpp>
#include <vector>
#include <algorithm>
#include <numeric>


//// --------------------------------------------------------
//// Testcase:: Independent
//// --------------------------------------------------------
void independent_fiber(size_t num_threads, size_t num_streams, size_t num_tasks) {
  FiberTaskScheduler ft_sched{num_threads, num_streams};

  std::vector<FiberTaskHandle> tasks(num_tasks);

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
    tasks[i] = ft_sched.emplace([&ft_sched, i, a, b, c, M, K, N, dim_grid, dim_block](cudaStream_t st)  {

      taro::cuda_matmul<<<dim_grid, dim_block, 0, st>>>(a, b, c + i * M * N, M, K, N);

      boost::fibers::cuda::waitfor_all(st);

      for(size_t k = 0; k < M * N; ++k) {
        REQUIRE(c[k + i * M * N] == (int)(M + K) * (K + N) * K);
      }
    });
  }

  ft_sched.schedule();
  ft_sched.wait();

  
  REQUIRE(cudaFree(a) == cudaSuccess);
  REQUIRE(cudaFree(b) == cudaSuccess);
  REQUIRE(cudaFree(c) == cudaSuccess);

}

//TEST_CASE("independent.fiber.2thread.3task" * doctest::timeout(300)) {
  //independent_fiber(2, 1, 3);
//}

//TEST_CASE("independent.fiber.2thread.9task" * doctest::timeout(300)) {
  //independent_fiber(2, 3, 9);
//}

//TEST_CASE("independent.fiber.2thread.18task" * doctest::timeout(300)) {
  //independent_fiber(2, 11, 18);
//}

//TEST_CASE("independent.fiber.2thread.19task" * doctest::timeout(300)) {
  //independent_fiber(2, 4, 19);
//}

//TEST_CASE("independent.fiber.3thread.2task" * doctest::timeout(300)) {
  //independent_fiber(3, 5, 2);
//}

//TEST_CASE("independent.fiber.3thread.4task" * doctest::timeout(300)) {
  //independent_fiber(3, 3, 4);
//}

//TEST_CASE("independent.fiber.3thread.18task" * doctest::timeout(300)) {
  //independent_fiber(3, 10, 18);
//}

//TEST_CASE("independent.fiber.4thread.1task" * doctest::timeout(300)) {
  //independent_fiber(4, 4, 1);
//}

//TEST_CASE("independent.fiber.4thread.11task" * doctest::timeout(300)) {
  //independent_fiber(4, 5, 11);
//}

//TEST_CASE("independent.fiber.4thread.38task" * doctest::timeout(300)) {
  //independent_fiber(4, 32, 38);
//}

TEST_CASE("independent.fiber.4threadm.123task" * doctest::timeout(300)) {
  independent_fiber(4, 12, 123);
}


