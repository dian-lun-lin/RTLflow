#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>
#include <coroflow/coroflow.hpp>
#include <vector>
#include <algorithm>
#include <numeric>

void cpu_task(size_t size) {
  std::vector<size_t> data(size, ::rand() % 10);
  std::sort(data.begin(), data.end());
}

cf::Coro hybrid_task(float* a, float* b, float* c, size_t M, size_t K, size_t N) {
}


void pipeline(size_t num_pipes, size_t num_lines) {
  cf::Coroflow cf{4};

  std::vector<cf::TaskHandle> pl(num_pipes * num_lines);
  std::vector<size_t> dataa(M * N * num_lines);
  std::vector<size_t> datab(N * K * num_lines);

  float* a;
  float* b; 
  float* c;
  size_t M{10};
  size_t K{10};
  size_t N{10};
  cudaMallocManaged(&a, M * K * sizeof(float));
  cudaMallocManaged(&b, K * N * sizeof(float));
  cudaMallocManaged(&c, M * N * sizeof(float));
  std::fill_n(a.begin(), M * N, M + N);
  std::fill_n(b.begin(), N * K, N + K);
  cudaStream_t st;
  cudaCreateStream(&st);

  for(size_t l = 0; l < num_lines; ++l) {
    for(size_t p = 0; p < num_pipes; ++p) {
      pl[l * num_pipes + p] = cf.emplace([st]() -> cf::Coro {
        cuda_matmul<<<8, 32, st>>>(a, b, c, M, K, N);
        co_await cf.cuda_suspend(st);

        for(const auto& x: c) {
          REQUIRE(x == (int)(M + N) * (N+K) * N);
        }

        cpu_task(2);
        cpu_task(3);
        cpu_task(4);
        co_return;
      }
    }
  }

  // dependencies
  // vertical
  for(size_t l = 0; l < num_lines - 1; ++l) {
    for(size_t p = 0; p < num_pipes; ++p) {
      pl[l * num_pipes + p].precede(pl[(l + 1) * num_pipes]);
    }
  }

  // horizontal
  for(size_t l = 0; l < num_lines; ++l) {
    for(size_t p = 0; p < num_pipes - 1; ++p) {
      pl[l * num_pipes + p].precede(pl[l * num_pipes + p + 1]);
    }
  }

  cf.schedule();
  cf.wait();

  
  REQUIRE(cudaFree(a) == cudaSuccess);
  REQUIRE(cudaFree(b) == cudaSuccess);
  REQUIRE(cudaFree(c) == cudaSuccess);

}

int main() {
  pipeline(3, 4);
}

////--------------------------------------------------------
////Testcase:: Serial pipeline
////--------------------------------------------------------

////o - o - o
////|   |   |
//// o - o - o
//// |   |   |
//// o - o - o

//void spipeline(size_t num_pipes, size_t num_lines, size_t num_threads) {
  //cf::Coroflow cf{num_threads};
  //std::vector<cf::TaskHandle> pl(num_lines * num_pipes);

  //std::vector<std::vector<int>> data(num_lines);
  //for(auto& d: data) {
    //d.resize(num_pipes);
    //for(auto& i: d) {
      //i = ::rand() % 10;
    //}
  //}
  //std::vector<int> counters(num_lines, 0);

  //for(size_t l = 0; l < num_lines; ++l) {
    //for(size_t p = 0; p < num_pipes; ++p) {
      //pl[l * num_pipes + p] = cf.emplace(
        //[&cf, l, p, &data, &counters]() -> cf::Coro {
          //for(int _ = 0; _ < rand() % 3; ++_) {
            //co_await cf.suspend();
          //}
          //counters[l] += data[l][p];
          //co_return;
      //});
    //}
  //}

  //// dependencies
  //// vertical
  //for(size_t l = 0; l < num_lines - 1; ++l) {
    //for(size_t p = 0; p < num_pipes; ++p) {
      //pl[l * num_pipes + p].precede(pl[(l + 1) * num_pipes + p]);
    //}
  //}

  //// horizontal
  //for(size_t l = 0; l < num_lines; ++l) {
    //for(size_t p = 0; p < num_pipes - 1; ++p) {
      //pl[l * num_pipes + p].precede(pl[l * num_pipes + p + 1]);
    //}
  //}

  //REQUIRE(cf.is_DAG());
  //cf.schedule();
  //cf.wait();

  //for(size_t i = 0; i < num_lines; ++i) {
    //REQUIRE(counters[i] == std::accumulate(data[i].begin(), data[i].end(), 0));
  //}

//}

//TEST_CASE("serial_pipeline.1pipe.1line.1thread" * doctest::timeout(300)) {
  //spipeline(1, 1, 1);
//}

//TEST_CASE("serial_pipeline.3pipes.1line.1thread" * doctest::timeout(300)) {
  //spipeline(3, 1, 1);
//}

//TEST_CASE("serial_pipeline.1pipe.3lines.1thread" * doctest::timeout(300)) {
  //spipeline(1, 3, 1);
//}

//TEST_CASE("serial_pipeline.3pipes.2lines.1thread" * doctest::timeout(300)) {
  //spipeline(3, 2, 1);
//}

//TEST_CASE("serial_pipeline.1pipe.1lines.2threads" * doctest::timeout(300)) {
  //spipeline(1, 1, 2);
//}

//TEST_CASE("serial_pipeline.1pipe.2lines.2threads" * doctest::timeout(300)) {
  //spipeline(1, 2, 2);
//}

//TEST_CASE("serial_pipeline.1pipe.3lines.2threads" * doctest::timeout(300)) {
  //spipeline(1, 3, 2);
//}

//TEST_CASE("serial_pipeline.1pipe.3lines.2threads" * doctest::timeout(300)) {
  //spipeline(1, 3, 2);
//}

//TEST_CASE("serial_pipeline.2pipes.1line.2threads" * doctest::timeout(300)) {
  //spipeline(2, 1, 2);
//}

//TEST_CASE("serial_pipeline.2pipes.2lines.2threads" * doctest::timeout(300)) {
  //spipeline(2, 2, 2);
//}

//TEST_CASE("serial_pipeline.3pipes.3lines.2threads" * doctest::timeout(300)) {
  //spipeline(3, 3, 2);
//}

//TEST_CASE("serial_pipeline.77pipes.11lines.2threads" * doctest::timeout(300)) {
  //spipeline(77, 11, 2);
//}

//TEST_CASE("serial_pipeline.1pipe.1line.3threads" * doctest::timeout(300)) {
  //spipeline(1, 1, 3);
//}

//TEST_CASE("serial_pipeline.1pipe.2lines.3threads" * doctest::timeout(300)) {
  //spipeline(1, 2, 3);
//}

//TEST_CASE("serial_pipeline.17pipes.99lines.3threads" * doctest::timeout(300)) {
  //spipeline(17, 99, 3);
//}

//TEST_CASE("serial_pipeline.53pipes.11lines.3threads" * doctest::timeout(300)) {
  //spipeline(53, 11, 3);
//}

//TEST_CASE("serial_pipeline.88pipes.91lines.3threads" * doctest::timeout(300)) {
  //spipeline(88, 91, 3);
//}

//TEST_CASE("serial_pipeline.1pipe.1line.4threads" * doctest::timeout(300)) {
  //spipeline(1, 1, 4);
//}

//TEST_CASE("serial_pipeline.1pipe.2lines.4threads" * doctest::timeout(300)) {
  //spipeline(1, 2, 4);
//}

//TEST_CASE("serial_pipeline.1pipe.3lines.4threads" * doctest::timeout(300)) {
  //spipeline(1, 3, 4);
//}

//TEST_CASE("serial_pipeline.2pipes.2lines.4threads" * doctest::timeout(300)) {
  //spipeline(2, 2, 4);
//}

//TEST_CASE("serial_pipeline.8pipes.11lines.4threads" * doctest::timeout(300)) {
  //spipeline(8, 11, 4);
//}

//TEST_CASE("serial_pipeline.48pipes.92lines.4threads" * doctest::timeout(300)) {
  //spipeline(48, 92, 4);
//}

//TEST_CASE("serial_pipeline.194pipes.551lines.4threads" * doctest::timeout(300)) {
  //spipeline(194, 551, 4);
//}

//// --------------------------------------------------------
//// Testcase:: Parallel pipeline
//// --------------------------------------------------------

//// o - o - o
//// |
//// o - o - o
//// |
//// o - o - o

//void ppipeline(size_t num_pipes, size_t num_lines, size_t num_threads) {
  //cf::Coroflow cf{num_threads};
  //std::vector<cf::TaskHandle> pl(num_lines * num_pipes);

  //std::vector<std::vector<int>> data(num_lines);
  //for(auto& d: data) {
    //d.resize(num_pipes);
    //for(auto& i: d) {
      //i = ::rand() % 10;
    //}
  //}
  //std::vector<int> counters(num_lines, 0);

  //for(size_t l = 0; l < num_lines; ++l) {
    //for(size_t p = 0; p < num_pipes; ++p) {
      //pl[l * num_pipes + p] = cf.emplace(
        //[&cf, l, p, &data, &counters]() -> cf::Coro {
          //for(int _ = 0; _ < rand() % 3; ++_) {
            //co_await cf.suspend();
          //}
          //counters[l] += data[l][p];
          //co_return;
      //});
    //}
  //}

  //// dependencies
  //// vertical
  //for(size_t l = 0; l < num_lines - 1; ++l) {
    //pl[l * num_pipes].precede(pl[(l + 1) * num_pipes]);
  //}

  //// horizontal
  //for(size_t l = 0; l < num_lines; ++l) {
    //for(size_t p = 0; p < num_pipes - 1; ++p) {
      //pl[l * num_pipes + p].precede(pl[l * num_pipes + p + 1]);
    //}
  //}

  //REQUIRE(cf.is_DAG());
  //cf.schedule();
  //cf.wait();

  //for(size_t i = 0; i < num_lines; ++i) {
    //REQUIRE(counters[i] == std::accumulate(data[i].begin(), data[i].end(), 0));
  //}

//}

//TEST_CASE("parallel_pipeline.1pipes.1lines.1threads" * doctest::timeout(300)) {
  //ppipeline(1, 1, 1);
//}

//TEST_CASE("parallel_pipeline.3pipes.1lines.1threads" * doctest::timeout(300)) {
  //ppipeline(3, 1, 1);
//}

//TEST_CASE("parallel_pipeline.1pipes.3lines.1threads" * doctest::timeout(300)) {
  //ppipeline(1, 3, 1);
//}

//TEST_CASE("parallel_pipeline.3pipes.2lines.1threads" * doctest::timeout(300)) {
  //ppipeline(3, 2, 1);
//}

//TEST_CASE("parallel_pipeline.1pipes.1lines.2threads" * doctest::timeout(300)) {
  //ppipeline(1, 1, 2);
//}

//TEST_CASE("parallel_pipeline.1pipes.2lines.2threads" * doctest::timeout(300)) {
  //ppipeline(1, 2, 2);
//}

//TEST_CASE("parallel_pipeline.1pipes.3lines.2threads" * doctest::timeout(300)) {
  //ppipeline(1, 3, 2);
//}

//TEST_CASE("parallel_pipeline.1pipes.3lines.2threads" * doctest::timeout(300)) {
  //ppipeline(1, 3, 2);
//}

//TEST_CASE("parallel_pipeline.2pipes.1lines.2threads" * doctest::timeout(300)) {
  //ppipeline(2, 1, 2);
//}

//TEST_CASE("parallel_pipeline.2pipes.2lines.2threads" * doctest::timeout(300)) {
  //ppipeline(2, 2, 2);
//}

//TEST_CASE("parallel_pipeline.3pipes.3lines.2threads" * doctest::timeout(300)) {
  //ppipeline(3, 3, 2);
//}

//TEST_CASE("parallel_pipeline.77pipes.11lines.2threads" * doctest::timeout(300)) {
  //ppipeline(77, 11, 2);
//}

//TEST_CASE("parallel_pipeline.1pipes.1lines.3threads" * doctest::timeout(300)) {
  //ppipeline(1, 1, 3);
//}

//TEST_CASE("parallel_pipeline.1pipes.2lines.3threads" * doctest::timeout(300)) {
  //ppipeline(1, 2, 3);
//}

//TEST_CASE("parallel_pipeline.17pipes.99lines.3threads" * doctest::timeout(300)) {
  //ppipeline(17, 99, 3);
//}

//TEST_CASE("parallel_pipeline.53pipes.11lines.3threads" * doctest::timeout(300)) {
  //ppipeline(53, 11, 3);
//}

//TEST_CASE("parallel_pipeline.88pipes.91lines.3threads" * doctest::timeout(300)) {
  //ppipeline(88, 91, 3);
//}

//TEST_CASE("parallel_pipeline.1pipes.1lines.4threads" * doctest::timeout(300)) {
  //ppipeline(1, 1, 4);
//}

//TEST_CASE("parallel_pipeline.1pipes.2lines.4threads" * doctest::timeout(300)) {
  //ppipeline(1, 2, 4);
//}

//TEST_CASE("parallel_pipeline.1pipes.3lines.4threads" * doctest::timeout(300)) {
  //ppipeline(1, 3, 4);
//}

//TEST_CASE("parallel_pipeline.2pipes.2lines.4threads" * doctest::timeout(300)) {
  //ppipeline(2, 2, 4);
//}

//TEST_CASE("parallel_pipeline.8pipes.11lines.4threads" * doctest::timeout(300)) {
  //ppipeline(8, 11, 4);
//}

//TEST_CASE("parallel_pipeline.48pipes.92lines.4threads" * doctest::timeout(300)) {
  //ppipeline(48, 92, 4);
//}

//TEST_CASE("parallel_pipeline.194pipes.551lines.4threads" * doctest::timeout(300)) {
  //ppipeline(194, 551, 4);
//}


