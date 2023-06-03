#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>
#include <coroflow/src/cuda/coroflow_v1.hpp>
#include <coroflow/src/cuda/coroflow_v2.hpp>
#include <coroflow/src/cuda/coroflow_v3.hpp>
#include <coroflow/src/cuda/coroflow_v4.hpp>
#include <coroflow/src/cuda/coroflow_v5.hpp>
#include <coroflow/src/cuda/algorithm.hpp>
#include <vector>
#include <algorithm>
#include <numeric>

//// --------------------------------------------------------
//// Testcase:: Map reduce
//// --------------------------------------------------------

void map_reduce_v1(size_t num_iters, size_t num_parallel_tasks, size_t num_threads) {
  cf::CoroflowV1 cf{num_threads};

  int counter{0};
  std::vector<int> buf(num_parallel_tasks, 0);

  std::vector<int> data(num_parallel_tasks, 0);
  std::iota(data.begin(), data.end(), 0);
  int ans = std::accumulate(data.begin(), data.end(), 0);

  auto src_t = cf.emplace([&cf]() -> cf::Coro {
    co_await cf.suspend();
  });


  for(size_t i = 0; i < num_iters; ++i) {

    auto reduce_t = cf.emplace([&cf, &buf, &counter, &ans, i]() -> cf::Coro {
      counter += std::accumulate(buf.begin(), buf.end(), 0);
      co_await cf.suspend();
      int res = ans * (i + 1);
      REQUIRE(counter == res);
      co_await cf.suspend();
      std::fill(buf.begin(), buf.end(), 0);
    });

    for(size_t t = 0; t < num_parallel_tasks; ++t) {
      auto map_t = cf.emplace([&cf, &buf, &data, t]() -> cf::Coro {
        for(int _ = 0; _ < rand() % 3; ++_) {
          co_await cf.suspend();
        }
        buf[t] += data[t];
      });

      src_t.precede(map_t);
      map_t.precede(reduce_t);
    }

    src_t = reduce_t;
    
  }

  REQUIRE(cf.is_DAG());
  cf.schedule();
  cf.wait();
}

TEST_CASE("map_reduce.1iter.1ptask.1thread" * doctest::timeout(300)) {
  map_reduce(1, 1, 1);
}

TEST_CASE("map_reduce.2iter.1ptask.1thread" * doctest::timeout(300)) {
  map_reduce(2, 1, 1);
}

TEST_CASE("map_reduce.2iter.2ptask.1thread" * doctest::timeout(300)) {
  map_reduce(2, 2, 1);
}

TEST_CASE("map_reduce.1iter.2ptask.2thread" * doctest::timeout(300)) {
  map_reduce(1, 2, 2);
}

TEST_CASE("map_reduce.3iter.1ptask.2thread" * doctest::timeout(300)) {
  map_reduce(3, 1, 2);
}

TEST_CASE("map_reduce.37iter.10ptask.2thread" * doctest::timeout(300)) {
  map_reduce(37, 10, 2);
}

TEST_CASE("map_reduce.1iter.2ptask.3thread" * doctest::timeout(300)) {
  map_reduce(1, 2, 3);
}

TEST_CASE("map_reduce.4iter.4ptask.3thread" * doctest::timeout(300)) {
  map_reduce(4, 4, 3);
}

TEST_CASE("map_reduce.17iter.5ptask.3thread" * doctest::timeout(300)) {
  map_reduce(17, 5, 3);
}

TEST_CASE("map_reduce.57iter.15ptask.3thread" * doctest::timeout(300)) {
  map_reduce(57, 15, 3);
}

TEST_CASE("map_reduce.2iter.3ptask.4thread" * doctest::timeout(300)) {
  map_reduce(2, 3, 4);
}

TEST_CASE("map_reduce.17iter.5ptask.4thread" * doctest::timeout(300)) {
  map_reduce(17, 5, 4);
}

TEST_CASE("map_reduce.10iter.91ptask.4thread" * doctest::timeout(300)) {
  map_reduce(10, 91, 4);
}

void map_reduce_v2(size_t num_iters, size_t num_parallel_tasks, size_t num_threads) {
  cf::CoroflowV2 cf{num_threads};

  int counter{0};
  std::vector<int> buf(num_parallel_tasks, 0);

  std::vector<int> data(num_parallel_tasks, 0);
  std::iota(data.begin(), data.end(), 0);
  int ans = std::accumulate(data.begin(), data.end(), 0);

  auto src_t = cf.emplace([&cf]() -> cf::Coro {
    co_await cf.suspend();
  });


  for(size_t i = 0; i < num_iters; ++i) {

    auto reduce_t = cf.emplace([&cf, &buf, &counter, &ans, i]() -> cf::Coro {
      counter += std::accumulate(buf.begin(), buf.end(), 0);
      co_await cf.suspend();
      int res = ans * (i + 1);
      REQUIRE(counter == res);
      co_await cf.suspend();
      std::fill(buf.begin(), buf.end(), 0);
    });

    for(size_t t = 0; t < num_parallel_tasks; ++t) {
      auto map_t = cf.emplace([&cf, &buf, &data, t]() -> cf::Coro {
        for(int _ = 0; _ < rand() % 3; ++_) {
          co_await cf.suspend();
        }
        buf[t] += data[t];
      });

      src_t.precede(map_t);
      map_t.precede(reduce_t);
    }

    src_t = reduce_t;
    
  }

  REQUIRE(cf.is_DAG());
  cf.schedule();
  cf.wait();
}

TEST_CASE("map_reduce.1iter.1ptask.1thread" * doctest::timeout(300)) {
  map_reduce(1, 1, 1);
}

TEST_CASE("map_reduce.2iter.1ptask.1thread" * doctest::timeout(300)) {
  map_reduce(2, 1, 1);
}

TEST_CASE("map_reduce.2iter.2ptask.1thread" * doctest::timeout(300)) {
  map_reduce(2, 2, 1);
}

TEST_CASE("map_reduce.1iter.2ptask.2thread" * doctest::timeout(300)) {
  map_reduce(1, 2, 2);
}

TEST_CASE("map_reduce.3iter.1ptask.2thread" * doctest::timeout(300)) {
  map_reduce(3, 1, 2);
}

TEST_CASE("map_reduce.37iter.10ptask.2thread" * doctest::timeout(300)) {
  map_reduce(37, 10, 2);
}

TEST_CASE("map_reduce.1iter.2ptask.3thread" * doctest::timeout(300)) {
  map_reduce(1, 2, 3);
}

TEST_CASE("map_reduce.4iter.4ptask.3thread" * doctest::timeout(300)) {
  map_reduce(4, 4, 3);
}

TEST_CASE("map_reduce.17iter.5ptask.3thread" * doctest::timeout(300)) {
  map_reduce(17, 5, 3);
}

TEST_CASE("map_reduce.57iter.15ptask.3thread" * doctest::timeout(300)) {
  map_reduce(57, 15, 3);
}

TEST_CASE("map_reduce.2iter.3ptask.4thread" * doctest::timeout(300)) {
  map_reduce(2, 3, 4);
}

TEST_CASE("map_reduce.17iter.5ptask.4thread" * doctest::timeout(300)) {
  map_reduce(17, 5, 4);
}

TEST_CASE("map_reduce.10iter.91ptask.4thread" * doctest::timeout(300)) {
  map_reduce(10, 91, 4);
}

void map_reduce_v3(size_t num_iters, size_t num_parallel_tasks, size_t num_threads, size_t num_streams) {
  cf::CoroflowV3 cf{num_threads, num_streams};

  int counter{0};
  std::vector<int> buf(num_parallel_tasks, 0);

  std::vector<int> data(num_parallel_tasks, 0);
  std::iota(data.begin(), data.end(), 0);
  int ans = std::accumulate(data.begin(), data.end(), 0);

  auto src_t = cf.emplace([&cf]() -> cf::Coro {
    co_await cf.suspend();
  });


  for(size_t i = 0; i < num_iters; ++i) {

    auto reduce_t = cf.emplace([&cf, &buf, &counter, &ans, i]() -> cf::Coro {
      counter += std::accumulate(buf.begin(), buf.end(), 0);
      co_await cf.suspend();
      int res = ans * (i + 1);
      REQUIRE(counter == res);
      co_await cf.suspend();
      std::fill(buf.begin(), buf.end(), 0);
    });

    for(size_t t = 0; t < num_parallel_tasks; ++t) {
      auto map_t = cf.emplace([&cf, &buf, &data, t]() -> cf::Coro {
        for(int _ = 0; _ < rand() % 3; ++_) {
          co_await cf.suspend();
        }
        buf[t] += data[t];
      });

      src_t.precede(map_t);
      map_t.precede(reduce_t);
    }

    src_t = reduce_t;
    
  }

  REQUIRE(cf.is_DAG());
  cf.schedule();
  cf.wait();
}

TEST_CASE("map_reduce.1iter.1ptask.1thread" * doctest::timeout(300)) {
  map_reduce(1, 1, 1);
}

TEST_CASE("map_reduce.2iter.1ptask.1thread" * doctest::timeout(300)) {
  map_reduce(2, 1, 1);
}

TEST_CASE("map_reduce.2iter.2ptask.1thread" * doctest::timeout(300)) {
  map_reduce(2, 2, 1);
}

TEST_CASE("map_reduce.1iter.2ptask.2thread" * doctest::timeout(300)) {
  map_reduce(1, 2, 2);
}

TEST_CASE("map_reduce.3iter.1ptask.2thread" * doctest::timeout(300)) {
  map_reduce(3, 1, 2);
}

TEST_CASE("map_reduce.37iter.10ptask.2thread" * doctest::timeout(300)) {
  map_reduce(37, 10, 2);
}

TEST_CASE("map_reduce.1iter.2ptask.3thread" * doctest::timeout(300)) {
  map_reduce(1, 2, 3);
}

TEST_CASE("map_reduce.4iter.4ptask.3thread" * doctest::timeout(300)) {
  map_reduce(4, 4, 3);
}

TEST_CASE("map_reduce.17iter.5ptask.3thread" * doctest::timeout(300)) {
  map_reduce(17, 5, 3);
}

TEST_CASE("map_reduce.57iter.15ptask.3thread" * doctest::timeout(300)) {
  map_reduce(57, 15, 3);
}

TEST_CASE("map_reduce.2iter.3ptask.4thread" * doctest::timeout(300)) {
  map_reduce(2, 3, 4);
}

TEST_CASE("map_reduce.17iter.5ptask.4thread" * doctest::timeout(300)) {
  map_reduce(17, 5, 4);
}

TEST_CASE("map_reduce.10iter.91ptask.4thread" * doctest::timeout(300)) {
  map_reduce(10, 91, 4);
}

