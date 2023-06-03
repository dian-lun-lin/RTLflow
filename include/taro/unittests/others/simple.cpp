#define DOCTEST_CONFIG_IMPLEMENT_WITH_MAIN
#include <doctest.h>
#include <coroflow/src/coroflow.hpp>
#include <vector>
#include <algorithm>
#include <numeric>

// --------------------------------------------------------
// Testcase:: Linear chain
// --------------------------------------------------------

// o - o - o - o

void linear_chain(size_t num_tasks, size_t num_threads) {
  int counter{0};
  cf::Coroflow cf{num_threads};
  std::vector<cf::TaskHandle> _tasks(num_tasks);

  for(size_t t = 0; t < num_tasks; ++t) {
    _tasks[t] = cf.emplace([t, &counter, &cf]() -> cf::Coro {
        REQUIRE(counter++ == t); 
        co_await cf.suspend();
    });
  }

  for(size_t t = 0; t < num_tasks - 1; ++t) {
    _tasks[t].precede(_tasks[t + 1]);
  }

  REQUIRE(cf.is_DAG());
  cf.schedule();
  cf.wait(); 
}

TEST_CASE("linear_chain.1thread" * doctest::timeout(300)) {
  linear_chain(1, 1);
}

TEST_CASE("linear_chain.2threads" * doctest::timeout(300)) {
  linear_chain(99, 2);
}

TEST_CASE("linear_chain.3threads" * doctest::timeout(300)) {
  linear_chain(712, 3);
}

TEST_CASE("linear_chain.4threads" * doctest::timeout(300)) {
  linear_chain(443, 4);
}

TEST_CASE("linear_chain.5threads" * doctest::timeout(300)) {
  linear_chain(1111, 5);
}

TEST_CASE("linear_chain.6threads" * doctest::timeout(300)) {
  linear_chain(2, 6);
}

TEST_CASE("linear_chain.7threads" * doctest::timeout(300)) {
  linear_chain(5, 7);
}

TEST_CASE("linear_chain.8threads" * doctest::timeout(300)) {
  linear_chain(9211, 8);
}

// --------------------------------------------------------
// Testcase:: Map reduce
// --------------------------------------------------------

void map_reduce(size_t num_iters, size_t num_parallel_tasks, size_t num_threads) {
  cf::Coroflow cf{num_threads};

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

TEST_CASE("map_reduce.2iters.1ptask.1thread" * doctest::timeout(300)) {
  map_reduce(2, 1, 1);
}

TEST_CASE("map_reduce.2iters.2ptasks.1thread" * doctest::timeout(300)) {
  map_reduce(2, 2, 1);
}

TEST_CASE("map_reduce.1iter.2ptasks.2threads" * doctest::timeout(300)) {
  map_reduce(1, 2, 2);
}

TEST_CASE("map_reduce.3iters.1ptasks.2threads" * doctest::timeout(300)) {
  map_reduce(3, 1, 2);
}

TEST_CASE("map_reduce.37iters.10ptasks.2threads" * doctest::timeout(300)) {
  map_reduce(37, 10, 2);
}

TEST_CASE("map_reduce.1iter.2ptasks.3threads" * doctest::timeout(300)) {
  map_reduce(1, 2, 3);
}

TEST_CASE("map_reduce.4iters.4ptasks.3threads" * doctest::timeout(300)) {
  map_reduce(4, 4, 3);
}

TEST_CASE("map_reduce.17iters.5ptasks.3threads" * doctest::timeout(300)) {
  map_reduce(17, 5, 3);
}

TEST_CASE("map_reduce.57iters.15ptasks.3threads" * doctest::timeout(300)) {
  map_reduce(57, 15, 3);
}

TEST_CASE("map_reduce.2iters.3ptasks.4threads" * doctest::timeout(300)) {
  map_reduce(2, 3, 4);
}

TEST_CASE("map_reduce.17iters.5ptasks.4threads" * doctest::timeout(300)) {
  map_reduce(17, 5, 4);
}

TEST_CASE("map_reduce.10iters.91ptasks.4threads" * doctest::timeout(300)) {
  map_reduce(10, 91, 4);
}

//--------------------------------------------------------
//Testcase:: Serial pipeline
//--------------------------------------------------------

//o - o - o
//|   |   |
// o - o - o
// |   |   |
// o - o - o

void spipeline(size_t num_pipes, size_t num_lines, size_t num_threads) {
  cf::Coroflow cf{num_threads};
  std::vector<cf::TaskHandle> pl(num_lines * num_pipes);

  std::vector<std::vector<int>> data(num_lines);
  for(auto& d: data) {
    d.resize(num_pipes);
    for(auto& i: d) {
      i = ::rand() % 10;
    }
  }
  std::vector<int> counters(num_lines, 0);

  for(size_t l = 0; l < num_lines; ++l) {
    for(size_t p = 0; p < num_pipes; ++p) {
      pl[l * num_pipes + p] = cf.emplace(
        [&cf, l, p, &data, &counters]() -> cf::Coro {
          for(int _ = 0; _ < rand() % 3; ++_) {
            co_await cf.suspend();
          }
          counters[l] += data[l][p];
          co_return;
      });
    }
  }

  // dependencies
  // vertical
  for(size_t l = 0; l < num_lines - 1; ++l) {
    for(size_t p = 0; p < num_pipes; ++p) {
      pl[l * num_pipes + p].precede(pl[(l + 1) * num_pipes + p]);
    }
  }

  // horizontal
  for(size_t l = 0; l < num_lines; ++l) {
    for(size_t p = 0; p < num_pipes - 1; ++p) {
      pl[l * num_pipes + p].precede(pl[l * num_pipes + p + 1]);
    }
  }

  REQUIRE(cf.is_DAG());
  cf.schedule();
  cf.wait();

  for(size_t i = 0; i < num_lines; ++i) {
    REQUIRE(counters[i] == std::accumulate(data[i].begin(), data[i].end(), 0));
  }

}

TEST_CASE("serial_pipeline.1pipe.1line.1thread" * doctest::timeout(300)) {
  spipeline(1, 1, 1);
}

TEST_CASE("serial_pipeline.3pipes.1line.1thread" * doctest::timeout(300)) {
  spipeline(3, 1, 1);
}

TEST_CASE("serial_pipeline.1pipe.3lines.1thread" * doctest::timeout(300)) {
  spipeline(1, 3, 1);
}

TEST_CASE("serial_pipeline.3pipes.2lines.1thread" * doctest::timeout(300)) {
  spipeline(3, 2, 1);
}

TEST_CASE("serial_pipeline.1pipe.1lines.2threads" * doctest::timeout(300)) {
  spipeline(1, 1, 2);
}

TEST_CASE("serial_pipeline.1pipe.2lines.2threads" * doctest::timeout(300)) {
  spipeline(1, 2, 2);
}

TEST_CASE("serial_pipeline.1pipe.3lines.2threads" * doctest::timeout(300)) {
  spipeline(1, 3, 2);
}

TEST_CASE("serial_pipeline.1pipe.3lines.2threads" * doctest::timeout(300)) {
  spipeline(1, 3, 2);
}

TEST_CASE("serial_pipeline.2pipes.1line.2threads" * doctest::timeout(300)) {
  spipeline(2, 1, 2);
}

TEST_CASE("serial_pipeline.2pipes.2lines.2threads" * doctest::timeout(300)) {
  spipeline(2, 2, 2);
}

TEST_CASE("serial_pipeline.3pipes.3lines.2threads" * doctest::timeout(300)) {
  spipeline(3, 3, 2);
}

TEST_CASE("serial_pipeline.77pipes.11lines.2threads" * doctest::timeout(300)) {
  spipeline(77, 11, 2);
}

TEST_CASE("serial_pipeline.1pipe.1line.3threads" * doctest::timeout(300)) {
  spipeline(1, 1, 3);
}

TEST_CASE("serial_pipeline.1pipe.2lines.3threads" * doctest::timeout(300)) {
  spipeline(1, 2, 3);
}

TEST_CASE("serial_pipeline.17pipes.99lines.3threads" * doctest::timeout(300)) {
  spipeline(17, 99, 3);
}

TEST_CASE("serial_pipeline.53pipes.11lines.3threads" * doctest::timeout(300)) {
  spipeline(53, 11, 3);
}

TEST_CASE("serial_pipeline.88pipes.91lines.3threads" * doctest::timeout(300)) {
  spipeline(88, 91, 3);
}

TEST_CASE("serial_pipeline.1pipe.1line.4threads" * doctest::timeout(300)) {
  spipeline(1, 1, 4);
}

TEST_CASE("serial_pipeline.1pipe.2lines.4threads" * doctest::timeout(300)) {
  spipeline(1, 2, 4);
}

TEST_CASE("serial_pipeline.1pipe.3lines.4threads" * doctest::timeout(300)) {
  spipeline(1, 3, 4);
}

TEST_CASE("serial_pipeline.2pipes.2lines.4threads" * doctest::timeout(300)) {
  spipeline(2, 2, 4);
}

TEST_CASE("serial_pipeline.8pipes.11lines.4threads" * doctest::timeout(300)) {
  spipeline(8, 11, 4);
}

TEST_CASE("serial_pipeline.48pipes.92lines.4threads" * doctest::timeout(300)) {
  spipeline(48, 92, 4);
}

TEST_CASE("serial_pipeline.194pipes.551lines.4threads" * doctest::timeout(300)) {
  spipeline(194, 551, 4);
}

// --------------------------------------------------------
// Testcase:: Parallel pipeline
// --------------------------------------------------------

// o - o - o
// |
// o - o - o
// |
// o - o - o

void ppipeline(size_t num_pipes, size_t num_lines, size_t num_threads) {
  cf::Coroflow cf{num_threads};
  std::vector<cf::TaskHandle> pl(num_lines * num_pipes);

  std::vector<std::vector<int>> data(num_lines);
  for(auto& d: data) {
    d.resize(num_pipes);
    for(auto& i: d) {
      i = ::rand() % 10;
    }
  }
  std::vector<int> counters(num_lines, 0);

  for(size_t l = 0; l < num_lines; ++l) {
    for(size_t p = 0; p < num_pipes; ++p) {
      pl[l * num_pipes + p] = cf.emplace(
        [&cf, l, p, &data, &counters]() -> cf::Coro {
          for(int _ = 0; _ < rand() % 3; ++_) {
            co_await cf.suspend();
          }
          counters[l] += data[l][p];
          co_return;
      });
    }
  }

  // dependencies
  // vertical
  for(size_t l = 0; l < num_lines - 1; ++l) {
    pl[l * num_pipes].precede(pl[(l + 1) * num_pipes]);
  }

  // horizontal
  for(size_t l = 0; l < num_lines; ++l) {
    for(size_t p = 0; p < num_pipes - 1; ++p) {
      pl[l * num_pipes + p].precede(pl[l * num_pipes + p + 1]);
    }
  }

  REQUIRE(cf.is_DAG());
  cf.schedule();
  cf.wait();

  for(size_t i = 0; i < num_lines; ++i) {
    REQUIRE(counters[i] == std::accumulate(data[i].begin(), data[i].end(), 0));
  }

}

TEST_CASE("parallel_pipeline.1pipes.1lines.1threads" * doctest::timeout(300)) {
  ppipeline(1, 1, 1);
}

TEST_CASE("parallel_pipeline.3pipes.1lines.1threads" * doctest::timeout(300)) {
  ppipeline(3, 1, 1);
}

TEST_CASE("parallel_pipeline.1pipes.3lines.1threads" * doctest::timeout(300)) {
  ppipeline(1, 3, 1);
}

TEST_CASE("parallel_pipeline.3pipes.2lines.1threads" * doctest::timeout(300)) {
  ppipeline(3, 2, 1);
}

TEST_CASE("parallel_pipeline.1pipes.1lines.2threads" * doctest::timeout(300)) {
  ppipeline(1, 1, 2);
}

TEST_CASE("parallel_pipeline.1pipes.2lines.2threads" * doctest::timeout(300)) {
  ppipeline(1, 2, 2);
}

TEST_CASE("parallel_pipeline.1pipes.3lines.2threads" * doctest::timeout(300)) {
  ppipeline(1, 3, 2);
}

TEST_CASE("parallel_pipeline.1pipes.3lines.2threads" * doctest::timeout(300)) {
  ppipeline(1, 3, 2);
}

TEST_CASE("parallel_pipeline.2pipes.1lines.2threads" * doctest::timeout(300)) {
  ppipeline(2, 1, 2);
}

TEST_CASE("parallel_pipeline.2pipes.2lines.2threads" * doctest::timeout(300)) {
  ppipeline(2, 2, 2);
}

TEST_CASE("parallel_pipeline.3pipes.3lines.2threads" * doctest::timeout(300)) {
  ppipeline(3, 3, 2);
}

TEST_CASE("parallel_pipeline.77pipes.11lines.2threads" * doctest::timeout(300)) {
  ppipeline(77, 11, 2);
}

TEST_CASE("parallel_pipeline.1pipes.1lines.3threads" * doctest::timeout(300)) {
  ppipeline(1, 1, 3);
}

TEST_CASE("parallel_pipeline.1pipes.2lines.3threads" * doctest::timeout(300)) {
  ppipeline(1, 2, 3);
}

TEST_CASE("parallel_pipeline.17pipes.99lines.3threads" * doctest::timeout(300)) {
  ppipeline(17, 99, 3);
}

TEST_CASE("parallel_pipeline.53pipes.11lines.3threads" * doctest::timeout(300)) {
  ppipeline(53, 11, 3);
}

TEST_CASE("parallel_pipeline.88pipes.91lines.3threads" * doctest::timeout(300)) {
  ppipeline(88, 91, 3);
}

TEST_CASE("parallel_pipeline.1pipes.1lines.4threads" * doctest::timeout(300)) {
  ppipeline(1, 1, 4);
}

TEST_CASE("parallel_pipeline.1pipes.2lines.4threads" * doctest::timeout(300)) {
  ppipeline(1, 2, 4);
}

TEST_CASE("parallel_pipeline.1pipes.3lines.4threads" * doctest::timeout(300)) {
  ppipeline(1, 3, 4);
}

TEST_CASE("parallel_pipeline.2pipes.2lines.4threads" * doctest::timeout(300)) {
  ppipeline(2, 2, 4);
}

TEST_CASE("parallel_pipeline.8pipes.11lines.4threads" * doctest::timeout(300)) {
  ppipeline(8, 11, 4);
}

TEST_CASE("parallel_pipeline.48pipes.92lines.4threads" * doctest::timeout(300)) {
  ppipeline(48, 92, 4);
}

TEST_CASE("parallel_pipeline.194pipes.551lines.4threads" * doctest::timeout(300)) {
  ppipeline(194, 551, 4);
}


