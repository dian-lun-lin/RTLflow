#include <coroflow/coroflow.hpp>
#include <vector>
#include <algorithm>
#include <numeric>
#include <coroflow/algorithms/matmul.hpp>
#include <cassert>
#include <cstring>
#include <chrono>

void cpu_set_value(
  size_t ns
) {
  std::this_thread::sleep_for(std::chrono::nanoseconds(ns));
}

__global__ void cuda_sleep(
  size_t ns
) {
  __nanosleep(ns);
}

cudaEvent_t gpu_evaluate(
  size_t ns
//float* A, float* B, float* C, size_t M, size_t K, size_t N
) {
  cudaEvent_t finish;
  cudaEventCreate(&finish);
  //cuda_matmul<<<4, 32>>>(A, B, C, M, K, N);
  cuda_sleep<<<8, 32>>>(ns);
  cudaEventRecord(finish);
  return finish;
}

cf::Coro hybrid_coro(
  size_t chain_size, size_t cpu_ns, size_t gpu_ns
//size_t chain_size, float* A, float* B, float* C, size_t M, size_t K, size_t N
) {

  for(size_t i = 0; i < chain_size; i++) {
    
    // cpu task
    cpu_set_value(cpu_ns);

    // gpu task
    auto finish = gpu_evaluate(gpu_ns);
    auto isdone = [&finish]() { return cudaEventQuery(finish) == cudaSuccess;  };
    while(!isdone()) {
      co_await cf::State::SUSPEND;
    }

    // cpu task
    cpu_set_value(cpu_ns);

    // gpu task
    auto finish2 = gpu_evaluate(gpu_ns);
    auto isdone2 = [&finish2]() { return cudaEventQuery(finish2) == cudaSuccess;  };
    while(!isdone2()) {
      co_await cf::State::SUSPEND;
    }
  }
  co_return;
}

void hybrid_task(
  size_t chain_size, size_t cpu_ns, size_t gpu_ns
) {

  for(size_t i = 0; i < chain_size; i++) {
    
    // cpu task
    cpu_set_value(cpu_ns);

    // gpu task
    auto finish = gpu_evaluate(gpu_ns);
    cudaEventSynchronize(finish);

    // cpu task
    cpu_set_value(cpu_ns);

    // gpu task
    auto finish2 = gpu_evaluate(gpu_ns);
    cudaEventSynchronize(finish2);
  }

  return;
}

void pipeline_coro(size_t num_threads, size_t chain_size, size_t num_pipes, size_t num_lines, size_t cpu_ns, size_t gpu_ns) {
  std::chrono::time_point<std::chrono::steady_clock> coro_tic;
  std::chrono::time_point<std::chrono::steady_clock> coro_toc;

  // memory allocation
  //std::vector<float*> As(num_lines, nullptr);
  //std::vector<float*> Bs(num_lines, nullptr);
  //std::vector<float*> Cs(num_lines, nullptr);
  //size_t M{4096};
  //size_t K{4096};
  //size_t N{4096};

  //for(auto& A: As) {
    //cudaMallocManaged(&A, M * K * sizeof(float));
  //}
  //for(auto& B: Bs) {
    //cudaMallocManaged(&B, K * N * sizeof(float));
  //}
  //for(auto& C: Cs) {
    //cudaMallocManaged(&C, M * N * sizeof(float));
  //}


  cf::Coroflow cf{num_threads};

  std::vector<std::vector<cf::TaskHandle>> pl(num_lines);
  for(auto&l: pl) {
    l.resize(num_pipes);
  }

  // emplace tasks
  for(size_t l = 0; l < num_lines; ++l) {
    for(size_t p = 0; p < num_pipes; ++p) {
      // TODO: is a function that calls coroutine considered as a coroutine?
      // TODO: is std::bind OK for binding coroutines with parameters?
      pl[l][p] = cf.emplace(
        //std::bind(hybrid_coro, chain_size, As[l], Bs[l], Cs[l], M, K, N)
        std::bind(hybrid_coro, chain_size, cpu_ns, gpu_ns)
      );
    }
  }

  // draw dependencies
  // vertical
  for(size_t l = 0; l < num_lines - 1; ++l) {
    //for(size_t p = 0; p < num_pipes; ++p) {
    pl[l][0].precede(pl[l + 1][0]);
    //}
  }

  // draw dependencies
  // horizontal
  for(size_t l = 0; l < num_lines; ++l) {
    for(size_t p = 0; p < num_pipes - 1; ++p) {
      pl[l][p].precede(pl[l][p + 1]);
    }
  }

  assert(cf.is_DAG());

  coro_tic = std::chrono::steady_clock::now();
  cf.schedule();
  cf.wait();
  coro_toc = std::chrono::steady_clock::now();
  auto coro_dur = std::chrono::duration_cast<std::chrono::milliseconds>(coro_toc - coro_tic).count();
  std::cout << "Coro time: " << coro_dur << "ms\n";

  // free memory
  //for(auto A: As) {
    //cudaFree(A);
  //}
  //for(auto B: Bs) {
    //cudaFree(B);
  //}
  //for(auto C: Cs) {
    //cudaFree(C);
  //}
}

void pipeline_task(size_t num_threads, size_t chain_size, size_t num_pipes, size_t num_lines, size_t cpu_ns, size_t gpu_ns) {

  std::chrono::time_point<std::chrono::steady_clock> task_tic;
  std::chrono::time_point<std::chrono::steady_clock> task_toc;

  // memory allocation
  //std::vector<float*> As(num_lines, nullptr);
  //std::vector<float*> Bs(num_lines, nullptr);
  //std::vector<float*> Cs(num_lines, nullptr);
  //size_t M{4096};
  //size_t K{4096};
  //size_t N{4096};

  //for(auto& A: As) {
    //cudaMallocManaged(&A, M * K * sizeof(float));
  //}
  //for(auto& B: Bs) {
    //cudaMallocManaged(&B, K * N * sizeof(float));
  //}
  //for(auto& C: Cs) {
    //cudaMallocManaged(&C, M * N * sizeof(float));
  //}


  cf::Coroflow cf{num_threads};

  std::vector<std::vector<cf::TaskHandle>> pl(num_lines);
  for(auto&l: pl) {
    l.resize(num_pipes);
  }

  // emplace tasks
  for(size_t l = 0; l < num_lines; ++l) {
    for(size_t p = 0; p < num_pipes; ++p) {
      pl[l][p] = cf.emplace(
        //std::bind(hybrid_task, chain_size, As[l], Bs[l], Cs[l], M, K, N)
        std::bind(hybrid_task, chain_size, cpu_ns, gpu_ns)
      );
    }
  }

  // draw dependencies
  // vertical
  for(size_t l = 0; l < num_lines - 1; ++l) {
    //for(size_t p = 0; p < num_pipes; ++p) {
    pl[l][0].precede(pl[l + 1][0]);
    //}
  }

  // draw dependencies
  // horizontal
  for(size_t l = 0; l < num_lines; ++l) {
    for(size_t p = 0; p < num_pipes - 1; ++p) {
      pl[l][p].precede(pl[l][p + 1]);
    }
  }

  assert(cf.is_DAG());

  task_tic = std::chrono::steady_clock::now();
  cf.schedule();
  cf.wait();
  task_toc = std::chrono::steady_clock::now();
  auto task_dur = std::chrono::duration_cast<std::chrono::milliseconds>(task_toc - task_tic).count();
  std::cout << "Task time: " << task_dur << "ms\n";

  // free memory
  //for(auto A: As) {
    //cudaFree(A);
  //}
  //for(auto B: Bs) {
    //cudaFree(B);
  //}
  //for(auto C: Cs) {
    //cudaFree(C);
  //}
}

int main(int argc, char* argv[]) {
  if(argc != 7) {
    std::cerr << "usage: ./bin/pipeline num_threads, chain_size, num_pipes, num_lines, cpu_ns, gpu_ns\n";
    std::exit(EXIT_FAILURE);
  }
  size_t num_threads = std::atoi(argv[1]);
  size_t chain_size = std::atoi(argv[2]);
  size_t num_pipes = std::atoi(argv[3]);
  size_t num_lines = std::atoi(argv[4]);
  size_t cpu_ns = std::atoi(argv[5]);
  size_t gpu_ns = std::atoi(argv[6]);

  std::cout << "configure: (" 
            << num_threads << ", "
            << chain_size << ", "
            << num_pipes << ", "
            << num_lines << ", "
            << cpu_ns << ", "
            << gpu_ns << "):\n";
  pipeline_coro(num_threads, chain_size, num_pipes, num_lines, cpu_ns, gpu_ns);
  pipeline_task(num_threads, chain_size, num_pipes, num_lines, cpu_ns, gpu_ns);
  std::cout << "\n";
}


