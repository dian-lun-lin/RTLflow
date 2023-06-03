# Taro
***T***ask-based ***A***synchronous programming system using C++ co***RO***utine.

Taro is a task-based asynchronous programming system that leverages C++ coroutines to achieve high-performance CPU-GPU computing. Taro employs a work-stealing algorithm that dynamically redistributes tasks between threads, avoiding the overhead of thread management and enabling concurrent task execution. By using coroutines, Taro enables the CPU to continue executing other tasks while waiting for the GPU to complete its work, thus avoiding blocking and improving overall performance. 


# Basic Usage
```cpp
#include <taro/cuda/taro.hpp>
int main() {
  taro::Taro taro{4, 4}; // (num_threads, num_streams)
  int* d_a, *d_b, *d_c;
  std::vector<int> h_c;
  size_t N;
  
  // H2D
  auto task_a = taro.emplace([&taro, d_a, N]() -> taro::Coro {
    std::vector<int> h_a(N * N);
    std::iota(h_a.begin(), h_a.end(), 0);
    co_await taro.cuda_suspend([&d_a, &h_a, N](cudaStream_t stream) {   
      cudaMallocAsync(&d_a, N * N * sizeof(int), stream);
      cudaMemcpyAsync(d_a, h_a.data(), N * N * sizeof(int), cudaMemcpyHostToDevice, stream);
    });
  });
  
  // H2D
  auto task_b = taro.emplace([&taro, d_b, N]() -> taro::Coro {
    std::vector<int> h_b(N * N);
    std::iota(h_b.begin(), h_b.end(), 0);
    co_await taro.cuda_suspend([&d_b, &h_b, N](cudaStream_t stream) {    
      cudaMallocAsync(&d_b, N * N * sizeof(int), stream);
      cudaMemcpyAsync(d_b, h_b.data(), N * N * sizeof(int), cudaMemcpyHostToDevice, stream);
    });
  });
  
  // matrix multiplication and D2H
  auto task_c = taro.emplace([&taro, &h_c, d_c, N]() -> taro::Coro {
    size BLOCK_SIZE{128};
    dim3 dim_grid((N - 1) / BLOCK_SIZE + 1, (N - 1) / BLOCK_SIZE + 1, 1);
    dim3 dim_block(BLOCK_SIZE, BLOCK_SIZE, 1);
    h_c.resize(N * N);
    co_await taro.cuda_suspend([&d_c, &h_c, =](cudaStream_t stream) {    
      cudaMallocAsync(&d_c, N * N * sizeof(int), stream)
      taro::cuda_matmul<<<dim_grid, dim_block, 0, stream>>>(d_a, d_b, d_c, N, N, N);
      cudaMemcpyAsync(h_c.data(), d_c, N * N * sizeof(int), cudaMemcpyDeviceToHost, stream);
    });
  });
  
  // free
  auto task_d = taro.emplace([&taro, d_a, d_b, d_c]() -> taro::Coro { 
    co_await taro.cuda_suspend([=](cudaStream_t stream) {    
      cudaFreeAsync(d_a, stream);
      cudaFreeAsync(d_b, stream);
      cudaFreeAsync(d_c, stream);
    });
  });
  
  // dependency
  // A -> C
  // B -> C
  // C -> D
  task_a.precede(task_c);
  task_b.precede(task_c);
  task_c.precede(task_d);
  
  taro.schedule();
  taro.wait();
}
```

# Pipeline
Pipeline in Taro is extremely efficient. We leverage symmetric corotuine transfer to avoid synchroniation overhead. Our pipeline only use one atomic variable and does not have to synchronize at the first pipe.


```cpp
#include <taro/cuda/taro.hpp>

template <typename T>
__global__
void count(T* count) {
  unsigned idx = blockIdx.x * blockDim.x + threadIdx.x;
  if(idx == 0) {
    ++(*count);
  }
}

int main() {
  size_t num_pipes{5};
  size_t num_tokens{300};
  int* counter;
  cudaMallocManaged(&counter, num_tokens * sizeof(int));
  cudaMemset(counter, 0, num_tokens * sizeof(int));

  taro::Taro taro{4, 4}; // (num_threads, num_streams)
  auto pipeline = taro::pipeline(taro, num_pipes, num_tokens); // (taro, num_pipes, num_tokens)
  
  // set first pipe
  pipeline.set_pipe(0, [counter, &taro, &pipeline]() -> taro::Coro { 
    size_t token = pipeline.fetch_token();
    for(;token < pipeline.num_tokens(); token = pipeline.fetch_token()) {

      int h_count = token + 1;
    
      co_await taro.cuda_suspend([=](cudaStream_t st) {
        count<<<8, 32, 0, st>>>(counter + token);
      });
      
      assert(h_count == *(count + token));
      co_await pipeline.step();
    }
    pipeline.stop();
  });

  // set remaining pipes
  for(size_t p = 1; p < num_pipes; ++p) {
    pipeline.set_pipe(p, [p, counter, &taro, &pipeline]() -> taro::Coro {
      while(1) {
        auto token = pipeline.token();
        int h_count = token + p + 1;
        co_await taro.cuda_suspend([=, &pipeline](cudaStream_t st) {
          count<<<8, 32, 0, st>>>(counter + token);
        }); 
        assert(h_count == *(count + token));
        co_await pipeline.step();
      }   
    }); 
  }
}
```
