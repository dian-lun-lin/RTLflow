#pragma once
#define CUDA_API_PER_THREAD_DEFAULT_STREAM 
#include <assert.h>

// Convenience function for checking CUDA runtime API results
// can be wrapped around any runtime API call. No-op in release builds.
inline
cudaError_t checkCudaError(cudaError_t result)
{
  if (result != cudaSuccess) {
    using namespace std::literals::string_literals;
    throw std::runtime_error("CUDA Runtime Error : "s + cudaGetErrorString(result));
    //assert(result == cudaSuccess);
  }
  return result;
}

template <typename C>
constexpr bool is_kernel_v = 
  std::is_invocable_r_v<void, C, cudaStream_t>;
