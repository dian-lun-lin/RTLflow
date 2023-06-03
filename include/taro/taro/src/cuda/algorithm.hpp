#pragma once

namespace taro { // begin of namespace taro ===================================

// ============================================================================
// row-major matrix multiplication
// ============================================================================

template <typename T>
__global__ void cuda_matmul(
  const T* A,
  const T* B,
  T* C,
  size_t M,
  size_t K,
  size_t N
) {
  __shared__ T A_tile[32][32];
  __shared__ T B_tile[32][32];

  size_t x = blockIdx.x * blockDim.x + threadIdx.x;
  size_t y = blockIdx.y * blockDim.y + threadIdx.y;

  T res = 0;

  for(size_t k = 0; k < K; k += 32) {
    if((threadIdx.x + k) < K && y < M) {
      A_tile[threadIdx.y][threadIdx.x] = A[y * K + threadIdx.x + k];
    }
    else{
      A_tile[threadIdx.y][threadIdx.x] = 0;
    }

    if((threadIdx.y + k) < K && x < N) {
      B_tile[threadIdx.y][threadIdx.x] = B[(threadIdx.y + k) * N + x];
    }
    else{
      B_tile[threadIdx.y][threadIdx.x] = 0;
    }

    __syncthreads();

    for(size_t i = 0; i < 32; ++i) {
      res += A_tile[threadIdx.y][i] * B_tile[i][threadIdx.x];
    }
    __syncthreads();
  }

  if(x < N && y < M) {
    C[y * N + x] = res;
  }
}

// ============================================================================
// row-wise matrix transpose
// ============================================================================

template <typename T>
__global__ void cuda_transpose(
  const T* d_in,
  T* d_out,
  size_t rows,
  size_t cols
) {
  __shared__ T tile[32][32];
  size_t x = blockIdx.x * 32 + threadIdx.x;
  size_t y = blockIdx.y * 32 + threadIdx.y;

  for(size_t i = 0; i < 32; i += 8) {
    if(x < cols && (y + i) < rows) {
      tile[threadIdx.y + i][threadIdx.x] = d_in[(y + i) * cols + x];
    }
  }

  __syncthreads();

  x = blockIdx.y * 32 + threadIdx.x;
  y = blockIdx.x * 32 + threadIdx.y;

  for(size_t i = 0; i < 32; i += 8) {
    if(x < rows && (y + i) < cols) {
      d_out[(y + i) * rows + x] = tile[threadIdx.x][threadIdx.y + i];
    }
  }
}

} // end of namespace taro =========================================================
