#pragma once

namespace taro { // begin of namespace taro =================================================================================


struct cudaStream {
  cudaStream_t st;
  size_t id;
};

struct cudaCallbackData {
  CoroflowV4* cf;
  Coro::promise_type* prom;
  size_t stream_id;
  //size_t num_kernels;
};

// cuda callback
void CUDART_CB _cuda_stream_callback_v4(cudaStream_t st, cudaError_t stat, void* void_args) {
  checkCudaError(stat);

  // unpack
  auto* data = (cudaCallbackData*) void_args;
  auto* cf = data->cf;
  auto* prom = data->prom;
  auto stream_id = data->stream_id;


  //auto* worker = cf->_this_worker();
  //if(worker) {
    //cf->_enqueue(&worker, cf->_tasks[prom->_id].get());
  //}
  //else {
  cf->_enqueue(cf->_tasks[prom->_id].get());
  //}
}

} // end of namespace taro ==============================================
