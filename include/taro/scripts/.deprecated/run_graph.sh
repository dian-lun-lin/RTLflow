#!/bin/bash
RUN_ALL=false
if [[ "$1" == "run_all" ]]; then
  RUN_ALL=true
elif [[ "$1" == "-h" ]]; then
  echo "usage1: ./run_graph.sh run_all graph graph_args"
  echo "usage2: ./run_graph.sh ... (see below)"
  ../bin/graph -h
  exit
elif [ "$#" -ne 7 ]; then
  echo "invalid command!"
  echo "usage1: ./run_graph.sh run_all graph graph_args"
  echo "usage2: ./run_graph.sh ... (see below)"
  ../bin/graph -h
  exit
fi

#-h,--help                   Print this help message and exit
#-g,--graph TEXT             select graph(SerialGraph, ParallelGraph, Tree, RandomDAG, MapReduce), default is SerialGraph
#-j,--job TEXT               select job(matmul, cudaflow_reduce), default is cudaflow_reduce
#-m,--mode INT=6             select version(1, 2, 3, ..., 7), default is 7
#-n,--matrix_size UINT       set matrix size NxN
#-a,--args INT ...           args for constructing a graph
#-t,--num_threads UINT       number of threads
#-s,--num_streams UINT       number of streams

echo "start running..."

if [ "$RUN_ALL" = true ]; then
  NUM_THREADS=(4)
  NUM_STREMAS=(4 32)
  NUM_TASKS=(1 2 4 8 16 32 64 128 256 512 1024)
  CHAIN_SIZE=(1)

  # 1ms 5ms 10ms 50ms
  CPU_NS=(1 5 10 50)
  GPU_NS=(1 5 10 50)
  TIMES=3


  for nt in ${NUM_THREADS[@]}; do
    for nta in ${NUM_TASKS[@]}; do
      for cs in ${CHAIN_SIZE[@]}; do
        for cn in ${CPU_NS[@]}; do
          for gn in ${GPU_NS[@]}; do
            for ((k=1; k<=$TIMES; ++k)); do
              ../bin/independent 2 $nt $nta $cs $cn $gn
            done
          done
        done
      done
    done
  done

else

  for ((k=1; k<=$7; ++k)); do
    ../bin/independent $1 $2 $3 $4 $5 $6
  done
  
fi

