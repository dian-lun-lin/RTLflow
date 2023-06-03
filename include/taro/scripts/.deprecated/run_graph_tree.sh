#!/bin/bash

# ParallelGraph

if [[ "$1" == "-h" ]]; then
  echo "usage: ./run_graph_2.sh job #degrees #levels"
  echo "job -> select job(matmul, cudaflow_reduce, sleep)"
  echo "#degrees -> number of degrees at each level"
  echo "#levels -> number of levels"
  exit
elif [ "$#" -ne 3 ]; then
  echo "invalid command!"
  echo "usage: ./run_graph_2.sh job #degrees #levels"
  echo "job -> select job(matmul, cudaflow_reduce, sleep)"
  echo "#degrees -> number of degrees at each level"
  echo "#levels -> number of levels"
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

NUM_THREADS=(2 4)
NUM_STREAMS=(2 4 32)
NUM_TASKS=(1000)
MODE=("TaroCBV1" "TaroCBV2" "TaroPV1" "cudaFlow")

# 1ms 5ms 10ms 50ms
#CPU_NS=(1 5 10 50)
#GPU_NS=(1 5 10 50)
TIMES=3

for m in ${MODE[@]}; do
  for nt in ${NUM_THREADS[@]}; do
    if [[ "$m" == "cudaFlow" ]]; then
      echo "mode: $m, #threads: $nt, #streams: default, job: $1, #degrees: $2, #levels: $3"
      for ((k=1; k<=$TIMES; ++k)); do
        ../bin/graph -j $1 -a $2 $3 -g Tree -m $m -t $nt -s $ns
      done
    elif [[ "$m" == "TaroCBV2" ]]; then
      echo "mode: $m, #threads: $nt, #streams: increase by algorithm, job: $1, #degrees: $2, #levels: $3"
      for ((k=1; k<=$TIMES; ++k)); do
        ../bin/graph -j $1 -a $2 $3 -g Tree -m $m -t $nt -s $ns
      done
      echo " "
    else
      for ns in ${NUM_STREAMS[@]}; do
        echo "mode: $m, #threads: $nt, #streams: $ns, job: $1, #degrees: $2, #levels: $3"
        for ((k=1; k<=$TIMES; ++k)); do
          ../bin/graph -j $1 -a $2 $3 -g Tree -m $m -t $nt -s $ns
        done
        echo " "
      done
    fi
  done
  echo "#####################"
done
