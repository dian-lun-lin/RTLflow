#!/bin/bash

# ParallelGraph

if [[ "$1" == "-h" ]]; then
  echo "usage: ./run_graph.sh graph graph_args CPU_overhead GPU_overhead"
  echo ""
  echo "graph: task graph to run (ParallelGraph SerialGraph Wavefront Tree Diamond RandomDAG)"
  echo "graph_args: ParallelGraph SerialGraph (#nodes), Wavefront (#pipes), Tree (#degree, #levels)"
  echo "CPU_overhead: CPU overhead for each task"
  echo "GPU_overhead: GPU overhead for each task"
  exit
elif [ "$#" -ne 4 ]; then
  echo "invalid command!"
  echo "usage: ./run_graph.sh graph graph_args CPU_overhead GPU_overhead"
  echo ""
  echo "graph: task graph to run (ParallelGraph SerialGraph Wavefront Tree Diamond RandomDAG)"
  echo "graph_args: ParallelGraph SerialGraph (#nodes), Wavefront (#pipes), Tree (#degree, #levels)"
  echo "CPU_overhead: CPU overhead for each task"
  echo "GPU_overhead: GPU overhead for each task"
  exit
  exit
fi

echo "start running..."

NUM_THREADS=(4)
NUM_STREAMS=(4)
#MODE=("cudaflow" "fiber" "taro_callback_v1" "taro_callback_v2" "taro_callback_v3" "taro_poll_v1" "taro_callback_taskflow")
MODE=("cudaflow" "fiber" "taro_callback_v1" "taro_callback_v3" "taro_poll_v1" "taro_callback_taskflow")
GRAPH=$1
GRAPH_ARGS=$2
CPU_OVERHEAD=$3
GPU_OVERHEAD=$4
BENCHMARK="loop"

# 1ms 5ms 10ms 50ms
#CPU_NS=(1 5 10 50)
#GPU_NS=(1 5 10 50)
TIMES=3
echo "=========================="
echo "graph: $GRAPH, graph args: $GRAPH_ARGS, benchmark: $BENCHMARK, CPU overhead: $CPU_OVERHEAD, GPU overhead: $GPU_OVERHEAD"
echo ""
for m in ${MODE[@]}; do
  echo "=== mode: $m" 
  for nt in ${NUM_THREADS[@]}; do
    if [[ "$m" == "cudaflow" ]]; then
      echo "#threads: $nt, #streams: default"
      for ((k=1; k<=$TIMES; ++k)); do
        perf stat ../../benchmarks/graph_$m -g $GRAPH --graph_args $GRAPH_ARGS -b $BENCHMARK --benchmark_args $CPU_OVERHEAD $GPU_OVERHEAD -t $nt
      done
      echo "#####################"
    elif [[ "$m" == "taro_callback_v2" ]]; then
      echo "#threads: $nt, #streams: increase by algorithm"
      for ((k=1; k<=$TIMES; ++k)); do
        perf stat ../../benchmarks/graph_$m -g $GRAPH --graph_args $GRAPH_ARGS -b $BENCHMARK --benchmark_args $CPU_OVERHEAD $GPU_OVERHEAD -t $nt
      done
      echo "#####################"
    else
      for ns in ${NUM_STREAMS[@]}; do
        echo "#threads: $nt, #streams: $ns"
        for ((k=1; k<=$TIMES; ++k)); do
          perf stat ../../benchmarks/graph_$m -g $GRAPH --graph_args $GRAPH_ARGS -b $BENCHMARK --benchmark_args $CPU_OVERHEAD $GPU_OVERHEAD -t $nt -s $ns
        done
      done
      echo "#####################"
    fi
  done
done
