#!/bin/bash
RUN_ALL=false
if [[ "$1" == "run_all" ]]; then
  RUN_ALL=true
elif [[ "$1" == "-h" ]]; then
  echo "usage1: ./run_independent.sh run_all"
  echo "usage2: ./run_independent.sh mode num_threads num_tasks chain_size cpu_ms gpu_ms times";
  echo "mode: 0 (task), 1 (coroutine), 2 (both)"
  exit
elif [ "$#" -ne 7 ]; then
  echo "invalid command!"
  echo "usage1: ./run_independent.sh run_all"
  echo "usage2: ./run_independent.sh mode num_threads num_tasks chain_size cpu_ms gpu_ms times";
  exit
fi

echo "start running..."

if [ "$RUN_ALL" = true ]; then
  NUM_THREADS=(4 2 1)
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

