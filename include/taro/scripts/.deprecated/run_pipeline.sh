#!/bin/bash
RUN_ALL=false
if [[ "$1" == "run_all" ]]; then
  RUN_ALL=true
elif [[ "$1" == "-h" ]]; then
  echo "usage1: ./run.sh run_all"
  echo "usage2: ./run.sh num_threads chain_size num_pipes num_lines cpu_ns gpu_ns times"
  exit
elif [ "$#" -ne 7 ]; then
  echo "invalid command!"
  echo "usage1: ./run.sh run_all"
  echo "usage2: ./run.sh num_threads chain_size num_pipes num_lines cpu_ns gpu_ns times"
  exit
fi

echo "start running..."

if [ "$RUN_ALL" = true ]; then
  NUM_THREADS=(4 2 1)
  CHAIN_SIZE=(1 2 4 8)
  NUM_PIPES=(1 2 4 8 16 32)
  NUM_LINES=(1 2 4 8 16 32)

  # 1macros 5macros 10macros 50macros 100macros 500macros 1ms 5ms 10ms 50ms 100ms 500ms
  CPU_NS=(1000 5000 10000 50000 100000 500000 1000000 5000000 10000000 50000000 100000000 500000000)
  GPU_NS=(1000 5000 10000 50000 100000 500000 1000000 5000000 10000000 50000000 100000000 500000000)
  TIMES=3


  for nt in ${NUM_THREADS[@]}; do
    for cs in ${CHAIN_SIZE[@]}; do
      for np in ${NUM_PIPES[@]}; do
        for nl in ${NUM_LINES[@]}; do
          for cn in ${CPU_NS[@]}; do
            for gn in ${GPU_NS[@]}; do
              for ((k=1; k<=$TIMES; ++k)); do
              ../bin/pipeline $nt $cs $np $nl $cn $gn
              done
            done
          done
        done
      done
    done
  done

else

  for ((k=1; k<=$7; ++k)); do
    ../bin/pipeline $1 $2 $3 $4 $5 $6
  done
  
fi

