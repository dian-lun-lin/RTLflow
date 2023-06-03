#pragma once
#include "../graph.hpp"
#include <3rd-party/CLI11/CLI11.hpp>

struct Configure {

  public:

    Configure(int argc, char** argv);

    Graph* g_ptr;
    std::string benchmark{"loop"};
    std::vector<int> benchmark_args;
    std::vector<int> graph_args;
    size_t num_threads;
    size_t num_streams{0};
    int status{0};

  private:

    CLI::App _app{"Graph Benchmark"};

    int _set(int argc, char** argv);

};


int Configure::_set(int argc, char** argv) {
  std::string graph{"ParallelGraph"};
  _app.add_option(
    "-g, --graph", 
    graph, 
    "select graph(SerialGraph, ParallelGraph, Tree, RandomDAG, MapReduce, Wavefront), default is ParallelGraph" 
  );

  _app.add_option(
    "-b, --benchmark",
    benchmark,
    "select benchmark(loop, data), default is loop"
  );

  _app.add_option(
    "--benchmark_args",
    benchmark_args,
    "args for a benchmark\n \
     loop: (CPU time per task, GPU time per task)\n \
     data:(data size per task)"
  );

  _app.add_option(
    "--graph_args",
    graph_args,
    "args for constructing a graph"
  );

  _app.add_option(
    "-t, --num_threads",
    num_threads,
    "number of threads"
  );

  _app.add_option(
    "-s, --num_streams",
    num_streams,
    "number of streams to run. ignore this arg if using TaroCBV2"
  );

  CLI11_PARSE(_app, argc, argv);

  if(graph == "SerialGraph") {
    assert(graph_args.size() == 1);
    g_ptr = new SerialGraph(graph_args[0]);
  }
  else if(graph == "ParallelGraph") {
    assert(graph_args.size() == 1);
    g_ptr = new ParallelGraph(graph_args[0]);
  }
  else if(graph == "Tree") {
    assert(graph_args.size() == 2);
    g_ptr = new Tree(graph_args[0], graph_args[1]);
  }
  else if(graph == "RandomDAG") {
    assert(graph_args.size() == 3);
    g_ptr = new RandomDAG(graph_args[0], graph_args[1], graph_args[2]);
  }
  else if(graph == "MapReduce") {
    assert(graph_args.size() == 2);
    g_ptr = new Diamond(graph_args[0], graph_args[1]);
  }
  else if(graph == "Wavefront") {
    assert(graph_args.size() == 1);
    g_ptr = new WavefrontGraph(graph_args[0]);
  }
  else {
    throw std::runtime_error("No such graph\n");
  }

  return 0;
}

Configure::Configure(int argc, char** argv) {
  status = _set(argc, argv);
}
