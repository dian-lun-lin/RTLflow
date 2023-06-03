#pragma once

#include "../base/graph_base.hpp"
#include <vector>
#include <chrono>

//  O O... O
//
//---------------------------------------------------------------------------------------------------------------
//ParallelGraph
//---------------------------------------------------------------------------------------------------------------

class ParallelGraph: public Graph {

  public:

    ParallelGraph(int num_nodes);

    ~ParallelGraph();

};

//---------------------------------------------------------------------------------------------------------------
//Definition of ParallelGraph
//---------------------------------------------------------------------------------------------------------------

ParallelGraph::ParallelGraph(int num_nodes):
  Graph{1}
{
  _graph.resize(1);
  _num_nodes = num_nodes;

  //graph
  std::vector<size_t> empty_out_node;

  for(int k = 0; k < _num_nodes; ++k) {
    _graph[0].emplace_back(0, k, empty_out_node);
  }

  //allocate_nodes();
}

ParallelGraph::~ParallelGraph() {
  free_nodes();
}

