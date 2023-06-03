#pragma once

#include "../base/graph_base.hpp"
#include <vector>
#include <chrono>

//O---O---O.....---O---O---O
//
//---------------------------------------------------------------------------------------------------------------
//SerialGraph
//---------------------------------------------------------------------------------------------------------------

class SerialGraph: public Graph {

  public:
    
    SerialGraph(int num_nodes);

    ~SerialGraph();

};

//---------------------------------------------------------------------------------------------------------------
//Definition of SerialGraph
//---------------------------------------------------------------------------------------------------------------
SerialGraph::SerialGraph(int num_nodes): 
  Graph{num_nodes}
{
  _num_nodes = num_nodes;
  _graph.resize(num_nodes);

  for(int l = 0; l < num_nodes; ++l) {
    std::vector<size_t> out_nodes(1, 0);
    _graph[l].emplace_back(l, 0, out_nodes);
  }

  //allocate_nodes();
}

SerialGraph::~SerialGraph() {
  free_nodes();
}
