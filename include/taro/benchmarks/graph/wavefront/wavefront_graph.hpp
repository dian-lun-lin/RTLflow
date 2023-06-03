#pragma once

#include "../base/graph_base.hpp"
#include <vector>
#include <chrono>

//     o
//    / \
//   o   o
//  / \ / \
// o   o   o
//  \ / \ /
//   o   o
//    \ / 
//     o
//
//     |
//     V
//  max_parallel_node
//
//---------------------------------------------------------------------------------------------------------------
//
// WavefrontGraph
//
//---------------------------------------------------------------------------------------------------------------

class WavefrontGraph: public Graph {

  public:
    
    WavefrontGraph(int max_parallel_node);

    ~WavefrontGraph();

};

//---------------------------------------------------------------------------------------------------------------
// Definition of WavefrontGraph
//---------------------------------------------------------------------------------------------------------------

WavefrontGraph::WavefrontGraph(int max_parallel_node): 
  Graph{2 * max_parallel_node - 1}
{
  _num_nodes = max_parallel_node * max_parallel_node;

  _graph.resize(2 * max_parallel_node - 1);

  for(int l = 0; l < _level; ++l) {
    size_t id{0};
    size_t base{0};

    size_t cur_level_num_nodes = std::min(l + 1, _level - (l + 1) + 1);

    std::vector<Node> cur_level_nodes;
    cur_level_nodes.reserve(cur_level_num_nodes);

    if(l != _level - 1) {
      if(l < _level - (l + 1)) {
        // first half
        for(size_t n = 0; n < cur_level_num_nodes; ++n) {
          std::vector<size_t> out_nodes(2);
          out_nodes[0] = base + n;
          out_nodes[1] = base + n + 1;
          cur_level_nodes.emplace_back(l, id++, out_nodes);
        }
      }
      else {
        // second half
        
        // first node
        std::vector<size_t> out_nodes_1(1);
        out_nodes_1[0] = base;
        cur_level_nodes.emplace_back(l, id++, out_nodes_1);

        // other nodes
        for(size_t n = 1; n < cur_level_num_nodes - 1; ++n) {
          std::vector<size_t> out_nodes(2);
          out_nodes[0] = base + n - 1;
          out_nodes[1] = base + n;
          cur_level_nodes.emplace_back(l, id++, out_nodes);
        }

        // last node
        std::vector<size_t> out_nodes_2(1);
        out_nodes_2[0] = base + cur_level_num_nodes - 2;
        cur_level_nodes.emplace_back(l, id++, out_nodes_2);
      }
    }
    else {
      std::vector<size_t> empty_out_nodes;
      cur_level_nodes.emplace_back(l, id++, empty_out_nodes);
    }


    _graph[l] = std::move(cur_level_nodes);
  }

}

WavefrontGraph::~WavefrontGraph() {
  free_nodes();
}
