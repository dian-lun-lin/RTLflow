#include <iostream>
#include <vector>
#include "scheduler.hpp"

Task task(Scheduler& sched, std::vector<size_t>& res) {
  std::cout << "Start execution...\n";

  co_await sched.suspend();

  std::vector<size_t> a(res.size()), b(res.size());
  for(size_t i = 0; i < 1000000; ++i) {
    std::iota(a.begin(), a.end(), i);
    std::iota(b.begin(), b.end(), i + 1);
    std::transform(a.begin(), a.end(), b.begin(), res.begin(),std::plus<size_t>());
    co_await sched.suspend();
  }

  std::cout << "Finish\n";
}


int main() {

  Scheduler sched(2);
  std::vector<size_t> res(1000);

  task(sched, res);

  sched.schedule();
}


