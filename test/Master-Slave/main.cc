#include "master.h"
#include "worker.h"

#include "iostream"

#define ZOOKEEPER_MASTER "127.0.0.1:2181"
#define ZOOKEEPER_WORKER "127.0.0.1:2182"
#define ZOOKEEPER_CLIENT "127.0.0.1:2183"

int main() {
  Master master;
  Worker worker1("worker1");
  Worker worker2("worker2");

  if (!master.init(ZOOKEEPER_MASTER)) {
    std::cout << "Master init failed! " << std::endl;
    return 0;
  }

  //Run for master
  if (!master.run_for_master()) {
    std::cout << "run_for_master failer!" << std::endl;
    return 0;
  }

  // Register for master
  if (!worker1.Register()) {
    std::cout << worker1.getname() << " failer!" << std::endl;
    return 0;
  }
  if (!worker2.Register()) {
    std::cout << worker2.getname() << " failer!" << std::endl;
    return 0;
  }
}