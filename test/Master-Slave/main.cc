#include "master.h"

#include "iostream"

#define ZOOKEEPER_MASTER "127.0.0.1:2181"
#define ZOOKEEPER_WORKER "127.0.0.1:2182"
#define ZOOKEEPER_CLIENT "127.0.0.1:2183"

int main() {
  Master master;

  if (!master.init(ZOOKEEPER_MASTER)) {
    std::cout << "Master init failed! " << std::endl;
    return 0;
  }

  //Run for master
  master.run_for_master();
}