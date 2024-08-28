#include "client.h"
#include "master.h"
#include "worker.h"

#include "iostream"

#define ZOOKEEPER_SERVER_CONN_STRING \
  "127.0.0.1:2181,127.0.0.1:2182,127.0.0.1:2183"

int main() {
  Master master;
  Worker worker1("worker1");
  Worker worker2("worker2");
  Client client;

  if (!master.init(ZOOKEEPER_SERVER_CONN_STRING)) {
    std::cout << "Master init failed! " << std::endl;
    return 0;
  }
  if (!worker1.init(ZOOKEEPER_SERVER_CONN_STRING)) {
    std::cout << "worker init failed! " << std::endl;
    return 0;
  }
  if (!worker2.init(ZOOKEEPER_SERVER_CONN_STRING)) {
    std::cout << "worker init failed! " << std::endl;
    return 0;
  }
  if (!client.init(ZOOKEEPER_SERVER_CONN_STRING)) {
    std::cout << "client init failed! " << std::endl;
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

  // Submit for task
  std::string task = "echo";
  client.submit_task(task);

  std::string status = client.task_wait();
  std::cout << task << " Execution Results : " << status << std::endl;

  return 0;
}