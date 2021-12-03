//
// Created by jaime on 6/25/2021.
//

#include <hermes/adapter/pubsub.h>
int main(int argc, char **argv) {
  hermes::pubsub::mpiInit(argc, argv);

  char *config_file = 0;
  if (argc == 2) {
    config_file = argv[1];
  }
  else{
    config_file = getenv(kHermesConf);
  }

  auto connect_ret = hermes::pubsub::connect(config_file, false);
  assert(connect_ret.Succeeded());

  auto attach_ret = hermes::pubsub::attach("test");
  assert(attach_ret.Succeeded());

  auto detach_ret = hermes::pubsub::detach("test");
  assert(detach_ret.Succeeded());

  auto disconnect_ret = hermes::pubsub::disconnect();
  assert(disconnect_ret.Succeeded());
}