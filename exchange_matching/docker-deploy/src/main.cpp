#include <cstdlib>
#include <iostream>
#include <string>

#include "account.h"
#include "symbol.h"
#include "utils.h"
// #include "pugixml.hpp"
using namespace std;

int main() {
  // Create DB
  PGconn *conn = init_db();
  if (!conn) {
    cerr << "Error: fail to initialize database connection" << endl;
    return EXIT_FAILURE;
  }
  remove_all_tables(conn);
  create_tables(conn);
  PQfinish(conn);
  const char *port = "12345";
  int server_fd = server_setup(port);
  while (true) {
    std::string client_ip;
    int client_fd = server_accept(server_fd, &client_ip);
    std::cout << "Connection from: " << client_ip << std::endl;

    // Create a new thread for each client connection
    pthread_t thread_id;
    ClientData *data = new ClientData{client_fd, client_ip};
    int rc = pthread_create(&thread_id, NULL, process_client, data);
    if (rc) {
      std::cerr << "Error: unable to create thread, " << rc << std::endl;
      close(client_fd);
      delete data;
    } else {
      pthread_detach(thread_id); // Detach the thread to release resources
                                    // automatically when it's done
    }
  }
}