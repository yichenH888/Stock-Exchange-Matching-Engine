#include <arpa/inet.h>
#include <netdb.h>
#include <postgresql/libpq-fe.h>
#include <sys/socket.h>
#include <unistd.h>
#include <stdio.h>
#include <stdlib.h>
#include <postgresql/postgres_ext.h>
#include <cstring>
#include <iostream>
#include <ctime>
#include <sstream>
#include <string>
#include <unordered_map>
#include <utility>
#include <vector>
#include <errno.h>
#include <pthread.h>


using namespace std;

struct Order {
    string symbol;
    double amount;
    double limit;
};

struct ClientData {
  int client_fd;
  std::string client_ip;
};


int server_setup(const char *port);
int client_setup(const char *hostname, const char *port);
int server_accept(int socket_fd, string *ip);
string receive_complete_message(int sender_fd, string &sender_message, int content_len);
int parse_xml_data(std::vector<std::pair<std::string, std::string>> &storage,
                   const std::string &xml_data, int &account_id);

string handle_add_account(PGconn *conn, string key, string val);
string handle_add_symbol(PGconn *conn, string key, string val);
string handle_order(PGconn *conn, string key, string val);
string handle_cancel(PGconn *conn, string key, string val, int transaction_account_id);
string handle_query(PGconn *conn, string key, string val);
void print_db(PGconn *conn);

Order parse_order_val(const std::string val);
string OpenOrder(PGconn *conn, int &order_id, int account_id, int symbol_id, Order order, std::string order_type, int timestamp);
vector<std::pair<int, double>> MatchOrder(PGconn *conn, int order_id, std::string order_type, int symbol_id, double amount, int account_id);
std::string ExecuteOrder(PGconn *conn, std::pair<int, double> matched_order, int order_id, int account_id, string order_type, int last_flag, Order inOrder);
string create_canceled_tag(int shares, double cancel_timestamp);
string create_executed_tag(int shares, double price, double execution_timestamp);
std::string create_xml_response(const std::vector<std::string>& results);
void send_xml_response(string xml_response, int receiver_fd);
int insert_transaction(PGconn *conn, int transaction_account_id);
void create_tables(PGconn *conn);
void remove_all_tables(PGconn *conn);
PGconn * init_db();
void *process_client(void *arg);
int receive_content_len(int client_fd);
int check_account_exists(int account_id, PGconn *conn);