#include "utils.h"

#include <sys/socket.h>

#include "pugixml.hpp"

using namespace std;
double left_amount = 0;
// Code derived from Prof. Rogers TCP example
int server_setup(const char *port) {
  int status;
  int socket_fd;
  struct addrinfo host_info;
  struct addrinfo *host_info_list;
  const char *hostname = NULL;

  memset(&host_info, 0, sizeof(host_info));

  host_info.ai_family = AF_UNSPEC;
  host_info.ai_socktype = SOCK_STREAM;
  host_info.ai_flags = AI_PASSIVE;

  status = getaddrinfo(hostname, port, &host_info, &host_info_list);
  if (status != 0) {
    cerr << "Error: cannot get address info for host" << endl;
    cerr << "  (" << hostname << "," << port << ")" << endl;
    return -1;
  } // if

  socket_fd = socket(host_info_list->ai_family, host_info_list->ai_socktype,
                     host_info_list->ai_protocol);
  if (socket_fd == -1) {
    cerr << "Error: cannot create socket" << endl;
    cerr << "  (" << hostname << "," << port << ")" << endl;
    return -1;
  } // if

  int yes = 1;
  status = setsockopt(socket_fd, SOL_SOCKET, SO_REUSEADDR, &yes, sizeof(int));
  status = bind(socket_fd, host_info_list->ai_addr, host_info_list->ai_addrlen);
  if (status == -1) {
    cerr << "Error: cannot bind socket" << endl;
    cerr << "  (" << hostname << "," << port << ")" << endl;
    return -1;
  } // if

  status = listen(socket_fd, 100);
  if (status == -1) {
    cerr << "Error: cannot listen on socket" << endl;
    cerr << "  (" << hostname << "," << port << ")" << endl;
    return -1;
  } // if
  freeaddrinfo(host_info_list);
  cout << "proxy setup success" << endl;
  return socket_fd;
}

// Code derived from Prof. Rogers TCP example
int client_setup(const char *hostname, const char *port) {
  int status;
  int socket_fd;
  struct addrinfo host_info;
  struct addrinfo *host_info_list;

  memset(&host_info, 0, sizeof(host_info));
  host_info.ai_family = AF_UNSPEC;
  host_info.ai_socktype = SOCK_STREAM;

  status = getaddrinfo(hostname, port, &host_info, &host_info_list);
  if (status != 0) {
    cerr << "Error: cannot get address info for host" << endl;
    cerr << "  (" << hostname << ", " << port << ")" << endl;
    return -1;
  } // if

  socket_fd = socket(host_info_list->ai_family, host_info_list->ai_socktype,
                     host_info_list->ai_protocol);
  if (socket_fd == -1) {
    cerr << "Error: cannot create socket" << endl;
    cerr << "  (" << hostname << ", " << port << ")" << endl;
    return -1;
  } // if

  status =
      connect(socket_fd, host_info_list->ai_addr, host_info_list->ai_addrlen);
  if (status == -1) {
    cerr << "Error: cannot connect to socket" << endl;
    cerr << "  (" << hostname << ", " << port << ")" << endl;
    return -1;
  } // if

  freeaddrinfo(host_info_list);
  cout << "connect server success" << endl;
  return socket_fd;
}

// Code derived from Prof. Rogers TCP example, updated by the group
int server_accept(int socket_fd, string *ip) {
  struct sockaddr_storage socket_addr;
  socklen_t socket_addr_len = sizeof(socket_addr);
  int client_connection_fd;
  client_connection_fd =
      accept(socket_fd, (struct sockaddr *)&socket_addr, &socket_addr_len);
  if (client_connection_fd == -1) {
    cerr << "Error: cannot accept connection on socket" << endl;
    return -1;
  } // if
  struct sockaddr_in *addr = (struct sockaddr_in *)&socket_addr;
  *ip = inet_ntoa(addr->sin_addr);
  cout << "connect client success" << endl;
  return client_connection_fd;
}

// Code derived from Proj 2
string receive_complete_message(int sender_fd, string &sender_message,
                                int content_len) {
  int cum_len = 0, received_len = 0;

  for (;;) {
    if (cum_len >= content_len)
      break;
    char buffer[100000] = {0};
    if ((received_len =
             recv(sender_fd, &buffer, sizeof(buffer), MSG_NOSIGNAL)) <= 0) {
      break;
    }
    string buffer_string(buffer, received_len);
    sender_message += buffer_string;
    cum_len += received_len;
  }
  return sender_message;
}

int parse_xml_data(std::vector<std::pair<std::string, std::string>> &storage,
                   const std::string &xml_data, int &account_id) {
  pugi::xml_document doc;
  pugi::xml_parse_result result = doc.load_string(xml_data.c_str());

  if (!result) {
    std::cerr << "Error parsing XML: " << result.description() << std::endl;
    return -1;
  }

  pugi::xml_node root_node = doc.first_child();

  std::string root_name = root_node.name();

  auto find_key = [&](const std::string &key)
      -> const std::pair<std::string, std::string> * {
    for (const auto &pair : storage) {
      if (pair.first == key) {
        return &pair;
      }
    }
    return nullptr;
  };

  if (root_name == "create") {
    for (pugi::xml_node child = root_node.first_child(); child;
         child = child.next_sibling()) {
      std::string node_name = child.name();

      if (node_name == "account") {
        int account_id = child.attribute("id").as_int();
        std::string key = "account_" + std::to_string(account_id);

        // if (find_key(key) != nullptr) {
        //   std::cerr << "Error: Account with id " << account_id
        //             << " already exists." << std::endl;
        //   return -1;
        // }

        double balance = child.attribute("balance").as_double();
        storage.emplace_back(key, std::to_string(balance));

      } else if (node_name == "symbol") {
        std::string symbol_str = child.attribute("sym").as_string();
        std::string key = "symbol_" + symbol_str;

        std::string value = "";
        for (pugi::xml_node account_node = child.child("account"); account_node;
             account_node = account_node.next_sibling("account")) {
          int account_id = account_node.attribute("id").as_int();
          int num_shares = account_node.text().as_int();
          value += std::to_string(account_id) + ": " +
                   std::to_string(num_shares) + ", ";
        }

        // Remove the trailing comma and space from the value string
        if (!value.empty()) {
          value.pop_back();
          value.pop_back();
        }

        storage.emplace_back(key, value);

      } else {
        std::cerr << "Error: Invalid child node <" << node_name
                  << "> in <create> node." << std::endl;
        return -1;
      }
    }
    return 1;
  } else if (root_name == "transactions") {
    account_id = root_node.attribute("id").as_int();

    for (pugi::xml_node child = root_node.first_child(); child;
         child = child.next_sibling()) {
      std::string node_name = child.name();

      if (node_name == "order") {
        std::string sym = child.attribute("sym").as_string();
        int amount = child.attribute("amount").as_int();
        double limit = child.attribute("limit").as_double();

        std::string key = "order_" + std::to_string(account_id);
        std::string value = "sym_" + sym + "_amount_" + std::to_string(amount) +
                            "_limit_" + std::to_string(limit);

        storage.emplace_back(key, value);
      } else if (node_name == "query") {
        std::string key = "query";
        int id = child.attribute("id").as_int();

        storage.emplace_back(key, std::to_string(id));
      } else if (node_name == "cancel") {
        std::string key = "cancel";
        int id = child.attribute("id").as_int();

        storage.emplace_back(key, std::to_string(id));
      } else {
        std::cerr << "Error: Invalid child node <" << node_name
                  << "> in <transactions> node." << std::endl;
        return -1;
      }
    }
    return 0;
  } else {
    std::cerr << "Error: XML does not have a valid top-level node. Expected "
                 "<create> or <transactions>."
              << std::endl;
    return -1;
  }
}

string handle_add_account(PGconn *conn, string key, string val) {
  // Start a transaction
  PGresult *result = PQexec(conn, "BEGIN;");
  while (true) {
    if (PQresultStatus(result) == PGRES_COMMAND_OK) {
      break;
    }
    cout << "Fail to begin transaction, retrying ..." << endl;
    PQclear(result);
    result = PQexec(conn, "BEGIN;");
  }
  PQclear(result);

  // Extract the account_id from the key
  int account_id = std::stoi(key.substr(8));

  // Check if the account_id already exists in the table
  std::string query = "SELECT COUNT(*) FROM accounts WHERE account_id = " +
                      std::to_string(account_id) + ";";
  result = PQexec(conn, query.c_str());

  if (PQresultStatus(result) != PGRES_TUPLES_OK) {
    std::string error_message = PQerrorMessage(conn);
    PQclear(result);
    PQexec(conn, "ROLLBACK;");
    return "<error id=\"" + std::to_string(account_id) +
           "\">Error executing query: " + error_message + "</error>";
  }

  int count = std::stoi(PQgetvalue(result, 0, 0));
  PQclear(result);

  if (count > 0) {
    // Account already exists, return error message
    PQexec(conn, "ROLLBACK;");
    return "<error id=\"" + std::to_string(account_id) +
           "\">Account already exists</error>";
  }

  // Insert the account into the table
  query = "INSERT INTO accounts (account_id, balance) VALUES (" +
          std::to_string(account_id) + ", " + val + ");";
  result = PQexec(conn, query.c_str());
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    std::string error_message = PQerrorMessage(conn);
    PQclear(result);
    PQexec(conn, "ROLLBACK;");
    return "<error id=\"" + std::to_string(account_id) +
           "\">Error adding account: " + error_message + "</error>";
  }

  PQclear(result);

  // Commit the transaction
  PQexec(conn, "COMMIT;");

  // Return the created message
  return "<created id=\"" + std::to_string(account_id) + "\"/>";
}

std::vector<std::pair<int, double>> parse_symbol_val(const std::string &val) {
  std::vector<std::pair<int, double>> account_positions;
  std::istringstream iss(val);
  std::string token;

  while (std::getline(iss, token, ',')) {
    int account_id = std::stoi(token.substr(0, token.find(':')));
    double amount = std::stod(token.substr(token.find(':') + 1));
    account_positions.push_back(std::make_pair(account_id, amount));
  }

  return account_positions;
}

string handle_add_symbol(PGconn *conn, string key, string val) {
  // Start a transaction
  PGresult *result = PQexec(conn, "BEGIN;");
  while (true) {
    if (PQresultStatus(result) == PGRES_COMMAND_OK) {
      break;
    }
    cout << "Fail to begin transaction, retrying ..." << endl;
    PQclear(result);
    result = PQexec(conn, "BEGIN;");
  }
  PQclear(result);

  // Extract the symbol from the key
  std::string symbol = key.substr(7);
  // Parse the val string to extract account_id and amount values
  std::vector<std::pair<int, double>> account_positions = parse_symbol_val(val);

  // Check if the symbol already exists in the symbols table
  std::string query = "SELECT symbol_id FROM symbols WHERE symbol = '" +
                      symbol + "' FOR UPDATE;";
  result = PQexec(conn, query.c_str());

  int symbol_id;

  if (PQresultStatus(result) == PGRES_TUPLES_OK && PQntuples(result) > 0) {
    // Symbol exists
    symbol_id = std::stoi(PQgetvalue(result, 0, 0));
  } else {
    // Symbol does not exist, insert it into the symbols table
    PQclear(result);
    query = "INSERT INTO symbols (symbol) VALUES ('" + symbol +
            "') RETURNING symbol_id;";
    result = PQexec(conn, query.c_str());

    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
      PQclear(result);
      PQexec(conn, "ROLLBACK;");
      return "<error sym=\"" + symbol + "\">Error adding symbol</error>";
    }

    symbol_id = std::stoi(PQgetvalue(result, 0, 0));
  }

  PQclear(result);

  // Update the positions table for each account and return the created messages
  std::stringstream response;

  for (const auto &[account_id, amount] : account_positions) {
    query = "INSERT INTO positions (account_id, symbol_id, amount) VALUES (" +
            std::to_string(account_id) + ", " + std::to_string(symbol_id) +
            ", " + std::to_string(amount) +
            ") "
            "ON CONFLICT (account_id, symbol_id) DO UPDATE SET amount = "
            "positions.amount + EXCLUDED.amount "
            "WHERE positions.account_id = " +
            std::to_string(account_id) + ";";
    result = PQexec(conn, query.c_str());

    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      PQclear(result);
      PQexec(conn, "ROLLBACK;");
      response << "<error sym=\"" + symbol + "\" id=\"" +
             std::to_string(account_id) + "\">Account does not exist</error>";
      continue;
    }

    PQclear(result);

    response << "<created sym=\"" << symbol << "\" id=\"" << account_id
             << "\"/>\n";
  }

  // Commit the transaction
  PQexec(conn, "COMMIT;");

  return response.str();
}

Order parse_order_val(const std::string val) {
  // Find the positions of the underscores
  size_t symPos = val.find("sym_");
  size_t amountPos = val.find("_amount_");
  size_t limitPos = val.find("_limit_");

  // Extract the variables from the string using substr() function
  std::string SYM = val.substr(symPos + 4, amountPos - symPos - 4);
  double AMOUNT =
      std::stod(val.substr(amountPos + 8, limitPos - amountPos - 8));
  double LIMIT = std::stod(val.substr(limitPos + 7));

  Order order = {SYM, AMOUNT, LIMIT};

  return order;
}

string OpenOrder(PGconn *conn, int &order_id, int account_id, int symbol_id,
                 Order order, std::string order_type, int timestamp) {
  string response;

  // Start a transaction
  PGresult *result = PQexec(conn, "BEGIN;");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    PQclear(result);
    return "<error id=\"" + std::to_string(account_id) +
           "\">Failed to start transaction</error>";
  }
  PQclear(result);

  std::string query = "INSERT INTO orders (account_id, symbol_id, order_type, "
                      "amount, limit_price, order_status, timestamp) "
                      "VALUES (" +
                      std::to_string(account_id) + ", " +
                      std::to_string(symbol_id) + ", '" + order_type + "', " +
                      std::to_string(order.amount) + ", " +
                      std::to_string(order.limit) + ", 'open', " +
                      std::to_string(timestamp) + ") RETURNING order_id;";
  result = PQexec(conn, query.c_str());

  if (PQresultStatus(result) != PGRES_TUPLES_OK) {
    PQclear(result);
    PQexec(conn, "ROLLBACK;");
    return "<error id=\"" + std::to_string(account_id) +
           "\">Error open order</error>";
  }

  order_id = std::stoi(PQgetvalue(result, 0, 0));
  PQclear(result);

  if (order_type == "Buy") {
    // update account balance
    query = "UPDATE accounts "
            "SET balance = balance - " +
            std::to_string(order.amount * order.limit) +
            " WHERE account_id = " + std::to_string(account_id) + ";";
    result = PQexec(conn, query.c_str());
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      PQclear(result);
      PQexec(conn, "ROLLBACK;");
      return "<error id=\"" + std::to_string(account_id) +
             "\">Error execute order</error>";
    }

    PQclear(result);
  } else {
    // update position amount
    query = "UPDATE positions "
            "SET amount = amount + " +
            std::to_string(order.amount) +
            " WHERE account_id = " + std::to_string(account_id) +
            " AND symbol_id = " + std::to_string(symbol_id) + ";";
    result = PQexec(conn, query.c_str());
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      PQclear(result);
      PQexec(conn, "ROLLBACK;");
      return "<error id=\"" + std::to_string(account_id) +
             "\">Error execute order</error>";
    }

    PQclear(result);
  }

  // Commit the transaction
  PQexec(conn, "COMMIT;");

  return response;
}

vector<std::pair<int, double>> MatchOrder(PGconn *conn, int order_id,
                                          std::string order_type, int symbol_id,
                                          double amount, int account_id) {
  // Start a transaction
  PGresult *result = PQexec(conn, "BEGIN;");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    PQclear(result);
    cerr << "error: Failed to start transaction" << endl;
    return {};
  }
  PQclear(result);

  // get all possible matched orders
  vector<int> matched_order_ids;
  vector<double> matched_order_amounts;
  std::vector<std::pair<int, double>> ret; // execute orders: id, amount
  std::string query;
  if (order_type == "Buy") {
    query = "SELECT order_id, amount "
            "FROM orders "
            "WHERE order_type = 'Sell' "
            "AND order_status = 'open' "
            "AND symbol_id = " +
            std::to_string(symbol_id) +
            " AND limit_price <= (SELECT limit_price FROM orders WHERE "
            "order_id = " +
            std::to_string(order_id) + ") " +
            "AND account_id != " + std::to_string(account_id) +
            " ORDER BY limit_price ASC, timestamp ASC FOR UPDATE;";
  } else {
    query = "SELECT order_id, amount "
            "FROM orders "
            "WHERE order_type = 'Buy' "
            "AND order_status = 'open' "
            "AND symbol_id = " +
            std::to_string(symbol_id) +
            " AND limit_price >= (SELECT limit_price FROM orders WHERE "
            "order_id = " +
            std::to_string(order_id) + ") " +
            "AND account_id != " + std::to_string(account_id) +
            " ORDER BY limit_price DESC, timestamp ASC FOR UPDATE;";
  }
  result = PQexec(conn, query.c_str());

  if (PQresultStatus(result) != PGRES_TUPLES_OK) {
    PQclear(result);
    PQexec(conn, "ROLLBACK;");
    cerr << "error: Unable to execute matched orders query" << endl;
    return ret;
  }
  int num_matched_orders = PQntuples(result);

  if (num_matched_orders == 0) { // no matched order
    PQexec(conn, "COMMIT;");
    return ret;
  }

  for (int i = 0; i < num_matched_orders; i++) {
    int order_id = std::stoi(PQgetvalue(result, i, 0));
    double order_amount = std::stoi(PQgetvalue(result, i, 1));
    matched_order_ids.push_back(order_id);
    matched_order_amounts.push_back(order_amount);
  }

  PQclear(result);

  // select based on amount
  int total_amount = amount;

  int i = 0;
  int left_amount = 0;
  while (total_amount * amount > 0 && i < num_matched_orders) {
    if (abs(total_amount) > abs(matched_order_amounts[i])) {
      ret.push_back(
          std::make_pair(matched_order_ids[i], matched_order_amounts[i]));
      total_amount += matched_order_amounts[i];
    } else {
      ret.push_back(std::make_pair(matched_order_ids[i], -total_amount));
      left_amount = abs(total_amount + matched_order_amounts[i]);
      total_amount = 0;
    }
    i++;
  }
  // Commit the transaction
  result = PQexec(conn, "COMMIT;");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    PQclear(result);
    cerr << "error: Failed to commit transaction" << endl;
    return {};
  }
  PQclear(result);

  return ret;
}

void print_db(PGconn *conn) {
  const char *table_names[] = {"symbols", "accounts",        "positions",
                               "orders",  "executed_orders", "canceled_orders"};

  PGresult *result = PQexec(conn, "BEGIN;");
  while (true) {
    if (PQresultStatus(result) == PGRES_COMMAND_OK) {
      break;
    }
    cout << "Fail to begin transaction, retrying ..." << endl;
    PQclear(result);
    result = PQexec(conn, "BEGIN;");
  }
  PQclear(result);
  for (const char *table_name : table_names) {
    std::string query = "SELECT * FROM " + std::string(table_name) + ";";
    PGresult *res = PQexec(conn, query.c_str());

    if (PQresultStatus(res) != PGRES_TUPLES_OK) {
      std::cerr << "Error fetching data from table " << table_name << ": "
                << PQerrorMessage(conn) << std::endl;
      PQclear(res);
      continue;
    }

    int num_rows = PQntuples(res);
    int num_cols = PQnfields(res);

    std::cout << "Table: " << table_name << std::endl;
    std::cout << "--------------------------------------------" << std::endl;

    // Print column names
    for (int col = 0; col < num_cols; ++col) {
      std::cout << PQfname(res, col) << "\t";
    }
    std::cout << std::endl;

    // Print table data
    for (int row = 0; row < num_rows; ++row) {
      for (int col = 0; col < num_cols; ++col) {
        std::cout << PQgetvalue(res, row, col) << "\t";
      }
      std::cout << std::endl;
    }

    std::cout << std::endl;

    PQclear(res);
  }

  // Rollback the transaction
  PGresult *rollback_res = PQexec(conn, "ROLLBACK");
  if (PQresultStatus(rollback_res) != PGRES_COMMAND_OK) {
    std::cerr << "Error rolling back transaction: " << PQerrorMessage(conn)
              << std::endl;
  }
  PQclear(rollback_res);
}

// Function to create the XML tag for canceled orders
std::string create_canceled_tag(int shares, double cancel_timestamp) {
  std::stringstream ss;
  ss << "<canceled shares=\"" << shares << "\" time=\"" << cancel_timestamp
     << "\"/>";
  return ss.str();
}

// Function to create the XML tag for executed orders
std::string create_executed_tag(int shares, double price,
                                double execution_timestamp) {
  std::stringstream ss;
  ss << "<executed shares=\"" << shares << "\" price=\"" << price
     << "\" time=\"" << execution_timestamp << "\"/>";
  return ss.str();
}

std::string create_xml_response(const std::vector<std::string> &results) {
  pugi::xml_document doc;
  pugi::xml_node results_node = doc.append_child("results");

  for (const std::string &result : results) {
    pugi::xml_document temp_doc;
    pugi::xml_parse_result parse_result = temp_doc.load_string(result.c_str());

    if (parse_result) {
      pugi::xml_node child = temp_doc.first_child();
      results_node.append_copy(child);
    }
  }

  std::ostringstream oss;
  doc.save(oss);
  return oss.str();
}

void send_xml_response(string xml_response, int receiver_fd) {
  size_t data_len = xml_response.size();
  size_t total_bytes_sent = 0;

  while (total_bytes_sent < data_len) {
    ssize_t bytes_sent =
        send(receiver_fd, xml_response.c_str() + total_bytes_sent,
             data_len - total_bytes_sent, 0);
    if (bytes_sent == -1) {
      std::cerr << "Error sending data: " << strerror(errno) << std::endl;
      break;
    }
    total_bytes_sent += bytes_sent;
  }
}

int insert_transaction(PGconn *conn, int transaction_account_id) {
  // Create SQL query to insert a new row and return the generated
  // transaction_id
  char query[256];
  snprintf(query, sizeof(query),
           "INSERT INTO transactions (account_id) VALUES (%d) RETURNING "
           "transaction_id;",
           transaction_account_id);

  // Execute the SQL query
  PGresult *res = PQexec(conn, query);

  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    std::cerr << "Insert failed: " << PQerrorMessage(conn) << std::endl;
    PQclear(res);
    PQfinish(conn);
    return 1;
  }
  // Fetch the result (transaction_id)
  int transaction_id = atoi(PQgetvalue(res, 0, 0));
  std::cout << "Inserted transaction with transaction_id: " << transaction_id
            << std::endl;

  // Clean up and close the database connection
  PQclear(res);
  return transaction_id;
}

void create_tables(PGconn *conn) {
  const char *create_symbols_table = "CREATE TABLE IF NOT EXISTS symbols ("
                                     "symbol_id SERIAL PRIMARY KEY,"
                                     "symbol VARCHAR NOT NULL UNIQUE);";

  const char *create_accounts_table =
      "CREATE TABLE IF NOT EXISTS accounts ("
      "account_id SERIAL PRIMARY KEY,"
      "balance NUMERIC NOT NULL CHECK (balance >= 0));";

  const char *create_positions_table =
      "CREATE TABLE IF NOT EXISTS positions ("
      "position_id SERIAL PRIMARY KEY,"
      "account_id INTEGER NOT NULL REFERENCES accounts(account_id),"
      "symbol_id INTEGER NOT NULL REFERENCES symbols(symbol_id),"
      "amount NUMERIC NOT NULL CHECK (amount > 0),"
      "UNIQUE (account_id, symbol_id));";

  // const char *create_transactions_table =
  //     "CREATE TABLE IF NOT EXISTS transactions ("
  //     "transaction_id SERIAL PRIMARY KEY,"
  //     "account_id INTEGER NOT NULL REFERENCES accounts(account_id));";

  const char *create_orders_table =
      "CREATE TABLE IF NOT EXISTS orders ("
      "order_id SERIAL PRIMARY KEY,"
      // "transaction_id INTEGER NOT NULL REFERENCES
      // transactions(transaction_id),"
      "account_id INTEGER NOT NULL REFERENCES accounts(account_id),"
      "symbol_id INTEGER NOT NULL REFERENCES symbols(symbol_id),"
      "order_type VARCHAR NOT NULL CHECK (order_type IN ('Buy', 'Sell')),"
      "amount NUMERIC NOT NULL,"
      "limit_price NUMERIC NOT NULL,"
      "order_status VARCHAR NOT NULL CHECK (order_status IN ('open', "
      "'executed', 'canceled')),"
      "timestamp NUMERIC NOT NULL);";

  const char *create_executed_orders_table =
      "CREATE TABLE IF NOT EXISTS executed_orders ("
      "order_id INTEGER NOT NULL REFERENCES orders(order_id),"
      "amount NUMERIC NOT NULL,"
      "execution_price NUMERIC NOT NULL,"
      "timestamp NUMERIC NOT NULL);";

  const char *create_canceled_orders_table =
      "CREATE TABLE IF NOT EXISTS canceled_orders ("
      "order_id INTEGER NOT NULL REFERENCES orders(order_id),"
      "cancel_timestamp NUMERIC NOT NULL);";

  const char *queries[] = {create_symbols_table, create_accounts_table,
                           create_positions_table,
                           // create_transactions_table,
                           create_orders_table, create_executed_orders_table,
                           create_canceled_orders_table};

  for (const char *query : queries) {
    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      std::cerr << "Error creating table: " << PQerrorMessage(conn)
                << std::endl;
      PQclear(res);
      return;
    }
    PQclear(res);
  }

  std::cout << "Tables created successfully!" << std::endl;
}

void remove_all_tables(PGconn *conn) {
  const char *queries[] = {"DROP TABLE IF EXISTS canceled_orders CASCADE;",
                           "DROP TABLE IF EXISTS executed_orders CASCADE;",
                           "DROP TABLE IF EXISTS orders CASCADE;",
                           "DROP TABLE IF EXISTS positions CASCADE;",
                           "DROP TABLE IF EXISTS accounts CASCADE;",
                           "DROP TABLE IF EXISTS symbols CASCADE;"};

  for (const char *query : queries) {
    PGresult *res = PQexec(conn, query);
    if (PQresultStatus(res) != PGRES_COMMAND_OK) {
      std::cerr << "Error dropping table: " << PQerrorMessage(conn)
                << std::endl;
    }
    PQclear(res);
  }

  std::cout << "Tables dropped successfully!" << std::endl;
}

PGconn *init_db() {
  const char *conninfo = "host=stock_db port=5432 dbname=postgres "
                         "user=postgres password=passw0rd";
  PGconn *conn = PQconnectdb(conninfo);

  if (PQstatus(conn) == CONNECTION_BAD) {
    std::cerr << "Connection to database failed: " << PQerrorMessage(conn)
              << std::endl;
    PQfinish(conn);
    return NULL;
  }
  return conn;
}

void *process_client(void *arg) {
  ClientData *data = static_cast<ClientData *>(arg);
  int client_fd = data->client_fd;
  std::string client_ip = data->client_ip;
  PGconn *conn = init_db();

  // The rest of the code to process the client connection goes here
  int content_len = receive_content_len(client_fd);
  // Read the XML data
  std::string xml_data;
  receive_complete_message(client_fd, xml_data, content_len);

  // Process commands
  std::vector<std::pair<std::string, std::string>> storage;
  const string xml_data_input = xml_data;
  int transaction_account_id;
  // int transaction_id;
  int parser_result =
      parse_xml_data(storage, xml_data_input, transaction_account_id);
  if (parser_result == -1) {
    std::cerr << "Parser Failed" << std::endl;
    close(client_fd);
    return nullptr;
  }
  vector<string> results;
  for (const auto &entry : storage) {
    string key = entry.first;
    string val = entry.second;
    cout << key << " " << val << endl;
    if (key.substr(0, 8) == "account_")
      results.push_back(
          handle_add_account(conn, key, val)); // handles add account request
    else if (key.substr(0, 7) == "symbol_")
      results.push_back(
          handle_add_symbol(conn, key, val)); // handles add symbol request
    else if (key.substr(0, 6) == "order_")
      results.push_back(
          handle_order(conn, key, val)); // handles order request, don't need
                                         // transaction_account_id (need update)
    else if (key.substr(0, 5) == "query")
      results.push_back(handle_query(conn, key, val)); // handles query request
    else if (key.substr(0, 6) == "cancel")
      results.push_back(handle_cancel(
          conn, key, val,
          transaction_account_id)); // handles cancel request, don't need
                                    // transaction_account_id (need update)
    else {
      std::cerr << "Non valid key in storage" << std::endl;
      return nullptr;
    }
  }
  // construct response xml
  string xml_response = create_xml_response(results);
  cout << "the result xml is: " << endl;
  cout << xml_response << endl;
  // send back response
  send_xml_response(xml_response, client_fd);
  print_db(conn);
  // Clean up and exit the thread
  close(client_fd);
  delete data;
  PQfinish(conn);
  pthread_exit(NULL);
}

int receive_content_len(int client_fd) {
  std::string num_str;
  char c;
  while (recv(client_fd, &c, 1, 0) > 0 && c != '\n') {
    num_str += c;
  }
  int content_len = std::stoi(num_str);
  return content_len;
}

std::string ExecuteOrder(PGconn *conn, std::pair<int, double> matched_order,
                         int order_id, int account_id, string order_type,
                         int last_flag, Order order) {
  int matched_order_id = matched_order.first;
  double matched_order_amount = matched_order.second;
  int matched_order_account_id = 0;
  int matched_order_symbol_id = 0;
  double limit_price = 0;

  // Start the transaction
  PGresult *result = PQexec(conn, "BEGIN;");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    PQclear(result);
    cerr << "error: Failed to begin transaction" << endl;
    return "<error id=\"" + std::to_string(account_id) +
           "\">Error execute order</error>";
  }
  PQclear(result);

  // update order status
  if (last_flag == 1 && left_amount != 0 && order_type == "Sell") {
    std::string query = "UPDATE orders "
                        "SET amount = " +
                        std::to_string(left_amount) +
                        "WHERE order_id = " + std::to_string(matched_order_id) +
                        ";";
    result = PQexec(conn, query.c_str());
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      // Rollback the transaction
      PQexec(conn, "ROLLBACK;");
      PQclear(result);
      return "<error id=\"" + std::to_string(account_id) +
             "\">Error execute order</error>";
    }
    PQclear(result);
  } else if (last_flag == 1 && left_amount != 0 && order_type == "Buy") {
    std::string query = "UPDATE orders "
                        "SET amount = " +
                        std::to_string(-left_amount) +
                        "WHERE order_id = " + std::to_string(matched_order_id) +
                        ";";
    result = PQexec(conn, query.c_str());
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      // Rollback the transaction
      PQexec(conn, "ROLLBACK;");
      PQclear(result);
      return "<error id=\"" + std::to_string(account_id) +
             "\">Error execute order</error>";
    }
    PQclear(result);
  } else {
    std::string query = "UPDATE orders "
                        "SET order_status = 'executed' "
                        "WHERE order_id = " +
                        std::to_string(matched_order_id) + ";";
    result = PQexec(conn, query.c_str());
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      // Rollback the transaction
      PQexec(conn, "ROLLBACK;");
      PQclear(result);
      return "<error id=\"" + std::to_string(account_id) +
             "\">Error execute order</error>";
    }
    PQclear(result);
  }

  // get order price, account_id, symbol_id of matched order
  std::string query = "SELECT limit_price, account_id, symbol_id "
                      "FROM orders "
                      "WHERE order_id = " +
                      std::to_string(matched_order_id) + " FOR UPDATE;";
  result = PQexec(conn, query.c_str());

  if (PQresultStatus(result) != PGRES_TUPLES_OK || PQntuples(result) == 0) {
    // Rollback the transaction
    PQexec(conn, "ROLLBACK;");
    PQclear(result);
    return "<error id=\"" + std::to_string(account_id) +
           "\">Error execute order</error>";
  }

  limit_price = std::stod(PQgetvalue(result, 0, 0));
  matched_order_account_id = std::stoi(PQgetvalue(result, 0, 1));
  matched_order_symbol_id = std::stoi(PQgetvalue(result, 0, 2));
  PQclear(result);

  std::time_t timestamp = std::time(nullptr);

  // update executed_orders
  query = "INSERT INTO executed_orders (order_id, amount, execution_price, "
          "timestamp) "
          "VALUES (" +
          std::to_string(order_id) + ", " +
          std::to_string(matched_order_amount) + ", " +
          std::to_string(limit_price) + ", " + std::to_string(timestamp) + ");";
  result = PQexec(conn, query.c_str());
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    // Rollback the transaction
    PQexec(conn, "ROLLBACK;");
    fprintf(stderr, "error: Unable to execute query: %s", PQerrorMessage(conn));
    PQclear(result);
    return "<error id=\"" + std::to_string(account_id) +
           "\">Error execute order</error>";
  }
  PQclear(result);

  query = "INSERT INTO executed_orders (order_id, amount, execution_price, "
          "timestamp) "
          "SELECT " +
          std::to_string(matched_order_id) + ", " +
          std::to_string(matched_order_amount) + ", " +
          std::to_string(limit_price) + ", " + std::to_string(timestamp) +
          " "
          "WHERE NOT EXISTS (SELECT * FROM executed_orders WHERE order_id = " +
          std::to_string(matched_order_id) + ");";
  result = PQexec(conn, query.c_str());
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    // Rollback the transaction
    PQexec(conn, "ROLLBACK;");
    PQclear(result);
    return "<error id=\"" + std::to_string(account_id) +
           "\">Error execute order</error>";
  }
  PQclear(result);

  if (order_type == "Buy") {
    query = // update position of original account
        "UPDATE positions "
        "SET amount = amount - " +
        std::to_string(matched_order_amount) +
        " WHERE account_id = " + std::to_string(account_id) +
        " AND symbol_id = " + std::to_string(matched_order_symbol_id) + ";";
    result = PQexec(conn, query.c_str());
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      // Rollback the transaction
      PQexec(conn, "ROLLBACK;");
      PQclear(result);
      return "<error id=\"" + std::to_string(account_id) +
             "\">Error execute order</error>";
    }
    PQclear(result);

    query = // update balance of matched order account
        "UPDATE accounts "
        "SET balance = balance - " +
        std::to_string(matched_order_amount * limit_price) +
        " WHERE account_id = " + std::to_string(matched_order_account_id) + ";";
    result = PQexec(conn, query.c_str());
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      // Rollback the transaction
      PQexec(conn, "ROLLBACK;");
      PQclear(result);
      return "<error id=\"" + std::to_string(account_id) +
             "\">Error execute order</error>";
    }

    PQclear(result);

    query = // update balance of original buy order account - refund money
        "UPDATE accounts "
        "SET balance = balance + " +
        std::to_string(
            abs(matched_order_amount * (limit_price - order.limit))) +
        " WHERE account_id = " + std::to_string(account_id) + ";";
    result = PQexec(conn, query.c_str());
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      // Rollback the transaction
      PQexec(conn, "ROLLBACK;");
      PQclear(result);
      return "<error id=\"" + std::to_string(account_id) +
             "\">Error execute order</error>";
    }
    PQclear(result);
  } else {
    query = // update balance of original sell order account
        "UPDATE accounts "
        "SET balance = balance + " +
        std::to_string(matched_order_amount * limit_price) +
        " WHERE account_id = " + std::to_string(account_id) + ";";
    result = PQexec(conn, query.c_str());
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      // Rollback the transaction
      PQexec(conn, "ROLLBACK;");
      PQclear(result);
      return "<error id=\"" + std::to_string(account_id) +
             "\">Error execute order</error>";
    }
    PQclear(result);

    query = // update position of matched order account
        "UPDATE positions "
        "SET amount = amount + " +
        std::to_string(matched_order_amount) +
        " WHERE account_id = " + std::to_string(matched_order_account_id) +
        " AND symbol_id = " + std::to_string(matched_order_symbol_id) + ";";
    result = PQexec(conn, query.c_str());
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
      // Rollback the transaction
      PQexec(conn, "ROLLBACK;");
      PQclear(result);
      return "<error id=\"" + std::to_string(account_id) +
             "\">Error execute order</error>";
    }

    PQclear(result);
  }

  // Commit the transaction
  result = PQexec(conn, "COMMIT;");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    cerr << "error: Failed to commit transaction" << endl;
    PQclear(result);
    return "<error id=\"" + std::to_string(account_id) +
           "\">Error execute order</error>";
  }
  PQclear(result);

  return "";
}

string handle_order(PGconn *conn, string key, string val) {
  // if (!check_account_exists(stoi(val), conn)) {
  //   return "<error id=\"" + val + ">Invalid Account ID<\"/error>" ;
  // }
  // Start a transaction
  // Extract the account_id from the key
  int account_id = std::stoi(key.substr(6));
  if (!check_account_exists(account_id, conn)) {
    return "<error id=\"" + val + ">Invalid Account ID<\"/error>";
  }
  // PQexec(conn, "BEGIN;");

  // Parse symbol, amount, limit attributes
  Order order = parse_order_val(val);

  // get symbol_id
  string query = "SELECT symbol_id FROM symbols WHERE symbol = '" +
                 order.symbol + "' FOR UPDATE;";

  PGresult *result = PQexec(conn, query.c_str());

  if (PQresultStatus(result) != PGRES_TUPLES_OK || PQntuples(result) == 0) {
    PQclear(result);
    PQexec(conn, "ROLLBACK;"); // Rollback the transaction
    return "<error id=\"" + std::to_string(account_id) +
           "\">Error executing query</error>";
  }
  int symbol_id = -1;

  if (PQntuples(result) > 0) {
    // Get the symbol_id value from the first row and first column of the result
    std::string symbol_id_str = PQgetvalue(result, 0, 0);
    symbol_id = std::stoi(symbol_id_str);
  }

  PQclear(result);

  // get account balance
  query = "SELECT balance FROM accounts WHERE account_id = " +
          std::to_string(account_id) + " FOR UPDATE;";
  result = PQexec(conn, query.c_str());

  if (PQresultStatus(result) != PGRES_TUPLES_OK || PQntuples(result) == 0) {
    PQclear(result);
    PQexec(conn, "ROLLBACK;"); // Rollback the transaction
    return "<error id=\"" + std::to_string(account_id) +
           "\">Error executing query</error>";
  }

  std::string balance_str = PQgetvalue(result, 0, 0);
  double balance = std::stod(balance_str);

  PQclear(result);

  // get account position amount
  std::string position_amount_str;
  double position_amount;
  query = "SELECT amount FROM positions WHERE account_id = " +
          std::to_string(account_id) +
          " AND symbol_id = " + std::to_string(symbol_id) + " FOR UPDATE;";
  result = PQexec(conn, query.c_str());

  if (PQresultStatus(result) != PGRES_TUPLES_OK) {
    PQclear(result);
    PQexec(conn, "ROLLBACK;"); // Rollback the transaction
    return "<error id=\"" + std::to_string(account_id) +
           "\">Error executing query</error>";
  }
  if (PQntuples(result) == 0) {
    position_amount = 0;
  } else {
    position_amount_str = PQgetvalue(result, 0, 0);
    position_amount = std::stod(position_amount_str);
  }

  PQclear(result);

  // get current timestamp
  std::time_t timestamp = std::time(nullptr);

  std::string response;

  int order_id;
  // The rest of the function remains the same
  // ...
  if (order.amount < 0) { // sell order
    if (position_amount < abs(order.amount)) {
      return "<error sym=\"" + order.symbol + "\" amount=\"" +
             std::to_string(order.amount) + "\" limit=\"" +
             std::to_string(order.limit) +
             "\">Insufficient shares are available</error>";
    }

    // open order
    response = OpenOrder(conn, order_id, account_id, symbol_id, order, "Sell",
                         timestamp);

    //// get order_id
    // query = "SELECT order_id FROM orders WHERE transaction_id = " +
    //         std::to_string(transaction_id) + ";";
    // result = PQexec(conn, query.c_str());

    // if (PQresultStatus(result) != PGRES_TUPLES_OK || PQntuples(result) == 0)
    // {
    //     PQclear(result);
    //     return "<error id=\"" + std::to_string(account_id) +
    //            "\">Error executing query</error>";
    // }

    // std::string order_id_str = PQgetvalue(result, 0, 0);
    // int order_id = std::stod(order_id_str);

    // PQclear(result);

    // match order
    std::vector<std::pair<int, double>> matched_orders =
        MatchOrder(conn, order_id, "Sell", symbol_id, order.amount, account_id);
    // ToDo: execute orders
    // If match, execute orders
    int matched_amount_total = 0;
    if (matched_orders.size() > 0) {
      size_t i = 0;
      int last_flag;
      for (auto matched_order : matched_orders) {
        if (i == matched_orders.size() - 1) {
          last_flag = true;
        } else {
          last_flag = false;
        }
        matched_amount_total += matched_order.second;
        response = ExecuteOrder(conn, matched_order, order_id, account_id,
                                "Sell", last_flag, order);
        i++;
      }
      if ((matched_amount_total + order.amount) != 0) {
        // update order to open and update amount
        std::string query =
            "UPDATE orders "
            "SET amount = " +
            std::to_string(matched_amount_total + order.amount) +
            "WHERE order_id = " + std::to_string(order_id) + ";";
        PGresult *result = PQexec(conn, query.c_str());
        if (PQresultStatus(result) != PGRES_COMMAND_OK) {
          PQclear(result);
          return "<error id=\"" + std::to_string(account_id) +
                 "\">Error execute order</error>";
        }
        // cout << "query: " << query << endl;
        // cout << "matched_amount_total: " << matched_amount_total << endl;
        // cout << "order.amount: " << order.amount << endl;
        PQclear(result);
      } else {
        std::string query = "UPDATE orders "
                            "SET order_status = 'executed' "
                            "WHERE order_id = " +
                            std::to_string(order_id) + ";";
        PGresult *result = PQexec(conn, query.c_str());
        if (PQresultStatus(result) != PGRES_COMMAND_OK) {
          PQclear(result);
          return "<error id=\"" + std::to_string(account_id) +
                 "\">Error execute order</error>";
        }
        PQclear(result);
      }
    }
  } else { // buy order
    // insufficient funds are available
    if (balance < (abs(order.amount) * order.limit)) {
      return "<error sym=\"" + order.symbol + "\" amount=\"" +
             std::to_string(order.amount) + "\" limit=\"" +
             std::to_string(order.limit) +
             "\">Insufficient funds are available</error>";
    }
    // open order
    response = OpenOrder(conn, order_id, account_id, symbol_id, order, "Buy",
                         timestamp);

    // // get order_id
    // query =
    //     "SELECT order_id "
    //     "FROM orders WHERE transaction_id = " +
    //     std::to_string(transaction_id) + ";";
    // result = PQexec(conn, query.c_str());

    // if (PQresultStatus(result) != PGRES_TUPLES_OK || PQntuples(result) == 0)
    // {
    //     PQclear(result);
    //     return "<error id=\"" + std::to_string(account_id) +
    //            "\">Error executing query</error>";
    // }

    // std::string order_id_str = PQgetvalue(result, 0, 0);
    // int order_id = std::stod(order_id_str);

    // PQclear(result);
    // cout << "order_id: " << order_id << endl;
    //  match order
    std::vector<std::pair<int, double>> matched_orders =
        MatchOrder(conn, order_id, "Buy", symbol_id, order.amount, account_id);
    // ToDo: execute orders
    // If match, execute orders
    int matched_amount_total = 0;
    // cout << "match_orders size: " << matched_orders.size() << endl;
    if (matched_orders.size() > 0) {
      size_t i = 0;
      int last_flag;
      for (auto matched_order : matched_orders) {
        if (i == matched_orders.size() - 1) {
          last_flag = true;
        } else {
          last_flag = false;
        }
        matched_amount_total += matched_order.second;
        response = ExecuteOrder(conn, matched_order, order_id, account_id,
                                "Buy", last_flag, order);
        i++;
      }
      if ((matched_amount_total + order.amount) != 0) {
        // update order to open and update amount
        std::string query =
            "UPDATE orders "
            "SET amount = " +
            std::to_string(order.amount + matched_amount_total) +
            "WHERE order_id = " + std::to_string(order_id) + ";";
        PGresult *result = PQexec(conn, query.c_str());
        if (PQresultStatus(result) != PGRES_COMMAND_OK) {
          PQclear(result);
          return "<error id=\"" + std::to_string(account_id) +
                 "\">Error execute order</error>";
        }
        cout << "query: " << query << endl;
        cout << "matched_amount_total: " << matched_amount_total << endl;
        cout << "order.amount: " << order.amount << endl;
        PQclear(result);
      } else {
        std::string query = "UPDATE orders "
                            "SET order_status = 'executed' "
                            "WHERE order_id = " +
                            std::to_string(order_id) + ";";
        PGresult *result = PQexec(conn, query.c_str());
        if (PQresultStatus(result) != PGRES_COMMAND_OK) {
          PQclear(result);
          return "<error id=\"" + std::to_string(account_id) +
                 "\">Error execute order</error>";
        }
        PQclear(result);
      }
    }
  }
  // Commit the transaction
  result = PQexec(conn, "COMMIT;");
  if (PQresultStatus(result) != PGRES_COMMAND_OK) {
    cerr << "error: Failed to commit transaction" << endl;
    PQclear(result);
    return "<error id=\"" + std::to_string(account_id) +
           "\">Error execute order</error>";
  }
  PQclear(result);

  if (response.empty() != 1) {
    return response;
  } else {
    response = "<opened sym=\"" + order.symbol + "\" amount=\"" +
               std::to_string(order.amount) + "\" limit=\"" +
               std::to_string(order.limit) + "\" id=\"" +
               std::to_string(order_id) + "\"/>";
  }
  return response;
}

string handle_cancel(PGconn *conn, string key, string val,
                     int transaction_account_id) {
  if (!check_account_exists(transaction_account_id, conn)) {
    return "<error id=\"" + to_string(transaction_account_id) +
           ">Invalid Account ID<\"/error>";
  }
  std::stringstream xml_result;
  int order_id = stoi(val); // Convert val to int for order_id

  // Begin transaction
  PGresult *result = PQexec(conn, "BEGIN;");
  while (true) {
    if (PQresultStatus(result) == PGRES_COMMAND_OK) {
      break;
    }
    cout << "Fail to begin transaction, retrying ..." << endl;
    PQclear(result);
    result = PQexec(conn, "BEGIN;");
  }
  PQclear(result);

  // Get open order for the given order_id and transaction_account_id
  char get_open_order[256];
  snprintf(get_open_order, sizeof(get_open_order),
           "SELECT o.order_id, o.amount, eo.amount AS executed_amount, "
           "o.limit_price, o.timestamp "
           "FROM orders o "
           "LEFT JOIN executed_orders eo ON o.order_id = eo.order_id "
           "WHERE o.order_id = %d AND o.account_id = %d AND "
           "o.order_status = 'open';",
           order_id, transaction_account_id);
  // cout << "------------------" << endl;
  // cout << "Account id is: " <<  transaction_account_id << endl;
  // cout << "Order id is: " << order_id << endl;
  // cout << "------------------" << endl;
  PGresult *open_order_res = PQexec(conn, get_open_order);
  int num_open_order = PQntuples(open_order_res);
  // cout << "------------------" << endl;
  // cout << "num_open_order is: " <<  num_open_order << endl;
  // cout << "------------------" << endl;
  if (num_open_order > 0) {
    int order_amount = atoi(PQgetvalue(open_order_res, 0, 1));
    int executed_amount = atoi(PQgetvalue(open_order_res, 0, 2));
    double limit_price = atof(PQgetvalue(open_order_res, 0, 3));
    double order_timestamp = atof(PQgetvalue(open_order_res, 0, 4));

    int canceled_shares = order_amount - executed_amount;
    xml_result << "<canceled id=\"" << order_id << "\">" << endl;
    xml_result << create_canceled_tag(canceled_shares, order_timestamp) << endl;

    // Add the canceled order to the canceled_orders table
    char insert_canceled_order[256];
    snprintf(insert_canceled_order, sizeof(insert_canceled_order),
             "INSERT INTO canceled_orders (order_id, cancel_timestamp) VALUES "
             "(%d, %lf);",
             order_id, order_timestamp);
    PQexec(conn, insert_canceled_order);

    // Update the order status to 'canceled'
    char update_order_status[256];
    snprintf(update_order_status, sizeof(update_order_status),
             "UPDATE orders SET order_status = 'canceled' WHERE order_id = %d;",
             order_id);
    PQexec(conn, update_order_status);

    // Add the remaining unexecuted amount times the limit price back to the
    // account balance
    int remaining_unexecuted_amount = order_amount - executed_amount;
    double refund_amount = remaining_unexecuted_amount * limit_price;
    char update_account_balance[256];
    snprintf(
        update_account_balance, sizeof(update_account_balance),
        "UPDATE accounts SET balance = balance + %lf WHERE account_id = %d;",
        refund_amount, transaction_account_id);
    PQexec(conn, update_account_balance);

    // Check if the order is partially executed
    if (executed_amount > 0) {
      // Get execution details for the executed order
      char get_executed_order[256];
      snprintf(get_executed_order, sizeof(get_executed_order),
               "SELECT amount, execution_price, timestamp "
               "FROM executed_orders WHERE order_id = %d FOR UPDATE;",
               order_id);

      PGresult *executed_order_res = PQexec(conn, get_executed_order);
      int executed_shares = atoi(PQgetvalue(executed_order_res, 0, 0));
      double execution_price = atof(PQgetvalue(executed_order_res, 0, 1));
      double execution_timestamp = atof(PQgetvalue(executed_order_res, 0, 2));
      xml_result << create_executed_tag(executed_shares, execution_price,
                                        execution_timestamp)
                 << endl;
      PQclear(executed_order_res);
    }

    xml_result << "</canceled>" << endl;
  } else {
    xml_result << "<error>Order not found or already canceled</error>" << endl;
  }

  // End transaction
  PQexec(conn, "COMMIT");

  PQclear(open_order_res);

  return xml_result.str();
}

string handle_query(PGconn *conn, string key, string val) {
  if (!check_account_exists(stoi(val), conn)) {
    return "<error id=\"" + val + ">Invalid Account ID<\"/error>";
  }
  if (key != "query")
    return "";

  stringstream xml_response;
  string order_id = val;

  // Begin transaction
  PQexec(conn, "BEGIN");

  // Build XML response header
  xml_response << "<status id=\"" << order_id << "\">" << std::endl;

  // Query open orders
  string open_orders_query =
      "SELECT order_id, amount FROM orders WHERE order_id = " + order_id +
      " AND order_id NOT IN (SELECT order_id FROM executed_orders "
      "UNION SELECT order_id FROM canceled_orders) FOR UPDATE;";
  PGresult *open_orders_result = PQexec(conn, open_orders_query.c_str());

  // Query canceled orders
  string canceled_orders_query =
      "SELECT orders.order_id, orders.amount, canceled_orders.cancel_timestamp "
      "FROM orders INNER JOIN canceled_orders ON orders.order_id = "
      "canceled_orders.order_id "
      "WHERE order_id = " +
      order_id + " FOR UPDATE;";
  PGresult *canceled_orders_result =
      PQexec(conn, canceled_orders_query.c_str());

  // Query executed orders
  string executed_orders_query =
      "SELECT orders.order_id, executed_orders.amount, "
      "executed_orders.execution_price, executed_orders.timestamp "
      "FROM orders INNER JOIN executed_orders ON orders.order_id = "
      "executed_orders.order_id "
      "WHERE order_id = " +
      order_id + " FOR UPDATE;";
  PGresult *executed_orders_result =
      PQexec(conn, executed_orders_query.c_str());

  int total_tuples = PQntuples(open_orders_result) +
                     PQntuples(canceled_orders_result) +
                     PQntuples(executed_orders_result);
  if (total_tuples == 0) {
    xml_response << " <error id=\"" << order_id
                 << "\"> Order does not exist </error>" << std::endl;
  } else {
    for (int i = 0; i < PQntuples(open_orders_result); i++) {
      xml_response << " <open shares=\"" << PQgetvalue(open_orders_result, i, 1)
                   << "\"/>" << std::endl;
    }
    for (int i = 0; i < PQntuples(canceled_orders_result); i++) {
      xml_response << " <canceled shares=\""
                   << PQgetvalue(canceled_orders_result, i, 1) << "\" time=\""
                   << PQgetvalue(canceled_orders_result, i, 2) << "\"/>"
                   << std::endl;
    }
    for (int i = 0; i < PQntuples(executed_orders_result); i++) {
      xml_response << " <executed shares=\""
                   << PQgetvalue(executed_orders_result, i, 1) << "\" price=\""
                   << PQgetvalue(executed_orders_result, i, 2) << "\" time=\""
                   << PQgetvalue(executed_orders_result, i, 3) << "\"/>"
                   << std::endl;
    }
  }
  // Build XML response footer
  xml_response << "</status>";

  // End transaction
  PQexec(conn, "COMMIT");

  PQclear(open_orders_result);
  PQclear(canceled_orders_result);
  PQclear(executed_orders_result);

  return xml_response.str();
}

int check_account_exists(int account_id, PGconn *conn) {
  if (conn == NULL) {
    fprintf(stderr, "Invalid connection\n");
    return -1;
  }

  // Begin transaction
  PGresult *res = PQexec(conn, "BEGIN;");
  while (true) {
    if (PQresultStatus(res) == PGRES_COMMAND_OK) {
      break;
    }
    cout << "Fail to begin transaction, retrying ..." << endl;
    PQclear(res);
    res = PQexec(conn, "BEGIN;");
  }
  PQclear(res);

  // Prepare query
  const char *query =
      "SELECT account_id FROM accounts WHERE account_id = $1 FOR UPDATE;";
  const int nParams = 1;
  const Oid paramTypes[1] = {23};
  const char *paramValues[1];
  char accountIdText[12]; // Buffer for the account_id integer as text, max 10
                          // digits + sign + null terminator
  snprintf(accountIdText, sizeof(accountIdText), "%d", account_id);
  paramValues[0] = accountIdText;

  // Execute query
  res = PQexecParams(conn, query, nParams, paramTypes, paramValues, NULL, NULL,
                     0);
  if (PQresultStatus(res) != PGRES_TUPLES_OK) {
    fprintf(stderr, "SELECT query failed: %s\n", PQerrorMessage(conn));
    PQclear(res);
    PQexec(conn, "ROLLBACK");
    return -1;
  }

  // Check if the account_id exists
  int accountExists = PQntuples(res) > 0 ? 1 : 0;

  // Clear result and commit transaction
  PQclear(res);
  res = PQexec(conn, "COMMIT");
  if (PQresultStatus(res) != PGRES_COMMAND_OK) {
    fprintf(stderr, "COMMIT command failed: %s\n", PQerrorMessage(conn));
    PQclear(res);
    return -1;
  }
  PQclear(res);

  return accountExists;
}
