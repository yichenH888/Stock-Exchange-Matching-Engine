#include <iostream>
#include <postgresql/libpq-fe.h>
#include <sstream>
#include <string>

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
std::string handle_cancel(PGconn *conn, std::string key, std::string val, int transaction_id,
                     int transaction_account_id) {
  std::stringstream xml_result;

  // Begin transaction
  PQexec(conn, "BEGIN");

  // Get open orders for the given transaction_id and transaction_account_id
  char get_open_orders[256];
  snprintf(get_open_orders, sizeof(get_open_orders),
           "SELECT o.order_id, o.amount, eo.amount AS executed_amount, "
           "o.limit_price, o.timestamp "
           "FROM orders o "
           "LEFT JOIN executed_orders eo ON o.order_id = eo.order_id "
           "WHERE o.transaction_id = %d AND o.account_id = %d AND "
           "o.order_status = 'open';",
           transaction_id, transaction_account_id);

  PGresult *open_orders_res = PQexec(conn, get_open_orders);
  int num_open_orders = PQntuples(open_orders_res);

  xml_result << "<canceled id=\"" << transaction_id << "\">" << std::endl;

  // Process open orders
  for (int i = 0; i < num_open_orders; ++i) {
    int order_id = atoi(PQgetvalue(open_orders_res, i, 0));
    int order_amount = atoi(PQgetvalue(open_orders_res, i, 1));
    int executed_amount = atoi(PQgetvalue(open_orders_res, i, 2));
    double limit_price = atof(PQgetvalue(open_orders_res, i, 3));
    double order_timestamp = atof(PQgetvalue(open_orders_res, i, 4));

    int canceled_shares = order_amount - executed_amount;
    xml_result << create_canceled_tag(canceled_shares, order_timestamp) << std::endl;

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
               "FROM executed_orders WHERE order_id = %d;",
               order_id);

      PGresult *executed_order_res = PQexec(conn, get_executed_order);
      int executed_shares = atoi(PQgetvalue(executed_order_res, 0, 0));
      double execution_price = atof(PQgetvalue(executed_order_res, 0, 1));
      double execution_timestamp = atof(PQgetvalue(executed_order_res, 0, 2));

      xml_result << create_executed_tag(executed_shares, execution_price,
                                        execution_timestamp) << std::endl;
      PQclear(executed_order_res);
    }
  }

  xml_result << "</canceled>" << std::endl;;

  // End transaction
  PQexec(conn, "COMMIT");

  PQclear(open_orders_res);

  return xml_result.str();
}


int main() {
  PGconn *conn = PQconnectdb("host=localhost port=5432 dbname=exchange_matching user=cy141 password=password");

  if (PQstatus(conn) == CONNECTION_BAD) {
    std::cerr << "Connection to database failed: " << PQerrorMessage(conn) << std::endl;
    PQfinish(conn);
    return 1;
  }

  // Create tables
  PQexec(conn, "DROP TABLE IF EXISTS accounts CASCADE;");
  PQexec(conn, "DROP TABLE IF EXISTS orders CASCADE;");
  PQexec(conn, "DROP TABLE IF EXISTS executed_orders CASCADE;");
  PQexec(conn, "DROP TABLE IF EXISTS canceled_orders CASCADE;");

  PQexec(conn, "CREATE TABLE accounts ("
              "account_id serial PRIMARY KEY,"
              "balance double precision NOT NULL);");

  PQexec(conn, "CREATE TABLE orders ("
              "order_id serial PRIMARY KEY,"
              "transaction_id integer NOT NULL,"
              "account_id integer NOT NULL REFERENCES accounts(account_id),"
              "amount integer NOT NULL,"
              "limit_price double precision NOT NULL,"
              "timestamp double precision NOT NULL,"
              "order_status varchar(255) NOT NULL);");

  PQexec(conn, "CREATE TABLE executed_orders ("
              "order_id integer PRIMARY KEY REFERENCES orders(order_id),"
              "amount integer NOT NULL,"
              "execution_price double precision NOT NULL,"
              "timestamp double precision NOT NULL);");

  PQexec(conn, "CREATE TABLE canceled_orders ("
              "order_id integer PRIMARY KEY REFERENCES orders(order_id),"
              "cancel_timestamp double precision NOT NULL);");

  // Insert test data
  PQexec(conn, "INSERT INTO accounts (account_id, balance) VALUES (1, 10000), (2, 20000);");

  PQexec(conn, "INSERT INTO orders (order_id, transaction_id, account_id, amount, limit_price, timestamp, order_status) VALUES "
              "(1, 1, 1, 100, 50, 1000, 'open'),"
              "(2, 1, 1, 200, 60, 1001, 'open'),"
              "(3, 2, 2, 300, 70, 1002, 'open'),"
              "(4, 2, 1, 400, 80, 1003, 'open'),"
              "(5, 3, 2, 500, 90, 1004, 'open');");

  PQexec(conn, "INSERT INTO executed_orders (order_id, amount, execution_price, timestamp) VALUES (2, 50, 60, 1005);");

  // Test handle_cancel function
  std::string cancel_result1 = handle_cancel(conn, "", "", 1, 1);
  std::cout << "Cancel result for transaction_id=1, account_id=1:\n" << cancel_result1 << std::endl;

  std::string cancel_result2  = handle_cancel(conn, "", "", 2, 2);
  std::cout << "Cancel result for transaction_id=2, account_id=2:\n" << cancel_result2 << std::endl;

  std::string cancel_result3 = handle_cancel(conn, "", "", 2, 1);
  std::cout << "Cancel result for transaction_id=2, account_id=1:\n" << cancel_result3 << std::endl;

  // Check account balances after cancellation
  PGresult *account_balance_res = PQexec(conn, "SELECT account_id, balance FROM accounts;");
  int num_accounts = PQntuples(account_balance_res);

  std::cout << "Account balances after cancellation:\n";
  for (int i = 0; i < num_accounts; ++i) {
    int account_id = atoi(PQgetvalue(account_balance_res, i, 0));
    double balance = atof(PQgetvalue(account_balance_res, i, 1));
    std::cout << "Account ID: " << account_id << ", Balance: $" << balance << std::endl;
  }

  PQclear(account_balance_res);
  PQfinish(conn);

  return 0;
}


