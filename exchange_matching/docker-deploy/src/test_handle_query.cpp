#include <iostream>
#include <postgresql/libpq-fe.h>
#include <sstream>
#include <string>

using std::cout;
using std::endl;
using std::string;
using std::stringstream;

string handle_query(PGconn *conn, string key, string val);
void check_connection(PGconn *conn);

int main() {
  // Replace these values with your PostgreSQL database connection details
  const char *conninfo = "host=localhost port=5432 dbname=exchange_matching "
                         "user=cy141 password=password";

  PGconn *conn = PQconnectdb(conninfo);
  check_connection(conn);

  // Create tables
  const char *create_transactions_table =
      "CREATE TABLE IF NOT EXISTS transactions ("
      "transaction_id SERIAL PRIMARY KEY,"
      "account_id INTEGER NOT NULL);";
  PQexec(conn, create_transactions_table);

  const char *create_orders_table =
      "CREATE TABLE IF NOT EXISTS orders ("
      "order_id SERIAL PRIMARY KEY,"
      "transaction_id INTEGER NOT NULL REFERENCES transactions(transaction_id),"
      "account_id INTEGER NOT NULL,"
      "order_type VARCHAR NOT NULL CHECK (order_type IN ('Buy', 'Sell')),"
      "amount NUMERIC NOT NULL,"
      "limit_price NUMERIC NOT NULL,"
      "timestamp TIMESTAMP NOT NULL);";
  PQexec(conn, create_orders_table);

  const char *create_executed_orders_table =
      "CREATE TABLE IF NOT EXISTS executed_orders ("
      "order_id INTEGER NOT NULL REFERENCES orders(order_id),"
      "amount NUMERIC NOT NULL,"
      "execution_price NUMERIC NOT NULL,"
      "timestamp TIMESTAMP NOT NULL);";
  PQexec(conn, create_executed_orders_table);

  const char *create_canceled_orders_table =
      "CREATE TABLE IF NOT EXISTS canceled_orders ("
      "order_id INTEGER NOT NULL REFERENCES orders(order_id),"
      "cancel_timestamp TIMESTAMP NOT NULL);";
  PQexec(conn, create_canceled_orders_table);

  // Add some test data to the tables

  // Add a transaction
    PGresult *result = PQexec(conn, "INSERT INTO transactions (account_id) VALUES (1) RETURNING transaction_id;");
    if (PQresultStatus(result) != PGRES_TUPLES_OK) {
        cout << "Failed to insert transaction" << endl;
        PQclear(result);
        return 1;
    }
    string transaction_id = PQgetvalue(result, 0, 0);
    PQclear(result);

    // Add orders using prepared statements
    const char *orders_insert = "INSERT INTO orders (transaction_id, account_id, order_type, amount, limit_price, timestamp) VALUES ($1, $2, $3, $4, $5, $6) RETURNING order_id;";
    PQprepare(conn, "orders_insert", orders_insert, 0, NULL);

    const char *orders_values[][6] = {
        {transaction_id.c_str(), "1", "Buy", "100", "50", "2023-04-01 10:00:00"},
        {transaction_id.c_str(), "1", "Sell", "50", "60", "2023-04-01 11:00:00"},
        {transaction_id.c_str(), "1", "Sell", "1000", "1000", "2023-04-01 11:00:00"}
    };

    for (const auto &order_values : orders_values) {
        result = PQexecPrepared(conn, "orders_insert", 6, order_values, NULL, NULL, 0);
        if (PQresultStatus(result) != PGRES_TUPLES_OK) {
            cout << "Failed to insert order" << endl;
            PQclear(result);
            return 1;
        }
        PQclear(result);
    }

    // Add executed order
    result = PQexec(conn, "INSERT INTO executed_orders (order_id, amount, execution_price, timestamp) VALUES (1, 100, 55, '2023-04-01 12:00:00');");
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        cout << "Failed to insert executed order" << endl;
        PQclear(result);
        return 1;
    }
    PQclear(result);

    // Add canceled order
    result = PQexec(conn, "INSERT INTO canceled_orders (order_id, cancel_timestamp) VALUES (2, '2023-04-01 13:00:00');");
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        cout << "Failed to insert canceled order" << endl;
        PQclear(result);
        return 1;
    }
    PQclear(result);
  // Test handle_query function
  string xml_response = handle_query(conn, "order", transaction_id);

  cout << "XML response for transaction_id " << transaction_id << ":\n"
       << xml_response << endl;
  // Clean up
  PQexec(conn, "DROP TABLE canceled_orders;");
  PQexec(conn, "DROP TABLE executed_orders;");
  PQexec(conn, "DROP TABLE orders;");
  PQexec(conn, "DROP TABLE transactions;");

  PQfinish(conn);

  return 0;
}

void check_connection(PGconn *conn) {
  if (PQstatus(conn) != CONNECTION_OK) {
    fprintf(stderr, "Connection to database failed: %s\n",
            PQerrorMessage(conn));
    PQfinish(conn);
    exit(1);
  }
}

string handle_query(PGconn *conn, string key, string val) {
  if (key != "order")
    return "";

  stringstream xml_response;
  string transaction_id = val;

  // Build XML response header
  xml_response << "<status id=\"" << transaction_id << "\">" << std::endl;

  // Query open orders
  string open_orders_query =
      "SELECT order_id, amount FROM orders WHERE transaction_id = " +
      transaction_id +
      " AND order_id NOT IN (SELECT order_id FROM executed_orders "
      "UNION SELECT order_id FROM canceled_orders);";
  PGresult *open_orders_result = PQexec(conn, open_orders_query.c_str());

  for (int i = 0; i < PQntuples(open_orders_result); i++) {
    xml_response << " <open shares=\"" << PQgetvalue(open_orders_result, i, 1)
                 << "\"/>" << std::endl;
  }

  // Query canceled orders
  string canceled_orders_query =
      "SELECT orders.order_id, orders.amount, canceled_orders.cancel_timestamp "
      "FROM orders INNER JOIN canceled_orders ON orders.order_id = "
      "canceled_orders.order_id "
      "WHERE transaction_id = " +
      transaction_id + ";";
  PGresult *canceled_orders_result =
      PQexec(conn, canceled_orders_query.c_str());

  for (int i = 0; i < PQntuples(canceled_orders_result); i++) {
    xml_response << " <canceled shares=\""
                 << PQgetvalue(canceled_orders_result, i, 1) << "\" time=\""
                 << PQgetvalue(canceled_orders_result, i, 2) << "\"/>"
                 << std::endl;
  }

  // Query executed orders
  string executed_orders_query =
      "SELECT orders.order_id, executed_orders.amount, "
      "executed_orders.execution_price, executed_orders.timestamp "
      "FROM orders INNER JOIN executed_orders ON orders.order_id = "
      "executed_orders.order_id "
      "WHERE transaction_id = " +
      transaction_id + ";";
  PGresult *executed_orders_result =
      PQexec(conn, executed_orders_query.c_str());

  for (int i = 0; i < PQntuples(executed_orders_result); i++) {
    xml_response << " <executed shares=\""
                 << PQgetvalue(executed_orders_result, i, 1) << "\" price=\""
                 << PQgetvalue(executed_orders_result, i, 2) << "\" time=\""
                 << PQgetvalue(executed_orders_result, i, 3) << "\"/>"
                 << std::endl;
  }

  // Build XML response footer
  xml_response << "</status>";

  PQclear(open_orders_result);
  PQclear(canceled_orders_result);
  PQclear(executed_orders_result);

  return xml_response.str();
}
