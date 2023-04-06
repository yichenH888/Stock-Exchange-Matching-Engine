#include "pugixml.hpp"
#include <iostream>
#include <string>
#include <unordered_map>

using namespace std;

void parse_xml(std::unordered_map<std::string, std::string> &storage,
               const std::string &xml_data) {
  pugi::xml_document doc;
  pugi::xml_parse_result result = doc.load_string(xml_data.c_str());

  if (!result) {
    std::cerr << "Error parsing XML: " << result.description() << std::endl;
    return;
  }

  pugi::xml_node root_node = doc.first_child();

  std::string root_name = root_node.name();

  if (root_name == "create") {
    for (pugi::xml_node child = root_node.first_child(); child;
         child = child.next_sibling()) {
      std::string node_name = child.name();

      if (node_name == "account") {
        int account_id = child.attribute("id").as_int();
        std::string key = "account_" + std::to_string(account_id);

        if (storage.find(key) != storage.end()) {
          std::cerr << "Error: Account with id " << account_id
                    << " already exists." << std::endl;
          return;
        }

        double balance = child.attribute("balance").as_double();
        storage[key] = std::to_string(balance);

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

        storage[key] = value;

      } else {
        std::cerr << "Error: Invalid child node <" << node_name
                  << "> in <create> node." << std::endl;
        return;
      }
    }
  } else if (root_name == "transactions") {
    int account_id = root_node.attribute("account_id").as_int();

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

        storage[key] = value;
      } else if (node_name == "query") {
        std::string key = "query";
        int id = child.attribute("id").as_int();

        storage[key] = std::to_string(id);
      } else if (node_name == "cancel") {
        std::string key = "cancel";
        int id = child.attribute("id").as_int();

        storage[key] = std::to_string(id);
      } else {
        std::cerr << "Error: Invalid child node <" << node_name
                  << "> in <transactions> node." << std::endl;
        return;
      }
    }
  } else {
    std::cerr << "Error: XML does not have a valid top-level node. Expected "
                 "<create> or <transactions>."
              << std::endl;
    return;
  }
}

void test_transactions() {
  std::unordered_map<std::string, std::string> storage;

  std::string xml_data = R"(
        <transactions account_id="1">
            <order sym="SPY" amount="200" limit="145.67"/>
            <query id="100"/>
            <cancel id="101"/>
        </transactions>
    )";

  parse_xml(storage, xml_data);

  for (const auto &entry : storage) {
    cout << entry.first << ": " << entry.second << endl;
  }
}

int main() {
  // Test XML data
  string xml_data = R"(<create>
        <account id="1" balance="1000"/>
        <account id="2" balance="2000"/>
        <symbol sym="AAPL">
            <account id="1">10</account>
            <account id="2">20</account>
        </symbol>
        <symbol sym="GOOGL">
            <account id="1">5</account>
            <account id="2">15</account>
        </symbol>
    </create>)";

  unordered_map<string, string> storage;

  parse_xml(storage, xml_data);

  // Display the contents of the storage
  for (const auto &entry : storage) {
    cout << entry.first << ": " << entry.second << endl;
  }

  test_transactions();

  return 0;
}
