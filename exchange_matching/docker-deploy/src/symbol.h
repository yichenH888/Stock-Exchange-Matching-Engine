#include <iostream>
#include <string>

class Symbol {
private:
    int symbol_id;
    std::string symbol;

public:
    // Constructor
    Symbol(int symbol_id, const std::string& symbol) {
        if (symbol.empty()) {
            throw std::invalid_argument("Symbol must not be an empty string.");
        }
        this->symbol_id = symbol_id;
        this->symbol = symbol;
    }

    // Accessor (getter) methods
    int get_symbol_id() const {
        return symbol_id;
    }

    std::string get_symbol() const {
        return symbol;
    }
};
