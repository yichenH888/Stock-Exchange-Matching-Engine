#include <iostream>
#include <stdexcept>

class Account {
private:
    int account_id;
    double balance;

public:
    // Constructor
    Account(int account_id, double initial_balance) {
        if (initial_balance < 0) {
            throw std::invalid_argument("Initial balance must be non-negative.");
        }
        this->account_id = account_id;
        this->balance = initial_balance;
    }

    // Accessor (getter) methods
    int get_account_id() const {
        return account_id;
    }

    double get_balance() const {
        return balance;
    }

    // Public method: update_balance
    void update_balance(double amount) {
        if (balance + amount < 0) {
            throw std::invalid_argument("Cannot update balance to a negative value.");
        }
        balance += amount;
    }
};
