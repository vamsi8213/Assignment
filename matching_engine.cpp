



#include <iostream>
#include <queue>
#include <unordered_map>
#include <thread>
#include <mutex>
#include <condition_variable>
#include <atomic>
#include <memory>
#include <random>
#include <chrono>

// Message structures as defined in the challenge
struct PlaceOrder {
    int orderId;
    int price;
    int amount;
    int clientId; // Added to track which client placed the order
};

struct CancelOrder {
    int orderId;
};

struct OrderPlaced {
    int orderId;
    int price;
    int amount;
};

struct OrderCanceled {
    int orderId;
    int reasonCode = 0; // 0: User canceled, 1: Fully traded
};

struct OrderTraded {
    int orderId;
    int tradedPrice;
    int tradedAmount;
};

struct RequestRejected {
    int orderId;
    int reasonCode; // 1: Invalid order, 2: Order not found
    std::string message;
};