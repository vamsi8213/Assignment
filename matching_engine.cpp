



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

// Internal order representation
struct Order {
    int orderId;
    int clientId;
    int price;
    int amount;
    int originalAmount;
    std::chrono::steady_clock::time_point timestamp;
    
    Order(int id, int client, int p, int amt) 
        : orderId(id), clientId(client), price(p), amount(amt), originalAmount(amt),
          timestamp(std::chrono::steady_clock::now()) {}
};

// Message wrapper for client communication
enum class MessageType {
    PLACE_ORDER,
    CANCEL_ORDER,
    ORDER_PLACED,
    ORDER_CANCELED,
    ORDER_TRADED,
    REQUEST_REJECTED
};

struct Message {
    MessageType type;
    int targetClientId;
    
    // Union-like storage for different message types
    PlaceOrder placeOrder;
    CancelOrder cancelOrder;
    OrderPlaced orderPlaced;
    OrderCanceled orderCanceled;
    OrderTraded orderTraded;
    RequestRejected requestRejected;
    
    Message(MessageType t, int clientId) : type(t), targetClientId(clientId) {}
};

// Thread-safe message queue
class MessageQueue {
private:
    std::queue<Message> queue_;
    std::mutex mutex_;
    std::condition_variable cv_;
    
public:
    void push(const Message& msg) {
        std::lock_guard<std::mutex> lock(mutex_);
        queue_.push(msg);
        cv_.notify_one();
    }
    
    bool pop(Message& msg, std::chrono::milliseconds timeout = std::chrono::milliseconds(100)) {
        std::unique_lock<std::mutex> lock(mutex_);
        if (cv_.wait_for(lock, timeout, [this] { return !queue_.empty(); })) {
            msg = queue_.front();
            queue_.pop();
            return true;
        }
        return false;
    }
};

// Comparators for priority queues
struct BuyOrderComparator {
    bool operator()(const std::shared_ptr<Order>& a, const std::shared_ptr<Order>& b) const {
        if (a->price != b->price) {
            return a->price < b->price; // Higher price has priority for buy orders
        }
        return a->timestamp > b->timestamp; // Earlier timestamp has priority
    }
};

struct SellOrderComparator {
    bool operator()(const std::shared_ptr<Order>& a, const std::shared_ptr<Order>& b) const {
        if (a->price != b->price) {
            return a->price > b->price; // Lower price has priority for sell orders
        }
        return a->timestamp > b->timestamp; // Earlier timestamp has priority
    }
};

class MatchingEngine {
private:
    // Order books
    std::priority_queue<std::shared_ptr<Order>, std::vector<std::shared_ptr<Order>>, BuyOrderComparator> buyOrders_;
    std::priority_queue<std::shared_ptr<Order>, std::vector<std::shared_ptr<Order>>, SellOrderComparator> sellOrders_;
    
    // Order tracking
    std::unordered_map<int, std::shared_ptr<Order>> activeOrders_;
    
    // Communication
    MessageQueue toEngine_;
    std::vector<std::shared_ptr<MessageQueue>> toClients_;
    
    // Threading
    std::atomic<bool> running_;
    std::mutex engineMutex_;
    
    void sendToClient(int clientId, const Message& msg) {
        if (clientId >= 0 && clientId < toClients_.size()) {
            toClients_[clientId]->push(msg);
        }
    }
    
    void processPlaceOrder(const PlaceOrder& order) {
        std::lock_guard<std::mutex> lock(engineMutex_);
        
        // Validate order
        if (order.amount <= 0 || order.price <= 0) {
            Message rejMsg(MessageType::REQUEST_REJECTED, order.clientId);
            rejMsg.requestRejected = {order.orderId, 1, "Invalid order parameters"};
            sendToClient(order.clientId, rejMsg);
            return;
        }
        
        // Check if order ID already exists
        if (activeOrders_.find(order.orderId) != activeOrders_.end()) {
            Message rejMsg(MessageType::REQUEST_REJECTED, order.clientId);
            rejMsg.requestRejected = {order.orderId, 1, "Order ID already exists"};
            sendToClient(order.clientId, rejMsg);
            return;
        }
        
        auto newOrder = std::make_shared<Order>(order.orderId, order.clientId, order.price, order.amount);
        activeOrders_[order.orderId] = newOrder;
        
        // Determine if this is a buy or sell order based on matching logic
        // For simplicity, we'll assume orders with even IDs are buy orders, odd IDs are sell orders
        bool isBuyOrder = (order.orderId % 2 == 0);
        
        if (isBuyOrder) {
            matchBuyOrder(newOrder);
        } else {
            matchSellOrder(newOrder);
        }
    }
    
    void matchBuyOrder(std::shared_ptr<Order> buyOrder) {
        // Try to match with sell orders
        while (buyOrder->amount > 0 && !sellOrders_.empty()) {
            auto bestSell = sellOrders_.top();
            
            if (buyOrder->price >= bestSell->price) {
                sellOrders_.pop();
                
                int tradeAmount = std::min(buyOrder->amount, bestSell->amount);
                int tradePrice = bestSell->price; // Use the resting order's price
                
                // Update amounts
                buyOrder->amount -= tradeAmount;
                bestSell->amount -= tradeAmount;
                
                // Send trade notifications
                Message buyTradeMsg(MessageType::ORDER_TRADED, buyOrder->clientId);
                buyTradeMsg.orderTraded = {buyOrder->orderId, tradePrice, tradeAmount};
                sendToClient(buyOrder->clientId, buyTradeMsg);
                
                Message sellTradeMsg(MessageType::ORDER_TRADED, bestSell->clientId);
                sellTradeMsg.orderTraded = {bestSell->orderId, tradePrice, tradeAmount};
                sendToClient(bestSell->clientId, sellTradeMsg);
                
                // Handle fully traded orders
                if (bestSell->amount == 0) {
                    activeOrders_.erase(bestSell->orderId);
                    Message cancelMsg(MessageType::ORDER_CANCELED, bestSell->clientId);
                    cancelMsg.orderCanceled = {bestSell->orderId, 1}; // Fully traded
                    sendToClient(bestSell->clientId, cancelMsg);
                } else {
                    sellOrders_.push(bestSell); // Put back partially filled order
                }
                
                if (buyOrder->amount == 0) {
                    activeOrders_.erase(buyOrder->orderId);
                    Message cancelMsg(MessageType::ORDER_CANCELED, buyOrder->clientId);
                    cancelMsg.orderCanceled = {buyOrder->orderId, 1}; // Fully traded
                    sendToClient(buyOrder->clientId, cancelMsg);
                    return;
                }
            } else {
                break; // No more matching possible
            }
        }
        
        // If order still has amount, add to buy book
        if (buyOrder->amount > 0) {
            buyOrders_.push(buyOrder);
            Message placedMsg(MessageType::ORDER_PLACED, buyOrder->clientId);
            placedMsg.orderPlaced = {buyOrder->orderId, buyOrder->price, buyOrder->amount};
            sendToClient(buyOrder->clientId, placedMsg);
        }
    }
    
    void matchSellOrder(std::shared_ptr<Order> sellOrder) {
        // Try to match with buy orders
        while (sellOrder->amount > 0 && !buyOrders_.empty()) {
            auto bestBuy = buyOrders_.top();
            
            if (sellOrder->price <= bestBuy->price) {
                buyOrders_.pop();
                
                int tradeAmount = std::min(sellOrder->amount, bestBuy->amount);
                int tradePrice = bestBuy->price; // Use the resting order's price
                
                // Update amounts
                sellOrder->amount -= tradeAmount;
                bestBuy->amount -= tradeAmount;
                
                // Send trade notifications
                Message sellTradeMsg(MessageType::ORDER_TRADED, sellOrder->clientId);
                sellTradeMsg.orderTraded = {sellOrder->orderId, tradePrice, tradeAmount};
                sendToClient(sellOrder->clientId, sellTradeMsg);
                
                Message buyTradeMsg(MessageType::ORDER_TRADED, bestBuy->clientId);
                buyTradeMsg.orderTraded = {bestBuy->orderId, tradePrice, tradeAmount};
                sendToClient(bestBuy->clientId, buyTradeMsg);
                
                // Handle fully traded orders
                if (bestBuy->amount == 0) {
                    activeOrders_.erase(bestBuy->orderId);
                    Message cancelMsg(MessageType::ORDER_CANCELED, bestBuy->clientId);
                    cancelMsg.orderCanceled = {bestBuy->orderId, 1}; // Fully traded
                    sendToClient(bestBuy->clientId, cancelMsg);
                } else {
                    buyOrders_.push(bestBuy); // Put back partially filled order
                }
                
                if (sellOrder->amount == 0) {
                    activeOrders_.erase(sellOrder->orderId);
                    Message cancelMsg(MessageType::ORDER_CANCELED, sellOrder->clientId);
                    cancelMsg.orderCanceled = {sellOrder->orderId, 1}; // Fully traded
                    sendToClient(sellOrder->clientId, cancelMsg);
                    return;
                }
            } else {
                break; // No more matching possible
            }
        }
        
        // If order still has amount, add to sell book
        if (sellOrder->amount > 0) {
            sellOrders_.push(sellOrder);
            Message placedMsg(MessageType::ORDER_PLACED, sellOrder->clientId);
            placedMsg.orderPlaced = {sellOrder->orderId, sellOrder->price, sellOrder->amount};
            sendToClient(sellOrder->clientId, placedMsg);
        }
    }
    
    void processCancelOrder(const CancelOrder& cancel) {
        std::lock_guard<std::mutex> lock(engineMutex_);
        
        auto it = activeOrders_.find(cancel.orderId);
        if (it == activeOrders_.end()) {
            // Order not found - could already be traded or invalid
            return;
        }
        
        auto order = it->second;
        activeOrders_.erase(it);
        
        // Note: In a real implementation, we'd need to remove from priority queues too
        // For simplicity, we'll let cancelled orders remain in queues but mark them as invalid
        order->amount = 0;
        
        Message cancelMsg(MessageType::ORDER_CANCELED, order->clientId);
        cancelMsg.orderCanceled = {cancel.orderId, 0}; // User canceled
        sendToClient(order->clientId, cancelMsg);
    }
    
public:
    MatchingEngine() : running_(false) {}
    
    void addClient(std::shared_ptr<MessageQueue> clientQueue) {
        toClients_.push_back(clientQueue);
    }
    
    void start() {
        running_ = true;
        
        std::thread([this]() {
            Message msg(MessageType::PLACE_ORDER, 0);
            
            while (running_) {
                if (toEngine_.pop(msg)) {
                    switch (msg.type) {
                        case MessageType::PLACE_ORDER:
                            processPlaceOrder(msg.placeOrder);
                            break;
                        case MessageType::CANCEL_ORDER:
                            processCancelOrder(msg.cancelOrder);
                            break;
                        default:
                            break;
                    }
                }
            }
        }).detach();
    }
    
    void stop() {
        running_ = false;
    }
    
    void submitOrder(const PlaceOrder& order) {
        Message msg(MessageType::PLACE_ORDER, order.clientId);
        msg.placeOrder = order;
        toEngine_.push(msg);
    }
    
    void submitCancel(const CancelOrder& cancel) {
        Message msg(MessageType::CANCEL_ORDER, 0);
        msg.cancelOrder = cancel;
        toEngine_.push(msg);
    }
    
    void printOrderBook() {
        std::lock_guard<std::mutex> lock(engineMutex_);
        
        std::cout << "\n=== ORDER BOOK ===" << std::endl;
        std::cout << "Buy Orders (Price - Amount):" << std::endl;
        auto buyOrdersCopy = buyOrders_;
        while (!buyOrdersCopy.empty()) {
            auto order = buyOrdersCopy.top();
            buyOrdersCopy.pop();
            if (order->amount > 0) {
                std::cout << "  " << order->price << " - " << order->amount << std::endl;
            }
        }
        
        std::cout << "Sell Orders (Price - Amount):" << std::endl;
        auto sellOrdersCopy = sellOrders_;
        while (!sellOrdersCopy.empty()) {
            auto order = sellOrdersCopy.top();
            sellOrdersCopy.pop();
            if (order->amount > 0) {
                std::cout << "  " << order->price << " - " << order->amount << std::endl;
            }
        }
        std::cout << "==================\n" << std::endl;
    }
};

class TestClient {
private:
    int clientId_;
    MatchingEngine* engine_;
    std::shared_ptr<MessageQueue> messageQueue_;
    std::atomic<bool> running_;
    std::mt19937 rng_;
    
public:
    TestClient(int id, MatchingEngine* engine) 
        : clientId_(id), engine_(engine), running_(false), rng_(std::random_device{}()) {
        messageQueue_ = std::make_shared<MessageQueue>();
        engine_->addClient(messageQueue_);
    }
    
    void start() {
        running_ = true;
        
        // Message handler thread
        std::thread([this]() {
            Message msg(MessageType::ORDER_PLACED, clientId_);
            
            while (running_) {
                if (messageQueue_->pop(msg)) {
                    handleMessage(msg);
                }
            }
        }).detach();
        
        // Order generation thread
        std::thread([this]() {
            std::this_thread::sleep_for(std::chrono::milliseconds(100 * clientId_)); // Stagger start
            
            for (int i = 0; i < 10 && running_; ++i) {
                generateRandomOrder(i);
                std::this_thread::sleep_for(std::chrono::milliseconds(500 + rng_() % 1000));
            }
        }).detach();
    }
    
    void stop() {
        running_ = false;
    }
    
private:
    void handleMessage(const Message& msg) {
        switch (msg.type) {
            case MessageType::ORDER_PLACED:
                std::cout << "Client " << clientId_ << ": Order " << msg.orderPlaced.orderId 
                         << " placed at price " << msg.orderPlaced.price 
                         << " for amount " << msg.orderPlaced.amount << std::endl;
                break;
                
            case MessageType::ORDER_TRADED:
                std::cout << "Client " << clientId_ << ": Order " << msg.orderTraded.orderId 
                         << " traded " << msg.orderTraded.tradedAmount 
                         << " at price " << msg.orderTraded.tradedPrice << std::endl;
                break;
                
            case MessageType::ORDER_CANCELED:
                std::cout << "Client " << clientId_ << ": Order " << msg.orderCanceled.orderId 
                         << " canceled (reason: " << msg.orderCanceled.reasonCode << ")" << std::endl;
                break;
                
            case MessageType::REQUEST_REJECTED:
                std::cout << "Client " << clientId_ << ": Order " << msg.requestRejected.orderId 
                         << " rejected: " << msg.requestRejected.message << std::endl;
                break;
                
            default:
                break;
        }
    }
    
    void generateRandomOrder(int sequence) {
        int orderId = clientId_ * 1000 + sequence * 10 + (clientId_ % 2); // Ensure proper buy/sell distribution
        int price = 95 + rng_() % 15; // Price between 95-110
        int amount = 10 + rng_() % 90; // Amount between 10-100
        
        PlaceOrder order{orderId, price, amount, clientId_};
        
        std::cout << "Client " << clientId_ << ": Placing " 
                 << (orderId % 2 == 0 ? "BUY" : "SELL") 
                 << " order " << orderId << " at price " << price 
                 << " for amount " << amount << std::endl;
        
        engine_->submitOrder(order);
    }
};

int main() {
    std::cout << "Starting Tether Matching Engine Demo\n" << std::endl;
    
    MatchingEngine engine;
    engine.start();
    
    // Create test clients
    TestClient client1(0, &engine);
    TestClient client2(1, &engine);
    
    client1.start();
    client2.start();
    
    // Let the simulation run
    std::this_thread::sleep_for(std::chrono::seconds(8));
    
    // Print final order book state
    engine.printOrderBook();
    
    // Stop everything
    client1.stop();
    client2.stop();
    engine.stop();
    
    std::this_thread::sleep_for(std::chrono::milliseconds(500)); // Allow cleanup
    
    std::cout << "Demo completed." << std::endl;
    
    return 0;
}