#include <sys/socket.h>
#include <vector>
#include <deque>
#include <mutex>
#include <chrono>
#include <memory>

enum class ClientType
{
    Unknown,
    Chat,
    Telemetry,
    File
};

enum class SessionState
{
    Handshake,
    Connected,
    Active,
    Closed
};
class Frame;

class ClientSession
{
public:

    int socket;
    ClientType type;
    SessionState state;

    // receive side
    std::vector<uint8_t> readBuffer;

    // send side
    std::deque<std::shared_ptr<Frame>> pendingQueue;

    bool sending = false;

  

    std::chrono::system_clock::time_point createdAt;
    std::chrono::steady_clock::time_point lastActivity;

    int errorCount;

    ClientSession(int s_) : socket(std::move(s_)),
                                            createdAt(std::chrono::system_clock::now()),
                                            lastActivity(std::chrono::steady_clock::now()),
                                            errorCount(0),
                                            state(SessionState::Handshake),
                                            type(ClientType::Unknown) {

                                            };

    // for adding authentication logic later
};