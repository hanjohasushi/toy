#include <memory>
#include <chrono>
#include <sys/uio.h>
#include <vector>

class Job
{
public:
    std::vector<struct iovec> buffs;
    int count;
    int bytesSent;
    std::chrono::system_clock::time_point createdAt;
    Job(int n) : createdAt(std::chrono::system_clock::now()), count(n) {};
};
