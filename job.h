#include "frame.h"
#include <memory>
#include <chrono>

class Job
{
public:
    Frame frame;
    int bytesSent;
    std::chrono::system_clock::time_point createdAt;
    Job(Frame f_) : createdAt(std::chrono::system_clock::now()), frame(std::move(f_)) {};
};
