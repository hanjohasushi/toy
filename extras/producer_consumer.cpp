#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <thread>
#include <deque>
#include <iostream>

struct Job
{
    int id;
    int noOfItr = 0;
    std::chrono::system_clock::time_point createdAt;
    Job(int id_, int itr_) : id(id_), noOfItr(itr_), createdAt(std::chrono::system_clock::now()) {};
};

struct Data
{
    int payload;
    int id;
};

struct Frame
{
    int id;
    int noOfItrReq;
};


int main()
{

    std::deque<Frame> submissionQ;

    // step 1 create submission fd
    int submissionfd = eventfd(0, 0);

// step 2 create epoll
int epollfd = epoll_create1(0);

//step 3 register event
epoll_event ev{};











    Data *data = new Data();
    data->id = 1729;
    data->payload = 10;

    // gets some kind of completion and get back id = 1729
    delete data;
    return 0;
}
