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

void transport(int epollfd)
{
    epoll_event evs[10];
    int n = epoll_wait(epollfd, evs, 64, -1);

    std::cout << " value of n when wpoll wakes " << n << "\nprinting events\n";

    for (auto &i : evs)
    {
        std::cout << "epoll event is " << i.events << " \n";
    }

    if (evs[0].events & EPOLLIN)
    {
        uint64_t buff;
        read(epollfd, &buff, sizeof(buff));
        std::cout << "received value " << buff << "\n";
    }
}

void produce(int epollfd, Data *data, std::deque<Frame> &squeue)
{
    // take data and produce frames
    int itrNeeded;

    if (data->payload < 4)
        itrNeeded = 3;

    else if (data->payload < 8)
        itrNeeded = 5;

    else
        itrNeeded = 7;

    Frame f;
    f.id = data->id;
    f.noOfItrReq = itrNeeded;

    // work is in progress
    std::this_thread::sleep_for(std::chrono::seconds(5));
    squeue.emplace_back(f);

    uint64_t value = 1;
    write(epollfd, &value, sizeof(value));

    std::cout << "Data written - now epoll must wake up\n";
}

int main()
{

    std::deque<Frame> submissionQ;

    // step 1 create submission fd
    int submissionfd = eventfd(0, 0);

    // step 2 create epoll
    int epollfd = epoll_create1(0);

    // step 3 register event
    epoll_event ev{};
    ev.events = EPOLLOUT;
    ev.data.fd = submissionfd;

    epoll_ctl(epollfd, EPOLL_CTL_ADD, submissionfd, &ev);

    std::thread transporter(transport, std::ref(epollfd));

    Data *data = new Data();
    data->id = 1729;
    data->payload = 10;

    std::thread producer(produce);

    transporter.join();
    producer.join();

    // gets some kind of completion and get back id = 1729
    delete data;

    std::this_thread::sleep_for(std::chrono::minutes(5));
    return 0;
}
