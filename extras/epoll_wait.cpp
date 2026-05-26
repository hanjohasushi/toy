#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <unistd.h>

#include <thread>
#include <iostream>

void worker(int epfd)
{
    epoll_event events[10];

    std::cout << "Sleeping in epoll wait\n";
    int n = epoll_wait(epfd, events, 10, -1);
    std::cout << "epoll wait woke up\n";

    if (events[0].events & EPOLLIN)
    {
        uint64_t value;
        read(epfd, &value, sizeof(value));

        std::cout << "received value " << value << "\n";
    }
}

int main()
{
    // create fd
    int efd = eventfd(0, 0);

    // create epoll
    int epfd = epoll_create1(0);

    // register events
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = efd;

    epoll_ctl(epfd, EPOLL_CTL_ADD, efd, &ev);

    std::thread t(worker, epfd);

    std::this_thread::sleep_for(std::chrono::seconds(10));

    uint64_t value = 1;
    write(efd, &value, sizeof(value));

    t.join();

    close(efd);
    close(epfd);
}