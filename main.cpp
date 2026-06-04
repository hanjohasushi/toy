#include <arpa/inet.h>
#include <errno.h>
#include <netinet/in.h>
#include <sys/socket.h>
#include <sys/uio.h>
#include <unistd.h>

#include <chrono>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <thread>
#include <random>

// producer consumer
#include <thread>
#include <mutex>
#include <condition_variable>
#include <deque>

#include "frame.h"
#include "job.h"

int main()
{
    // Step 1: Create client socket
    int clientSocket = socket(AF_INET, SOCK_STREAM, 0);

    if (clientSocket < 0)
    {
        std::cout << "Socket creation failed: "
                  << strerror(errno) << "\n";
        return 1;
    }
    else
    {
        std::cout << "Socket creation successful.\n";
    }

    // Step 2: Define server address
    sockaddr_in serverAddr{};
    serverAddr.sin_family = AF_INET;
    serverAddr.sin_port = htons(9271);

    // Localhost
    // if (inet_pton(AF_INET, "10.0.2.2", &serverAddr.sin_addr) <= 0)
    if (inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr) <= 0)
    {
        std::cout << "Invalid address.\n";
        close(clientSocket);
        return 1;
    }

    // Step 3: Connect to server
    int connection =
        connect(clientSocket,
                (sockaddr *)&serverAddr,
                sizeof(serverAddr));

    if (connection < 0)
    {
        std::cout << "Connect failed: "
                  << strerror(errno) << "\n";

        close(clientSocket);
        return 1;
    }
    else
    {
        std::cout << "Socket connection successful.\n";
    }

    ///////////////////// threads //////////////////////

    // application layer is owner of memory/ payload
    // later struct or some way to pass this pointer

    std::deque<Job> submissionQueue;
    std::mutex queueMtx;
    std::condition_variable queueCV;

    // transport thread sleep until submission queue filled
    std::thread transportThread([&queueMtx, &submissionQueue, &queueCV, &clientSocket]()
                                {
                                    // while(true){
                                    std::unique_lock<std::mutex> lock(queueMtx);
                                    queueCV.wait(lock, [&]
                                                 { return !submissionQueue.empty(); });

                                    Job job = submissionQueue.front();
                                    lock.unlock();

                                    // step1 call epoll here and consume it
                                    int bytesSent = writev(clientSocket, job.buffs.data(), job.count);

                                    if (bytesSent == 0)
                                    {
                                        std::cout << "send attempted, socket is closed or check\n";
                                    }

                                    if (bytesSent < 0)
                                    {
                                        std::cout << "Send failed: "
                                                  << strerror(errno) << "\n";
                                    }
                                    else
                                    {
                                        std::cout << "Data sending successful.\n";
                                    }
                                    std::cout << "error at transport " << errno << std::endl;

                                    // step2 when job completes then we post into completion queue

                                    // step3 at last we pop front from submission queue

                                    submissionQueue.pop_front();
                                    // }
                                });

    // producer thread
    std::thread producerThread([&queueMtx, &submissionQueue, &queueCV]()
                               {
                                   std::string payload(100 * 1024, 'A');

                                   Frame f;
                                   f.header.magic = 0xCAFE;
                                   f.header.version = 0x01;
                                   f.header.flags = 0x00;
                                   f.header.type = 0x01;
                                   f.header.length = payload.size();
                                   f.header.reserved = 0x00;

                                   f.payload.data =
                                       reinterpret_cast<const uint8_t *>(payload.data());

                                   f.payload.len = payload.size();

                                   Job j(2);
                                   iovec i1;
                                   i1.iov_base = &f.header;
                                   i1.iov_len = sizeof(Header);
                                   j.buffs.push_back(i1);

                                   iovec i2;
                                   i2.iov_base = const_cast<uint8_t *>(f.payload.data);
                                   i2.iov_len = f.payload.len;
                                   j.buffs.push_back(i2);

                                   {
                                       std::lock_guard<std::mutex> lock(queueMtx);
                                       submissionQueue.push_front(j);
                                   }
                                   queueCV.notify_one();

                                   // auto *data1 = "Hello there! from client";
                                   // std::cout << "frame 1 size " << strlen(data1) << std::endl;
                                   // Frame f1;
                                   //                                    f1.header.magic = 0xCAFE;
                                   //                                    f1.header.version = 0x01;
                                   //                                    f1.header.flags = 0x00;
                                   //                                    f1.header.type = 0x01;
                                   //                                    f1.header.length = strlen(data1);
                                   //                                    f1.header.reserved = 0x00;

                                   //                                    f1.payload.data = reinterpret_cast<const uint8_t *>(data1);
                                   //                                    f1.payload.len = strlen(data1);

                                   //                                    Job jf1(2);

                                   //                                    iovec if1;
                                   //                                    if1.iov_base = &f1.header;
                                   //                                    if1.iov_len = sizeof(Header);
                                   //                                    jf1.buffs.push_back(if1);

                                   //                                    iovec if2;
                                   //                                    if2.iov_base = const_cast<uint8_t *>(f1.payload.data);
                                   //                                    if2.iov_len = f1.payload.len;
                                   //                                    jf1.buffs.push_back(if2);

                                   //                                    {
                                   //                                        std::lock_guard<std::mutex> lock(queueMtx);
                                   //                                        submissionQueue.push_front(jf1);
                                   //                                    }
                                   //                                    queueCV.notify_one();

                                   // std::this_thread::sleep_for(std::chrono::seconds(6));

                                   // auto *data2f = "this is frame 2 incoming";
                                   // std::cout << "frame 2 size " << strlen(data2f) << std::endl;
                                   // Frame f2;
                                   //                                    f2.header.magic = 0xCAFE;
                                   //                                    f2.header.version = 0x01;
                                   //                                    f2.header.flags = 0x00;
                                   //                                    f2.header.type = 0x01;
                                   //                                    f2.header.length = strlen(data2f);
                                   //                                    f2.header.reserved = 0x00;

                                   //                                    f2.payload.data = reinterpret_cast<const uint8_t *>(data2f);
                                   //                                    f2.payload.len = strlen(data2f);

                                   //                                    Job jf2(2);

                                   //                                    iovec if20;
                                   //                                    if20.iov_base = &f2.header;
                                   //                                    if20.iov_len = sizeof(Header);
                                   //                                    jf2.buffs.push_back(if20);

                                   //                                    iovec if21;
                                   //                                    if21.iov_base = const_cast<uint8_t *>(f2.payload.data);
                                   //                                    if21.iov_len = f2.payload.len;
                                   //                                    jf2.buffs.push_back(if21);

                                   //                                    {
                                   //                                        std::lock_guard<std::mutex> lock(queueMtx);
                                   //                                        submissionQueue.push_front(jf2);
                                   //                                    }
                                   //                                    queueCV.notify_one();

                                   // std::this_thread::sleep_for(std::chrono::seconds(8));

                                   //                                    auto *completedata = "this is third frame !";
                                   //                                    auto *datap = "th";

                                   //                                    Frame f;
                                   //                                    f.header.magic = 0xCAFE;
                                   //                                    f.header.version = 0x01;
                                   //                                    f.header.flags = 0x00;
                                   //                                    f.header.type = 0x01;
                                   //                                    f.header.length = strlen(completedata);
                                   //                                    f.header.reserved = 0x00;

                                   //                                    f.payload.data = reinterpret_cast<const uint8_t *>(datap);
                                   //                                    f.payload.len = strlen(datap);

                                   //                                    Job job1(2);

                                   //                                    iovec ioh;
                                   //                                    ioh.iov_base = &f.header;
                                   //                                    ioh.iov_len = sizeof(Header);
                                   //                                    job1.buffs.push_back(ioh);

                                   //                                    iovec io1;
                                   //                                    io1.iov_base = const_cast<uint8_t *>(f.payload.data);
                                   //                                    io1.iov_len = f.payload.len;
                                   //                                    job1.buffs.push_back(io1);
                                   //                                    {
                                   //                                        std::lock_guard<std::mutex> lock(queueMtx);
                                   //                                        submissionQueue.push_front(job1);
                                   //                                    }
                                   //                                    queueCV.notify_one();

                                   //                                    std::this_thread::sleep_for(std::chrono::seconds(3));

                                   //                                    auto *data2 = "is is thir";
                                   //                                    Job job2(1);
                                   //                                    iovec io2;
                                   //                                    io2.iov_base = const_cast<char *>(data2);
                                   //                                    io2.iov_len = strlen(data2);
                                   //                                    job2.buffs.push_back(io2);
                                   //                                    {
                                   //                                        std::lock_guard<std::mutex> lock(queueMtx);
                                   //                                        submissionQueue.push_front(job2);
                                   //                                    }
                                   //                                    queueCV.notify_one();

                                   //                                    std::this_thread::sleep_for(std::chrono::seconds(5));

                                   //                                    auto *data3 = "d frame !";
                                   //                                    Job job3(1);
                                   //                                    iovec io3;
                                   //                                    io3.iov_base = const_cast<char *>(data3);
                                   //                                    io3.iov_len = strlen(data3);
                                   //                                    job3.buffs.push_back(io3);
                                   //                                    {
                                   //                                        std::lock_guard<std::mutex> lock(queueMtx);
                                   //                                        submissionQueue.push_front(job3);
                                   //                                    }
                                   //                                    queueCV.notify_one();
                               });

    transportThread.join();
    producerThread.join();

    std::this_thread::sleep_for(std::chrono::seconds(20));

    char buffer[1024];
    while (true)
    {

        ssize_t bytes_read = recv(clientSocket, &buffer, sizeof(buffer), 0);
        std::string msg(buffer, bytes_read);
        std::cout << "server says " << msg << std::endl;
    }

    // Cleanup
    close(clientSocket);

    return 0;
}
