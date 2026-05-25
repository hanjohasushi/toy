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
    serverAddr.sin_port = htons(6347);

    // Localhost
    // if (inet_pton(AF_INET, "127.0.0.1", &serverAddr.sin_addr) <= 0)
    if (inet_pton(AF_INET, "10.0.2.2", &serverAddr.sin_addr) <= 0)
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
    auto *data = "hello from client";

    std::deque<Job> submissionQueue;
    std::mutex queueMtx;
    std::condition_variable queueCV;

    // transport thread sleep until submission queue filled
    std::thread transportThread([&queueMtx, &submissionQueue, &queueCV, &clientSocket]()
                                {
                                    std::unique_lock<std::mutex> lock(queueMtx);
                                    queueCV.wait(lock, [&]
                                                 { return !submissionQueue.empty(); });

                                    Job job = submissionQueue.front();
                                    lock.unlock();

                                    // step 0 prepare frame into wsabuff/ iovec
                                    iovec vecs[2];
                                   vecs[0].iov_base = &job.frame.header;
                                   vecs[0].iov_len = sizeof(Header);

                                   std::cout << "size of header actually " << sizeof(Header) << "\n";

                                   vecs[1].iov_base = const_cast<uint8_t *>(job.frame.payload.data);
                                   vecs[1].iov_len = job.frame.payload.len;

                                    // step1 call epoll here and consume it
                                    int bytesSent = writev(clientSocket, vecs, 2);

                                    if (bytesSent < 0)
                                        {
                                            std::cout << "Send failed: "
                                                    << strerror(errno) << "\n";
                                        }
                                    else
                                        {
                                            std::cout << "Data sending successful.\n";
                                        }

                                    // step2 when job completes then we post into completion queue

                                    // step3 at last we pop front from submission queue
                                    
                                    submissionQueue.pop_front(); });

    // producer thread
    std::thread producerThread([&queueMtx, &submissionQueue, &queueCV, &data]()
                               {
                                   Frame f;
                                   f.header.magic = 0xCAFE;
                                   f.header.version = 0x01;
                                   f.header.flags = 0x00;
                                   f.header.type = 0x01;
                                   f.header.length = strlen(data);
                                   f.header.reserved = 0x00;

                                   f.payload.data = reinterpret_cast<const uint8_t *>(data);
                                   f.payload.len = strlen(data);

                                   

                                   Job job(f);

                                   {
                                       std::lock_guard<std::mutex> lock(queueMtx);
                                       submissionQueue.push_front(job);
                                   }

                                   queueCV.notify_one(); });

    transportThread.join();
    producerThread.join();

    // Cleanup
    close(clientSocket);

    return 0;
}
