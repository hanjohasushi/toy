#include <sys/epoll.h>
#include <sys/eventfd.h>
#include <sys/socket.h>
#include <unistd.h>
#include <netinet/in.h>
#include <fcntl.h>
#include <errno.h>

#include "clientSession.h"
#include "frame.h"

#include <cstring>

#include <thread>
#include <deque>
#include <unordered_map>
#include <iostream>

enum class EventSourceType
{
    SubmissionEvent,
    ListenerEvent,
    ClientEvent
};

struct EventContext
{
    EventSourceType type;
    int fd;

    EventContext(EventSourceType t, int f) : type(t), fd(f)
    {
    }
};

struct SubmissionContext
{
    EventContext base;
    std::deque<Frame> *submissionQueue;

    SubmissionContext(
        int fd, std::deque<Frame> *q) : base(EventSourceType::SubmissionEvent, fd), submissionQueue(q)
    {
    }
};

struct ListenerContext
{
    EventContext base;
    int port;

    // *imp later add protocol structure
    ListenerContext(int fd, int port_) : base(EventSourceType::ListenerEvent, fd), port(port_) {}
};

using SessionId = u_int32_t;
struct ClientContext
{
    EventContext base;
    SessionId id;
    ClientSession *session;
    ClientContext(int fd, ClientSession *ptr, SessionId id_) : base(EventSourceType::ClientEvent, fd), session(ptr), id(id_) {}
};

void processReadBuffer(ClientSession &session)
{
    while (true)
    {
        // not enough header)
        if (session.readBuffer.size() < sizeof(Header))
            return;

        Header hdr;
        std::memcpy(&hdr,
                    session.readBuffer.data(),
                    sizeof(Header));

        size_t frameSize =
            sizeof(Header) + hdr.length;

        std::cout << "we now know the frame size " << frameSize << std::endl;

        // not enough payload
        if (session.readBuffer.size() < frameSize)
            return;

        // extract frame - move to some higher level app buffer
        // for now its simple string so
        std::string payload(
            reinterpret_cast<char *>(session.readBuffer.data() + sizeof(Header)), hdr.length);

        // dispatch frame
        std::cout << "complete msg - " << payload << std::endl;

        // remove consumed bytes
        session.readBuffer.erase(
            session.readBuffer.begin(),
            session.readBuffer.begin() + frameSize);
    }
}

void disconnectClient(int epollfd, ClientContext *cctx, std::unordered_map<SessionId, ClientSession> &sessions)
{
    epoll_ctl(epollfd, EPOLL_CTL_DEL, cctx->base.fd, nullptr);
    close(cctx->base.fd);
    sessions.erase(cctx->id);
    delete cctx;
}

void transport(int epollfd)
{
    epoll_event evs[64];
    SessionId nextSessionId = 1;
    std::unordered_map<SessionId, ClientSession> sessions;

    while (true)
    {
        int n = epoll_wait(epollfd, evs, 64, -1);
        std::cout << " value of n when wpoll woke " << n << "\n";

        for (int i = 0; i < n; i++)
        {
            EventContext *ctx = reinterpret_cast<EventContext *>(evs[i].data.ptr);

            switch (ctx->type)
            {
            case EventSourceType::SubmissionEvent:
            {
                // Step 1 read fd
                uint64_t buff;
                read(ctx->fd, &buff, sizeof(buff));
                std::cout << "submission event, received value " << buff << "\n";

                SubmissionContext *sctx = reinterpret_cast<SubmissionContext *>(ctx);

                // Step 2 process queue
                std::cout << "queue size = " << sctx->submissionQueue->size();
                std::cout << "now drain the queue and perform \n";
                std::this_thread::sleep_for(std::chrono::seconds(2));
                break;
            }

            case EventSourceType::ListenerEvent:
            {
                std::cout << "listener event\n";
                // Step 1 read fd
                sockaddr_in client_addr{};
                socklen_t client_len = sizeof(client_addr);

                // Step 2 accept
                int client_fd = accept(ctx->fd,
                                       reinterpret_cast<sockaddr *>(&client_addr),
                                       &client_len);

                if (client_fd == -1)
                {
                    std::cout << "error code during accept is " << errno;
                    break;
                }

                int flags = fcntl(client_fd, F_GETFL, 0);
                fcntl(client_fd, F_SETFL, flags | O_NONBLOCK);

                std::cout << "Client accepted with fd " << client_fd << std::endl;

                sessions.emplace(nextSessionId, ClientSession(client_fd));
                ClientSession *cs = &sessions.at(nextSessionId);

                ClientContext *cctx = new ClientContext(client_fd, cs, nextSessionId++);

                // step 3 register event
                epoll_event evc{};
                evc.events = EPOLLIN;
                evc.data.ptr = cctx;

                epoll_ctl(epollfd, EPOLL_CTL_ADD, client_fd, &evc);
                std::cout << "Client registered with epoll\n";
                break;
            }

            case EventSourceType::ClientEvent:
            {
                std::cout << "client event occured \n";
                ClientContext *cctx = reinterpret_cast<ClientContext *>(ctx);

                size_t totalBytesReceived = 0;

                if (evs[i].events & (EPOLLERR | EPOLLHUP))
                {
                    disconnectClient(epollfd, cctx, sessions);
                    continue;
                }

                if (evs[i].events & (EPOLLIN | EPOLLRDHUP))
                {
                    char buffer[4096];

                    while (true)
                    {
                        ssize_t bytes_read = recv(ctx->fd, buffer, sizeof(buffer), 0);

                        totalBytesReceived += bytes_read;

                        std::cout
                            << "recv " << bytes_read << " bytes, total=" << totalBytesReceived << "\n";

                        if (bytes_read > 0)
                        {
                            // append to read buffer
                            cctx->session->readBuffer
                                .insert(cctx->session->readBuffer.end(), buffer, buffer + bytes_read);
                        }
                        else if (bytes_read == 0)
                        {
                            // orderly shutdown
                            // disconnect client
                            disconnectClient(epollfd, cctx, sessions);
                            break;
                        }
                        else
                        {
                            if (errno == EAGAIN || errno == EWOULDBLOCK)
                            {
                                // socket drained
                                break;
                            }

                            // actual recv error
                            // disconnect client
                            disconnectClient(epollfd, cctx, sessions);
                            break;
                        }
                    }
                    // call parser
                    processReadBuffer(*cctx->session);
                }

                if (evs[i].events & EPOLLOUT)
                {
                    // later
                }
                break;
            }

            default:
                std::cout << "default case - wrong flow \n";
            }
            std::cout << "Itr complete \n\n";
        }
    }
}

int main()
{
    std::deque<Frame> submissionQ;

    // step 1 create submission fd
    // int submissionfd = eventfd(0, 0);
    int submissionfd = eventfd(0, EFD_NONBLOCK);

    std::cout << "submission fd is " << submissionfd << "\n";

    // step 2 create structures
    int epollfd = epoll_create1(0);

    SubmissionContext *sctx = new SubmissionContext(submissionfd, &submissionQ);

    // step 3 register event
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.ptr = sctx;

    epoll_ctl(epollfd, EPOLL_CTL_ADD, submissionfd, &ev);

    // step 1 create server fd
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    int flags = fcntl(server_fd, F_GETFL, 0);
    fcntl(server_fd, F_SETFL, flags | O_NONBLOCK);

    std::cout << "server fd is " << server_fd << "\n";

    // step 2 allow fast restart
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));

    // step 3 bind socket
    int port = 9271;
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(port);

    bind(server_fd, reinterpret_cast<sockaddr *>(&server_addr), sizeof(server_addr));

    // step 4 listen
    listen(server_fd, SOMAXCONN);

    ListenerContext *lctx = new ListenerContext(server_fd, port);

    // step 5 register
    epoll_event evs{};
    evs.events = EPOLLIN;
    evs.data.ptr = lctx;

    std::cout << "error " << errno << "\n";

    int status = epoll_ctl(epollfd, EPOLL_CTL_ADD, server_fd, &evs);
    if (status == -1)
        std::cout << errno;
    else
        std::cout << "listner successfully registered ";

   std::thread transporter(transport, std::ref(epollfd));

    transporter.join();

    delete sctx;
    delete lctx;

    std::this_thread::sleep_for(std::chrono::minutes(5));
    return 0;
}
