// epoll_server.cpp
//
// Linux TCP server using:
// - socket()
// - bind()
// - listen()
// - epoll
// - non-blocking sockets
//
// Listens on port 9271
//
// Build:
//   g++ -std=c++17 -O2 epoll_server.cpp -o epoll_server
//
// Run:
//   ./epoll_server
//
// Test:
//   telnet 127.0.0.1 9271
//   nc 127.0.0.1 9271

#include <arpa/inet.h>
#include <errno.h>
#include <fcntl.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/socket.h>
#include <unistd.h>

#include <cstring>
#include <iostream>
#include <string>

constexpr int PORT = 9271;
constexpr int MAX_EVENTS = 64;
constexpr int BUFFER_SIZE = 4096;

// Set fd to non-blocking mode
bool setNonBlocking(int fd)
{
    int flags = fcntl(fd, F_GETFL, 0);
    if (flags == -1)
    {
        perror("fcntl(F_GETFL)");
        return false;
    }

    if (fcntl(fd, F_SETFL, flags | O_NONBLOCK) == -1)
    {
        perror("fcntl(F_SETFL)");
        return false;
    }

    return true;
}

int main()
{
    // ---------------------------------------------------------
    // Create server socket
    // ---------------------------------------------------------
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd == -1)
    {
        perror("socket");
        return 1;
    }

    // Allow fast restart
    int opt = 1;
    if (setsockopt(server_fd,
                   SOL_SOCKET,
                   SO_REUSEADDR,
                   &opt,
                   sizeof(opt)) < 0)
    {
        perror("setsockopt");
        close(server_fd);
        return 1;
    }

    // ---------------------------------------------------------
    // Make server socket non-blocking
    // ---------------------------------------------------------
    if (!setNonBlocking(server_fd))
    {
        close(server_fd);
        return 1;
    }

    // ---------------------------------------------------------
    // Bind socket to port 9271
    // ---------------------------------------------------------
    sockaddr_in server_addr{};
    server_addr.sin_family = AF_INET;
    server_addr.sin_addr.s_addr = INADDR_ANY;
    server_addr.sin_port = htons(PORT);

    if (bind(server_fd,
             reinterpret_cast<sockaddr*>(&server_addr),
             sizeof(server_addr)) < 0)
    {
        perror("bind");
        close(server_fd);
        return 1;
    }

    // ---------------------------------------------------------
    // Listen
    // ---------------------------------------------------------
    if (listen(server_fd, SOMAXCONN) < 0)
    {
        perror("listen");
        close(server_fd);
        return 1;
    }

    std::cout << "Server listening on port " << PORT << std::endl;

    // ---------------------------------------------------------
    // Create epoll instance
    // ---------------------------------------------------------
    int epoll_fd = epoll_create1(0);
    if (epoll_fd == -1)
    {
        perror("epoll_create1");
        close(server_fd);
        return 1;
    }

    // ---------------------------------------------------------
    // Add server socket to epoll
    // ---------------------------------------------------------
    epoll_event ev{};
    ev.events = EPOLLIN;
    ev.data.fd = server_fd;

    if (epoll_ctl(epoll_fd, EPOLL_CTL_ADD, server_fd, &ev) == -1)
    {
        perror("epoll_ctl: server_fd");
        close(server_fd);
        close(epoll_fd);
        return 1;
    }

    epoll_event events[MAX_EVENTS];

    // ---------------------------------------------------------
    // Event loop
    // ---------------------------------------------------------
    while (true)
    {
        int nfds = epoll_wait(epoll_fd,
                              events,
                              MAX_EVENTS,
                              -1);

        if (nfds == -1)
        {
            if (errno == EINTR)
                continue;

            perror("epoll_wait");
            break;
        }

        for (int i = 0; i < nfds; ++i)
        {
            int fd = events[i].data.fd;

            // -------------------------------------------------
            // New incoming connection
            // -------------------------------------------------
            if (fd == server_fd)
            {
                while (true)
                {
                    sockaddr_in client_addr{};
                    socklen_t client_len = sizeof(client_addr);

                    int client_fd = accept(server_fd,
                                           reinterpret_cast<sockaddr*>(&client_addr),
                                           &client_len);

                    if (client_fd == -1)
                    {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                        {
                            // No more clients to accept
                            break;
                        }

                        perror("accept");
                        break;
                    }

                    // Make client socket non-blocking
                    if (!setNonBlocking(client_fd))
                    {
                        close(client_fd);
                        continue;
                    }

                    // Add client to epoll
                    epoll_event client_ev{};
                    client_ev.events = EPOLLIN | EPOLLET;
                    client_ev.data.fd = client_fd;

                    if (epoll_ctl(epoll_fd,
                                  EPOLL_CTL_ADD,
                                  client_fd,
                                  &client_ev) == -1)
                    {
                        perror("epoll_ctl: client_fd");
                        close(client_fd);
                        continue;
                    }

                    char ip[INET_ADDRSTRLEN];

                    inet_ntop(AF_INET,
                              &client_addr.sin_addr,
                              ip,
                              sizeof(ip));

                    std::cout << "New client connected: "
                              << ip
                              << ":"
                              << ntohs(client_addr.sin_port)
                              << " fd="
                              << client_fd
                              << std::endl;
                }
            }
            else
            {
                // -------------------------------------------------
                // Handle client data
                // -------------------------------------------------
                bool client_closed = false;

                while (true)
                {
                    char buffer[BUFFER_SIZE];

                    ssize_t bytes_read = recv(fd,
                                              buffer,
                                              sizeof(buffer),
                                              0);

                    if (bytes_read > 0)
                    {
                        std::string msg(buffer, bytes_read);

                        std::cout << "Client fd "
                                  << fd
                                  << " says: "
                                  << msg
                                  << std::endl;

                        // Echo back to client
                        ssize_t bytes_sent = send(fd,
                                                  buffer,
                                                  bytes_read,
                                                  0);

                        if (bytes_sent == -1)
                        {
                            perror("send");
                        }
                    }
                    else if (bytes_read == 0)
                    {
                        // Client disconnected
                        std::cout << "Client fd "
                                  << fd
                                  << " disconnected"
                                  << std::endl;

                        client_closed = true;
                        break;
                    }
                    else
                    {
                        if (errno == EAGAIN || errno == EWOULDBLOCK)
                        {
                            // All data processed
                            break;
                        }

                        perror("recv");
                        client_closed = true;
                        break;
                    }
                }

                if (client_closed)
                {
                    epoll_ctl(epoll_fd,
                              EPOLL_CTL_DEL,
                              fd,
                              nullptr);

                    close(fd);
                }
            }
        }
    }

    close(server_fd);
    close(epoll_fd);

    return 0;
}