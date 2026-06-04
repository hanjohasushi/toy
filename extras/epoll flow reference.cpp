if (events & (EPOLLERR | EPOLLHUP | EPOLLRDHUP))
{
    disconnectClient(...);
    continue;
}

if (events & EPOLLIN)
{
     while (true)
    {
        ssize_t bytes_read = recv(...);

        if (bytes_read > 0)
        {
            // append to read buffer
            // parse frames/messages
        }
        else if (bytes_read == 0)
        {
            // orderly shutdown
            // disconnect client
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
            break;
        }
    }
}

if (events & EPOLLOUT)
{
    while (!sendQueue.empty())
    {
        ssize_t bytes_sent = send(...);

        if (bytes_sent > 0)
        {
            // advance operation
        }
        else
        {
            if (errno == EAGAIN || errno == EWOULDBLOCK)
            {
                // kernel send buffer full
                break;
            }

            // actual send error
            // disconnect client
            break;
        }
    }
}