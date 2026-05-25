#include <thread>
#include <deque>
#include <mutex>
#include <condition_variable>
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

void produce(Data *data, std::deque<Frame> &queue, std::condition_variable &cv, std::mutex &mtx)
{

    std::this_thread::sleep_for(std::chrono::seconds(3));
    {
        std::lock_guard<std::mutex> lock(mtx);
        Frame f;
        f.id = data->id;
        f.noOfItrReq = data->payload;
        queue.push_front(f);
    }
    std::cout << "notify cv\n";
    cv.notify_one();
}

void transport(std::deque<Frame> &queue, std::condition_variable &qcv, std::mutex &mtx)
{
    std::deque<Job> inFlightQueue;

    while (true)
    {
        std::unique_lock<std::mutex> lock(mtx);
        std::cout << "here before lock\n";

        qcv.wait(lock, [&]()
                 { return !queue.empty() || !inFlightQueue.empty(); });

        std::cout << "here after lock\n";

        // admits submissions
        if (!queue.empty())
        {
            Frame f = queue.front();
            inFlightQueue.emplace_back(f.id, f.noOfItrReq);
            queue.pop_front();
        }
        lock.unlock();

        // progresses in-flight operations
        Job &job = inFlightQueue.front();
        job.noOfItr--;

        // now job is complete
        if (job.noOfItr <= 0)
        {
            // generate some completion
            // std::condition_variable &ncv
            inFlightQueue.pop_front();
        }

        std::cout << "data remaining " << job.noOfItr << "\n";
    }
}

int main()
{

    std::deque<Frame> submissionQ;
    std::condition_variable qCV;
    std::mutex qmtx;

    Data *data = new Data();
    data->id = 1729;
    data->payload = 10;

    std::thread producer(produce, data, std::ref(submissionQ), std::ref(qCV), std::ref(qmtx));
    // std::thread producer(produce, data);

    std::thread transporter(transport, std::ref(submissionQ), std::ref(qCV), std::ref(qmtx));

    producer.join();
    transporter.join();

    // gets some kind of completion and get back id = 1729
    delete data;
    return 0;
}
