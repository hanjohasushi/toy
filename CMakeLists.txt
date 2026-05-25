cmake_minimum_required(VERSION 3.15)
project(toy LANGUAGES CXX)

# add_executable(toy main.cpp)
add_executable(toy extras/producer_consumer.cpp)
add_executable(toy1 extras/epoll_wait.cpp)

find_package(Threads REQUIRED)
target_link_libraries(toy PRIVATE Threads::Threads)

target_link_libraries(toy1 PRIVATE Threads::Threads)