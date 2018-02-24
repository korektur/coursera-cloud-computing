cmake_minimum_required(VERSION 3.9)
project(mp2)

set(CMAKE_CXX_STANDARD 14)

file(GLOB SOURCE_FILES *.cpp *.h)

add_executable(mp2 ${SOURCE_FILES})
