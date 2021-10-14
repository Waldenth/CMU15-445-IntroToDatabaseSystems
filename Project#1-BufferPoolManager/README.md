所有`*_test.cpp`都要把`DISABLED_SampleTest`改成`SampleTest`，删除前缀`DISABLED_`

切到2020-Fall commit下，2021有的类设计改了(DiskManager)

# 坑死我了！！！！

- [x] TASK #1 - LRU REPLACEMENT POLICY
- [x] TASK #2 - BUFFER POOL MANAGER
- [x] TASK  ADDITIONAL  CLOCK POLICY

```bash
aaron@ubuntu:~/CMU15-445/Lab1BUSTUB/bustub$ sudo su
root@ubuntu:/home/aaron/CMU15-445/Lab1BUSTUB/bustub# cd build
root@ubuntu:/home/aaron/CMU15-445/Lab1BUSTUB/bustub/build# make lru_replacer_test
[  3%] Built target thirdparty_murmur3
[ 86%] Built target bustub_shared
[ 90%] Built target gtest
[ 93%] Built target gmock
[ 96%] Built target gmock_main
[ 98%] Linking CXX executable lru_replacer_test
[100%] Built target lru_replacer_test
root@ubuntu:/home/aaron/CMU15-445/Lab1BUSTUB/bustub/build# ./test/lru_replacer_test 
Running main() from gmock_main.cc
[==========] Running 1 test from 1 test suite.
[----------] Global test environment set-up.
[----------] 1 test from LRUReplacerTest
[ RUN      ] LRUReplacerTest.SampleTest
[       OK ] LRUReplacerTest.SampleTest (0 ms)
[----------] 1 test from LRUReplacerTest (0 ms total)

[----------] Global test environment tear-down
[==========] 1 test from 1 test suite ran. (0 ms total)
[  PASSED  ] 1 test.
root@ubuntu:/home/aaron/CMU15-445/Lab1BUSTUB/bustub/build# make clock_replacer_test
[  3%] Built target thirdparty_murmur3
[ 86%] Built target bustub_shared
[ 90%] Built target gtest
[ 93%] Built target gmock
[ 96%] Built target gmock_main
[100%] Built target clock_replacer_test
root@ubuntu:/home/aaron/CMU15-445/Lab1BUSTUB/bustub/build# ./test/clock_replacer_test
Running main() from gmock_main.cc
[==========] Running 1 test from 1 test suite.
[----------] Global test environment set-up.
[----------] 1 test from ClockReplacerTest
[ RUN      ] ClockReplacerTest.SampleTest
[       OK ] ClockReplacerTest.SampleTest (0 ms)
[----------] 1 test from ClockReplacerTest (0 ms total)

[----------] Global test environment tear-down
[==========] 1 test from 1 test suite ran. (0 ms total)
[  PASSED  ] 1 test.
root@ubuntu:/home/aaron/CMU15-445/Lab1BUSTUB/bustub/build# make buffer_pool_manager_test
[  3%] Built target thirdparty_murmur3
[ 86%] Built target bustub_shared
[ 90%] Built target gtest
[ 93%] Built target gmock
[ 96%] Built target gmock_main
[ 98%] Linking CXX executable buffer_pool_manager_test
[100%] Built target buffer_pool_manager_test
root@ubuntu:/home/aaron/CMU15-445/Lab1BUSTUB/bustub/build# ./test/buffer_pool_manager_test 
Running main() from gmock_main.cc
[==========] Running 2 tests from 1 test suite.
[----------] Global test environment set-up.
[----------] 2 tests from BufferPoolManagerTest
[ RUN      ] BufferPoolManagerTest.BinaryDataTest
[       OK ] BufferPoolManagerTest.BinaryDataTest (1 ms)
[ RUN      ] BufferPoolManagerTest.SampleTest
[       OK ] BufferPoolManagerTest.SampleTest (0 ms)
[----------] 2 tests from BufferPoolManagerTest (2 ms total)

[----------] Global test environment tear-down
[==========] 2 tests from 1 test suite ran. (2 ms total)
[  PASSED  ] 2 tests.
```

