list(APPEND INDEPENDENT_UNITTESTS
  callback_taskflow
  callback_v1
  callback_v2
  callback_v3
  callback_v4
  poll_v1
)

foreach(unittest IN LISTS INDEPENDENT_UNITTESTS)
  cuda_add_executable(independent_${unittest} ${TARO_UTEST_DIR}/independent/${unittest}.cu)
  target_include_directories(independent_${unittest} PRIVATE ${TARO_3RD_PARTY_DIR}/doctest)
  target_link_libraries(independent_${unittest} Threads::Threads cuda)
  doctest_discover_tests(independent_${unittest})
endforeach()

# fiber
cuda_add_executable(independent_fiber ${TARO_UTEST_DIR}/independent/fiber.cu)
target_include_directories(independent_fiber PRIVATE ${TARO_3RD_PARTY_DIR}/doctest)
target_link_libraries(independent_fiber boost_fiber boost_context cuda)
doctest_discover_tests(independent_fiber)
