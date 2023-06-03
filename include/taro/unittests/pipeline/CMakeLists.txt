list(APPEND PIPELINE_UNITTESTS
  callback_v4
)

foreach(unittest IN LISTS PIPELINE_UNITTESTS)
  cuda_add_executable(pipeline_${unittest} ${TARO_UTEST_DIR}/pipeline/${unittest}.cu)
  target_include_directories(pipeline_${unittest} PRIVATE ${TARO_3RD_PARTY_DIR}/doctest)
  target_link_libraries(pipeline_${unittest} Threads::Threads cuda)
  doctest_discover_tests(pipeline_${unittest})
endforeach()

