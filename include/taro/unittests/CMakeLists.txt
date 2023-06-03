
enable_testing()
include(${TARO_3RD_PARTY_DIR}/doctest/doctest.cmake)

list(APPEND TARO_UNITTESTS
  independent
  linear_chain
  pipeline
)

foreach(unittest IN LISTS TARO_UNITTESTS)
  add_subdirectory(${TARO_UTEST_DIR}/${unittest})
endforeach()
