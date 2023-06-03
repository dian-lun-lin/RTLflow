#include <cstdlib>
#include <iostream>
#include <memory>
#include <string>
#include <thread>

#include <boost/intrusive_ptr.hpp>

#include <boost/fiber/all.hpp>

inline
void fn( std::string const& str, int n) {
	for ( int i = 0; i < n; ++i) {
    std::cout << "thread id: "<< std::this_thread::get_id() << std::endl;
		std::cout << i << ": " << str << std::endl;
		boost::this_fiber::yield();
	}
}

int main() {
    try {
        boost::fibers::fiber f1( fn, "abc", 5);
        std::cerr << "f1 : " << f1.get_id() << std::endl;
        f1.join();
        std::cout << "done." << std::endl;

        return EXIT_SUCCESS;
    } catch ( std::exception const& e) {
        std::cerr << "exception: " << e.what() << std::endl;
    } catch (...) {
        std::cerr << "unhandled exception" << std::endl;
    }
	return EXIT_FAILURE;
}
