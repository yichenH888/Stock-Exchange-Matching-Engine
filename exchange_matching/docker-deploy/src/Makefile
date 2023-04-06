CXX = g++
CXXFLAGS = -Wall -Wextra -std=c++17 -pthread
LDFLAGS = -lpq -pthread

all: exchange_matching

main.o: main.cpp utils.h account.h symbol.h pugixml.hpp
	$(CXX) $(CXXFLAGS) -c main.cpp

# test_parse.o: test_parse.cpp utils.h pugixml.hpp
# 	$(CXX) $(CXXFLAGS) -c test_parse.cpp

utils.o: utils.cpp utils.h
	$(CXX) $(CXXFLAGS) -c utils.cpp

pugixml.o: pugixml.cpp pugixml.hpp pugiconfig.hpp
	$(CXX) $(CXXFLAGS) -c pugixml.cpp

exchange_matching: main.o utils.o pugixml.o
	$(CXX) $(CXXFLAGS) -o exchange_matching main.o utils.o pugixml.o $(LDFLAGS)

# test_parse: utils.o
# 	$(CXX) $(CXXFLAGS) -o test_parse test_parse.o utils.o $(LDFLAGS)

clean:
	rm -f main *.o
