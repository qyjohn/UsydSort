all:
	g++ -std=c++11 MultiServer.cpp -lpthread -o bin/MultiServer
	g++ -std=c++11 SortServer.cpp -lpthread -o bin/SortServer
	g++ -std=c++11 Sender.cpp -o bin/Sender
