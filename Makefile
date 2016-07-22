all:
	g++ -std=c++11 SortServer.cpp -lpthread -o SortServer
	g++ -std=c++11 Sender.cpp -o Sender
