all:
	g++ SortServer.cpp -lpthread -o SortServer
	g++ Sender.cpp -o Sender
