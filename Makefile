CC=g++
CFLAGS=-std=c++11

all:
	$(CC) $(CFLAGS) MultiServerQ.cpp -lpthread -o bin/MultiServerQ
	$(CC) $(CFLAGS) MultiServerV.cpp -lpthread -o bin/MultiServerV
	$(CC) $(CFLAGS) MultiServerMultex.cpp -lpthread -o bin/MultiServerMultex
	$(CC) $(CFLAGS) MultiServerV5.cpp -lpthread -o bin/MultiServerV5
	$(CC) $(CFLAGS) MultiServerV6.cpp -lpthread -o bin/MultiServerV6
	$(CC) $(CFLAGS) SortServer.cpp -lpthread -o bin/SortServer
	g++ -std=c++11 Sender.cpp -o bin/Sender
	g++ -std=c++11 SenderV5.cpp -o bin/SenderV5
	g++ -std=c++11 SenderV6.cpp -o bin/SenderV6
	g++ -std=c++11 GenBatch.cpp -o bin/GenBatch
	g++ -std=c++11 Validate.cpp -o bin/Validate
	g++ -std=c++11 Test.cpp -o bin/Test
