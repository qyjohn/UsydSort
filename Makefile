all:
	g++ -std=c++11 MultiServerQ.cpp -lpthread -o bin/MultiServerQ
	g++ -std=c++11 MultiServerV.cpp -lpthread -o bin/MultiServerV
	g++ -std=c++11 SortServer.cpp -lpthread -o bin/SortServer
	g++ -std=c++11 Sender.cpp -o bin/Sender
	g++ -std=c++11 GenBatch.cpp -o bin/GenBatch
	g++ -std=c++11 Validate.cpp -o bin/Validate
