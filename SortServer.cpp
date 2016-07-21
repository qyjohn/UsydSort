#include <string.h>
#include <unistd.h>
#include <stdio.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <iostream>
#include <fstream>
#include <strings.h>
#include <stdlib.h>
#include <string>
#include <pthread.h>
#include <queue>

using namespace std;

int create_server(int port);
void *sort_thread(void *args);
bool test_exit(char* test);

class SortRecord
{
	public:
	char m_data[100];
	bool operator<(const SortRecord& other) const
	{

		if (memcmp(m_data, other.m_data, 10) < 0) 
			return false;
		else
			return true;
	}
};

struct thread_args
{
	int socket;
	std::priority_queue<SortRecord> *queue;
};


int main(int argc, char* argv[])
{
	// Check command syntax
	if (argc < 3)
	{
		cerr << "Syntax : ./SortServer <port> <total_threads>" << endl;
		return 0;
	}
    
	// Create the server socket
	int portNo = atoi(argv[1]);
	int listenFd = create_server(portNo);
	{
		if (listenFd <=0)	exit(1);
	}

	// Wait for all Sender connections
	// For each Sender, create a pthread with a dedicated queue to handle the incoming data
	int noThread = 0;
	int totalThread = atoi(argv[2]);
	struct sockaddr_in clntAdd;
	socklen_t len = sizeof(clntAdd);	
	pthread_t threadA[totalThread];
	std::priority_queue<SortRecord> queues[totalThread];
	while (noThread < totalThread)
	{
		//this is where client connects. svr will hang in this mode until client conn
		int clientSocket = accept(listenFd, (struct sockaddr *)&clntAdd, &len);
		if (clientSocket < 0)
		{
			cerr << "Cannot accept connection" << endl;
			return 0;
		}
        
		// Create a new thread to handle the input
		struct thread_args	myArgs;
		myArgs.socket = clientSocket;
		myArgs.queue = &queues[noThread];		
		pthread_create(&threadA[noThread], NULL, sort_thread, (void*) &myArgs); 
		noThread++;
	}
    
	// Wait for all theads to exit
	for(int i = 0; i < totalThread; i++)
	{
		pthread_join(threadA[i], NULL);
	}
    
	// Merge all queues
	for(int i = 1; i < totalThread; i++)
	{
		while(!queues[i].empty()) 
		{
			queues[0].push(queues[i].top());
			queues[i].pop();
		}    
	}
    
	// Save sorted results to file
	std::ofstream outfile ("new2.txt",std::ofstream::binary);
	while(!queues[0].empty()) 
	{
		outfile.write (queues[0].top().m_data, 100);
		queues[0].pop();
	}
	outfile.close();
}

/**
 *
 * Create the server socket to accept incoming Sender connections
 *
 */

int create_server(int port)
{
	// check port range
	if((port > 65535) || (port < 2000))
	{
		cerr << "Please enter a port number between 2000 - 65535" << endl;
		return 0;
	}
	    
	//create socket
	int listenFd = socket(AF_INET, SOCK_STREAM, 0);	    
	if(listenFd < 0)
	{
		cerr << "Cannot open socket" << endl;
		return 0;
	}
	    
	// initialization
	struct sockaddr_in svrAdd;
	bzero((char*) &svrAdd, sizeof(svrAdd));	    
	svrAdd.sin_family = AF_INET;
	svrAdd.sin_addr.s_addr = INADDR_ANY;
	svrAdd.sin_port = htons(port);
	    
	//bind socket
	if(bind(listenFd, (struct sockaddr *)&svrAdd, sizeof(svrAdd)) < 0)
	{
		cerr << "Cannot bind" << endl;
		return 0;
	}
	    
	// listen on socket
	listen(listenFd, 5);
	return listenFd;
}

/**
 *
 * A sorting thread, handling the data from one particular Sender.
 * The incoming data is pushed into a std::priority_queue.
 *
 */

void *sort_thread (void *args)
{
	struct thread_args *myArgs = (struct thread_args*) args;
	int sock = myArgs->socket;	// the socket
	std::priority_queue<SortRecord> *q = myArgs->queue;	// the queue

	// Print out debug message
	cout << "Thread No: " << pthread_self() << endl;
	char buffer[100], temp[100];
    
	int i=0, j=0, size=0, marker=0;
	while(true)
	{
		// Attempt to receive 100 bytes each time
		size=recv(sock, buffer, 100, 0);
		for (i=0; i<size; i++)
		{
			temp[marker] = buffer[i];
			marker++;
			if (marker == 100)	// 100 bytes per record
			{
				// Create a SortRecord
				SortRecord sr;
				for (j=0; j<100; j++)	sr.m_data[j] = temp[j];
				q->push(sr);

				// Start another new record
				marker = 0;
			}
		}
	
		// Test if the client disconnects
		if ((marker == 4) && test_exit(temp))	break;
	}

	// Print out debug message
	cout << "\nExciting thread " << pthread_self() << endl;
	close(sock);
}


/**
 *
 * Checking the EXIT signal from a Sender.
 *
 */

bool test_exit(char* test)
{
	if ((test[0] == 'E') && (test[1] == 'X') &&(test[2] == 'I') && (test[3] == 'T'))
		return true;
	else
		return false;
}
