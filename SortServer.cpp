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

void *task1(void *);
bool test_exit(char* test);
string exit_string = "EXIT";


template<typename T> void print_queue(T& q) 
{
	while(!q.empty()) 
	{
		std::cout << q.top();
		q.pop();
	}
	std::cout << '\n';
}

class SortRecord
{
	public:
	char m_data[100];
	unsigned int  m_d1, m_d2, m_d3;
	bool operator<(const SortRecord& other) const
	{

		if (memcmp(m_data, other.m_data, 10) < 0) 
			return false;
		else
			return true;
	}
};

std::ostream& operator<<(std::ostream &strm, const SortRecord &rec) 
{
	return strm << "SortRecord (" << rec.m_d1 << rec.m_d2 <<rec.m_d3 << ")\n";
}


static int connFd;

int main(int argc, char* argv[])
{
    int pId, portNo, listenFd, totalThread;
    socklen_t len; //store size of the address
    bool loop = false;
    struct sockaddr_in svrAdd, clntAdd;
    std::priority_queue<int> q;
    
    if (argc < 3)
    {
        cerr << "Syntam : ./server <port> <total_threads>" << endl;
        return 0;
    }
    
    portNo = atoi(argv[1]);
    totalThread = atoi(argv[2]);
    pthread_t threadA[totalThread];
    
    if((portNo > 65535) || (portNo < 2000))
    {
        cerr << "Please enter a port number between 2000 - 65535" << endl;
        return 0;
    }
    
    //create socket
    listenFd = socket(AF_INET, SOCK_STREAM, 0);
    
    if(listenFd < 0)
    {
        cerr << "Cannot open socket" << endl;
        return 0;
    }
    
    bzero((char*) &svrAdd, sizeof(svrAdd));
    
    svrAdd.sin_family = AF_INET;
    svrAdd.sin_addr.s_addr = INADDR_ANY;
    svrAdd.sin_port = htons(portNo);
    
    //bind socket
    if(bind(listenFd, (struct sockaddr *)&svrAdd, sizeof(svrAdd)) < 0)
    {
        cerr << "Cannot bind" << endl;
        return 0;
    }
    
    listen(listenFd, 5);
    
    len = sizeof(clntAdd);
    
    int noThread = 0;
	int clientSocket;
    while (noThread < totalThread)
    {
        cout << "Listening" << endl;

        //this is where client connects. svr will hang in this mode until client conn
        clientSocket = accept(listenFd, (struct sockaddr *)&clntAdd, &len);

        if (clientSocket < 0)
        {
            cerr << "Cannot accept connection" << endl;
            return 0;
        }
        else
        {
            cout << "Connection successful" << endl;
        }
        
//        pthread_create(&threadA[noThread], NULL, task1, NULL); 
	pthread_create(&threadA[noThread], NULL, task1, (void*) &clientSocket); 
        noThread++;
    }
    
    for(int i = 0; i < totalThread; i++)
    {
        pthread_join(threadA[i], NULL);
    }
    
    
}



//void *task1 (void *dummyPt)
void *task1 (void *clientSocket)
{
	int sock = *(int*) clientSocket;

    cout << "Thread No: " << pthread_self() << endl;
    char buffer[100], temp[100];
    bzero(buffer, 101);
    bzero(temp, 101);
    
    int i, j, size;
    int marker = 0;
	std::priority_queue<SortRecord> q;
    
	
    while(true)
    {
        bzero(buffer, 101);
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
			q.push(sr);

			// Start another new record
			marker = 0;
		}

	}
	
	// Test if the client disconnects
	if ((marker == 4) && test_exit(temp))	break;
    }

    cout << "\nClosing thread and conn" << endl;
    close(sock);

	// Save to file
	std::ofstream outfile ("new.txt",std::ofstream::binary);
	while(!q.empty()) 
	{
		outfile.write (q.top().m_data, 100);
		q.pop();
	}
	outfile.close();

}


bool test_exit(char* test)
{
	if ((test[0] == 'E') && (test[1] == 'X') &&(test[2] == 'I') && (test[3] == 'T'))
		return true;
	else
		return false;
}
