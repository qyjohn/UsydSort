#include <string.h>
#include <cstring>
#include <unistd.h>
#include <stdio.h>
#include <netdb.h>
#include <sys/types.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <iostream>
#include <fstream>
#include <sstream>
#include <iomanip>
#include <strings.h>
#include <stdlib.h>
#include <string>
#include <time.h>
#include <vector>
#include <math.h>
using namespace std;

// An array of SortServer
int totalServers = 0;
int hashBar = 0;
std::vector<int> SortServers;

int open_connection(char* host, int port)
{
	int listenFd;
	struct sockaddr_in svrAdd;
	struct hostent *server;

	//create client skt
	listenFd = socket(AF_INET, SOCK_STREAM, 0);    
	if(listenFd < 0)
	{
		cerr << "Cannot open socket" << endl;
		return 0;
	}
    
	// check target host
	server = gethostbyname(host);    
	if(server == NULL)
	{
		cerr << "Host does not exist" << endl;
		return 0;
	}
    
	// some necessary preparation
	bzero((char *) &svrAdd, sizeof(svrAdd));
	svrAdd.sin_family = AF_INET;
	bcopy((char *) server -> h_addr, (char *) &svrAdd.sin_addr.s_addr, server -> h_length);
	svrAdd.sin_port = htons(port);
    
	// create the connection
	int checker = connect(listenFd,(struct sockaddr *) &svrAdd, sizeof(svrAdd));    
	if (checker < 0)
	{
		cerr << "Cannot connect!" << endl;
		return 0;
	}
	else
	{
		return listenFd;
	}
}

void initialize()
{
	// Get a list of SortServer from servers.cfg
	// This is a global configuration file for all Sender
	std::ifstream conf_file("servers.cfg");
	char line[1024];
	while (conf_file.getline(line, 1024))
	{
		char* server = strtok(line, ":");
		int port = atoi(strtok(NULL, ":"));
		
		// Create a socket connection to the SortServer
		int socket = open_connection(server, port);
		// Add the socket to the vector
		SortServers.push_back(socket);
		cout << "Server: " << server << "\tPort: " << port << "\n";
		totalServers++;
	}
}

int send_file(char* filename)
{
	// create an ifstream from the file
	ifstream in(filename, ifstream::in | ios::binary);

	if (in)	// the file was open successfully
	{
		// Get file size
		in.seekg(0, std::ios::end);
		int size = in.tellg();
		// Create a buffer as large as the file itself
		char * buffer = new char[in.tellg()];
		// Go back to the beginning of the file and read the whole thing
		in.seekg(0, std::ios::beg);
		in.read(buffer, size);
		// Close the file
		in.close();

		// We know that each record is 100 bytes 
		int count = size / 100;
		char record[100];
		for (int i=0; i<count; i++)
		{
			int start = 100*i;
			for (int j=0; j<100; j++)
			{
				record[j] = buffer[start+j];
			}
			int key = (unsigned char) record[0];
			int target = floor(key/hashBar);
			write(SortServers.at(target), record, 100);
		}

		// free the memory being used by the file buffer
		delete[] buffer;

	}
}

int send_exit_signal()
{
	// Send EXIT signal
	string exit_string = "EXIT";
	for (int i=0; i< totalServers; i++)
	{
		write(SortServers.at(i), exit_string.c_str(), 4);
	}
}

int main(int argc, char* argv[])
{    
	// Get a list of SortServer from servers.cfg and create socket connections
	initialize();
	hashBar = ceil(256 / totalServers);
	cout << "Total Servers: " << totalServers << "\n";
	cout << "Hash Bar: " << hashBar << "\n";	
	
	// Get a list of data files to send, this list is in a text file with the filename in argv[1]
	std::ifstream conf_file(argv[1]);
	char line[1024];
	// Send the data to SortServer, one file by one file
	while (conf_file.getline(line, 1024))
	{
		cout << "File: " << line << "\n";
		send_file(line);
	}

	// Send EXIT signal to SortServer
	send_exit_signal();
}

