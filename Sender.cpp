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
using namespace std;

/*
	void calculate()
	{
		// Parsing the 10-byte key into three unsigned int for comparision
		m_d1 = (m_data[0] << 24) + (m_data[1] << 16) + (m_data[2] << 8) + m_data[3];
		m_d2 = (m_data[4] << 24) + (m_data[5] << 16) + (m_data[6] << 8) + m_data[7];
		m_d3 = (m_data[8] << 8) + m_data[9];
	}

*/

// An array of SortServer
int * SortServers;

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

int send_file(char* filename)
{
	// create an ifstream from the file
	ifstream in(filename, ifstream::in | ios::binary);
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

	
/*	ofstream outfile ("new.txt", ofstream::binary);
	outfile.write (buffer, size);
	outfile.close();
*/
	write(SortServers[0], buffer, size);
	string exit_string = "EXIT";
	write(SortServers[0], exit_string.c_str(), 4);
/*
	// We know that each record is 100 bytes 
	int count = size / 100;
	char record[100];
	for (int i=0; i<count; i++)
	{
	}
*/
}

int main(int argc, char* argv[])
{    

	int portNo = atoi(argv[1]);
	SortServers = new int[1];
	SortServers[0] = open_connection("localhost", portNo);
	send_file(argv[2]);
/*	int count = 0;

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
*/
/*
	while (in)	

//	while (strlen(buffer) != 0)
	{
		bzero(buffer, 101);
		in.read(buffer, 100);
		if (strlen(buffer) !=0)
		{			
			write(client, buffer, 100);
			count++;
			cout << count << "\t" << strlen(buffer) << "\n";
		}	
//		bzero(buffer, 101);
//		in.read(buffer, 100);
	}
	cout << "Record Count: " << count << "\n";
*/
}

