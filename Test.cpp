#include <string.h>
#include <unistd.h>
#include <dirent.h>
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
#include <thread>
#include <fcntl.h>
#include <malloc.h>
#include <math.h>

using namespace std;

int main(int argc, char* argv[])
{
	int threads = 128;
	unsigned concurentThreadsSupported = std::thread::hardware_concurrency();
	int batch = (int) (threads / concurentThreadsSupported);
	
	cout << "Batches: " << batch << "\n";
}