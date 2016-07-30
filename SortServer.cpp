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
#include <fcntl.h>
#include <malloc.h>

using namespace std;

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


int create_server(int port);
void *sort_thread(void *args);
bool test_exit(char* test);
void save_queue_to_file_buffer_io(std::priority_queue<SortRecord> *q, char* filename);
void save_queue_to_file_direct_io(std::priority_queue<SortRecord> *q, char* filename);
void merge_temp_files_and_save(char* dir, char* output_file);
void load_temp_file_to_queue(std::priority_queue<SortRecord> *q, char* filename);
time_t start_time, current_time;

struct thread_args
{
	// mode = 0 (default mode)
	//
	// In default mode, the thread save the incoming data in a single queue. This applies to 
	// compute resources with sufficient memory (or with sufficient swap). After the thread
	// exits, the main program simply merge the data from different queues and save the data
	// to disk.
	//
	// mode = 1 (batch mode)
	// In batch mode, the thread save the incoming data in the queue to disk in batches
	// Each batch consists of 1,000,000 records, which is 100,000,000 bytes. This applies to
	// compute resources with limited memory (or with limited swap). After the thread exits,
	// the main program needs to load all the data from disk and merge them again, then save
	// the merged data to disk.

	int mode;

	// Temporary directory for intermediate data, only used when mode=1
	char* temp_dir;
	int socket;
	std::priority_queue<SortRecord> *queue;
};


/**
 *
 * UsydSort - SortServer
 *
 * main() method
 * 1. create server socket and wait for client (Sender) connections
 * 2. for each client, create a pthread with a queue to handle incoming data
 * 3. wait for all threads to complete
 * 4. merge all the queues, save the merged and sorted data into a file
 *
 */

int main(int argc, char* argv[])
{
	time(&start_time);
	
	// Check command syntax
	if (argc < 4)
	{
		cerr << "Syntax : ./SortServer <port> <total_threads> <mode> <output_filename> [temp_dir]" << endl;
		return 0;
	}
    
	// Create the server socket
	int port = atoi(argv[1]);
	int listenFd = create_server(port);
	{
		if (listenFd <=0)	exit(1);
	}

	// Wait for all Sender connections
	// For each Sender, create a pthread with a dedicated queue to handle the incoming data
	int noThread = 0;
	int totalThread = atoi(argv[2]);
	int mode = atoi(argv[3]);
	// If the application works in batch mode, make sure that we clean up the temporary
	// directory before everything get started.
	if (mode == 1)
	{
		char command[1024];
		sprintf(command, "exec rm -r %s/*", argv[5]);
		system(command);
	}

	struct sockaddr_in clntAdd;
	socklen_t len = sizeof(clntAdd);	
	pthread_t threadA[totalThread];
	std::priority_queue<SortRecord> queues[totalThread];
	time(&current_time);
	cout << current_time << "\tWaiting for Sender to connect...\n";
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
		myArgs.mode = mode;	
		if (mode == 1)
		{
			myArgs.temp_dir = argv[5];
		}
		pthread_create(&threadA[noThread], NULL, sort_thread, (void*) &myArgs); 
		noThread++;
	}
    
	// Wait for all theads to exit
	for(int i = 0; i < totalThread; i++)
	{
		pthread_join(threadA[i], NULL);
	}

	// Default mode
	// Simply merge all the queues from different threads and then write the merged queue
	// to the final output file
	if (mode == 0)
	{
		// Merge all queues
		time(&current_time);
		cout << current_time << "\tMerging sorting results from different threads\n";
		for(int i = 1; i < totalThread; i++)
		{
			while(!queues[i].empty()) 
			{
				queues[0].push(queues[i].top());
				queues[i].pop();
			}    
		}

		// Save sorted results to file, the filename is specified in argv[4]
		time(&current_time);
		cout << current_time << "\tSaving sorted results to output file " << argv[4] << "\n";
//		save_queue_to_file_buffer_io(&queues[0], argv[4]);
		save_queue_to_file_direct_io(&queues[0], argv[4]);
	}
	// Batch mode
	// Data in a large number of small files in the temp directory, 1,000,000 records in each file.
	// Need to load the data from these intermediate files and merge them together. After that, 
	// save the merged content to the final output file
	else if (mode == 1)
	{
		merge_temp_files_and_save(argv[5], argv[4]);
	}
	
	// Finally done
	time(&current_time);
	cout << current_time << "\tDone!\n";
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
	int mode = myArgs->mode;
	char *temp_dir = myArgs->temp_dir;
	std::priority_queue<SortRecord> *q = myArgs->queue;	// the queue

	// Print out debug message
	time(&current_time);
	cout << current_time << "\tThread Start: " << pthread_self() << "\tMode: " << mode << endl;
	char buffer[100], temp[100];
    
	// Default mode
	if (mode == 0)
	{
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
	}
	else if (mode == 1)
	{
		int temp_file_id = 0;
		int temp_rec_count = 0;
		int i=0, j=0, size=0, marker=0;
		char filename_string[100];

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
					temp_rec_count++;

					// Save 1,000,000 records to disk
					if (temp_rec_count == 1000000)
					{
						// Get the filename and save the records
						time(&current_time);
						sprintf(filename_string, "%s/%lu-%d", temp_dir, pthread_self(), temp_file_id);
						cout << current_time << "\tThread "<< pthread_self() << " saving 1,000,000 records to " << filename_string << "\n";
						save_queue_to_file_buffer_io(q, filename_string);

						// Others
						temp_file_id++;
						temp_rec_count = 0;						
					}

					// Start another new record
					marker = 0;
				}
			}
	
			// Test if the client disconnects
			if ((marker == 4) && test_exit(temp))	break;
		}
		// In batch mode, need to write the final left over records in the queue to temp file
		// Get the filename
		time(&current_time);
		sprintf(filename_string, "%s/%lu-%d", temp_dir, pthread_self(), temp_file_id);
		cout << current_time << "\tThread "<< pthread_self() << " saving all other records to " << filename_string << "\n";
		save_queue_to_file_buffer_io(q, filename_string);
	}

	// Print out debug message
	time(&current_time);
	cout << current_time << "\tThread exit: " << pthread_self() << endl;
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


/**
 *
 * Save the content in a queue to an output file using BufferIO.
 *
 */

void save_queue_to_file_buffer_io(std::priority_queue<SortRecord> *q, char* filename)
{
		std::ofstream outfile (filename,std::ofstream::binary);
		while(!q->empty()) 
		{
			outfile.write (q->top().m_data, 100);
			q->pop();
		}
		outfile.close();
}


/**
 *
 * Save the content in a queue to an output file using DirectIO.
 *
 */

void save_queue_to_file_direct_io(std::priority_queue<SortRecord> *q, char* filename)
{
	// Use DirectIO to save data in 512 record blocks
	if (q->size() >= 512)
	{
		char filename_1[1024];
		sprintf(filename_1, "%s-1", filename);
		int fd = open(filename_1, O_RDWR | O_CREAT | O_DIRECT | O_TRUNC, 0644);
		// DirectIO buffer, 512 is block size, 51200 is buffer size for 512 records
		int i, j, base;
		char record[100];	// each record is 100 byte
		char buffer[51200];	// 512 records = 51200 bytes
		void *temp = memalign(512, 51200);
		while (q->size() >=512)
		{
			base = 0;
			for (i=0; i<512; i++)	// Get 512 records at a time
			{
				memcpy(record, q->top().m_data, 100);
//				cout << "Done strcpy\n";
				for (j=0; j<100; j++)
				{
					buffer[base] = record[j];
					base++;
				}
				q->pop();
			}
			memcpy(temp, buffer, 51200);
			write(fd, temp, 51200);
		}
		free(temp);
		close(fd);
	}

	// Use BufferIO to save the rese data
	if (!q->empty())
	{
		char filename_2[1024];
		sprintf(filename_2, "%s-2", filename);
		std::ofstream outfile (filename_2,std::ofstream::binary);
		while(!q->empty()) 
		{
			outfile.write (q->top().m_data, 100);
			q->pop();
		}
		outfile.close();
	}
}


/**
 *
 * Load intermediate data from a bunch of files in the temporary directory into a single queue.
 * Then save the queue to the final output file.
 *
 */

void merge_temp_files_and_save(char* dir, char* output_file)
{
	DIR *dpdf;
	struct dirent *epdf;
	char filename_string[100];
	std::priority_queue<SortRecord> q;

	dpdf = opendir(dir);
	if (dpdf != NULL)
	{
		// Load data from all the files in the temporary directory
		while (epdf = readdir(dpdf))
		{
			if ((strcmp(epdf->d_name, ".") && strcmp(epdf->d_name, "..")))
			{
				sprintf(filename_string, "%s/%s",dir, epdf->d_name);
				time(&current_time);
				cout << current_time << "\tMerging temp file" << filename_string << "...\n";
				load_temp_file_to_queue(&q, filename_string);
			}
		}
	}

	// Save merged results to the final output file
	time(&current_time);
	cout << current_time << "\tSaving sorted results to output file " << output_file << "\n";
	save_queue_to_file_buffer_io(&q, output_file);
}

/**
 *
 * Load data from a single intermediate file into the queue.
 *
 */

void load_temp_file_to_queue(std::priority_queue<SortRecord> *q, char* filename)
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
		int i=0, j=0, start = 0;
		char record[100];
		for (i=0; i<count; i++)
		{
			start = 100*i;
			// Create a SortRecord
			SortRecord sr;
			for (j=0; j<100; j++)	sr.m_data[j] = buffer[start+j];
			q->push(sr);
		}

		// free the memory being used by the file buffer
		delete[] buffer;
	}
}
