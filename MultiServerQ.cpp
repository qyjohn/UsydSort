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


void log_progress(char* msg);
int create_server(int port);
void *sort_thread(void *args);
void *server_thread(void *args);

bool test_exit(char* test);
void save_queue_to_file_buffer_io(std::priority_queue<SortRecord> *q, char* filename, int thread);
void save_queue_to_file_direct_io(std::priority_queue<SortRecord> *q, char* filename, int thread);
void merge_temp_files_and_save(char* dir, char* output_file, int thread, int io_mode);
void load_temp_file_to_queue(std::priority_queue<SortRecord> *q, char* filename);

time_t current_time;
char message[1024];

struct server_thread_args
{
	// Node name
	char node[100];
	// Server port
	int port;
	// Thread ID
	int thread_id;
	// Operation mode
	int mode;
	// Total number of clients sending data
	int total_clients;
	// Work folder
	char *work_folder;
	// Temporary directory
	char temp_dir[1024];
	// Final output file prefix
	char output_filename[1024];
	
	// CPU cores and total batches
	int cpu_cores, batches;
	// IO mode
	int io_mode;
};

struct sort_thread_args
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
	int server_thread_id, sender_id;
	// Temporary directory for intermediate data, only used when mode=1
	char* temp_dir;
	int socket;
	std::priority_queue<SortRecord> *queue;
};



/**
 * UsydSoft - MultiServer
 *
 * By default, this MultiServer starts N sorting server threads to accept incoming data. The
 * first server thread is the master thread, working in default memory-only mode. All other
 * server thread are slave threads, working in shaffle mode - intermediate data are written to 
 * disks for shaffling in a later statge.
 *
 * With the MultiServer, the master server thread uses port 5000, and the first and third slave 
 * threads then use port 5001 and 5002, etc. Intermediate data are written to [Folder]/1, [Folder]/2 
 * for each slave thread. 
 *
 * The main method accepts a node id, and the number of server threads to create. The final 
 * output files are [Folder]/output/[node]-0-1 for and [Folder]/output/[node]-0-2 for the first thread,
 * where the first file is created by DirectIO and the second file is created by BufferIO. The
 * output files are [Folder]/output/[node]-[thread]-1 and [Folder]/output/[node]-[thread]-2 for the 
 * slave threads, where the first file is created by DirectIO and the second file is created by 
 * BufferIO.
 *
 */


int main(int argc, char* argv[])
{
	int i=0;
	char *node_name = argv[1];
	int port_00 = atoi(argv[2]);
	int threads = atoi(argv[3]);
	int clients = atoi(argv[4]);
	char *work_folder = argv[5];
	int io_mode = atoi(argv[6]);	// 0 is DirectIO, 1 is BufferIO
	
	// Get the number of CPU cores
	unsigned cpu_cores = std::thread::hardware_concurrency();
	// When shuffling data, each time we run N threads, N equals to the number of CPU cores
	int batches = (int) (threads / cpu_cores);

	pthread_t all_server_threads[threads];
	struct server_thread_args args[threads];
	for (i=0; i<threads; i++)
	{
		strcpy(args[i].node, node_name);
		args[i].port = port_00 + i;
		args[i].thread_id = i;
		// Only the first server thread works in default mode, no need to save intermediate
		// data onto disk
		if (i < cpu_cores)
		{
			args[i].mode = 0;
		}
		// All other server threads need to save intermediate data onto disk to reduce
		// overall memory footprint
		else
		{
			args[i].mode = 1;
		}
		args[i].total_clients = clients;
		args[i].cpu_cores = cpu_cores;
		args[i].batches = batches;
		args[i].work_folder = work_folder;
		args[i].io_mode = io_mode;

		// The temporary folder for intermediate data
		sprintf(args[i].temp_dir, "%s/%04d", work_folder, i);
		// Make sure the temp folder exist
		char command[1024];
		sprintf(command, "exec mkdir -p %s", args[i].temp_dir);
		system(command);
		// The output filename for this server thread
		sprintf(args[i].output_filename, "%s/%s-%04d", work_folder, node_name, i);

		// Create a server thread
		pthread_create(&all_server_threads[i], NULL, server_thread, (void*) &args[i]); 
	}

	// Wait for all the server threads to exit. 
	for (i=0; i<threads; i++)
	{
		pthread_join(all_server_threads[i], NULL);
	}
}

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

void *server_thread(void *args)
{
	struct server_thread_args *myArgs = (struct server_thread_args*) args;
	// Node name
	char* this_node = myArgs->node; 
	// Server port
	int this_port = myArgs->port;
	// Thread ID
	int this_thread_id = myArgs->thread_id;
	// Operation mode
	int this_mode = myArgs->mode;
	// Total number of clients sending data
	int this_total_clients = myArgs->total_clients;
	// Work folder
	char* work_folder = myArgs->work_folder;
	// Temporary directory
	char* this_temp_dir = myArgs->temp_dir;
	// Final output file prefix
	char* this_output_filename = myArgs->output_filename;
    // CPU cores and total batches
    int cpu_cores = myArgs->cpu_cores;
    int batches = myArgs->batches;
    int io_mode = myArgs->io_mode;
    
	// Create the server socket
	sprintf(message, "Thread %04d: port %d, mode %d.", this_thread_id, this_port, this_mode);
	log_progress(message);
	int listenFd = create_server(this_port);
	{
		if (listenFd <=0)	exit(1);
	}

	// If the application works in batch mode, make sure that we clean up the temporary
	// directory before everything get started.
	if (this_mode == 1)
	{
		char command[1024];
		sprintf(command, "exec rm -r %s/*", this_temp_dir);
		system(command);
	}

	struct sockaddr_in clntAdd[this_total_clients];
	pthread_t threadA[this_total_clients];
	socklen_t len[this_total_clients];
	std::priority_queue<SortRecord> queues[this_total_clients];

	int clientSocket[this_total_clients];
	struct sort_thread_args	sortArgs[this_total_clients];
	sprintf(message, "Thread %04d: Waiting for %d sender nodes to connect.", this_thread_id, this_total_clients);
	log_progress(message);
	// Wait for all Sender connections
	// For each Sender, create a pthread with a dedicated queue to handle the incoming data
	int noThread = 0;
	while (noThread < this_total_clients)
	{
		//this is where client connects. svr will hang in this mode until client conn
		len[noThread] = sizeof(clntAdd[noThread]);
		clientSocket[noThread] = accept(listenFd, (struct sockaddr *)&clntAdd[noThread], &len[noThread]);
		if (clientSocket[noThread] < 0)
		{
			cerr << "Cannot accept connection" << endl;
			return 0;
		}
        
		// Create a new thread to handle the input
		sortArgs[noThread].server_thread_id = this_thread_id;
		sortArgs[noThread].sender_id = noThread;
		sortArgs[noThread].socket = clientSocket[noThread];
		sortArgs[noThread].queue = &queues[noThread];	
		sortArgs[noThread].mode = this_mode;
		if (this_mode == 1)
		{
			sortArgs[noThread].temp_dir = this_temp_dir;
		}
		pthread_create(&threadA[noThread], NULL, sort_thread, (void*) &sortArgs[noThread]); 
		noThread++;
	}
    
	// Wait for all theads to exit
	for(int i = 0; i < this_total_clients; i++)
	{
		pthread_join(threadA[i], NULL);
	}

	// Default mode
	// Simply merge all the queues from different threads and then write the merged queue
	// to the final output file
	if (this_mode == 0)
	{
		// Merge all queues
		sprintf(message, "Thread %04d: Merging sorting results from different sender nodes.", this_thread_id);
		log_progress(message);
		for(int i = 1; i < this_total_clients; i++)
		{
			while(!queues[i].empty()) 
			{
				queues[0].push(queues[i].top());
				queues[i].pop();
			}    
		}

		// Save sorted results to file
		sprintf(message, "Thread %04d: Saving sorted results to output file %s.", this_thread_id, this_output_filename);
		log_progress(message);
		if (io_mode == 0)
		{
			save_queue_to_file_direct_io(&queues[0], this_output_filename, this_thread_id);
		}
		else
		{
			save_queue_to_file_buffer_io(&queues[0], this_output_filename, this_thread_id);	
		}
		sprintf(message, "Thread %04d: Output file %s has been written to disk.", this_thread_id, this_output_filename);
		log_progress(message);
	}

	// After doing my own work, start to merge the intermediate data from the batch mode server threads.
	// Those server threads have exited after receiving data.
	for (int i=1; i<batches; i++)
	{
		char target_temp_dir[1024], target_output_filename[1024];
		int target_thread_id = i*cpu_cores + this_thread_id;
		sprintf(target_temp_dir, "%s/%04d", work_folder, target_thread_id);
		sprintf(target_output_filename, "%s/%s-%04d", work_folder, this_node, target_thread_id);

		sprintf(message, "Thread %04d: Merging intermediate results for thread %-4d.", this_thread_id, target_thread_id);
		log_progress(message);
		merge_temp_files_and_save(target_temp_dir, target_output_filename, this_thread_id, io_mode);

	}

	// Server thread exits
	sprintf(message, "Thread %04d: Exit gracefully.", this_thread_id);
	log_progress(message);
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
	struct sort_thread_args *myArgs = (struct sort_thread_args*) args;
	int server_thread_id = myArgs->server_thread_id;
	int sender_id = myArgs->sender_id;
	int sock = myArgs->socket;	// the socket
	int mode = myArgs->mode;
	char *temp_dir = myArgs->temp_dir;
	std::priority_queue<SortRecord> *q = myArgs->queue;	// the queue

	// Print out debug message
	sprintf(message, "Thread %04d: Sender %04d is now connected.", server_thread_id, sender_id);
	log_progress(message);
	
	int buffer_size = 100000;
	char buffer[buffer_size];
	int i=0, j=0, size=0, count=0, marker=0, buffer_start=0, buffer_left=0;
    
	// Default mode
	if (mode == 0)
	{
		while(true)
		{
			// Attempt to receive [buffer_size] bytes each time, unless the client disconnects
			size=recv(sock, buffer, buffer_size, MSG_WAITALL);

			count = (int) (size / 100);
			for (i=0; i<count; i++)
			{
				// Create a SortRecord
				SortRecord sr;
				memcpy(sr.m_data, buffer + 100*i, 100);
				q->push(sr);
			}
			
			// Check exit signal
			marker = size % 100;
			if (marker == 4)
			{
				if (test_exit(buffer+size-4)) break;
			}	
				
		}
	}
	// Batch mode
	else if (mode == 1)
	{
		int temp_file_id = 0;
		int temp_count = 0;
		char filename_string[1024];
		std::ofstream outfile;
		
		sprintf(filename_string, "%s/%lu-%05d", temp_dir, pthread_self(), temp_file_id);
		outfile.open(filename_string,std::ofstream::binary);
				
		while(true)
		{
			// Attempt to receive [buffer_size] bytes each time, unless the client disconnects
			size=recv(sock, buffer, buffer_size, MSG_WAITALL);
			if (size == buffer_size)
			{
				outfile.write (buffer, buffer_size);
				temp_count++;
				
				if (temp_count == 1000)	// 1000 writes already, if a buffer is 1000 record, then we already have 100,000,000 bytes = 100 MB already
				{
					// close the current intermediate file
					outfile.close();
					
					// create a new intermediate file 
					temp_file_id++;
					sprintf(filename_string, "%s/%lu-%05d", temp_dir, pthread_self(), temp_file_id);
					outfile.open(filename_string,std::ofstream::binary);
				}
			}
			else	// End of stream
			{
				outfile.write (buffer, size-4);
				outfile.close();
				break;
			}
		}
	}


	// Print out debug message
	sprintf(message, "Thread %04d: Sender %04d is now disconnected successfully.", server_thread_id, sender_id);
	log_progress(message);
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

void save_queue_to_file_buffer_io(std::priority_queue<SortRecord> *q, char* filename, int thread)
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

void save_queue_to_file_direct_io(std::priority_queue<SortRecord> *q, char* filename, int thread)
{
	int i, j, base;
	char buffer[51200];	// 512 records = 51200 bytes

	// Use DirectIO to save data in 512 record blocks
	if (q->size() >= 512)
	{
		char filename_1[1024];
		sprintf(filename_1, "%s-1", filename);
		sprintf(message, "Thread %04d: DirectIO writing to file %s.", thread, filename_1);
		log_progress(message);

		int fd = open(filename_1, O_RDWR | O_CREAT | O_DIRECT | O_TRUNC, 0644);
		// DirectIO buffer, 512 is block size, 51200 is buffer size for 512 records
		void *temp = memalign(512, 51200);
		while (q->size() >=512)
		{
			base = 0;
			for (i=0; i<512; i++)	// Get 512 records at a time
			{
				base = i * 100;
				memcpy(buffer+base, q->top().m_data, 100);
				q->pop();
			}
			memcpy(temp, buffer, 51200);
			write(fd, temp, 51200);
		}
		free(temp);
		close(fd);

		sprintf(message, "Thread %04d: DirectIO closing file %s.", thread, filename_1);
		log_progress(message);
	}

	// Use BufferIO to save the rese data
	if (!q->empty())
	{
		char filename_2[1024];
		sprintf(filename_2, "%s-2", filename);
		sprintf(message, "Thread %04d: BufferIO writing to file %s.", thread, filename_2);
		log_progress(message);
		std::ofstream outfile (filename_2,std::ofstream::binary);
		i = 0;
		while(!q->empty()) 
		{
			base = i * 100;
			memcpy(buffer+base, q->top().m_data, 100);
			q->pop();
			i++;
		}
		outfile.write (buffer, i*100);
		outfile.close();
		sprintf(message, "Thread %04d: BufferIO closing file %s.", thread, filename_2);
		log_progress(message);
	}
}


/**
 *
 * Load intermediate data from a bunch of files in the temporary directory into a single queue.
 * Then save the queue to the final output file.
 *
 */

void merge_temp_files_and_save(char* dir, char* output_file, int thread, int io_mode)
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
				sprintf(message, "Thread %04d: Merging intermediate records from %s.", thread, filename_string);
				log_progress(message);
				load_temp_file_to_queue(&q, filename_string);
			}
		}
	}

	// Save merged results to the final output file
	sprintf(message, "Thread %04d: Saving sorted results to output file %s.", thread, output_file);
	log_progress(message);
	if (io_mode == 0)
	{
		save_queue_to_file_direct_io(&q, output_file, thread);
	}
	else
	{
		save_queue_to_file_buffer_io(&q, output_file, thread);	
	}
	sprintf(message, "Thread %04d: Output file %s has been written to disk.", thread, output_file);
	log_progress(message);
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
			memcpy(sr.m_data, buffer+start, 100);
			q->push(sr);
		}

		// free the memory being used by the file buffer
		delete[] buffer;
	}
}

void log_progress(char* msg)
{
	time(&current_time);
	cout << current_time << "\t" << msg << "\n";
}
