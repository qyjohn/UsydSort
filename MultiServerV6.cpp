/*
 *  MultiServerV5 <total_nodes> <node_id> <server_port> <total_partitions> <in_memory_partitions> <io_mode> <work_folder>
 *
 */

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


/**
 *
 * SortRecord - one record to be sorted
 *
 */
 
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

/**
 *
 * SortPartition - a partition on the sort server
 *
 * A partition can be in-memory or on-disk. 
 *
 */
 
class SortPartition
{
	public:
		int partition_id;	// partition id
		int partition_type;	// 0 is in-memory partition, 1 is on-disk (intermediate) partition
		pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
		std::priority_queue<SortRecord> q;	
		char *work_folder;
		// The following parameters are used for intermediate partitions only
		int buffer_size=1000000;
		int record_counter = 0;
		int file_counter   = 0;
		char filename[1024];
		char *buffer;
		
		// Initialize the partition
		void initialize(int id, int type, char* folder)
		{
			partition_id = id;
			partition_type = type;
			work_folder = folder;
			if (partition_type == 1)
			{
				buffer = new char[100*buffer_size];
			}
		}

		// Queue operations
		void push(SortRecord sr)
		{
			pthread_mutex_lock(&mutex);
			q.push(sr);
			pthread_mutex_unlock(&mutex);
		}
		SortRecord top(){return q.top();}
		void pop()      {q.pop();}
		size_t size()   {return q.size();}
		bool empty()    {return q.empty();}
	
		// On-disk (Intermediate ) partition operations
		void add_intermediate_record(char* data)
		{
			pthread_mutex_lock(&mutex);
			memcpy(buffer+100*record_counter, data, 100);
			record_counter++;
			
			if (record_counter == buffer_size)
			{
				sprintf(filename, "%s/%04d/%04d", work_folder, partition_id, file_counter);
				std::ofstream outfile(filename,std::ofstream::binary);
				outfile.write (buffer, 100*buffer_size);
				outfile.close();
	
				file_counter++;
				record_counter = 0;
			}
			
			pthread_mutex_unlock(&mutex);
		}
		
		void final_save_intermediate_buffer()
		{
			if (record_counter > 0)
			{
				sprintf(filename, "%s/%04d/%04d", work_folder, partition_id, file_counter);
				std::ofstream outfile(filename,std::ofstream::binary);
				outfile.write (buffer, 100*record_counter);
				outfile.close();
				delete[] buffer;
			}
		}
		
		void load_disk_data()
		{
			DIR *dpdf;
			struct dirent *epdf;
			char filename_string[1024];
		
			dpdf = opendir(work_folder);
			if (dpdf != NULL)
			{
				// Load data from all the files in the temporary directory
				while (epdf = readdir(dpdf))
				{
					if ((strcmp(epdf->d_name, ".") && strcmp(epdf->d_name, "..")))
					{
						sprintf(filename_string, "%s/%s", work_folder, epdf->d_name);
						load_temp_file(filename_string);
					}
				}
			}
		}
		
		void load_temp_file(char* filename)
		{
			// create an ifstream from the file
			ifstream in(filename, ifstream::in | ios::binary);
		
			if (in)	// the file was open successfully
			{
				// Get file size
				in.seekg(0, std::ios::end);
				int size = in.tellg();
				// Create a buffer as large as the file itself
				char * local_buffer = new char[in.tellg()];
				// Go back to the beginning of the file and read the whole thing
				in.seekg(0, std::ios::beg);
				in.read(local_buffer, size);
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
					memcpy(sr.m_data, local_buffer+start, 100);
					q.push(sr);
				}
		
				// free the memory being used by the file buffer
				delete[] local_buffer;
			}
		}
		
		void save_queue_to_file_buffer_io(char* filename)
		{
			if (!q.empty())	// only create a file when there are records to write
			{
				std::ofstream outfile (filename,std::ofstream::binary);
				while(!q.empty()) 
				{
					outfile.write (q.top().m_data, 100);
					q.pop();
				}
				outfile.close();
			}
		}
		
		void save_queue_to_file_direct_io(char* filename)
		{
			int i, j, base;
			char* buffer = new char[51200];	// 512 records = 51200 bytes
		
			// Use DirectIO to save data in 512 record blocks
			if (q.size() >= 512)
			{
				char filename_1[1024];
				sprintf(filename_1, "%s-1", filename);
		
				int fd = open(filename_1, O_RDWR | O_CREAT | O_DIRECT | O_TRUNC, 0644);
				// DirectIO buffer, 512 is block size, 51200 is buffer size for 512 records
				void *temp = memalign(512, 51200);
				while (q.size() >=512)
				{
					base = 0;
					for (i=0; i<512; i++)	// Get 512 records at a time
					{
						base = i * 100;
						memcpy(buffer+base, q.top().m_data, 100);
						q.pop();
					}
					memcpy(temp, buffer, 51200);
					write(fd, temp, 51200);
				}
				free(temp);
				close(fd);
			}
		
			// Use BufferIO to save the rese data
			if (!q.empty())
			{
				char filename_2[1024];
				sprintf(filename_2, "%s-2", filename);
				std::ofstream outfile (filename_2,std::ofstream::binary);
				i = 0;
				while(!q.empty()) 
				{
					base = i * 100;
					memcpy(buffer+base, q.top().m_data, 100);
					q.pop();
					i++;
				}
				outfile.write (buffer, i*100);
				outfile.close();
			}
			
			delete[] buffer;
		}
		
		void flush_queue_to_disk(int io_mode)
		{
			if (io_mode == 0)
			{
				save_queue_to_file_buffer_io(filename);
			}
			else
			{
				save_queue_to_file_buffer_io(filename);
			}
		}
		
};

/**
 *
 *
 *
 *
 *
 */
 
class SortDataPlan
{
	public:
	int i, hash_bar, lower_range;
	int node_count, node_id, cpu_cores, total_partitions, in_memory_partitions, io_mode;
	char *work_folder;
	std::vector<SortPartition> partitions;
	std::vector<int> partitions_to_save;
	pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

	void initialize(int count, int id, int cores, int total, int memory, char* folder, int mode)
	{
		node_count = count;
		node_id = id;
		cpu_cores = cores;
		total_partitions = total;
		in_memory_partitions = memory;
		work_folder = folder;
		io_mode = mode;
		
		hash_bar = floor (65536 / (node_count * total_partitions));
		lower_range = floor(65535 * node_id / node_count);
		
		// Initialize all partitions
		SortPartition all_partitions[total_partitions];
		for (i=0; i<total_partitions; i++)
		{
			if (i<memory)
			{
				// in-memory partition
				all_partitions[i].initialize(i, 0, work_folder);
			}
			else
			{
				// intermediate (on-disk) partition
				all_partitions[i].initialize(i, 1, work_folder);
			}
			partitions.push_back(all_partitions[i]);
		} 
	}
	
};



void log_progress(char* msg);
int  create_server(int port);
void *server_thread(void *args);
void *sort_thread(void *args);
void *save_data_thread(void *args);
bool test_exit(char* test);
void save_queue_to_file_buffer_io(std::priority_queue<SortRecord> *q, char* filename);
void save_queue_to_file_direct_io(std::priority_queue<SortRecord> *q, char* filename);
void merge_temp_files_and_save(std::priority_queue<SortRecord> *q, char* dir, char* output_file, int io_mode);
void load_temp_file_to_queue(std::priority_queue<SortRecord> *q, char* filename);

time_t current_time;
char message[1024];

struct server_thread_args
{
	int port_no, node_id;
	SortDataPlan *data_plan;
};

struct sort_thread_args
{
	int socket, sender_id;
	SortDataPlan *data_plan;
};


struct save_data_thread_args
{
	int thread_id;	
	int io_mode;
	int cpu_cores;
	int partition_factor;
	int in_memory_factor;
	char *work_folder;
};



/**
 * UsydSoft - MultiServer V5
 *
 * In this version, we employ the following design philosophies:
 * (1) Each node has a single server daemon running on a single port. When a sending node connects, 
 *     the server daemon launches a new thread to receive data.
 * (2) Each node also runs a single sender. The sender establishes one TCP/IP connection with each 
 *     server node. So, this is a N-to-N network. For example, if we have 4 nodes all together, each
 *     sender will have 4 outgoing connections, while each server daemon will also have 4 incoming 
 *     connections.
 * (3) Each sender partitions the input data, and sends the data to different nodes.
 * (4) Each server daemon further partitions the data received into an array of SortRecordQueue. We
 *     will experiment with N x cpu_cores partitions to fully utilize all the vCPU cores. For example, 
 *     the i2.8xlarge instance has 32 vCPU cores, so we will partitions the data into 32, or 64, or 
 *     96, or 128 local partitions. Each partition is represented by a SortRecordQueue.
 * (5) When the total data size is bigger than total memory, we will need to buffer part of the data.
 *     In this case, we can try to buffer 1/2 or 2/3 or 3/4 of the data. This can be handled by the 
 *     number of partitions. For example, if the local partition id is greater then the number of vCPU
 *     cores, then the incoming data for this partition partitions needs to be on local disk.
 *     
 *
 */


int main(int argc, char* argv[])
{
	int i=0;
	int total_nodes      = atoi(argv[1]); // the number of nodes in the cluster
	int node_id          = atoi(argv[2]); // the id of this node
	int port_no          = atoi(argv[3]); // server port
	int total_partitions = atoi(argv[4]); // toal partitions 
	int in_memory_partitions = atoi(argv[5]); // in-memory partitions
	int io_mode          = atoi(argv[6]); // 0 is DirectIO, 1 is BufferIO
	char *work_folder    = argv[7];
	
	// Get the number of CPU cores
	unsigned cpu_cores = std::thread::hardware_concurrency();
	// No matter how many partitions we have, we only create N = cpu_cores x in_memory_factor SortRecordQueue for 
	// in-memory storage. Data for other partitions are considered as intermediate data and we store
	// them on to disk immediately for further read-sort-write.
	int hash_bar = floor (65536 / (total_nodes * total_partitions));
	int lower_range = floor(65535 * node_id / total_nodes);
	SortDataPlan	data_plan;
	data_plan.initialize(total_nodes, node_id, cpu_cores, total_partitions, in_memory_partitions, work_folder, io_mode);
	

	// Clean up the work folder by doing a "rm -Rf" and then make sure that it is created again
	char command[1024];
	sprintf(command, "exec rm -Rf %s", work_folder);
	system(command);
	sprintf(command, "exec mkdir -p %s", work_folder);
	system(command);

	// Creating the necessary folder, file handle and counter for intermediate partitions
	for (i=in_memory_partitions; i<total_partitions; i++)
	{
		sprintf(command, "exec mkdir -p %s/%04d", work_folder, i);
		system(command);
	}

	// Create the server thread and wait for the server thread to exit
	pthread_t sort_server_thread;
	struct server_thread_args args;
	args.node_id     = node_id;
	args.data_plan   = &data_plan;
	pthread_create(&sort_server_thread, NULL, server_thread, (void*) &args); 
	pthread_join(sort_server_thread, NULL);
	
	// Now, launch N threads to save the data to disk, N = cpu_cores
	pthread_t save_data_threads[in_memory];
	struct save_data_thread_args save_args[in_memory];
	for (i=0; i<in_memory; i++)
	{
		save_args[i].thread_id = i;
		save_args[i].io_mode   = io_mode;
		save_args[i].cpu_cores = cpu_cores;
		save_args[i].partition_factor = partition_factor;
		save_args[i].in_memory_factor = in_memory_factor;
		save_args[i].queue = &sr_queues[i];
		save_args[i].work_folder = work_folder;
		pthread_create(&save_data_threads[i], NULL, save_data_thread, (void*) &save_args[i]); 
	}
	for (i=0; i<in_memory; i++)
	{
		pthread_join(save_data_threads[i], NULL);
	}

	// Create the server socket
	sprintf(message, "Done!");
	log_progress(message);
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
	int i, j;
	SortDataPlan *data_plan = (SortDataPlan*)  
	struct server_thread_args *myArgs = (struct server_thread_args*) args;
	int port_no       = myArgs->port_no;
	SortDataPlan *data_plan = myArgs->data_plan;
	int total_nodes   = data_plan->node_count;
    
	// Create the server socket
	sprintf(message, "Launching sort server daemon on port %d.", port_no);
	log_progress(message);
	int listenFd = create_server(port_no);
	{
		if (listenFd <=0)	exit(1);
	}

	struct sockaddr_in clntAdd[total_nodes];
	pthread_t          senderThreads[total_nodes];
	socklen_t          len[total_nodes];
	int                clientSocket[total_nodes];
	struct sort_thread_args	sortArgs[total_nodes];

	sprintf(message, "Waiting for %d sender nodes to connect.", total_nodes);
	log_progress(message);
	// Wait for all Sender connections
	// For each Sender, create a pthread with a dedicated queue to handle the incoming data
	for (i=0; i<total_nodes; i++)
	{
		//this is where client connects. svr will hang in this mode until client conn
		len[i] = sizeof(clntAdd[i]);
		clientSocket[i] = accept(listenFd, (struct sockaddr *)&clntAdd[i], &len[i]);
		if (clientSocket[i] < 0)
		{
			cerr << "Cannot accept connection" << endl;
			return 0;
		}
        
		// Create a new thread to handle the input
		sortArgs[i].data_plan   = data_plan;
		sortArgs[i].socket      = clientSocket[i];
		sortArgs[i].sender_id   = i;
		pthread_create(&senderThreads[i], NULL, sort_thread, (void*) &sortArgs[i]); 
	}
    
	// Wait for all sender theads to exit
	for(i=0; i<total_nodes; i++)
	{
		pthread_join(senderThreads[i], NULL);
	}
	
	// Flush all the intermediate data to disk
	for (i=data_plan->in_memory_partitions; i<data_plan_total_partitions; i++)
	{
		data_plan->partitions[i].final_save_intermediate_buffer();
	} 
	
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
	SortDataPlan *data_plan = myArgs->data_plan;
	int sock      = myArgs->socket;	// the socket
	int sender_id = myArgs->sender_id;
	int hash_bar    = data_plan->hash_bar;
	int lower_range = data_plan->lower_range;
	int partitions  = data_plan->total_partitions;
	int in_memory   = data->in_memory_partitions;

	// Print out debug message
	sprintf(message, "Sender %04d is now connected.", sender_id);
	log_progress(message);
	
	// read 1000 records at a time, requiring 1000 x 100 = 100000 bytes for the buffer
	int buffer_count = 1000;
	int buffer_size = 100*buffer_count;
	char buffer[buffer_size], temp[100];
	int i=0, j=0, size=0, count=0, marker=0, buffer_start=0, buffer_left=0;
    
	// Read while the client is still connected. The client side will disconnect from the server
	// when the data transfer is completed.
	while(true)
	{
		// Attempt to receive [buffer_size] bytes each time, unless the client disconnects
		size=recv(sock, buffer, buffer_size, MSG_WAITALL);

		// Check the number of records
		if (size == buffer_size)
		{
			count = buffer_count;
		}
		else
		{
			count = floor(size / 100);
		}

		// Create individual records
		for (i=0; i<count; i++)
		{
			int key = (unsigned char) buffer[100*i] * 256;
			int partition = floor ((key - lower_range) / hash_bar);
			if (partition >= partitions)
			{
				partition = partitions - 1;
			}

			if (partition < in_memory)	// In memory
			{
				// Create a SortRecord
				SortRecord sr;
				memcpy(sr.m_data, buffer + 100*i, 100);
				data_plan->partitions.at(partition).push(sr);
			}
			else	// on disk
			{
				// Push the data into inermediate queue
				memcpy(temp, buffer + 100*i, 100);
				data_plan->partitions.at(partition).add_intermediate_record(temp);
			}
		}

		// Check exit signal
		marker = size % 100;
		if (marker == 4)
		{
			if (test_exit(buffer+size-4)) break;
		}	
	}

	// Print out debug message
	sprintf(message, "Sender %04d is now disconnected successfully.", sender_id);
	log_progress(message);
	close(sock);
}

void *save_data_thread (void *args)
{
	struct save_data_thread_args *myArgs = (struct save_data_thread_args*) args;
	int thread_id = myArgs->thread_id;
	int io_mode = myArgs->io_mode;
	int cpu_cores = myArgs->cpu_cores;
	int partition_factor = myArgs->partition_factor;
	int in_memory_factor = myArgs->in_memory_factor;
	SortRecordQueue *queue = myArgs->queue;
	char* work_folder = myArgs->work_folder;
	char folder[1024];
	char filename[1024];
	
	sprintf(filename, "%s/%05d.out", work_folder, thread_id);
	if (io_mode == 0)
	{
		save_queue_to_file_direct_io(&queue->q, filename);
	}
	else if (io_mode == 1)
	{
		save_queue_to_file_buffer_io(&queue->q, filename);
	}

	// Also, need to work in the intermediate data set 
	for (int i=1; i<partition_factor/in_memory_factor; i++)
	{
		int partition_id = i*cpu_cores*in_memory_factor + thread_id;
		sprintf(folder, "%s/%04d", work_folder, partition_id);
		sprintf(filename, "%s/%05d.out", work_folder, partition_id);
		merge_temp_files_and_save(&queue->q, folder, filename, io_mode);
	}

}

void *save_intermediate_data_thread(void *args)
{
	// Get filename and buffer
	struct save_intermediate_thread_args *myArgs = (struct save_intermediate_thread_args*) args;
	char* filename = myArgs->filename;
	char* buffer   = myArgs->buffer;
	int size     = myArgs->size;

	// Write buffer to file
	std::ofstream outfile(filename,std::ofstream::binary);
	outfile.write (buffer, size);
	outfile.close();

	cout << filename << "Delete buffer\n";
	// Free the memory
	delete[] buffer;
	cout << filename << "Done\n";
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
	if (!q->empty())	// only create a file when there are records to write
	{
		sprintf(message, "BufferIO writing to file %s.", filename);
		log_progress(message);

		std::ofstream outfile (filename,std::ofstream::binary);
		while(!q->empty()) 
		{
			outfile.write (q->top().m_data, 100);
			q->pop();
		}
		outfile.close();

		sprintf(message, "BufferIO closing file %s.", filename);
		log_progress(message);
	}
}


/**
 *
 * Save the content in a queue to an output file using DirectIO.
 *
 */

void save_queue_to_file_direct_io(std::priority_queue<SortRecord> *q, char* filename)
{
	int i, j, base;
	char* buffer = new char[51200];	// 512 records = 51200 bytes

	// Use DirectIO to save data in 512 record blocks
	if (q->size() >= 512)
	{
		char filename_1[1024];
		sprintf(filename_1, "%s-1", filename);
		sprintf(message, "DirectIO writing to file %s.", filename_1);
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

		sprintf(message, "DirectIO closing file %s.", filename_1);
		log_progress(message);
	}

	// Use BufferIO to save the rese data
	if (!q->empty())
	{
		char filename_2[1024];
		sprintf(filename_2, "%s-2", filename);
		sprintf(message, "BufferIO writing to file %s.", filename_2);
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
		sprintf(message, "BufferIO closing file %s.", filename_2);
		log_progress(message);
	}
	
	delete[] buffer;
}


/**
 *
 * Load intermediate data from a bunch of files in the temporary directory into a single queue.
 * Then save the queue to the final output file.
 *
 */

void merge_temp_files_and_save(std::priority_queue<SortRecord> *q, char* dir, char* output_file, int io_mode)
{
	DIR *dpdf;
	struct dirent *epdf;
	char filename_string[1024];
//	std::priority_queue<SortRecord> q;

	dpdf = opendir(dir);
	if (dpdf != NULL)
	{
		// Load data from all the files in the temporary directory
		while (epdf = readdir(dpdf))
		{
			if ((strcmp(epdf->d_name, ".") && strcmp(epdf->d_name, "..")))
			{
				sprintf(filename_string, "%s/%s",dir, epdf->d_name);
//				sprintf(message, "Merging intermediate records from %s.", filename_string);
//				log_progress(message);
				load_temp_file_to_queue(q, filename_string);
			}
		}
	}

	// Save merged results to the final output file
	if (io_mode == 0)
	{
		save_queue_to_file_direct_io(q, output_file);
	}
	else
	{
		save_queue_to_file_buffer_io(q, output_file);	
	}
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
