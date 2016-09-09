/*
 *  MultiServerV5 <total_nodes> <node_id> <server_port> <total_partitions> <in_memory_partitions> <io_mode> <work_folder> <lazy_load>
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
		int node_id;
		int partition_id;	// partition id
		int partition_type;	// 0 is in-memory partition, 1 is on-disk (intermediate) partition
		pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;
		std::priority_queue<SortRecord> q;	
		char* work_folder;
		// The following parameters are used for intermediate partitions only
		int buffer_size=1000000;
		int record_counter = 0;
		int file_counter   = 0;
		char filename[1024], message[1024];
		char *buffer;
		
		// Initialize the partition
		void initialize(int node, int partition, int type, char* folder)
		{
			node_id = node;
			partition_id = partition;
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
		
		void load_disk_data(bool lazy_load)
		{
			// 10 GB memory lower bound
			// Lazy load only occurs when the current available memory is greater than the lower bound
			unsigned long mem_lower_bound = 10000000; 

			sprintf(message, "Loading intermediate partition %04d", partition_id);
			log_progress(message);

			DIR *dpdf;
			struct dirent *epdf;
			char filename_string[1024];
			char load_folder[1024];

			sprintf(load_folder, "%s/%04d", work_folder, partition_id);
			dpdf = opendir(load_folder);
			if (dpdf != NULL)
			{
				// Load data from all the files in the temporary directory
				while (epdf = readdir(dpdf))
				{
					if ((strcmp(epdf->d_name, ".") && strcmp(epdf->d_name, "..")))
					{
						sprintf(filename_string, "%s/%s", load_folder, epdf->d_name);
						if (lazy_load)
						{
							// check available memory before doing lazy load
							while (get_mem_available() < mem_lower_bound)
							{
								sleep(1);
							}
						}
						load_temp_file(filename_string);
					}
				}
			}
		}
		
		void load_temp_file(char* filename)
		{
			sprintf(message, "Loading %s", filename);
			log_progress(message);

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
			char* local_buffer = new char[51200];	// 512 records = 51200 bytes
		
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
						memcpy(local_buffer+base, q.top().m_data, 100);
						q.pop();
					}
					memcpy(temp, local_buffer, 51200);
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
					memcpy(local_buffer+base, q.top().m_data, 100);
					q.pop();
					i++;
				}
				outfile.write (buffer, i*100);
				outfile.close();
			}
			
			delete[] local_buffer;
		}
		
		void flush_queue_to_disk(int io_mode)
		{
			sprintf(message, "Flushing partition %04d", partition_id);
			log_progress(message);

			char filename[1024];
			sprintf(filename, "%s/%04d-%05d.out", work_folder, node_id, partition_id);
			if (io_mode == 0)
			{
				save_queue_to_file_buffer_io(filename);
			}
			else
			{
				save_queue_to_file_buffer_io(filename);
			}

			sprintf(message, "Completed partition %04d", partition_id);
			log_progress(message);
		}

		void log_progress(char* msg)
		{
			time_t current_time;
			time(&current_time);
			cout << current_time << "\t" << msg << "\n";
		}
		
		unsigned long get_mem_available() 
		{
			std::string token;
			std::ifstream file("/proc/meminfo");
			while(file >> token) 
			{
				if(token == "MemAvailable:") 
				{
					unsigned long mem;
					if(file >> mem) 
					{
						return mem;
					} 
					else 
					{
						return 0;       
					}
				}
				
				// ignore rest of the line
				file.ignore(std::numeric_limits<std::streamsize>::max(), '\n');
			}
			
			return 0; // nothing found
		}
		
		
};

/**
 *
 * SortDataPlan
 *
 *
 *
 */
 
class SortDataPlan
{
	public:
	int i, hash_bar, lower_range, partitions_done;
	int node_count, node_id, cpu_cores, total_partitions, in_memory_partitions, io_mode;
	bool lazy_load;
	char *work_folder;
	std::vector<SortPartition> partitions;
	std::vector<int> partitions_to_flush, partitions_to_load;
	pthread_mutex_t mutex = PTHREAD_MUTEX_INITIALIZER;

	void initialize(int count, int id, int cores, int total, int memory, char* folder, int mode, bool load)
	{
		node_count = count;
		node_id = id;
		cpu_cores = cores;
		total_partitions = total;
		in_memory_partitions = memory;
		work_folder = folder;
		io_mode = mode;
		lazy_load = load;
		
		hash_bar = floor (65536 / (node_count * total_partitions));
		lower_range = floor(65535 * node_id / node_count);

		partitions_done = 0;
		
		// Initialize all partitions
		SortPartition all_partitions[total_partitions];
		for (i=0; i<total_partitions; i++)
		{
			if (i<memory)
			{
				// in-memory partition
				all_partitions[i].initialize(node_id, i, 0, work_folder);
			}
			else
			{
				// intermediate (on-disk) partition
				all_partitions[i].initialize(node_id, i, 1, work_folder);
			}
			partitions.push_back(all_partitions[i]);
		} 
	}

	bool is_all_flushed()
	{
		if (partitions_done == total_partitions)
		{
			return true;
		}
		else
		{
			return false;
		}
	}

	bool is_flush_queue_empty()
	{
		return partitions_to_flush.empty();
	}

	int get_partition_to_flush()
	{
		pthread_mutex_lock(&mutex);
		int p;
		if (partitions_to_flush.empty())
		{
			p = -1;
		}
		else
		{
			p = partitions_to_flush.back();
			partitions_to_flush.pop_back();
			partitions_done++;
		}
		pthread_mutex_unlock(&mutex);
		return p;
	}

	bool is_load_queue_empty()
	{
		return partitions_to_load.empty();
	}

	int get_partition_to_load()
	{
		pthread_mutex_lock(&mutex);
		int p;
		if (partitions_to_load.empty())
		{
			p = -1;
		}
		else
		{
			p = partitions_to_load.back();
			partitions_to_load.pop_back();
		}
		pthread_mutex_unlock(&mutex);
		return p;
	}

	void add_partition_to_flush(int p)
	{
		pthread_mutex_lock(&mutex);
		partitions_to_flush.push_back(p);
		pthread_mutex_unlock(&mutex);
	}
	
};



void log_progress(char* msg);
int  create_server(int port);
void *server_thread(void *args);
void *sort_thread(void *args);
void *flush_data_thread(void *args);
void *load_data_thread(void *args);
bool test_exit(char* test);

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


struct flush_data_thread_args
{
	int thread_id;	
	SortDataPlan *data_plan;
};

struct load_data_thread_args
{
	int thread_id;	
	SortDataPlan *data_plan;
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

	bool lazy_load;	// lazy load
	if (atoi(argv[8]) > 0)
	{
		lazy_load = true;
	}
	else
	{
		lazy_load = false;
	}
	
	// Get the number of CPU cores
	unsigned cpu_cores = std::thread::hardware_concurrency();
	int hash_bar = floor (65536 / (total_nodes * total_partitions));
	int lower_range = floor(65535 * node_id / total_nodes);
	SortDataPlan	data_plan;
	data_plan.initialize(total_nodes, node_id, cpu_cores, total_partitions, in_memory_partitions, work_folder, io_mode, lazy_load);
	

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
	args.port_no	 = port_no;
	args.data_plan   = &data_plan;
	pthread_create(&sort_server_thread, NULL, server_thread, (void*) &args); 
	pthread_join(sort_server_thread, NULL);	

	// Now, launch N threads to save the data to disk, N = cpu_cores
	pthread_t flush_data_threads[cpu_cores];
	struct flush_data_thread_args flush_args[cpu_cores];
	for (i=0; i<cpu_cores; i++)
	{
		flush_args[i].thread_id = i;
		flush_args[i].data_plan = &data_plan;
		pthread_create(&flush_data_threads[i], NULL, flush_data_thread, (void*) &flush_args[i]); 
	}

	if (lazy_load)
	{
		// Also, launch N threads to load the data from disk, N = cpu_cores / 2
		// is 1/2 enough? might try 1/4 or something smaller
		int lazy_load_thread_count = (int) (cpu_cores / 2);
		pthread_t load_data_threads[lazy_load_thread_count];
		struct load_data_thread_args load_args[lazy_load_thread_count];
		for (i=0; i<lazy_load_thread_count; i++)
		{
			load_args[i].thread_id = i;
			load_args[i].data_plan = &data_plan;
			pthread_create(&load_data_threads[i], NULL, load_data_thread, (void*) &load_args[i]); 
		}
	}

	// Wait for the flush threads to exit
	for (i=0; i<cpu_cores; i++)
	{
		pthread_join(flush_data_threads[i], NULL);
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
	for (i=data_plan->in_memory_partitions; i<data_plan->total_partitions; i++)
	{
		data_plan->partitions[i].final_save_intermediate_buffer();
	} 
	
	// Data Plan
	for (i=0; i<data_plan->in_memory_partitions; i++)
	{
		data_plan->partitions_to_flush.push_back(i);
	}

	for (i=data_plan->in_memory_partitions; i<data_plan->total_partitions; i++)
	{
		data_plan->partitions_to_load.push_back(i);
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
	int in_memory   = data_plan->in_memory_partitions;

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

void *flush_data_thread (void *args)
{
	struct flush_data_thread_args *myArgs = (struct flush_data_thread_args*) args;
	int thread_id = myArgs->thread_id;
	SortDataPlan *data_plan = myArgs->data_plan;

	if (data_plan->lazy_load)
	{
		while (!data_plan->is_all_flushed())
		{
			int partition_id = data_plan->get_partition_to_flush();
			if (partition_id != -1) // not empty
			{
				data_plan->partitions.at(partition_id).flush_queue_to_disk(data_plan->io_mode);
			}
			else
			{
				// No partition to flush, then load some data from on-disk partitions
				int load_partition_id = data_plan->get_partition_to_load();
				if (load_partition_id != -1) // not empty
				{			
					bool lazy_load = false;
					data_plan->partitions.at(load_partition_id).load_disk_data(lazy_load);
					data_plan->add_partition_to_flush(load_partition_id);
				}
			}
		}
	}
	else
	{
		while (!data_plan->is_flush_queue_empty())
		{
			int partition_id = data_plan->get_partition_to_flush();
			if (partition_id != -1) // not empty
			{
				data_plan->partitions.at(partition_id).flush_queue_to_disk(data_plan->io_mode);
			}
		}
		while (!data_plan->is_load_queue_empty())
		{
			int partition_id = data_plan->get_partition_to_load();
			if (partition_id != -1) // not empty
			{			
				data_plan->partitions.at(partition_id).load_disk_data(true);
				data_plan->partitions.at(partition_id).flush_queue_to_disk(data_plan->io_mode);
			}
		}
	}
}


void *load_data_thread (void *args)
{
	struct load_data_thread_args *myArgs = (struct load_data_thread_args*) args;
	int thread_id = myArgs->thread_id;
	SortDataPlan *data_plan = myArgs->data_plan;

	while (!data_plan->is_load_queue_empty())
	{
		int partition_id = data_plan->get_partition_to_load();
		if (partition_id != -1) // not empty
		{			
			bool lazy_load = true;
			data_plan->partitions.at(partition_id).load_disk_data(lazy_load);
			data_plan->add_partition_to_flush(partition_id);
		}
	}
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
 * Print out messages to console.
 *
 */

void log_progress(char* msg)
{
	time(&current_time);
	cout << current_time << "\t" << msg << "\n";
}

