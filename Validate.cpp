#include <iostream>
#include <fstream>
using namespace std;

int main(int argc, char* argv[])
{    
	char *prefix = argv[1];
	int total = atoi(argv[2]);
	char command[1024];

	// Remove existing final summary file
	sprintf(command, "rm %s-all.sum", prefix);
	system(command);
	for (int i=0; i<total; i++)
	{
		// Validate one output file
		sprintf(command, "valsort -o %s-%04d.sum %s-%04d", prefix, i, prefix, i);
		cout << "\n" << command << "\n";
		system(command);
		// Merge to the final summary file
		sprintf(command, "cat %s-%04d.sum >> %s-all.sum", prefix, i, prefix);
		system(command);

	}

	// Validate final output file
	sprintf(command, "valsort -s %s-all.sum", prefix);
	system(command);

	return 0;
}

