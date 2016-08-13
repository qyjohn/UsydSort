#include <iostream>
#include <fstream>
#include <queue>
#include <vector>
#include <sys/types.h>
#include <dirent.h>
#include <string.h>
using namespace std;

int main(int argc, char* argv[])
{    
	char *dir = argv[1];
	char *out = argv[2];
	char command[1024];
	std::vector<std::string> files;

	if (argc < 3)
	{
		cout << "Usage: Validate <target_folder> <output_filename>\n";
		return 0;
	}

	DIR *dpdf;
	struct dirent *epdf;
	dpdf = opendir(dir);
	// Get a list of the files in the work folder, sorted by filename
	if (dpdf != NULL)
	{
		// Load data from all the files in the temporary directory
		while (epdf = readdir(dpdf))
		{
			if (epdf->d_type != DT_DIR)
			{
				std::string filename = epdf->d_name;
				files.push_back(filename);
			}
		}
	}

	// Sort the filenames 
	std::make_heap(files.begin(), files.end());
	std::sort_heap(files.begin(), files.end());

	for (int i=0; i<files.size(); i++)
	{
		sprintf(command, "valsort -o %s/%s.sum %s/%s", dir, files[i].c_str(), dir, files[i].c_str());
		cout << "\n" << command << "\n";
		system(command);
		// Merge to the final summary file
		sprintf(command, "cat %s/%s.sum >> %s/%s", dir, files[i].c_str(), dir, out);
		system(command);
	}

	// Validate final output file
	sprintf(command, "valsort -s %s/%s", dir, out);
	cout << "\n" << command << "\n";
	system(command);

	return 0;
}

