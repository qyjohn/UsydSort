#include <iostream>
#include <fstream>
using namespace std;

int main(int argc, char* argv[])
{    
	char *file_name = argv[1];
	int start = atoi(argv[2]);
	int end = atoi(argv[3]);
	char content[1024];

	ofstream myfile;
	myfile.open (file_name);
	for (int i=start; i<=end; i++)
	{
		sprintf(content, "/data/Input/input_%07d\n", i);
		myfile << content;		
	}
	myfile.close();

	return 0;
}

