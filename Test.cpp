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
#include <unordered_map>

using namespace std;

class Foo
{
	public:
	std::unordered_map<std::string, std::string> map;
};

Foo* create_foo();

Foo* create_foo()
{
	Foo *f = new Foo();
	return f;
}

int main(int argc, char* argv[])
{
	Foo *f = create_foo();
	f->map["key1"]="value1";
	f->map["key2"]="value2";

	cout << f->map["key1"] << "\n";
	cout << f->map["key2"] << "\n";
}
