/**
 
 * Key/value stores play a significant role in modern distributed systems.  They
 * allow a program to easily store data to a high-availability server, and to
 * easily retrieve that data.  A key/value store may be persistent, or it may be
 * an in-memory cache.  Key/value stores are usually replicated and distributed,
 * but we won't worry about those features for this assignment.  Examples of 
 * key/value stores include Redis, MongoDB, Memcached, DynamoDB, Firebase, 
 * CouchBase, Voldemort, and many more.  Since key/value stores do not track 
 * many-one relationships among elements, they are sometimes called "NoSQL"
 * databases.
 *
 * At a bare minimum, a key/value store must provide the same interface that a 
 * C++ map would offer:
 *   - get(key) 
 *       Returns the value associated with a key, if present, or null otherwise
 *   - del(key)
 *       Removes key and its associated value from the store, and returns true.
 *       If the key wasn't in the store, returns false.
 *   - put(key, value) 
 *       Adds a mapping from key to value, if no prior mapping for key exists.  
 *       If a mapping existed, then it is modified, so that the mapping from key
 *       will be to the provided value.
 *
 * Note that even implementing this interface is not trivial, because the 
 * key/value store must be highly concurrent.  That is, it should use as many 
 * threads as are available, and should allow operations on different data 
 * elements to proceed simultaneously as much as possible.
 * 
 * To the above interface, we will add the following methods:
 *   - where(regexp)
 *       Returns all of the key/value pairs in the data set for which the key
 *       matches the regular expression provided by regexp.
 *   - reduce(JavaScript filter, JavaScript combine)
 *       Runs the provided JavaScript code and returns the result.  See below 
 *       for more details.
 * 
 * As you may have guessed, in order to be as general as possible, key/value 
 * stores typically treat the key as a string, and the value as a byte array.  
 * If there is any structure to the values, the key/value store does not know 
 * about it.  So, for example, a reduce() filter may know that the value can be 
 * parsed as a JSON object, but the key/value store itself would not know that 
 * the value was anything other than a byte array.
 * 
 * To implement the key/value store data structure, you cannot simply use the 
 * std::unordered_map, because it does not support concurrency.  You will need 
 * to create your own data structure.  The easiest approach is to create a large
 * array (at least 2^16 elements), where each array element holds a linked list.  
 * You can use the built-in C++ hash function to hash a key to one of these 
 * lists, and you can achieve concurrency by locking at the granularity of 
 * lists.  Note that for where() and reduce(), this means your result may not be
 * "consistent", because your code will only lock one list at a time.  Once you 
 * have this implemented, testing get(), del(), and put() should be easy.
 * 
 * To implement where(), your code should use the C++ regex library.  That is, 
 * you should go through one list at a time, and for each key, use the regex to 
 * determine if it matches, and if so, add *a copy of* the key/value pair to the
 * result that you will return.  (Note: copying is essential for concurrency 
 * and consistency reasons)
 * 
 * To implement reduce(), you will need to move state back and forth between C++
 * and JavaScript.  The high-level behavior is that the key/value store should 
 * lock each bucket, run filter() on each key/value pair in the bucket, and 
 * whenever filter() returns a non-null value, the result should be added to a
 * vector.  Then the key/value store should execute the provided combine() 
 * function on the vector to produce a single result that gets returned to the 
 * caller.
 * 
 * Finally, it's important for us to specify the protocol to be used for sending 
 * requests to the k/v store over the network, and for receiving results from 
 * the k/v store.  In general, our format will be similar to the HTTP protocol: 
 * we will make use of newlines and lines of text for metadata, and 
 * variable-length content for binary data.  Note: it is acceptable to forbid 
 * newlines within keys.  However, a value may contain arbitrary binary data, to
 * include \0 and \n characters.
 * 
 * put:
 *   - first line: PUT\n
 *   - second line: KEY-LEN: number\n
 *   - third line: the key, ending with \n
 *   - fourth line: VAL-LEN: number\n
 *   - remaining lines: the value (VAL-LEN bytes)
 * 
 * get:
 *   - first line: GET\n
 *   - second line: KEY-LEN: number\n
 *   - third line: the key, ending with \n
 * 
 * del:
 *   - first line: DEL\n
 *   - second line: KEY-LEN: number\n
 *   - third line: the key, ending with \n
 * 
 * where:
 *   - first line: WHERE\n
 *   - second line: REGEX-LEN: number\n
 *   - third line: the regex, ending with \n
 * 
 * reduce:
 *   - first line: REDUCE\n
 *   - second line: FILTER-LEN: number\n
 *   - third line: the filter code, ending with \n
 *   - fourth line: COMBINE-LEN: number\n
 *   - fifth line: the combine code, ending with \n
 * 
 * responses to GET and REDUCE will be of the following form:
 *   - first line: either OK\n or ERROR\n
 *   - second line: RESULT-LEN: number\n
 *   - remaining lines: the result value (RESULT-LEN bytes)
 * 
 * responses to PUT and DEL will be of the following form
 *   - first line: either OK\n or ERROR\n
 * 
 * responses to WHERE will be of the following form:
 *   - first line: either OK\n or ERROR\n
 *   - second line: NUM-RESULTS: number\n
 *   - next line: RESULT-KEY-LEN: number\n
 *   - next line: RESULT-KEY-LEN bytes of key, followed by \n
 *   - next line: RESULT-VAL-LEN: number\n
 *   - next line: RESULT-VAL-LEN bytes of result, followed by \n
 *   - repeat previous four lines as many times as needed for a total of 
 *     NUM-RESULTS results
 * 
 * Note: when ERROR is returned, there should be no further data transmitted in 
 * the response.
 */

#include <iostream>
#include <libgen.h>
#include <unistd.h>
#include <cstdlib>
#include <thread>
#include <utility>
#include <boost/asio.hpp>
#include <ctime>
#include <string>
#include <fstream>
#include <sstream>
#include <boost/array.hpp>
#include<stdio.h>
#include<fcntl.h>
#include<termios.h>
#include<sys/types.h>
#include<netinet/in.h>
#include<arpa/inet.h>
#include<errno.h>
#include<time.h>
#include<stdlib.h>
#include<sys/time.h>
#include <sys/wait.h>
#include <signal.h>
#include <ctype.h>          
#include <netdb.h>
#include<dirent.h>

#include <functional>
#include <chrono>
#include <future>
#include <cstdio>
#include <boost/filesystem/path.hpp>
#include <boost/filesystem/operations.hpp>
#include <openssl/sha.h>
#include <unistd.h>
#include <stdio.h>
#include <cstddef>
#include <string>
#include <iostream>
#include <ostream>
#include <vector>
#include <v8.h>
#include "HandleReduce.h"
#include "Print.h"
#include "global.h"
#include <iostream>
#include <numeric>
#include <vector>
#include <string>
#include "KeyValueStruct.h"

using boost::asio::ip::tcp;
namespace fs = boost::filesystem;
using namespace std;
 
 
 //Define the key/value structure to use.
KeyValueStructure<std::string, std::vector<char>, tableSize> kvstore;
  
 auto append = [](std::vector<char> v, const std::string& s) 
        { v.insert(v.end(), s.begin(), s.end()); return v; };


//this fucntion is used to serve get request
string serve_get(string input)
{
  string line;
  std::istringstream f(input);
  string result;
  std::vector<std::string> info;
  /* std::vector<char> info2;


  for(char& c : input) {
      info2.push_back(c);
} */

  while (std::getline(f, line)) {
        info.push_back(line);
    }

    //verify the size
  string size=info[1];
  string key=info[2]; 
  std::vector<char> value;
   value= kvstore.get(key); 
   if (atoi(size.c_str())==key.size())
   {
if (value.size()>0)
   {
      result+="OK\n";
      result+=to_string(value.size());
      result+="\n";
      std::string str(value.begin(),value.end());
      result+=str;

   }
   else
      result+="ERROR\n";
    

    return result;
   }
   else
   {
     result="inconsistent input";
         return result;

   }
   

}

//this function is used to serve put request
string serve_put(string input)
{
  string line;
  std::istringstream f(input);
  string result;
  std::vector<std::string> info;

  while (std::getline(f, line)) {
        info.push_back(line);
    }
  string size_key=info[1];
  string key=info[2]; 
  string size_value=info[3];
  string value_inter;
  string inter;
   
  //transform the value into array of bytes and put it in the key/value structure

  std::vector<std::string>::const_iterator begin = info.begin()+4;
	std::vector<std::string>::const_iterator last = info.begin() + info.size();
	std::vector<std::string> new_arr(begin, last);

  for (int j=0;j<new_arr.size()-1;j++)
      new_arr[j]+="\n";
 

    
    std::vector<char> value = std::accumulate(new_arr.begin(), new_arr.end(), std::vector<char>(), append);

    kvstore.put(key,value); 
   
  result+="OK\n";
  return result;
    
 /*  if ((atoi(size_key.c_str())==key.size())&(value.size()==atoi(size_value.c_str())))
  {
      kvstore.put(key,value); 
   
  result+="OK\n";
  }
    else
   {
     result="inconsistent input";
         return result;

   } */

}

//this function is used to serve delete request.
string serve_del(string input)
{
  string line;
  std::istringstream f(input);
  string result;
  std::vector<std::string> info;

  while (std::getline(f, line)) {
        info.push_back(line);
    }

    //verify the size
  string size=info[1];
  string key=info[2]; 
  bool remove_result;
   remove_result= kvstore.remove(key); 
   if (atoi(size.c_str())==key.size())
   {
if (remove_result)
   {
      result+="OK\n";
      

   }
   else
      result+="ERROR\n";
    

    return result;
   }
   else
   {
     result="inconsistent input";
         return result;

   }
   
}

//this function is used to serve where request
string serve_where(string input)
{
  string line;
  std::istringstream f(input);
  string result;
  std::vector<std::string> info;
  std::string key_entity;
  
  std::vector<char> value_entity;
  int size_key;
  int size_value;
  

  while (std::getline(f, line)) {
        info.push_back(line);
    }

    //verify the size
  string size=info[1];
  string key=info[2]; 
  std::vector<KeyValueUnit<std::string, std::vector<char>>> where_result;
   where_result=kvstore.where(key);

   //transform the result of where into the adequat format to send it to client.

  /*  if (atoi(size.c_str())==key.size())
   { */
if (where_result.size()>0)
   {
      result="OK\n"+to_string(where_result.size())+"\n";
      for (int i=0;i<where_result.size();i++)
      {
          key_entity=(&(where_result[i]))->getKey();
          size_key=key_entity.size();
          value_entity=(&(where_result[i]))->getValue();
          size_value=value_entity.size();
          std::string string_value_entity(value_entity.begin(),value_entity.end());
          result=result+to_string(size_key)+"\n"+key_entity+"\n"+to_string(size_value)+"\n"+string_value_entity+"\n";
      }
      

      

   }
   else
      result+="ERROR\n";
    

    return result;
  /*  }
   else
   {
     result="inconsistent input";
         return result;

   } */
}

//this function is used to serve reduce request
string serve_reduce(string input)
{
  string line;
  std::istringstream f(input);
  string result;
  std::vector<std::string> info;
  int index;


  while (std::getline(f, line)) {
        info.push_back(line);
    }
  
  //read the filter and combine codes.

  string size_filter=info[1];
  std::vector<std::string>::const_iterator begin = info.begin()+2;
	std::vector<std::string>::const_iterator last = info.begin() + info.size();
	std::vector<std::string> new_arr(begin, last);

  for (int j=0;j<new_arr.size()-1;j++)
      new_arr[j]+="\n";
 
  std::vector<char> value = std::accumulate(new_arr.begin(), new_arr.end(), std::vector<char>(), append);
  std::vector<char>::const_iterator begin2 = value.begin();
  std::vector<char>::const_iterator last2 = value.begin() + atoi(size_filter.c_str());
  std::vector<char> new_arr2(begin2, last2);
  std::vector<char>::const_iterator begin3 = value.begin() + atoi(size_filter.c_str());
  std::vector<char>::const_iterator last3 = value.end();
  std::vector<char> new_arr3(begin3, last3);
  vector<char>::iterator itr = find(new_arr3.begin(), new_arr3.end(), '\n');
  if(itr != new_arr3.end())
  {
    index = itr - new_arr3.begin();
  }
  std::vector<char>::const_iterator begin4 = new_arr3.begin();
  std::vector<char>::const_iterator last4 = new_arr3.begin() + index;
  std::vector<char> new_arr4(begin4, last4);
  std::vector<char>::const_iterator begin5 = new_arr3.begin() + index;
  std::vector<char>::const_iterator last5 = new_arr3.end();
  std::vector<char> new_arr5(begin5, last5);

  //new_arr2 contains the filter code and new_arr5 contains the combine code.

  string filter_function(new_arr2.begin(),new_arr2.end());
  string combine_function(new_arr5.begin(),new_arr5.end());

  //execute reduce using the filter and combine codes.

   std::string result22=execute_reduce(filter_function,combine_function,&kvstore);

   //the final result can be OK or ERROR depending on the outcome of execute_reduce.

  if (result22.size()>0)
   {
     if (result22=="undefined")
     result+="ERROR\n";
     /* else if (atoi(result22.c_str())==0)
     result+="ERROR\n"; */
     else
     result="OK\n"+to_string(result22.size())+"\n"+result22;

      

   }

   else{

     result+="ERROR\n";
   }

return result;

    

}



//This function contains the functionalities that the server provides depending on the request
//of a client. Every client will have an independant session. So, the server can serve multiple
//clients.

void session(tcp::socket sock)
{

   
    boost::array<char, 1> buf;
    size_t file_size = 0;
    string line;
    string result;
   
    
    try
    {
        boost::system::error_code error;

        boost::asio::streambuf request_buf;
        boost::asio::read_until(sock, request_buf, "\n\n");
        std::istream request_stream(&request_buf);
        request_stream >> file_size;
        request_stream.read(buf.c_array(), 0); 
        string data2;
        //the server reads the data from client

        for (;;)
        {
            size_t len = sock.read_some(boost::asio::buffer(buf), error);
            if (len>0)
                data2+=buf.elems[0];
            if (data2.length()== file_size)
            
                break; // file was received
            if (error)
            {
                std::cout << error << std::endl;
                break;
            }

            
        }
        std::cout << "received " << file_size << " bytes.\n";
       
        std::istringstream f(data2);
        //read the first line to know the type of request
        std::getline(f, line);
        //serve the request of a client
        if(line=="GET")
        {
          result=serve_get(data2);

        }

        else if(line=="PUT")
        {
            result=serve_put(data2);
        }

        else if(line=="DEL")
        {
            result=serve_del(data2);
        }

        else if(line=="WHERE")
        {
            result=serve_where(data2);
        }

        else if(line=="REDUCE")
        {
            result=serve_reduce(data2);
        }

		//the server sends back an answer to a client depending on his request
		
          
		  boost::asio::write(sock, boost::asio::buffer(result, result.length()));

                       
    }
    catch (std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }
	
}

//in this function, the server is listening to a port and accepts incoming connections. Then, assigns
//every connection to an independant thread.

void server(boost::asio::io_service& io_service, unsigned short port)
{
  tcp::acceptor a(io_service, tcp::endpoint(tcp::v4(), port));
  for (;;)
  {
    tcp::socket sock(io_service);
    a.accept(sock);
    std::cout << "New connection" << std::endl;
    
    std::thread(session, std::move(sock)).detach();
	
    
  }
}

/** Print some helpful usage information */
void usage(const char *progname) {
  using std::cout;
  cout << "KeyValue Server\n";
  cout << "  Usage: " << progname << " [options]\n";
  cout << "    -p <int> : Port on which to listen (default 41100)\n";
  cout << "    -h       : print this message\n";
}

int main(int argc, char *argv[]) {
  // Config vars that we get via getopt
  int port = 41100;       // random seed
  bool show_help = false; // show usage?

  
  // Parse the command line options:
  int o;
  while ((o = getopt(argc, argv, "p:h")) != -1) {
    switch (o) {
    case 'h':
      show_help = true;
      break;
    case 'p':
      port = atoi(optarg);
      break;
    default:
      show_help = true;
      break;
    }
  }

  // Print help and exit
  if (show_help) {
    usage(basename(argv[0]));
    exit(0);
  }

  // Print the configuration... this makes results of scripted experiments
  // much easier to parse
  std::cout << "p = " << port << std::endl;

  // Time for you to start writing code :)

  try
  {	
    
    boost::asio::io_service io_service;
    server(io_service, port);

  }
  catch (std::exception& e)
  {
    std::cerr << "Exception: " << e.what() << "\n";
  }

  return 0;
}
