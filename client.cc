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
#include <boost/algorithm/string.hpp>

namespace fs = boost::filesystem;
using namespace std;
using boost::asio::ip::tcp;

//this function is used to transform the data to the appropriate format for get request
string format_get(string input_file)
{
std::ifstream myfile (input_file);
string line;
string data;
while ((! myfile.eof()))
    {
        getline (myfile,line);
        data+=line;
        
        
    }
myfile.close();
string data_get="GET\n"+to_string(data.size())+"\n"+data;


return data_get;

}

//this function is used to transform the data to the appropriate format for put request

string format_put(string input_file)
{
std::ifstream myfile (input_file);
string line;
string value;
getline (myfile,line);
string key=line;
while ((! myfile.eof()))
    {
        getline (myfile,line);
        value+=line;
        value+="\n";
        
        
    }
myfile.close();
value = value.substr(0, value.size()-2);

string data_put="PUT\n"+to_string(key.size())+"\n"+key+"\n"+to_string(value.size())+"\n"+value;


return data_put;


}

//this function is used to transform the data to the appropriate format for delete request

string format_delete(string input_file)
{
std::ifstream myfile (input_file);
string line;
string data;
while ((! myfile.eof()))
    {
        getline (myfile,line);
        data+=line;
        
        
    }
myfile.close();

string data_del="DEL\n"+to_string(data.size())+"\n"+data;


return data_del;


}

//this function is used to transform the data to the appropriate format for where request

string format_where(string input_file)
{
std::ifstream myfile (input_file);
string line;
string data;
while ((! myfile.eof()))
    {
        getline (myfile,line);
        boost::replace_all(line, "\\\\", "\\");
        data+=line;    
    }

myfile.close();
string data_where="WHERE\n"+to_string(data.size())+"\n"+data;

return data_where;

}

//this function is used to transform the data to the appropriate format for reduce request

string format_reduce(string filter_file,string combine_file)
{
std::ifstream myfile (filter_file);
string line;
string filter;
while ((! myfile.eof()))
    {
        getline (myfile,line);
        filter+=line;
        filter+="\n";   
    }

myfile.close();
filter = filter.substr(0, filter.size()-2);

std::ifstream myfile2 (combine_file);
string combine;
while ((! myfile2.eof()))
    {
        getline (myfile2,line);
        combine+=line;
        combine+="\n";
        
        
    }
myfile2.close();
combine = combine.substr(0, combine.size()-2);

string data_reduce="REDUCE\n"+to_string(filter.size())+"\n"+filter+"\n"+to_string(combine.size())+"\n"+combine;

return data_reduce;

}

int main(int argc, char* argv[])
{
    if (argc != 2)
    {
        std::cerr << "Usage: " << argv[0] << " <ServerAddress:port> " << std::endl;
        
        return 0;
    } 
    try
    {

        //connect to IP address and port of the serve 
        std::string server_ip_or_host = argv[1];
        // std::string server_ip_or_host = "127.0.0.1:41100";

        size_t pos = server_ip_or_host.find(':'); 
        if (pos==std::string::npos)
            return 0;
        std::string port_string = server_ip_or_host.substr(pos+1);
        server_ip_or_host = server_ip_or_host.substr(0, pos);
        boost::asio::io_service io_service;
        tcp::resolver resolver(io_service);
        tcp::resolver::query query(server_ip_or_host, port_string);
        tcp::resolver::iterator endpoint_iterator = resolver.resolve(query);
        tcp::resolver::iterator end;
        tcp::socket socket(io_service);
        boost::system::error_code error = boost::asio::error::host_not_found;
        while (error && endpoint_iterator != end)
        {
            socket.close();
            socket.connect(*endpoint_iterator++, error);
        }
        if (error)
            return 0;
        std::cout << "connected to " << argv[1] << std::endl;
        boost::array<char, 1024> buf;

        
        char name[1024];
        char filter_file[1024];
        char combine_file[1024];
        int success =0;
        FILE *fs;
        const char* fs_name;
        char option[1024];
        string data_from_file;
        int success_choice=0;
        string choice_string;
        //vector of possible operations
        std::vector<std::string> PossibleOperations =
        {
            "GET",
            "PUT",
            "DEL",
            "WHERE",
            "REDUCE",
        };

        std::string input;

        //repeat until the client provides a valid operation.
        do{
            std::cout<<"Enter the type of operation (GET,PUT,DEL,WHERE,REDUCE): ";
            scanf("%s",option); 
            std::string choice_string2(option);
            auto it = std::find(PossibleOperations.begin(), PossibleOperations.end(), choice_string2);
            if (it == PossibleOperations.end())
            {
            }
            else{

                choice_string=choice_string2;
                success_choice=1;
            }
        
        }while(success_choice<1);
          

        //repeat until the client provides a valid path for the data.
        do{

          if (choice_string!="REDUCE")
          {
          //if the operation is not reduce, the client need to provide only one file that contains
          //the data.
          std::cout<<"Enter path of the file to send its content : ";
          scanf("%s",name); 
          fs_name=name; 
          fs= fopen(fs_name, "r");
          if(fs == NULL)
          {
            fprintf(stderr, "ERROR: File %s not found. (errno = %d)\n", fs_name, errno);
          }
          else
          {
            success=1;
            
            //transform the data to the adequat format to respect the protocole used
            //to send data to the server

            if (choice_string.compare("GET")==0)
                data_from_file=format_get(name);
            else if(choice_string.compare("PUT")==0)
                data_from_file=format_put(name);
            else if(choice_string.compare("DEL")==0)
                data_from_file=format_delete(name);
            else if(choice_string.compare("WHERE")==0)
                data_from_file=format_where(name);

          }
          }
          else
          {
          //if the operation is reduce, the client needs to provide 2 files: one contains filter 
          //code and the second contains combine code.

          std::cout<<"Enter the path of filter function : (a filter function must be: function filter(key,value){do something; return value} )";
          scanf("%s",filter_file); 
          fs_name=filter_file; 
          fs= fopen(fs_name, "r");
          if(fs == NULL)
          {
            fprintf(stderr, "ERROR: File %s not found. (errno = %d)\n", fs_name, errno);
          }
          else
          {
                std::cout<<"Enter the path of combine function : (a combine function must be: function combine(a){do something; return res} ) ";
                scanf("%s",combine_file); 
                fs_name=combine_file; 
                fs= fopen(fs_name, "r");
                if(fs == NULL)
                {
                    fprintf(stderr, "ERROR: File %s not found. (errno = %d)\n", fs_name, errno);
                }
                else
                {
                        data_from_file=format_reduce(filter_file,combine_file);
                        success=1;
                }
                
          }
          }

            

          
        }while(success<1);
        std::cout<<"Sending "<<choice_string.c_str()<<" request to the Server...\n";
        std::istringstream source_file(data_from_file);
        source_file.seekg(0, ios::end);
        int size = source_file.tellg();
        size_t file_size = data_from_file.size();
        source_file.seekg(0);
        
        boost::asio::streambuf request;
        std::ostream request_stream(&request);
        request_stream << file_size <<"\n\n";
        boost::asio::write(socket, request);

        //send data to the server.
       
        for (;;)
        {

            if (source_file.eof()==false)
            {
                source_file.read(buf.c_array(), (std::streamsize)buf.size());
                if (source_file.gcount()<=0)
                {
                    std::cout << "read error " << std::endl;
                    return 0;
                }
                boost::asio::write(socket, boost::asio::buffer(buf.c_array(), 
                source_file.gcount()),
                boost::asio::transfer_all(), error);
                if (error)
                {
                    std::cout << "send error:" << error << std::endl;
                    return 0;
                }
            }
            else
                break;
        }
        std::cout << "send " << choice_string.c_str()<<" request" << " completed successfully.\n";

       
        std::cout << "Please wait until you receive data from server :\n";
       

        //read the response of the server.
        char data[1024];
        size_t length = socket.read_some(boost::asio::buffer(data), error);

        
         std::cout << "Your data is:\n ";
         std::cout.write(data,length);
         std::cout<<"\n";
        
         
    }
    catch (std::exception& e)
    {
        std::cerr << e.what() << std::endl;
    }
    return 0;
}




