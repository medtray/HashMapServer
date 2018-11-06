#pragma once
#include <boost/filesystem.hpp>
#include <iostream>
#include <string>
#include <v8.h>
#include "global.h"
#include <mutex>
#include <shared_mutex>
#include <cstddef>
#include "KeyValueStruct.h"
#include "Print.h"
#include <thread>



using namespace v8;



std::string execute_reduce(const std::string filter,const std::string combine, KeyValueStructure<std::string, std::vector<char>, tableSize>* kvstore) {
// Need a locker and a handle scope
std::string aa;
//Isolate is required for parallel multi threading using JavaScript codes.
thread_local Isolate *thread_isolate;
thread_isolate = Isolate::New();
  thread_isolate->Enter();
 

while (true) {
    Locker lock(thread_isolate);
    HandleScope handle_scope;
    //read the filter code
v8::Handle<v8::String> code = v8::String::New(filter.c_str(), filter.length());
    //read the combine code
v8::Handle<v8::String> code2 = v8::String::New(combine.c_str(), combine.length());

string name1="filter";
string name22="combine";
v8::Handle<v8::Value> name=v8::String::New(name1.c_str());
v8::Handle<v8::Value> name2=v8::String::New(name22.c_str());

//define a global template
  v8::Handle<v8::ObjectTemplate> globalTemplate = v8::ObjectTemplate::New();

// Create the context
v8::Handle<v8::Context> context2 = v8::Context::New(NULL, globalTemplate);
v8::Context::Scope context_scope(context2);

v8::Handle<v8::Object> js_int = kvstore->create_js_object(kvstore);
  
if (code->Length() == 0)
return "";
// Switch to the provided execution context
// Try to compile the script code
v8::TryCatch try_catch; // this picks up errors
v8::Handle<v8::Script> script = v8::Script::Compile(code, name);
// Error if script empty (probably compilation error)
if (script.IsEmpty()) {

return "";
}
// Run the script. If result empty, probably runtime error
v8::Handle<v8::Value> result = script->Run();
if (result.IsEmpty()) {

return "";
}
// If there is a result, print it to the console
if (!result->IsUndefined() && !result.IsEmpty()) {
v8::String::Utf8Value str(result);
std::cout << *str << std::endl;
}

if (code2->Length() == 0)
return "";

v8::Handle<v8::Script> script2 = v8::Script::Compile(code2, name2);
// Error if script empty (probably compilation error)
if (script2.IsEmpty()) {

return "";
}
// Run the script. If result empty, probably runtime error
v8::Handle<v8::Value> result20 = script2->Run();
if (result20.IsEmpty()) {

return "";
}
// If there is a result, print it to the console
if (!result20->IsUndefined() && !result20.IsEmpty()) {
v8::String::Utf8Value str2(result20);
std::cout << *str2 << std::endl;
}



Handle<v8::Object> global = context2->Global();
//add filter function to the global context
Handle<v8::Value> value = global->Get(String::New("filter"));
Handle<v8::Function> func = v8::Handle<v8::Function>::Cast(value);
//we need two arguments for filter function
Handle<Value> args[2];
Handle<Value> js_result;
int final_result;
std::string result2;
std::vector<std::string> it;

//add combine function to the global context
Handle<v8::Value> value2 = global->Get(String::New("combine"));
Handle<v8::Function> func2 = v8::Handle<v8::Function>::Cast(value2);
//we need one argument for combine function
Handle<Value> args2[1];
Handle<Value> js_result2;



for (unsigned hashValue=0; hashValue<tableSize; hashValue++ )
        {
            std::mutex &m = kvstore->mutexes[hashValue];
            m.lock();
            KeyValueUnit<std::string, std::vector<char>> *entry = kvstore->MainStruct[hashValue];

        while (entry != NULL) {
           
            
            std::string s=entry->getKey();
            std::vector<char> data=entry->getValue();

            args[0] = v8::String::New(s.c_str());
            
            args[1] = v8::String::New(&data[0], data.size());
                
                //call filter on every entity using the key and value
                   js_result = func->Call(global, 2, args);

                  //check that the result is not NULL to avoid segmentation fault error for
                  //the server

                   if (*js_result)
                   {
                       v8::String::Utf8Value result_inter(js_result->ToString());

                   result2 = std::string(*result_inter);
                    //add non NULL and empty results to the vector
                    if (result2!="")
                   it.push_back(result2);
                   }
                   

            entry = entry->getpoinTo();
        }
        m.unlock();
        }
 

//build a v8 array that will be used for combine function
v8::Local<v8::Array> vector_for_combine; 

vector_for_combine = v8::Array::New(it.size()); 
       
      for(int i = 0; i < it.size(); i++) { 
         
          v8::Handle<v8::Number> y = v8::Number::New( i ); 
          
          v8::Handle<v8::String>  s = v8::String::New( it[i].c_str() ); 
          vector_for_combine->Set( y, s); 
        
      } 

args2[0] =vector_for_combine;
//call the combine function on the prepared v8 array

js_result2 = func2->Call(global, 1, args2);

 //make sure that the result is not NULL to avoid segmentation fault error that can 
 //stop the server

      if (*js_result2)
    {             
        v8::String::Utf8Value result_inter2(js_result2->ToString());
        aa = std::string(*result_inter2);
     }
 break;
  }
  //quit the isolate before returning the final result
  thread_isolate->Exit();
  thread_isolate->Dispose();

//return the final result of reduce
return aa;
}







