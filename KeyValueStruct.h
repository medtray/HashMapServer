#pragma once
#include "KeyValueUnit.h"
#include <cstddef>
#include <mutex>
#include <shared_mutex>
#include<vector>
#include <regex>
#include "tbb/tbb.h"
#include "tbb/task_scheduler_init.h"
#include <v8.h>

using namespace tbb;
using namespace std;
using namespace v8;

template <typename Key, typename Value, size_t tableSize>

class KeyValueStructure
{

    public:
    
    KeyValueUnit<Key, Value> *MainStruct[tableSize];  //define the main structure of key/value stores
    std::array<std::mutex, tableSize> mutexes; //define the mutexes

    KeyValueStructure() : //constructor of the class
    MainStruct()  
        
    {
    }


    public:
    
    //get a value by key
    const Value get(const Key &key)
    {
        Value value;
        //use a hash function
        std::hash<std::string> hash_fn;
        unsigned long hashValue = hash_fn((key))%tableSize;
        //lock the bucket
        std::mutex &m = mutexes[hashValue];
         m.lock();
        //ssleep(15);
        KeyValueUnit<Key, Value> *unit = MainStruct[hashValue];
            

        while (unit != NULL) {
            //if the key of the unit is equal to the given key, return the value
            if (unit->getKey() == key) {
                value = unit->getValue();
                //unlock the bucket before returning the value
                m.unlock();
                return value;
            }

            unit = unit->getpoinTo();
        }
        //unlock the bucket before returning the value
        m.unlock();
        return value;
    }


    //get entities in key/value store that match a regular expression
    std::vector<KeyValueUnit<Key, Value>> where(const Key &key)
    {

        std::vector<KeyValueUnit<Key, Value>> it;
        std::regex regex(key);
        int grain_size=10;
        //use parallel_for to extract the entities from linked lists
        parallel_for( tbb::blocked_range<size_t>(0, tableSize,grain_size), 
        [&](const blocked_range<size_t>& r) {
        for (unsigned hashValue=r.begin(); hashValue!=r.end(); hashValue++ )
        {
            std::mutex &m = mutexes[hashValue];
            m.lock();
            KeyValueUnit<Key, Value> *entry = MainStruct[hashValue];
            

            while (entry != NULL)
            {
                std::string s=entry->getKey();
                if(std::regex_match(s, regex)) 
                    
                {
                    it.push_back(*entry);

                }

                entry = entry->getpoinTo();
            }
            m.unlock();
        }
        }
        ,simple_partitioner());
        
    return it;
    }
 
    //put a key/value entity in the key/value structure
    void put(const Key &key, const Value &value)
    {

        std::hash<std::string> hash_fn;
        unsigned long hashValue = hash_fn((key))%tableSize;
        std::mutex &m = mutexes[hashValue];
         m.lock();
        KeyValueUnit<Key, Value> *previousUnit = NULL;
        KeyValueUnit<Key, Value> *unit = MainStruct[hashValue];

        while (unit != NULL && unit->getKey() != key) {
            previousUnit = unit;
            unit = unit->getpoinTo();
        }

        if (unit == NULL) {
            unit = new KeyValueUnit<Key, Value>(key, value);

            if (previousUnit == NULL) {
                // the linked list is empty, so insert the entity as the first element in the bucket
                MainStruct[hashValue] = unit;

            } else {
                //in a given linked list, the last element points to the new entity
                previousUnit->setpoinTo(unit);
            }

        } else {
            // if the key already exists, just update the value of this key
            unit->setValue(value);
        }
        m.unlock();
    }

    //remove a key/value entity from the key/value structure
    bool remove(const Key &key)
    {
       
        std::hash<std::string> hash_fn;
        unsigned long hashValue = hash_fn((key))%tableSize;
        std::mutex &m = mutexes[hashValue];
         m.lock();
        KeyValueUnit<Key, Value> *previousUnit = NULL;
        KeyValueUnit<Key, Value> *unit = MainStruct[hashValue];

        while (unit != NULL && unit->getKey() != key) {
            previousUnit = unit;
            unit = unit->getpoinTo();
        }

        if (unit == NULL) {
            // the key is not found
            m.unlock();
            return false;

        } else {
            if (previousUnit == NULL) {
                // the bucket contains the key/value entity that is pointed to by the deleted
                //entity
                MainStruct[hashValue] = unit->getpoinTo();

            } else {
                // the previous unit points to the key/value entity that is pointed to by the deleted
                //entity

                previousUnit->setpoinTo(unit->getpoinTo());
            }

            delete unit;
            m.unlock();
            return true;
        }
        
    }

    


KeyValueStructure<Key,Value,tableSize> *UnwrapCppInt(v8::Handle<v8::Object> jsObject) {
v8::Handle<v8::External> pointer =
v8::Handle<v8::External>::Cast(jsObject->GetInternalField(0));
return static_cast<KeyValueStructure<Key,Value,tableSize> *>(pointer->Value());
}


//Create a JavaScript wrapper around a C++ object
v8::Handle<v8::Object> create_js_object(KeyValueStructure<Key,Value,tableSize> *instance) {
v8::HandleScope scope;
// make an empty object, set it so we can put a C++ object in it
v8::Handle<v8::ObjectTemplate> base_tpl = v8::ObjectTemplate::New();
base_tpl->SetInternalFieldCount(1);
// Create the actual template object. It's persistent, so our C++ object
// doesn't get reclaimed
v8::Persistent<v8::ObjectTemplate> real_tpl =
v8::Persistent<v8::ObjectTemplate>::New(base_tpl);
// Allocate JS object, wrap the C++ instance with it
v8::Handle<v8::Object> result = real_tpl->NewInstance();
result->SetInternalField(0, v8::External::New(instance));
// Return the js object, make sure it doesn't get reclaimed by the scope
return scope.Close(result);
}

static v8::Handle<v8::Value> js_remove(const v8::Arguments &args) {
v8::Locker locker; v8::HandleScope scope;
KeyValueStructure<Key,Value,tableSize> k;
KeyValueStructure<Key,Value,tableSize> *o = k.UnwrapCppInt(args.This());
v8::String::Utf8Value param1(args[0]->ToString());
std::string from = std::string(*param1);
o->remove(from);
return v8::Undefined();
}

static v8::Handle<v8::Value> js_get(const v8::Arguments &args) {
v8::Locker locker; v8::HandleScope scope;
KeyValueStructure<Key,Value,tableSize> k;
// get the this pointer, call the method, and we're done
KeyValueStructure<Key,Value,tableSize> *o = k.UnwrapCppInt(args.This());
v8::String::Utf8Value param1(args[0]->ToString());
std::string from = std::string(*param1);
if (o != NULL) {
    std::vector<char> data=o->get(from);
    
return scope.Close(v8::String::New(&data[0], data.size()));
}
return v8::Undefined();
} 


   
};
