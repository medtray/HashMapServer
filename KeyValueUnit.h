#pragma once
#include <cstddef>

template <typename Key, typename Value>
class KeyValueUnit
{

    private:
    
    Key _key; //key of one unit
    Value _value; //value of one unit
    KeyValueUnit *_poinTo; //every unit should point to other unit or NULL
    

public:
    KeyValueUnit(const Key &key, const Value &value) :  //constructor
        _key(key), _value(value), _poinTo(NULL)
    {
    }

    Key getKey() const
    {
        return _key;  //getter of key
    }


    Value getValue() const
    {
        return _value;  //getter of value
    }

    void setValue(Value value)
    {
        _value = value;  //setter of value
    }

    KeyValueUnit *getpoinTo() const
    {
        return _poinTo;  //getter of pointer 
    }

    void setpoinTo(KeyValueUnit *poinTo)
    {
        _poinTo = poinTo; //setter of pointer
    }


};