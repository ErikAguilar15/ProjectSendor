#include "DataTypeClass.h"


//default constructor
TableInfo::TableInfo()
{
	t_name = "";
	t_data_path = "";
	t_number_of_tuples = 0;
}

//parameterized constructor
TableInfo::TableInfo(string table_name, int number_of_tuples, string data_path)
{
	t_name = table_name;
	t_data_path = data_path;
	t_number_of_tuples = number_of_tuples;
}

void TableInfo::Swap(TableInfo & withMe)
{
	SWAP(t_name, withMe.get_name());
	SWAP(t_data_path, withMe.get_data_path());
	SWAP(t_number_of_tuples, withMe.get_number_of_tuples());
	SWAP(list_of_attributes, withMe.get_Schema());
}

void TableInfo::CopyFrom(TableInfo & withMe)
{
	this->t_name = withMe.get_name();
	this->t_data_path = withMe.get_name();
	this->t_number_of_tuples = withMe.get_number_of_tuples();
	this->list_of_attributes.Clear();
	this->list_of_attributes = withMe.get_Schema();
}

void TableInfo::set_name(string table_name)
{
	t_name = table_name;
}

void TableInfo::set_number_of_tuples(int number_of_tuples)
{
	t_number_of_tuples = number_of_tuples;
}

void TableInfo::set_data_path(string data_path)
{
	t_data_path = data_path;
}

void TableInfo::set_Schema(const Schema & _other)
{
	list_of_attributes = _other;
}



string& TableInfo::get_name()
{
	return t_name;
}

int& TableInfo::get_number_of_tuples()
{
	return t_number_of_tuples;
}

string& TableInfo::get_data_path()
{
	return t_data_path;
}

Schema& TableInfo::get_Schema()
{
	return list_of_attributes;
}

string TableInfo::convertType(Type tempType)
{
	string newType;

	switch (tempType) {
	case String:
		newType = "STRING";
		break;
	case Integer:
		newType = "INTEGER";
		break;
	case Float:
		newType = "FLOAT";
		break;
	default:
		newType = "UNKNOWN";
		break;
	}

	return newType;
}



