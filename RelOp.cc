#include <iostream>
#include <sstream>
#include "RelOp.h"
#include "EfficientMap.h"

using namespace std;


//---------------------------------------------------------------------------------------------------------------------------------------------
ostream& operator<<(ostream& _os, RelationalOp& _op) {
	return _op.print(_os);
}


//-------------------------------------------------------SCAN --------------------------------------------------------------------------------------
Scan::Scan(Schema& _schema, DBFile& _file, string _tableName) {
	schema = _schema;
	file = _file;
	tableName = _tableName;
}
Scan::~Scan() {

}

bool Scan::GetNext(Record& _record){

	//cout << "Run Scan: GETNEXT" << endl;
	//file.MoveFirst();
	//file.GetNext(_record);
	int check = file.GetNext(_record);
	//cout << "SCAN check value: " << check << endl;

	//Test for printing records
	/*
	while (file.GetNext(_record)) {
					_record.print(cout, schema);
					cout << endl;
	}*/

	if(check == 0){
		return true;
	} else return false;

}
string Scan::getTableName() {
	return tableName;
}
DBFile& Scan::getfile() {
	return file;
}
Schema& Scan::getSchema() {
	return schema;
}
ostream& Scan::print(ostream& _os) {
	return _os << "SCAN: SCHEMA IN/OUT: " << schema << endl;
}

//-------------------------------------------------------SELECT --------------------------------------------------------------------------------------
Select::Select(Schema& _schema, CNF& _predicate, Record& _constants,
	RelationalOp* _producer, string _tableName) {
	schema = _schema;
	predicate = _predicate;
	constants = _constants;
	producer = _producer;
	tableName = _tableName;
}
Select::~Select() {

}

bool Select::GetNext(Record& _record){

	//cout << "Run Select: GETNEXT" << endl;

	while (true) {
		int check = producer->GetNext(_record);
		//producer->print(cout);
		//cout << "SELECT check value: " << check << endl;
		if (!check) return false;
		else {
				check = predicate.Run(_record, constants);
				if (check) return true;
		}
	}

	return false;
}
string Select::getTableName() {
	return tableName;
}
Schema& Select::getSchema() {
	return schema;
}
CNF& Select::getPredicate() {
	return predicate;
}
Record& Select::getRecords() {
	return constants;
}
RelationalOp* Select::getProducer() {
	return producer;
}
ostream& Select::print(ostream& _os) {
	return _os << "SELECT: SCHEMA IN/OUT: " << schema << endl;
}


//-------------------------------------------------------PROJECT --------------------------------------------------------------------------------------
Project::Project(Schema& _schemaIn, Schema& _schemaOut, int _numAttsInput,
	int _numAttsOutput, int* _keepMe, RelationalOp* _producer) {

	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	numAttsInput = _numAttsInput;
	numAttsOutput = _numAttsOutput;
	keepMe = _keepMe;
	producer = _producer;

}
Project::~Project() {

}

bool Project::GetNext(Record& _record){

	//cout << "Run Project: GETNEXT" << endl;
	int check = producer->GetNext(_record);
	//producer->print(cout);

	//cout << "PROJECT check value: " << check << endl;

	for (int i = 0; i < numAttsOutput; i++) {
		//cout << "check keepme indexes " << keepMe[i] << endl;
	}
	if (check) {
		//fix bottom line, look at keepme
		_record.Project(keepMe, numAttsOutput, numAttsInput);
		return true;
	} else return false;

}
Schema& Project::getSchemaIn() {
	return schemaIn;
}
Schema& Project::getSchemaOut() {
	return schemaOut;
}
int Project::getNumAttsInput() {
	return numAttsInput;
}
int Project::getNumAttsOutput() {
	return numAttsOutput;
}
int* Project::getKeepMe() {
	return keepMe;
}
RelationalOp* Project::getProducer() {
	return producer;
}
ostream& Project::print(ostream& _os) {
	return _os << "PROJECT: SCHEMA IN: " << schemaIn << " SCHEMA OUT: " << schemaOut <<  endl;
}


//-------------------------------------------------------JOIN --------------------------------------------------------------------------------------
Join:: Join(Schema& _schemaLeft, Schema& _schemaRight, Schema& _schemaOut,
	CNF& _predicate, RelationalOp* _left, RelationalOp* _right) {
	schemaLeft = _schemaLeft;
	schemaRight = _schemaRight;
	schemaOut = _schemaOut;
	predicate = _predicate;
	left = _left;
	right = _right;

}
Schema& Join::getLeftSchema() {
	return schemaLeft;
}
Schema& Join::getRightSchema() {
	return schemaRight;
}
Schema& Join::getSchemaOut() {
	return schemaOut;
}
CNF& Join::getPredicate() {
	return predicate;
}
RelationalOp* Join::getLeftRelationalOp() {
	return left;
}
RelationalOp* Join::getRightRelationalOp() {
	return right;
}
Join::~Join() {

}

bool Join::GetNext(Record& _record){


}
ostream& Join::print(ostream& _os) {
	return _os << "JOIN: SCHEMA LEFT: " << schemaLeft << " SCHEMA RIGHT: " << schemaRight << " SCHEMA OUT:" << schemaOut << endl;
}


//-------------------------------------------------------DUPLICATE REMOVAL --------------------------------------------------------------------------------------
DuplicateRemoval::DuplicateRemoval(Schema& _schema, RelationalOp* _producer) {
	schema = _schema;
	producer = _producer;
}
DuplicateRemoval::~DuplicateRemoval() {

}

bool DuplicateRemoval::GetNext(Record& _record){
	//cout << "Run Duplicate Removal: GETNEXT" << endl;
	while(producer->GetNext(_record)) {
		stringstream ss;
		_record.print(ss, schema);
		/*unordered_set<string>::iterator it = hashTable.find(ss.str());
		if (it == hashTable.end()) {
			hashTable.insert(ss.str());
			return true;
		}*/
	}
	return false;

}
Schema& DuplicateRemoval::getSchema() {
	return schema;
}
RelationalOp* DuplicateRemoval::getProducer() {
	return producer;
}
ostream& DuplicateRemoval::print(ostream& _os) {
	return _os << "DISTINCT: SCHEMA: " << schema << endl;
}


//-------------------------------------------------------SUM --------------------------------------------------------------------------------------
Sum::Sum(Schema& _schemaIn, Schema& _schemaOut, Function& _compute,
	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	compute = _compute;
	producer = _producer;
}
Sum::~Sum() {

}

bool Sum::GetNext(Record& _record){

	cout << "Run Sum: GETNEXT" << endl;
	if (recordSent) return false;
	int intSum = 0;
	double doubleSum = 0;
	while(producer->GetNext(_record)) {
		int intResult = 0;
		double doubleResult = 0;
		Type t = compute.Apply(_record, intResult, doubleResult);
		if (t == Integer)	intSum+= intResult;
		if (t == Float)		doubleSum+= doubleResult;
	}

	double val = doubleSum + (double)intSum;
	char* recSpace = new char[PAGE_SIZE];
  int currentPosInRec = sizeof (int) * (2);
	((int *) recSpace)[1] = currentPosInRec;
	*((double *) &(recSpace[currentPosInRec])) = val;
	currentPosInRec += sizeof (double);
	((int *) recSpace)[0] = currentPosInRec;

	Record sumRecord;
	sumRecord.CopyBits( recSpace, currentPosInRec );
	delete [] recSpace;
	_record = sumRecord;
	recordSent = 1;
	return true;

}
Schema& Sum::getSchemaIn() {
	return schemaIn;
}
Schema& Sum::getSchemaOut() {
	return schemaOut;
}
Function& Sum::getCompute() {
	return compute;
}
RelationalOp* Sum::getProducer() {
	return producer;
}
ostream& Sum::print(ostream& _os) {
	return _os << "SUM: SCHEMA IN: " << schemaIn << " SCHEMA OUT: " << schemaOut << endl;
}


//-------------------------------------------------------GROUP BY --------------------------------------------------------------------------------------
GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	groupingAtts = _groupingAtts;
	compute = _compute;
	producer = _producer;
}
GroupBy::~GroupBy() {

}

bool GroupBy::GetNext(Record& _record){

	cout << "Run GroupBy: GETNEXT" << endl;
	_record.Project(groupingAtts.whichAtts, groupingAtts.numAtts, schemaIn.GetNumAtts());

	int i = 0;
	int runningIntSum = 0;
	int runningDoubleSum = 0;
	int iterator = 0;
	int vectorIterator = 1;

	vector<Attribute> attributeStorage;
	vector<string> attributeNames;
	Schema copy = schemaOut;
	attributeStorage = copy.GetAtts();
	for(i = 1; i < copy.GetNumAtts(); i++){
		attributeNames.push_back(attributeStorage[i].name);
	}
	while(producer->GetNext(_record)){

		KeyString name = attributeStorage[vectorIterator].name;
		KeyDouble value;
		int point = ((int*) _record.GetBits())[iterator + 1];
		if(groups.IsThere(name)){
			if(attributeStorage[vectorIterator].type == Integer){
				int *currentInt = (int*) &(_record.GetBits()[point]);
				runningIntSum += *currentInt;
				value = groups.Find(name);
				groups.Remove(name, name, value);
				value = runningIntSum;
				groups.Insert(name, value);
			}
			else if (attributeStorage[vectorIterator].type == Float){
				double *currentDouble = (double*) &(_record.GetBits()[point]);
				runningDoubleSum += *currentDouble;
				value = groups.Find(name);
				groups.Remove(name, name, value);
				value = runningDoubleSum;
				groups.Insert(name, value);
			}
		} else {
			cout << "Not Found" << endl;
			if(attributeStorage[vectorIterator].type == Integer){
				int *currentInt = (int*) &(_record.GetBits()[point]);
				value = *currentInt;
				groups.Insert(name, value);
			}
			else if(attributeStorage[vectorIterator].type == Float){
				double *currentDouble = (double*) &(_record.GetBits()[point]);
				value = *currentDouble;
				groups.Insert(name, value);
			}
		}
		vectorIterator++;
		return true;
	}
		groups.MoveToStart();
		for(int i = 0; i < groups.Length(); i++){
			cout << groups.CurrentData() << endl;
			groups.Advance();
		}
		return false;
	}

Schema& GroupBy::getSchemaIn() {
	return schemaIn;
}
Schema& GroupBy::getSchemaOut() {
	return schemaOut;
}
OrderMaker& GroupBy::getGroupingAtts() {
	return groupingAtts;
}
Function& GroupBy::getCompute() {
	return compute;
}
RelationalOp* GroupBy::getProducer() {
	return producer;
}
ostream& GroupBy::print(ostream& _os) {
	return _os << "GROUP BY: SCHEMA IN: " << schemaIn << " SCHEMA OUT: " << schemaOut << endl;
}


//-------------------------------------------------------WRITE OUT --------------------------------------------------------------------------------------
WriteOut::WriteOut(Schema& _schema, string& _outFile, RelationalOp* _producer) {
	schema = _schema;
	outFile = _outFile;
	producer = _producer;
	outFileStream.open(outFile);
}
WriteOut::~WriteOut() {
	outFileStream.close();
}

bool WriteOut::GetNext(Record& _record){
	//cout << "Run WriteOut: GETNEXT" << endl;
	//producer->print(cout);
	while (producer->GetNext(_record)) {
		//cout << "WRITEOUT" << endl;

		_record.print(outFileStream,schema);
		outFileStream<<endl;

		_record.print(cout, schema);
		cout << endl;
	}

	/*bool writeout = producer->GetNext(_record);
	if (!writeout) {
		outFileStream.close();
		return false;
	}
	_record.print(outFileStream,schema);
	outFileStream<<endl;
	return writeout;*/

	return true;
}
Schema & WriteOut::getSchema() {
	return schema;
}
string& WriteOut::getOutFile() {
	return outFile;
}
RelationalOp* WriteOut::getProducer() {
	return producer;
}
ostream& WriteOut::print(ostream& _os) {
	return _os << "WRITEOUT: FILE: " << outFile << " SCHEMA: " << schema << endl;
}


//---------------------------------------------------------------------------------------------------------------------------------------------
ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	return _os << "QUERY EXECUTION TREE";
}
