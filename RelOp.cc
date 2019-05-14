#include <iostream>
#include <sstream>
#include "RelOp.h"
#include "EfficientMap.h"
#include "TwoWayList.h"
#include "Swapify.h"
#include "Comparison.h"

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
		//_record.print(cout, schema);
		//cout << endl;
		return true;
	} else {
		//cout << "nothing" << endl;
		return false;
	}
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
	return _os << "SCAN: SCHEMA IN/OUT: " << schema << endl << endl;
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
	return _os << "SELECT: SCHEMA IN/OUT: " << schema << endl << endl;
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

	cout << "Run Project: GETNEXT" << endl;
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
	return _os << "PROJECT: SCHEMA IN: " << schemaIn << endl << " SCHEMA OUT: " << schemaOut << endl << endl;
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
	running = true;

//create a comparison based off what predicate we have (==)

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
//cout << "OUTPUT SOMETHING" << endl;
			//Need to make an if statement to determine which to use
			NestedLoop(_record);

			//Hash(_record);
}

	//----------------Nested-Loop Join--------------//
bool Join::NestedLoop(Record& _record){
	Record rec;
	//Record currentRecord;
	//bool running = true;//TODO: put this into the constructor

	//To check number of tuples

//Build phase

	//Condition to make sure we build before beginning probe phase
	if(running){
		//get left node because it should be the smaller value
		while(left->GetNext(rec)){
			//cout << schemaLeft;
			//rec.print(cout, schema);
			//cout << endl;
			TwoWayJoins.Insert(rec);
		}
		running = false;
		TwoWayJoins.MoveToFinish();
	}
	//cout << "ANYTHING" << endl;

	cout << TwoWayJoins.Length() << endl;
	//cout << "something22";

//Probe Phase

			//if(left is at the end, left.movetostart)
			//If statement to check if reaches end of the left (boolean tracker)
			//Store previous tuple somewhere in the class
			//cout << schemaRight;
			//Run until we have no more records to join
			while(true){

				if(TwoWayJoins.AtEnd()){
					//cout << "345" << endl;
					//Use right node for joins and start at the beginning of the list
					if(!right->GetNext(currentRecord)){
						cout << "WHAT IS THIS " + ii << endl;
						return false;
					}
					ii++;
					TwoWayJoins.MoveToStart();
				}
				//cout << "PLEASE WORK" << endl;
				//set to current to find matching relation
				//currRec = TwoWayJoins.Current();
				//Run from comparison.cc to compare records (true if holds, false otherwise)
				Record *rec1 = &TwoWayJoins.Current();
				//cout << "123" << endl;
				//currentRecord.print(cout, schemaRight);
				//cout << endl;
				//cout << schemaRight << endl;

				//rec1.print(cout, schemaLeft);
				//cout << endl;

				if(predicate.Run(currentRecord, *rec1)){
					//append the records for join
					//cout << "OUTPUT SOMETHING ELSE HERE" << endl;
					_record.AppendRecords(currentRecord, *rec1, schemaRight.GetNumAtts(), schemaLeft.GetNumAtts());
					//cout << schemaOut;
					_record.print(cout, schemaOut);
					cout << endl;
					//cout << "SOMETHING PLEASE";
					//cout << endl;
					TwoWayJoins.Advance();
					return true;
				}
				//Continue moving forward through the list
				TwoWayJoins.Advance();
				//cout << "Are we even advancing?????" << endl;
			}
			cout << "IS IT FINISHED JOINING" << endl;
		}
		//return false; move to beginning for the if statement

	//----------------------Hash Join------------------------//
bool Join::Hash(Record& _record){
//Build phase
Record rec;
Record currRec;
KeyString data;
bool running = true;

//Condition to make sure we build before beginning probe phase
if(running){
	//OrderMaker(left, right);
	//get left node because it should be the smaller value
	while(left->GetNext(rec)){
		//multiMap.Insert(rec, data);
	}
	running = false;
}

}





ostream& Join::print(ostream& _os) {
	return _os << "JOIN: SCHEMA LEFT: " << schemaLeft << endl << " SCHEMA RIGHT: " << schemaRight << endl << " SCHEMA OUT:" << schemaOut << endl << endl;
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
		_record.GetBits();
		//Boolean to see if we found a duplicate or not
		bool foundDuplicate = removalSet.find(ss.str()) != removalSet.end();
		//If we found a duplicate, add it to removal set
		if(!foundDuplicate) {
			removalSet.insert(ss.str());
			return true;
		}
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
	return _os << "DISTINCT: SCHEMA: " << schema << endl << endl;
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
	//Keep running sums of each type
	int intSum = 0;
	double doubleSum = 0;
	while(producer->GetNext(_record)) {
		int intResult = 0;
		double doubleResult = 0;
		Type t = compute.Apply(_record, intResult, doubleResult);
		//Check which of the two possible types and add appropriately
		if (t == Integer)	intSum+= intResult;
		if (t == Float)		doubleSum+= doubleResult;
	}
	//Positioning and type conversions
	double val = doubleSum + (double)intSum;
	char* recSpace = new char[PAGE_SIZE];
  int currentPosInRec = sizeof (int) * (2);
	((int *) recSpace)[1] = currentPosInRec;
	*((double *) &(recSpace[currentPosInRec])) = val;
	currentPosInRec += sizeof (double);
	((int *) recSpace)[0] = currentPosInRec;

	//copy actual sum into new variable to otput
	Record sumRecord;
	sumRecord.CopyBits(recSpace, currentPosInRec);
	//Free memory
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
	return _os << "SUM: SCHEMA IN: " << schemaIn << endl << " SCHEMA OUT: " << schemaOut << endl << endl;
}


//-------------------------------------------------------GROUP BY --------------------------------------------------------------------------------------
GroupBy::GroupBy(Schema& _schemaIn, Schema& _schemaOut, OrderMaker& _groupingAtts,
	Function& _compute,	RelationalOp* _producer) {
	schemaIn = _schemaIn;
	schemaOut = _schemaOut;
	groupingAtts.Swap(_groupingAtts);
	//groupingAtts = _groupingAtts; //THIS DOESNT WORK
	//cout << "creating groupby: " << groupingAtts << endl;
	compute = _compute;
	producer = _producer;

	counter = 0;
	phase = 0;
}
GroupBy::~GroupBy() {

}

bool GroupBy::GetNext(Record& _record){

	//split into two phases
	//out << "Run GroupBy: GETNEXT" << endl;
	//_record.Project(groupingAtts.whichAtts, groupingAtts.numAtts, schemaIn.GetNumAtts());

	//check for aggregates
	bool check = compute.HasOps();

	//Build map
	if (phase == 0) {

		Record rec;
		//cout << "groupby producer : " << producer->print(cout) << endl;
		while (producer->GetNext(rec)) {

			int i = 0;
			//rec.print(cout, schemaOut);
			//cout << endl;

			double result = 0;
			Schema copy = schemaOut;
			//attributeStorage = copy.GetAtts();

			if (check) {

				int runningIntSum = 0;
				double runningDoubleSum = 0;
				compute.Apply(rec, runningIntSum, runningDoubleSum);
				result = runningDoubleSum + (double)runningIntSum;

				//cout << "hello" << endl;
				vector<int> keep;
				for(i = 1; i < copy.GetNumAtts(); i++){
					keep.push_back(i);
				}

				copy.ProjectX(keep);

			}
			else {
				//cout << "here" << endl;
				vector<int> keep;
				for(i = 1; i < copy.GetNumAtts(); i++){
					keep.push_back(i);
				}

				copy.ProjectX(keep);
			}

			//cout << "check " << groupingAtts.numAtts << endl;
			rec.Project(&groupingAtts.whichAtts[0], groupingAtts.numAtts, schemaIn.GetNumAtts());
			//cout << "check1" << endl;
			KeyString name = rec.makeKey(copy);
			KeyString name2 = name;
			//cout << name << endl;

			//cout << "clout" << endl;
			if (!groups.IsThere(name)) {
				KeyDouble value = result;
				groups.Insert(name,value);
				groupsRecs.Insert(name2, rec); //name is empty? why?
			} else {
				KeyDouble value;
				value = groups.Find(name);
				groups.Remove(name, name, value);
				value = value + result;
				groups.Insert(name, value);
			}

		}

		groups.MoveToStart();
		groupsRecs.MoveToStart();
		//groupsRecs.Retreat();
		phase = 1;

	}

	//cout << "phase 2" << endl;
	//Iterate group and keep running sum
	if (phase == 1) {

		if (!groups.AtEnd()) {

			//cout << groups.CurrentKey() << endl;
			//cout << groups.CurrentData() << endl;
			Record rec;

			if (check) {

				//cout << "2" << endl;
				Record recSum;
				char* recBits[1];
				int recSize;
				bool recType = compute.GetType();

				if(recType == Float) {
					*((double *) recBits) = groups.CurrentData();
					recSize = sizeof(double);
				} else { // resType == Integer
					*((int *) recBits) = (int)groups.CurrentData();
					recSize = sizeof(int);
				}

				char* recComplete = new char[sizeof(int) + sizeof(int) + recSize];
				((int*) recComplete)[0] = 2*sizeof(int) + recSize;
				((int*) recComplete)[1] = 2*sizeof(int);
				memcpy(recComplete+2*sizeof(int), recBits, recSize);

				//cout << "2.2" << endl;
				recSum.Consume(recComplete);

				//cout << "2.3" << endl;
				rec.AppendRecords(recSum, groupsRecs.CurrentData(), 1, schemaOut.GetNumAtts() - 1);

				rec.print(cout, schemaOut);
				cout << endl;
				//groupsRecs.CurrentData().Nullify();
			}
			else {
				rec.Swap(groupsRecs.CurrentData());
				//cout << "3" << endl;
			}

			_record = rec;
			//_record.print(cout, schemaOut);
			groups.Advance();
			groupsRecs.Advance();

			return true; //FIX THE RETURN

		}
		else {

			return false;

		}

	}

	/*
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
	*/

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
	cout << "Run WriteOut: GETNEXT" << endl;
	//producer->print(cout);
	while (producer->GetNext(_record)) {
		//cout << "WRITEOUT" << endl;

		_record.print(outFileStream,schema);
		outFileStream<<endl;

		//_record.print(cout, schema);
		//cout << endl;
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
	return _os << "WRITEOUT: FILE: " << outFile << " SCHEMA: " << schema << endl <<  endl;
}


//---------------------------------------------------------------------------------------------------------------------------------------------
ostream& operator<<(ostream& _os, QueryExecutionTree& _op) {
	return _os << "QUERY EXECUTION TREE";
}
