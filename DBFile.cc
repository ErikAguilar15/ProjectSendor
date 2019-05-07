#include <string>

#include "Config.h"
#include "Record.h"
#include "Schema.h"
#include "File.h"
#include "DBFile.h"

using namespace std;


DBFile::DBFile () : fileName("") {
}

DBFile::~DBFile () {
}

DBFile::DBFile(const DBFile& _copyMe) :
	file(_copyMe.file),	fileName(_copyMe.fileName) {}

DBFile& DBFile::operator=(const DBFile& _copyMe) {
	// handle self-assignment first
	if (this == &_copyMe) return *this;

	file = _copyMe.file;
	fileName = _copyMe.fileName;

	return *this;
}

int DBFile::Create (char* f_path, FileType f_type) {
	ftype = f_type;
	fileName = f_path;
	return file.Open(0,f_path);
}

int DBFile::Open (char* f_path) {
	fileName = f_path;
	cout << fileName << endl;
	return file.Open(1, f_path);
}

void DBFile::Load (Schema& schema, char* textFile) {
	MoveFirst();
	FILE * pFile;
	pFile = fopen(textFile,"r");
	while (true) {
		Record record;
		if (record.ExtractNextRecord (schema, *pFile)){
			//record.print(cout, schema);
			//cout << endl;
			AppendRecord(record);
		} else break;
	}
		file.AddPage(page, currentPage++);
		page.EmptyItOut();
		fclose(pFile);
}

int DBFile::Close () {
	int test = file.Close();
	if(test == -1){
		cout << "Failed to close DBFile" << endl;
	}
	return test;
}

void DBFile::MoveFirst () {
	currentPage = 0;
	file.GetPage(page, currentPage);
}

void DBFile::AppendRecord (Record& rec) {
	MoveFirst();
	if (!page.Append(rec)){
		file.AddPage(page, currentPage++);
		//cout << file.GetLength() << endl;
		currentPage++;
		page.EmptyItOut();
		page.Append(rec);
	}
}

int DBFile::GetNext (Record& rec) {
	//MoveFirst();
	//cout << "running getnext on page " << currentPage << endl;
	int check = page.GetFirst(rec);
	if(check){
		//success
		return 0;
	}else{
		//Check if you've reached the end of the file
		if(currentPage == file.GetLength()){
			return -1;
		}else{
			//Move onto the next page
			file.GetPage(page,currentPage++);
			//Get the first record of the page
			check = page.GetFirst(rec);
			return 0;
		}
	}
}
