/*
   Copyright 2017 Kai Huebl (kai@huebl-sgh.de)

   Lizenziert gemäß Apache Licence Version 2.0 (die „Lizenz“); Nutzung dieser
   Datei nur in Übereinstimmung mit der Lizenz erlaubt.
   Eine Kopie der Lizenz erhalten Sie auf http://www.apache.org/licenses/LICENSE-2.0.

   Sofern nicht gemäß geltendem Recht vorgeschrieben oder schriftlich vereinbart,
   erfolgt die Bereitstellung der im Rahmen der Lizenz verbreiteten Software OHNE
   GEWÄHR ODER VORBEHALTE – ganz gleich, ob ausdrücklich oder stillschweigend.

   Informationen über die jeweiligen Bedingungen für Genehmigungen und Einschränkungen
   im Rahmen der Lizenz finden Sie in der Lizenz.

   Autor: Kai Huebl (kai@huebl-sgh.de)
 */

#include "OpcUaStackCore/Base/Log.h"

#include "OpcUaDB/odbc/Connection.h"

using namespace OpcUaStackCore;

namespace OpcUaDB
{

	Connection::Connection(void)
	: env_()
	, dbc_()
	{
	}

	Connection::~Connection(void)
	{
	}

	bool
	Connection::connect(void)
	{
		SQLRETURN ret;

		// allocate environment
		ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env_);
		if ((ret == SQL_SUCCESS) || (ret == SQL_SUCCESS_WITH_INFO)) {
			logError("connect - SQLAllocHandle error");
			return false;
		}

		// ODBC: Version: Set
		ret = SQLSetEnvAttr(env_, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
		if ((ret == SQL_SUCCESS) || (ret == SQL_SUCCESS_WITH_INFO)) {
			logError("connect - SQLSetEnvAttr error");
			SQLFreeHandle(SQL_HANDLE_ENV, env_);
			return false;
		}

		// DBC: Allocate
		ret = SQLAllocHandle(SQL_HANDLE_DBC, env_, &dbc_);
		if ((ret == SQL_SUCCESS) || (ret == SQL_SUCCESS_WITH_INFO)) {
			logError("connect - SQLAllocHandle error");
			SQLFreeHandle(SQL_HANDLE_ENV, env_);
			return false;
		}

	    // DBC: Connect
		ret = SQLConnect(
		    dbc_,
		    (SQLCHAR*) "MySQL_Test", SQL_NTS,
		    (SQLCHAR*) "scott", SQL_NTS,
		    (SQLCHAR*) "tiger", SQL_NTS
		);
		if ((ret == SQL_SUCCESS) || (ret == SQL_SUCCESS_WITH_INFO)) {
			logError("connect - SQLConnect error");
			SQLFreeHandle(SQL_HANDLE_ENV, env_);
			SQLFreeHandle(SQL_HANDLE_DBC, dbc_);
			return false;
		}

		return true;
	}

	bool
	Connection::disconnect(void)
	{
		// disconnect from database
	    SQLDisconnect(dbc_);
	    SQLFreeHandle(SQL_HANDLE_DBC, dbc_);
	    SQLFreeHandle(SQL_HANDLE_ENV, env_);
		return true;
	}

	void
	Connection::logError(const std::string& message)
	{
		if (!hstmt_) {
			Log(Error, message);
			return;
		}

		unsigned char szData[100];
		unsigned char sqlState[10];
		unsigned char msg[SQL_MAX_MESSAGE_LENGTH + 1];
		SQLINTEGER nErr;
		SQLSMALLINT cbmsg;

		while (SQLError(0, 0, hstmt_, sqlState, &nErr, msg, sizeof(msg), &cbmsg) == SQL_SUCCESS) {
		    Log(Error, message)
		        .parameter("SQLState", sqlState)
		        .parameter("NativeError", nErr)
		        .parameter("Msg", msg);

		}
	}
}


#if 0
// Connecting_with_SQLConnect.cpp
// compile with: user32.lib odbc32.lib
#include <windows.h>
#include <sqlext.h>
#include <mbstring.h>
#include <stdio.h>

#define MAX_DATA 100
#define MYSQLSUCCESS(rc) ((rc == SQL_SUCCESS) || (rc == SQL_SUCCESS_WITH_INFO) )

class direxec {
   RETCODE rc; // ODBC return code
   HENV henv; // Environment
   HDBC hdbc; // Connection handle
   HSTMT hstmt; // Statement handle

   unsigned char szData[MAX_DATA]; // Returned data storage
   SDWORD cbData; // Output length of data
   unsigned char chr_ds_name[SQL_MAX_DSN_LENGTH]; // Data source name

public:
   direxec(); // Constructor
   void sqlconn(); // Allocate env, stat, and conn
   void sqlexec(unsigned char *); // Execute SQL statement
   void sqldisconn(); // Free pointers to env, stat, conn, and disconnect
   void error_out(); // Displays errors
};

// Constructor initializes the string chr_ds_name with the data source name.
// "Northwind" is an ODBC data source (odbcad32.exe) name whose default is the Northwind database
direxec::direxec() {
   _mbscpy_s(chr_ds_name, SQL_MAX_DSN_LENGTH, (const unsigned char *)"Northwind");
}

// Allocate environment handle and connection handle, connect to data source, and allocate statement handle.
void direxec::sqlconn() {
   SQLAllocEnv(&henv);
   SQLAllocConnect(henv, &hdbc);
   rc = SQLConnect(hdbc, chr_ds_name, SQL_NTS, NULL, 0, NULL, 0);

   // Deallocate handles, display error message, and exit.
   if (!MYSQLSUCCESS(rc)) {
      SQLFreeConnect(henv);
      SQLFreeEnv(henv);
      SQLFreeConnect(hdbc);
      if (hstmt)
         error_out();
      exit(-1);
   }

   rc = SQLAllocStmt(hdbc, &hstmt);
}

// Execute SQL command with SQLExecDirect() ODBC API.
void direxec::sqlexec(unsigned char * cmdstr) {
   rc = SQLExecDirect(hstmt, cmdstr, SQL_NTS);
   if (!MYSQLSUCCESS(rc)) {  //Error
      error_out();
      // Deallocate handles and disconnect.
      SQLFreeStmt(hstmt,SQL_DROP);
      SQLDisconnect(hdbc);
      SQLFreeConnect(hdbc);
      SQLFreeEnv(henv);
      exit(-1);
   }
   else {
      for ( rc = SQLFetch(hstmt) ; rc == SQL_SUCCESS ; rc=SQLFetch(hstmt) ) {
         SQLGetData(hstmt, 1, SQL_C_CHAR, szData, sizeof(szData), &cbData);
         // In this example, the data is sent to the console; SQLBindCol() could be called to bind
         // individual rows of data and assign for a rowset.
         printf("%s\n", (const char *)szData);
      }
   }
}

// Free the statement handle, disconnect, free the connection handle, and free the environment handle.
void direxec::sqldisconn() {
   SQLFreeStmt(hstmt,SQL_DROP);
   SQLDisconnect(hdbc);
   SQLFreeConnect(hdbc);
   SQLFreeEnv(henv);
}

// Display error message in a message box that has an OK button.
void direxec::error_out() {
   unsigned char szSQLSTATE[10];
   SDWORD nErr;
   unsigned char msg[SQL_MAX_MESSAGE_LENGTH + 1];
   SWORD cbmsg;

   while (SQLError(0, 0, hstmt, szSQLSTATE, &nErr, msg, sizeof(msg), &cbmsg) == SQL_SUCCESS) {
      sprintf_s((char *)szData, sizeof(szData), "Error:\nSQLSTATE=%s, Native error=%ld, msg='%s'", szSQLSTATE, nErr, msg);
      MessageBox(NULL, (const char *)szData, "ODBC Error", MB_OK);
   }
}

int main () {
   direxec x;   // Declare an instance of the direxec object.
   x.sqlconn();   // Allocate handles, and connect.
   x.sqlexec((UCHAR FAR *)"SELECT FirstName, LastName FROM employees");   // Execute SQL command
   x.sqldisconn();   // Free handles and disconnect
}

#endif

