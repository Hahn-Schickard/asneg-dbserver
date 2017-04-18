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
	: env_(nullptr)
	, dbc_(nullptr)
	, stmt_(nullptr)
	{
	}

	Connection::~Connection(void)
	{
		cleanup();
	}

	bool
	Connection::init(void)
	{
		SQLRETURN ret;

		// allocate environment
		ret = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env_);
		if ((ret != SQL_SUCCESS) && (ret != SQL_SUCCESS_WITH_INFO)) {
			logError("init - SQLAllocHandle error", SQL_HANDLE_ENV);
			cleanup();
			return false;
		}

		// ODBC: Version: Set
		ret = SQLSetEnvAttr(env_, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);
		if ((ret != SQL_SUCCESS) && (ret != SQL_SUCCESS_WITH_INFO)) {
			logError("init - SQLSetEnvAttr error", SQL_HANDLE_ENV);
			cleanup();
			return false;
		}

		// DBC: Allocate
		ret = SQLAllocHandle(SQL_HANDLE_DBC, env_, &dbc_);
		if ((ret != SQL_SUCCESS) && (ret != SQL_SUCCESS_WITH_INFO)) {
			logError("init - SQLAllocHandle error", SQL_HANDLE_DBC);
			cleanup();
			return false;
		}

		return true;
	}

	bool
	Connection::cleanup(void)
	{
		if (dbc_ != nullptr) {
			SQLDisconnect(dbc_);
		    SQLFreeHandle(SQL_HANDLE_DBC, dbc_);
		    dbc_ = nullptr;
		}
		if (env_ != nullptr) {
			SQLFreeHandle(SQL_HANDLE_ENV, env_);
			env_ = nullptr;
		}
		return true;
	}

	bool
	Connection::connect(void)
	{
		SQLRETURN ret;

		// init
		if (dbc_ == nullptr) {
			if (!init()) {
				cleanup();
				return false;
			}
		}

	    // DBC: Connect
		ret = SQLConnect(
		    dbc_,
		    (SQLCHAR*)"PostgreSQL TestData", SQL_NTS,
		    NULL, 0,
		    NULL, 0
		);
		if ((ret != SQL_SUCCESS) && (ret != SQL_SUCCESS_WITH_INFO)) {
			logError("connect - SQLConnect error", SQL_HANDLE_DBC);
			cleanup();
			return false;
		}

		return true;
	}

	bool
	Connection::disconnect(void)
	{
		// disconnect from database
	    SQLDisconnect(dbc_);
		return true;
	}

	bool
	Connection::execDirect(const std::string& statement)
	{
		SQLRETURN ret;

		// check
		if (dbc_ == nullptr) {
			return false;
		}

		// allocate sql statement handle
		ret = SQLAllocHandle(SQL_HANDLE_STMT, dbc_, &stmt_);
		if ((ret != SQL_SUCCESS) && (ret != SQL_SUCCESS_WITH_INFO)) {
			logError("execDirect - SQLAllocHandle error", SQL_HANDLE_STMT);
			cleanup();
			return false;
		}

		// execute sql satement
		ret = SQLExecDirect(stmt_, (SQLCHAR*)statement.c_str(), SQL_NTS);
		if ((ret != SQL_SUCCESS) && (ret != SQL_SUCCESS_WITH_INFO)) {
			logError("execDirect - SQLExecDirect error", SQL_HANDLE_STMT);
			SQLFreeHandle(SQL_HANDLE_STMT, stmt_);
			cleanup();
			return false;
		}

		// get description for all columns in the result set
		ColDescription::Vec colDescriptionVec;
		if (!describe(colDescriptionVec)) {
			SQLFreeHandle(SQL_HANDLE_STMT, stmt_);
			cleanup();
			return false;
		}

		ColDescription::Vec::iterator it;
		for (it = colDescriptionVec.begin(); it != colDescriptionVec.end(); it++) {
			std::cout << "Col: " << it->colName_ << std::endl;
		}

		// free the sql statement handle
		SQLFreeHandle(SQL_HANDLE_STMT, stmt_);
		stmt_ = nullptr;

		return true;
	}

	bool
	Connection::describe(ColDescription& colDescription)
	{
		SQLRETURN ret;

		// get the description for one column in the resultset.
		ret = SQLDescribeCol(
			stmt_,
			colDescription.colNumber_,
			colDescription.colName_,
			sizeof(colDescription.colName_),
			&colDescription.nameLen_,
			&colDescription.dataType_,
			&colDescription.colSize_,
			&colDescription.decimalDigits_,
			&colDescription.nullable_
		);
		if (ret != SQL_SUCCESS) {
			return false;
		}

		return true;
	}

	bool
	Connection::describe(ColDescription::Vec& colDescriptionVec)
	{
		ColDescription colDescription;
		colDescription.colNumber_ = 1;
		colDescriptionVec.clear();

		// get the description for all columns in the resultset
		while (describe(colDescription)) {
			colDescriptionVec.push_back(colDescription);
			colDescription.colNumber_++;
		}

		return true;
	}

	void
	Connection::logError(const std::string& message, uint32_t handle)
	{
		unsigned char szData[100];
		unsigned char sqlState[10];
		unsigned char msg[SQL_MAX_MESSAGE_LENGTH + 1];
		SQLINTEGER nErr;
		SQLSMALLINT cbmsg;

		Log(Error, message);

		switch (handle)
		{
			case SQL_HANDLE_ENV:
			{
				if (env_ == nullptr) return;
				while (SQLError(env_, 0, 0, sqlState, &nErr, msg, sizeof(msg), &cbmsg) == SQL_SUCCESS) {
					Log(Error, "environment error")
					    .parameter("SQLState", sqlState)
					    .parameter("NativeError", nErr)
					    .parameter("Msg", msg);
				}
				break;
			}

			case SQL_HANDLE_DBC:
			{
				if (dbc_ == nullptr) return;
				while (SQLError(0, dbc_, 0, sqlState, &nErr, msg, sizeof(msg), &cbmsg) == SQL_SUCCESS) {
					Log(Error, "database connection error")
					    .parameter("SQLState", sqlState)
					    .parameter("NativeError", nErr)
					    .parameter("Msg", msg);
				}
				break;
			}

			case SQL_HANDLE_STMT:
			{
				if (stmt_ == nullptr) return;
				while (SQLError(0, 0, stmt_, sqlState, &nErr, msg, sizeof(msg), &cbmsg) == SQL_SUCCESS) {
					Log(Error, "statement error")
					    .parameter("SQLState", sqlState)
					    .parameter("NativeError", nErr)
					    .parameter("Msg", msg);
				}
				break;
			}

			default:
			{
				break;
			}
		}
	}
}


#if 0
// You can delete this line if you
002
	// are not using Microsoft compiler.
003
	#include "stdafx.h"
004
	////////////////////////////////////////
005
	#include <windows.h>
006
	#include <sql.h>
007
	#include<sqltypes.h>
008
	#include<sqlext.h>
009
	#include <string>
010
	#include <vector>
011
	#include <iostream>
012
	using namespace std;
013
	// You can delete this line if you
014
	// are not using Microsoft VC++ 2008/2010 compiler.
015
	#pragma warning(disable: 4996)
016
	////////////////////////////////////////
017

018

019
	// Define The ODBC_Class Class
020
	class ODBC_Class
021
	{
022
	    struct ColDescription
023
	    {
024
	        SQLSMALLINT colNumber;
025
	        SQLCHAR colName[80];
026
	        SQLSMALLINT nameLen;
027
	        SQLSMALLINT dataType;
028
	        SQLULEN colSize;
029
	        SQLSMALLINT decimalDigits;
030
	        SQLSMALLINT nullable;
031
	    };
032
	// Attributes
033
	public:
034
	    SQLHANDLE EnvHandle;
035
	    SQLHANDLE ConHandle;
036
	    SQLHANDLE StmtHandle;
037
	    SQLRETURN rc;
038
	    vector<ColDescription> cols;
039
	    vector< vector<string> > colData;
040
	// Operations
041
	public:
042
	    ODBC_Class(); // Constructor
043
	    ~ODBC_Class(); // Destructor
044
	    SQLRETURN GetResultset();
045
	    void DescribeColumns();
046
	private:
047
	    _inline SQLRETURN Describe(ColDescription& c);
048
	    SQLRETURN GetColData(int colnum, string& str);
049
	};
050

051
	// Define The Class Constructor
052
	ODBC_Class::ODBC_Class()
053
	{
054
	    // Initialize The Return Code Variable
055
	    rc = SQL_SUCCESS;
056
	    // Allocate An Environment Handle
057
	    rc = SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &EnvHandle);
058
	    // Set The ODBC Application Version To 3.x
059
	    if (rc == SQL_SUCCESS)
060
	        rc = SQLSetEnvAttr(EnvHandle, SQL_ATTR_ODBC_VERSION,
061
	            (SQLPOINTER) SQL_OV_ODBC3, SQL_IS_UINTEGER);
062
	    // Allocate A Connection Handle
063
	    if (rc == SQL_SUCCESS)
064
	        rc = SQLAllocHandle(SQL_HANDLE_DBC, EnvHandle, &ConHandle);
065
	}
066

067
	// Define The Class Destructor
068
	ODBC_Class::~ODBC_Class()
069
	{
070
	    // Free The Connection Handle
071
	    if (ConHandle != NULL)
072
	        SQLFreeHandle(SQL_HANDLE_DBC, ConHandle);
073
	    // Free The Environment Handle
074
	    if (EnvHandle != NULL)
075
	        SQLFreeHandle(SQL_HANDLE_ENV, EnvHandle);
076
	}
077

078
	// Get the data for one column and return the info in the form
079
	// of a std::string.  The ODBC driver will make all necessary
080
	// data conversions from whatever type the data is in the database
081
	// to SQL_CHAR.  You could make this function more comples by
082
	// getting the return type as it appears in the database then constructing
083
	// a VARIANT object to hold the data.
084
	SQLRETURN ODBC_Class::GetColData(int colnum, string& str)
085
	{
086
	    SQLCHAR buf[255] = {0};
087
	    if( (rc = SQLGetData(StmtHandle, colnum, SQL_CHAR, buf, sizeof(buf), NULL)) == SQL_SUCCESS)
088
	        str = reinterpret_cast<char*>(buf);
089
	    return rc;
090
	}
091

092
	//
093
	// Define The ShowResults() Member Function
094
	SQLRETURN ODBC_Class::GetResultset()
095
	{
096
	    // Get all column description
097
	    DescribeColumns();
098
	    // erase anything that's in the colData vector
099
	    colData.clear();
100
	    // fetch a row from the resultset
101
	    while( SQLFetch(StmtHandle) == SQL_SUCCESS)
102
	    {
103
	        // vector of strings to hold the column data
104
	        vector<string> col;
105
	        string data;
106
	        // column counter
107
	        int i = 1;
108
	        // get the data for each column and add it to
109
	        // the col vector
110
	        while( GetColData(i, data) == SQL_SUCCESS)
111
	        {
112
	            col.push_back(data);
113
	            ++i; // increment the column number
114
	        }
115
	        // add column data to the colData vector
116
	        colData.push_back(col);
117
	    }
118
	    return SQL_SUCCESS;
119
	}
120

121
	// Get the description for one column in the resultset.
122
	// This was made a seprate function to simplify the coding
123
	SQLRETURN  ODBC_Class::Describe(ColDescription& c)
124
	{
125
	    return SQLDescribeCol(StmtHandle,c.colNumber,
126
	        c.colName, sizeof(c.colName), &c.nameLen,
127
	        &c.dataType, &c.colSize, &c.decimalDigits, &c.nullable);
128
	}
129

130
	// Get the description for all the columns in the resultset.
131
	void ODBC_Class::DescribeColumns()
132
	{
133
	    ColDescription c;
134
	    c.colNumber = 1;
135
	    cols.clear();
136
	    while( Describe(c) == SQL_SUCCESS)
137
	    {
138
	        cols.push_back(c);
139
	        ++c.colNumber;
140
	    }
141

142
	}
143

144

145
	/*-----------------------------------------------------------------*/
146
	/* The Main Function */
147
	/*-----------------------------------------------------------------*/
148
	int main()
149
	{
150
	    // Declare The Local Memory Variables
151
	    SQLRETURN rc = SQL_SUCCESS;
152
	    SQLCHAR DBName[] = "Northwind";
153
	    SQLCHAR SQLStmt[255] = {0};
154
	    // Create An Instance Of The ODBC_Class Class
155
	    ODBC_Class Example;
156
	    // Connect To The Northwind Sample Database
157
	    if (Example.ConHandle != NULL)
158
	    {
159
	        rc = SQLConnect(Example.ConHandle, DBName, SQL_NTS,
160
	            (SQLCHAR *) "", SQL_NTS, (SQLCHAR *) "", SQL_NTS);
161
	        // Allocate An SQL Statement Handle
162
	        rc = SQLAllocHandle(SQL_HANDLE_STMT, Example.ConHandle,
163
	                &Example.StmtHandle);
164
	        if (rc == SQL_SUCCESS)
165
	        {
166
	            // Define A SELECT SQL Statement
167
	            strcpy((char *) SQLStmt, "SELECT * FROM categories");
168
	            // Prepare And Execute The SQL Statement
169
	            rc = SQLExecDirect(Example.StmtHandle, SQLStmt, SQL_NTS);
170
	            // Display The Results Of The SQL Query
171
	            if (rc == SQL_SUCCESS)
172
	            {
173
	                Example.GetResultset();
174
	                // At this point you would want to do something
175
	                // with the resultset, such as display it.
176
	            }
177
	        }
178
	        // Free The SQL Statement Handle
179
	        if (Example.StmtHandle != NULL)
180
	            SQLFreeHandle(SQL_HANDLE_STMT, Example.StmtHandle);
181
	        // Disconnect From The Northwind Sample Database
182
	        rc = SQLDisconnect(Example.ConHandle);
183
	    }
184
	    // Return To The Operating System
185
	    return 0;
186
	}


#endif

