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

#ifndef __OpcUaDB_Connection_h__
#define __OpcUaDB_Connection_h__

#include <sql.h>
#include <sqlext.h>
#include <iostream>
#include <stdint.h>

namespace OpcUaDB
{

	class ColDescription
    {
      public:
		typedef std::vector<ColDescription> Vec;

        SQLSMALLINT colNumber_;
        SQLCHAR colName_[80];
        SQLSMALLINT nameLen_;
        SQLSMALLINT dataType_;
        SQLULEN colSize_;
        SQLSMALLINT decimalDigits_;
        SQLSMALLINT nullable_;
    };

	class ResultSet
	{
	  public:
		typedef std::vector<std::vector<std::string > > TableData;
		ResultSet(void);
		~ResultSet(void);

		uint32_t columnNumber(void);
		uint32_t rowNumber(void);
		bool out(std::ostream& os);

		ColDescription::Vec colDescriptionVec_;
		TableData tableData_;
	};

	class Connection
	{
	  public:
		Connection(void);
		~Connection(void);

		bool init(void);
		bool cleanup(void);

		bool connect(void);
		bool disconnect(void);
		bool execDirect(const std::string& statement);

	  private:
		bool getColData(uint32_t col, std::string& data);
		bool getResultSet(ResultSet& resultSet);
		bool describe(ColDescription& colDescription);
		bool describe(ColDescription::Vec& colDescriptionVec);
		void logError(const std::string& message, uint32_t handle = 0);

	    SQLHENV env_;
	    SQLHDBC dbc_;
	    HSTMT stmt_;
	};

}

#endif
