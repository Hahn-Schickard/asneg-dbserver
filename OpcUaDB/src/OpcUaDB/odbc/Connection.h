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

namespace OpcUaDB
{

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
		void logError(const std::string& message);

	    SQLHENV env_;
	    SQLHDBC dbc_;
	    HSTMT hstmt_;
	};

}

#endif
