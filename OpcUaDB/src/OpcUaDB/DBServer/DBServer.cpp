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

#include "OpcUaStackCore/Base/os.h"
#include "OpcUaStackCore/Base/Log.h"
#include "OpcUaDB/DBServer/DBServer.h"
#include "OpcUaDB/odbc/Connection.h"

namespace OpcUaDB
{

	DBServer::DBServer(void)
	{
		execSQLDirect();
	}

	DBServer::~DBServer(void)
	{
	}

	bool
	DBServer::startup(void)
	{
		execSQLDirect();
		return true;
	}

	bool
	DBServer::shutdown(void)
	{
		return true;
	}

	bool
	DBServer::execSQLDirect(void)
	{
		bool success;
		Connection connection;

		// connect to database
		success = connection.connect();
		if (!success) {
			return false;
		}

		// execute sql statement
		success = connection.execDirect("select * from TestTable");
		if (!success) {
			connection.disconnect();
			return false;
		}

		// disconnect to database
		success = connection.disconnect();
		if (!success) {
			return false;
		}

		std::cout << "sql query success..." << std::endl;
		return true;
	}
}


