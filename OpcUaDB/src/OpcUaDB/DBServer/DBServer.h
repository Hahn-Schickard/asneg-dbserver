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

#ifndef __OpcUaDB_DBServer_h__
#define __OpcUaDB_DBServer_h__

#include "OpcUaStackCore/Application/ApplicationMethodContext.h"
#include "OpcUaStackServer/Application/ApplicationIf.h"
#include "OpcUaDB/DBServer/DBModelConfig.h"
#include "OpcUaDB/odbc/Connection.h"

using namespace OpcUaStackServer;

namespace OpcUaDB
{

	class DBServer
	{
	  public:
		typedef std::map<uint32_t, uint32_t> NamespaceMap;

		DBServer(void);
		~DBServer(void);

		void applicationServiceIf(ApplicationServiceIf* applicationServiceIf);
		void dbModelConfig(DBModelConfig* dbModelConfig);

		bool startup(void);
		bool shutdown(void);

	  private:
		bool getNamespaceInfo(void);
		bool registerCalls(void);
		bool registerIdentAccessCall(void);
		bool registerSQLAccessCall(void);
		void identAccessCall(ApplicationMethodContext* applicationMethodContext);
		void sqlAccessCall(ApplicationMethodContext* applicationMethodContext);

		bool execSQLDirect(const std::string& sqlQuery, OpcUaVariantArray::SPtr& outputArguments);
		bool createResultError(
			const std::string& resultCode,
			OpcUaVariantArray::SPtr& outputArguments
		);
		bool createResultSet(
			ResultSet& resultSet,
			OpcUaVariant::SPtr& statusCode,
			OpcUaVariant::SPtr& header,
			OpcUaVariant::SPtr& data);

		NamespaceMap namespaceMap_;
		ApplicationServiceIf* applicationServiceIf_;
		DBModelConfig* dbModelConfig_;

	    Callback identAccessCallback_;
	    Callback sqlAccessCallback_;
	};

}

#endif
