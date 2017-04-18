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
#include "OpcUaStackServer/ServiceSetApplication/ApplicationService.h"
#include "OpcUaDB/DBServer/DBServer.h"
#include "OpcUaDB/odbc/Connection.h"

using namespace OpcUaStackServer;

namespace OpcUaDB
{

	DBServer::DBServer(void)
	: applicationServiceIf_(nullptr)
	, dbModelConfig_(nullptr)
	, namespaceMap_()
	{
	}

	DBServer::~DBServer(void)
	{
	}

	void
	DBServer::applicationServiceIf(ApplicationServiceIf* applicationServiceIf)
	{
		applicationServiceIf_ = applicationServiceIf;
	}

	void
	DBServer::dbModelConfig(DBModelConfig* dbModelConfig)
	{
		dbModelConfig_ = dbModelConfig;
	}

	bool
	DBServer::startup(void)
	{
		// mapping namespace uris
		if (!getNamespaceInfo()) {
			return false;
		}

		// register calls
		if (!registerCalls()) {
			return false;
		}

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
		connection.name(dbModelConfig_->databaseConfig().name());
		success = connection.connect();
		if (!success) {
			return false;
		}

		// execute sql statement
		success = connection.execDirect("select * from \"TestData\"");
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

	bool
	DBServer::getNamespaceInfo(void)
	{
		// read namespace array
		ServiceTransactionNamespaceInfo::SPtr trx = constructSPtr<ServiceTransactionNamespaceInfo>();
		NamespaceInfoRequest::SPtr req = trx->request();
		NamespaceInfoResponse::SPtr res = trx->response();

		applicationServiceIf_->sendSync(trx);
		if (trx->statusCode() != Success) {
			std::cout << "NamespaceInfoResponse error" << std::endl;
			return false;
		}

		// create namespace mapping table
		namespaceMap_.clear();

		for (uint32_t idx=0; idx<dbModelConfig_->opcUaAccessConfig().namespaceUris().size(); idx++) {
			std::string namespaceName = dbModelConfig_->opcUaAccessConfig().namespaceUris()[idx];
			uint32_t namespaceIndex = idx+1;

			bool found = false;
			NamespaceInfoResponse::Index2NamespaceMap::iterator it;
			for (it = res->index2NamespaceMap().begin();
				 it != res->index2NamespaceMap().end();
				 it++)
			{
				if (it->second == namespaceName) {
					found = true;
					namespaceMap_.insert(std::make_pair(namespaceIndex, it->first));
					break;
				}
			}

			if (!found) {
				Log(Error, "namespace name not exist on own server")
				    .parameter("NamespaceName", namespaceName);
				return false;
			}
		}

		return true;
	}

	bool
	DBServer::registerCalls(void)
	{
		// FIXME: todo
		return true;
	}

	void
	DBServer::identAccessCall(ApplicationMethodContext* applicationMethodContext)
	{
		// FIXME: todo
	}

	void
	DBServer::sqlAccessCall(ApplicationMethodContext* applicationMethodContext)
	{
		// FIXME: todo
	}
}


