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

using namespace OpcUaStackServer;

namespace OpcUaDB
{

	DBServer::DBServer(void)
	: applicationServiceIf_(nullptr)
	, dbModelConfig_(nullptr)
	, namespaceMap_()
	, identAccessCallback_(boost::bind(&DBServer::identAccessCall, this, _1))
	, sqlAccessCallback_(boost::bind(&DBServer::sqlAccessCall, this, _1))
	, accessCallback_(boost::bind(&DBServer::accessCall, this, _1))
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

		return true;
	}

	bool
	DBServer::shutdown(void)
	{
		return true;
	}

	bool
	DBServer::execSQLDirect(const std::string& sqlQuery, OpcUaVariantArray::SPtr& outputArguments)
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
		// select * from "TestData"
		success = connection.execDirect(sqlQuery);
		if (!success) {
			Log(Error, "sql query error")
			    .parameter("SQLQuery", sqlQuery);
			connection.disconnect();
			return false;
		}

		// get result set
		ResultSet& resultSet = connection.resultSet();
		resultSet.out(std::cout);
		std::string statusCode;
		OpcUaStringArray::SPtr header = constructSPtr<OpcUaStringArray>();
		OpcUaStringArray::SPtr data = constructSPtr<OpcUaStringArray>();

		if (!createResultSet(resultSet, statusCode, header, data)) {
			connection.disconnect();
			return false;
		}
		OpcUaString::SPtr sc = constructSPtr<OpcUaString>();
		sc->value(statusCode);

		OpcUaVariant::SPtr variant;
		outputArguments->resize(3);
		variant = constructSPtr<OpcUaVariant>();
		variant->set(sc);
		outputArguments->set(0, variant);
		variant = constructSPtr<OpcUaVariant>();
		OpcUaVariantValue::Vec variantVec;
		for (uint32_t idx=0; idx<header->size(); idx++) {
			OpcUaString::SPtr x = constructSPtr<OpcUaString>();
			x->value("xx");
			OpcUaVariantValue v;
			v.variant(x);
			variantVec.push_back(v);
		}
		variant->variant(variantVec);
		std::cout << "__" << std::endl;
		variant->out(std::cout);
		std::cout << "__" << std::endl;
		outputArguments->set(1, variant);
		variant = constructSPtr<OpcUaVariant>();
		variant->set(data);
		outputArguments->set(2, variant);

		std::cout << "__" << std::endl;
		outputArguments->out(std::cout);
		std::cout << "__" << std::endl;

		// disconnect to database
		success = connection.disconnect();
		if (!success) {
			return false;
		}

		return true;
	}

	bool
	DBServer::createResultSet(
		ResultSet& resultSet,
		std::string& statusCode,
		OpcUaStringArray::SPtr& header,
		OpcUaStringArray::SPtr& data
	)
	{
		// create header
		header->resize(resultSet.colDescriptionVec_.size());
		for (uint32_t idx=0; idx<resultSet.colDescriptionVec_.size(); idx++) {
			OpcUaString::SPtr headerString = constructSPtr<OpcUaString>();
			headerString->value((char*)resultSet.colDescriptionVec_[idx].colName_);
			header->set(idx, headerString);
		}

		// create data
		data->resize(resultSet.tableData_.size() * resultSet.colDescriptionVec_.size());
		for (uint32_t i=0; i<resultSet.tableData_.size(); i++) {
			for (uint32_t j=0; j<resultSet.colDescriptionVec_.size(); j++) {
				OpcUaString::SPtr dataString = constructSPtr<OpcUaString>();
				dataString->value(resultSet.tableData_[i][j]);
				data->push_back(dataString);
			}
		}

		statusCode = "Success";
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
		if (!registerIdentAccessCall()) {
			return false;
		}
		if (!registerSQLAccessCall()) {
			return false;
		}
		if (!registerAccessCall()) {
			return false;
		}
		return true;
	}

	bool
	DBServer::registerIdentAccessCall(void)
	{
	  	ServiceTransactionRegisterForward::SPtr trx = constructSPtr<ServiceTransactionRegisterForward>();
	  	RegisterForwardRequest::SPtr req = trx->request();
	  	RegisterForwardResponse::SPtr res = trx->response();

        req->forwardInfoSync()->methodService().setCallback(identAccessCallback_);
	  	req->nodesToRegister()->resize(1);

	  	OpcUaNodeId::SPtr nodeId = constructSPtr<OpcUaNodeId>();
	    *nodeId = dbModelConfig_->opcUaAccessConfig().identAccess().nodeId();

  		NamespaceMap::iterator it;
  		it = namespaceMap_.find(nodeId->namespaceIndex());
  		if (it == namespaceMap_.end()) {
  			Log(Error, "namespace index not exist in opc ua model")
  				.parameter("NodeId", *nodeId);
  			return false;
  		}
  		nodeId->namespaceIndex(it->second);

  		req->nodesToRegister()->set(0, nodeId);

	  	applicationServiceIf_->sendSync(trx);
	  	if (trx->statusCode() != Success) {
	  		Log(Error, "register forward response error")
	  		    .parameter("StatusCode", OpcUaStatusCodeMap::shortString(trx->statusCode()));
	  		return false;
	  	}

	  	if (res->statusCodeArray()->size() != 1) {
	  		Log(Error, "register forward result error");
	  		return false;
	  	}

  		OpcUaStatusCode statusCode;
  		res->statusCodeArray()->get(0, statusCode);
  		if (statusCode != Success) {
	  		Log(Error, "register forward value error")
	  		    .parameter("StatusCode", OpcUaStatusCodeMap::shortString(statusCode));
  			return false;
  		}

    	return true;
	}

	bool
	DBServer::registerSQLAccessCall(void)
	{
	  	ServiceTransactionRegisterForward::SPtr trx = constructSPtr<ServiceTransactionRegisterForward>();
	  	RegisterForwardRequest::SPtr req = trx->request();
	  	RegisterForwardResponse::SPtr res = trx->response();

        req->forwardInfoSync()->methodService().setCallback(sqlAccessCallback_);
	  	req->nodesToRegister()->resize(1);

	  	OpcUaNodeId::SPtr nodeId = constructSPtr<OpcUaNodeId>();
	    *nodeId = dbModelConfig_->opcUaAccessConfig().sqlAccess().nodeId();

  		NamespaceMap::iterator it;
  		it = namespaceMap_.find(nodeId->namespaceIndex());
  		if (it == namespaceMap_.end()) {
  			Log(Error, "namespace index not exist in opc ua model")
  				.parameter("NodeId", *nodeId);
  			return false;
  		}
  		nodeId->namespaceIndex(it->second);

  		req->nodesToRegister()->set(0, nodeId);

	  	applicationServiceIf_->sendSync(trx);
	  	if (trx->statusCode() != Success) {
	  		Log(Error, "register forward response error")
	  		    .parameter("StatusCode", OpcUaStatusCodeMap::shortString(trx->statusCode()));
	  		return false;
	  	}

	  	if (res->statusCodeArray()->size() != 1) {
	  		Log(Error, "register forward result error");
	  		return false;
	  	}

  		OpcUaStatusCode statusCode;
  		res->statusCodeArray()->get(0, statusCode);
  		if (statusCode != Success) {
	  		Log(Error, "register forward value error")
	  		    .parameter("StatusCode", OpcUaStatusCodeMap::shortString(statusCode));
  			return false;
  		}

    	return true;
	}

	bool
	DBServer::registerAccessCall(void)
	{
	  	ServiceTransactionRegisterForward::SPtr trx = constructSPtr<ServiceTransactionRegisterForward>();
	  	RegisterForwardRequest::SPtr req = trx->request();
	  	RegisterForwardResponse::SPtr res = trx->response();

        req->forwardInfoSync()->methodService().setCallback(accessCallback_);
	  	req->nodesToRegister()->resize(1);

	  	OpcUaNodeId::SPtr nodeId = constructSPtr<OpcUaNodeId>();
	    nodeId->set("DBAcess", 1);

  		NamespaceMap::iterator it;
  		it = namespaceMap_.find(nodeId->namespaceIndex());
  		if (it == namespaceMap_.end()) {
  			Log(Error, "namespace index not exist in opc ua model")
  				.parameter("NodeId", *nodeId);
  			return false;
  		}
  		nodeId->namespaceIndex(it->second);

  		req->nodesToRegister()->set(0, nodeId);

	  	applicationServiceIf_->sendSync(trx);
	  	if (trx->statusCode() != Success) {
	  		Log(Error, "register forward response error")
	  		    .parameter("StatusCode", OpcUaStatusCodeMap::shortString(trx->statusCode()));
	  		return false;
	  	}

	  	if (res->statusCodeArray()->size() != 1) {
	  		Log(Error, "register forward result error");
	  		return false;
	  	}

  		OpcUaStatusCode statusCode;
  		res->statusCodeArray()->get(0, statusCode);
  		if (statusCode != Success) {
	  		Log(Error, "register forward value error")
	  		    .parameter("StatusCode", OpcUaStatusCodeMap::shortString(statusCode));
  			return false;
  		}

    	return true;
	}

	void
	DBServer::identAccessCall(ApplicationMethodContext* applicationMethodContext)
	{
		std::cout << "call ident access call" << std::endl;
		// FIXME: todo

		applicationMethodContext->statusCode_ = Success;
	}

	void
	DBServer::sqlAccessCall(ApplicationMethodContext* applicationMethodContext)
	{
		// check input arguments
		if (applicationMethodContext->inputArguments_->size() != 1) {
			Log(Error, "input argument size error in sql access call");
			applicationMethodContext->statusCode_ = BadInvalidArgument;
			return;
		}

		// get variant value
		OpcUaVariant::SPtr value;
		if (!applicationMethodContext->inputArguments_->get(0, value)) {
			Log(Error, "variant value error in sql access call");
			applicationMethodContext->statusCode_ = BadInvalidArgument;
			return;
		}
		if (value->isArray()) {
			Log(Error, "variant type error in sql access call");
			applicationMethodContext->statusCode_ = BadInvalidArgument;
			return;
		}
		if (value->variantType() != OpcUaBuildInType_OpcUaString) {
			Log(Error, "variant type error in sql access call");
			applicationMethodContext->statusCode_ = BadInvalidArgument;
			return;
		}

		// get sql query
		OpcUaString::SPtr sqlQuery;
		sqlQuery = value->getSPtr<OpcUaString>();
		if (sqlQuery.get() == nullptr) {
			Log(Error, "sql query error in sql access call");
			applicationMethodContext->statusCode_ = BadInvalidArgument;
			return;
		}

		// execute sql query
		if (!execSQLDirect(sqlQuery->value(), applicationMethodContext->outputArguments_)) {
			Log(Error, "database error");
			applicationMethodContext->statusCode_ = BadInternalError;
			return;
		}

		std::cout << "..." << applicationMethodContext->outputArguments_->size() << std::endl;
		applicationMethodContext->outputArguments_->out(std::cout);
		std::cout << "..." << std::endl;
		applicationMethodContext->statusCode_ = Success;
	}

	void
	DBServer::accessCall(ApplicationMethodContext* applicationMethodContext)
	{
		if (applicationMethodContext->methodNodeId_ == dbModelConfig_->opcUaAccessConfig().identAccess().nodeId()) {
			identAccessCall(applicationMethodContext);
			return;
		}
		else if (applicationMethodContext->methodNodeId_ == dbModelConfig_->opcUaAccessConfig().sqlAccess().nodeId()) {
			sqlAccessCall(applicationMethodContext);
			return;
		}
		applicationMethodContext->statusCode_ = BadNotSupported;
	}
}


