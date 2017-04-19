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
			createResultError("Error - database connection", outputArguments);
			return true;
		}

		// execute sql statement
		// select * from "TestData"
		success = connection.execDirect(sqlQuery);
		if (!success) {
			Log(Error, "sql query error")
			    .parameter("SQLQuery", sqlQuery);
			createResultError("Error - database access", outputArguments);
			connection.disconnect();
			return true;
		}

		// get result set
		ResultSet& resultSet = connection.resultSet();
		OpcUaVariant::SPtr statusCode = constructSPtr<OpcUaVariant>();
		OpcUaVariant::SPtr header = constructSPtr<OpcUaVariant>();
		OpcUaVariant::SPtr data = constructSPtr<OpcUaVariant>();

		if (!createResultSet(resultSet, statusCode, header, data)) {
			connection.disconnect();
			return false;
		}
		outputArguments->resize(3);
		outputArguments->set(0, statusCode);
		outputArguments->set(1, header);
		if (data->arrayLength() > 0) {
			outputArguments->set(2, data);
		}

		// disconnect to database
		success = connection.disconnect();
		if (!success) {
			return false;
		}

		return true;
	}

	bool
	DBServer::createResultError(
		const std::string& resultCode,
		OpcUaVariantArray::SPtr& outputArguments
	)
	{
		OpcUaVariant::SPtr statusCode = constructSPtr<OpcUaVariant>();
		statusCode->set(constructSPtr<OpcUaString>(resultCode));

		outputArguments->resize(1);
		outputArguments->set(0, statusCode);

		return true;
	}

	bool
	DBServer::createResultSet(
		ResultSet& resultSet,
		OpcUaVariant::SPtr& statusCode,
		OpcUaVariant::SPtr& header,
		OpcUaVariant::SPtr& data
	)
	{
		// create header
		OpcUaVariantValue::Vec variantVec1;
		for (uint32_t idx=0; idx<resultSet.colDescriptionVec_.size(); idx++) {
			OpcUaString::SPtr value = constructSPtr<OpcUaString>();
			value->value((char*)resultSet.colDescriptionVec_[idx].colName_);
			OpcUaVariantValue v;
			v.variant(value);
			variantVec1.push_back(v);
		}
		header->variant(variantVec1);

		// create data
		OpcUaVariantValue::Vec variantVec2;
		for (uint32_t i=0; i<resultSet.tableData_.size(); i++) {
			for (uint32_t j=0; j<resultSet.colDescriptionVec_.size(); j++) {
				OpcUaString::SPtr value = constructSPtr<OpcUaString>();
				value->value(resultSet.tableData_[i][j]);
				OpcUaVariantValue v;
				v.variant(value);
				variantVec2.push_back(v);
			}
		}
		data->variant(variantVec2);

		// create status code
		statusCode->set(constructSPtr<OpcUaString>("Success"));
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
		// check input arguments
		if (applicationMethodContext->inputArguments_->size() != 2) {
			Log(Error, "input argument size error in ident access call");
			applicationMethodContext->statusCode_ = BadInvalidArgument;
			return;
		}

		// get variant value (identifier)
		OpcUaVariant::SPtr valueIdentifier;
		if (!applicationMethodContext->inputArguments_->get(0, valueIdentifier)) {
			Log(Error, "variant identifier value error in ident access call");
			applicationMethodContext->statusCode_ = BadInvalidArgument;
			return;
		}
		if (valueIdentifier->isArray()) {
			Log(Error, "variant identifier type error in ident access call");
			applicationMethodContext->statusCode_ = BadInvalidArgument;
			return;
		}
		if (valueIdentifier->variantType() != OpcUaBuildInType_OpcUaString) {
			Log(Error, "variant identifier type error in ident access call");
			applicationMethodContext->statusCode_ = BadInvalidArgument;
			return;
		}

		// get identifier
		OpcUaString::SPtr identifier;
		identifier = valueIdentifier->getSPtr<OpcUaString>();
		if (identifier.get() == nullptr) {
			Log(Error, "identifier error in ident access call");
			applicationMethodContext->statusCode_ = BadInvalidArgument;
			return;
		}

		// get variant value (parameter)
		OpcUaVariant::SPtr valueParameter;
		if (!applicationMethodContext->inputArguments_->get(1, valueParameter)) {
			Log(Error, "variant parameter value error in ident access call");
			applicationMethodContext->statusCode_ = BadInvalidArgument;
			return;
		}
		if (!valueParameter->isArray()) {
			Log(Error, "variant parameter type error in ident access call");
			applicationMethodContext->statusCode_ = BadInvalidArgument;
			return;
		}
		if (valueParameter->variantType() != OpcUaBuildInType_OpcUaString) {
			if (valueParameter->arrayLength() != 0) {
				Log(Error, "variant parameter type error in ident access call");
				applicationMethodContext->statusCode_ = BadInvalidArgument;
				return;
			}
		}

		// get parameter
		std::vector<std::string> parameterVec;
		if (valueParameter->arrayLength() != 0) {
			OpcUaVariantValue::Vec::iterator it;
			OpcUaVariantValue::Vec variantValueVec = valueParameter->variant();
			for (it=variantValueVec.begin(); it!=variantValueVec.end(); it++) {
				OpcUaVariantValue vv = *it;
				OpcUaString::SPtr para = vv.variantSPtr<OpcUaString>();
				if (para.get() == nullptr) {
					Log(Error, "parameter error in ident access call");
					applicationMethodContext->statusCode_ = BadInvalidArgument;
					return;
				}
				parameterVec.push_back(para->value());
			}
		}

		// map id to sql query
		OpcUaAccessConfig::SQLQueryMap& map = dbModelConfig_->opcUaAccessConfig().sqlQueryMap();
		OpcUaAccessConfig::SQLQueryMap::iterator it;
		it = map.find(identifier->value().c_str());
		if (it == map.end()) {
			createResultError("Error - query id unknown", applicationMethodContext->outputArguments_);
			applicationMethodContext->statusCode_ = Success;
			return;
		}
		std::string sqlQuery = it->second;


		// subst sql query
		for (uint32_t idx=0; idx<parameterVec.size(); idx++) {
			std::stringstream ss;
			ss << "%" << (uint32_t)(idx+1);
			boost::replace_all(sqlQuery, ss.str(), parameterVec[idx]);
		}

		// execute sql query
		if (!execSQLDirect(sqlQuery, applicationMethodContext->outputArguments_)) {
			Log(Error, "database error");
			applicationMethodContext->statusCode_ = BadInternalError;
			return;
		}

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

		// get variant value (sql query)
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


