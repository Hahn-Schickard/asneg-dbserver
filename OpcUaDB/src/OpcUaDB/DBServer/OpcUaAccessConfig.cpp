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
#include "OpcUaDB/DBServer/OpcUaAccessConfig.h"

using namespace OpcUaStackCore;

namespace OpcUaDB
{

	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------
	//
	// class OpcUaAccessConfig
	//
	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------
	OpcUaAccessConfig::OpcUaAccessConfig(void)
	: namespaceUris_()
	, identAccess_()
	, sqlAccess_()
	, sqlQueryMap_()
	{
	}

	OpcUaAccessConfig::~OpcUaAccessConfig(void)
	{
	}

	void
	OpcUaAccessConfig::configFileName(const std::string& configFileName)
	{
		configFileName_ = configFileName;
	}

	OpcUaAccessConfig::NamespaceUris&
	OpcUaAccessConfig::namespaceUris(void)
	{
		return namespaceUris_;
	}

	OpcUaReferenceConfig&
	OpcUaAccessConfig::identAccess(void)
	{
		return identAccess_;
	}

	OpcUaReferenceConfig&
	OpcUaAccessConfig::sqlAccess(void)
	{
		return sqlAccess_;
	}

	OpcUaAccessConfig::SQLQueryMap&
	OpcUaAccessConfig::sqlQueryMap(void)
	{
		return sqlQueryMap_;
	}

	bool
	OpcUaAccessConfig::decode(Config& config)
	{
		bool success;

		// decode NamespaceUris element
		boost::optional<Config> namespaceUris = config.getChild("NamespaceUris");
		if (!namespaceUris) {
			Log(Error, "element missing in config file")
				.parameter("Element", "DBModel.OpcUaAccess.NamespaceUris")
				.parameter("ConfigFileName", configFileName_);
			return false;
		}
		if (!decodeNamespaceUris(*namespaceUris)) {
			return false;
		}

		// decode ident access
		boost::optional<Config> identAccess = config.getChild("IdentAccess");
		if (!identAccess) {
			Log(Error, "element missing in config file")
				.parameter("Element", "DBModel.OpcUaAccess.IdentAccess")
				.parameter("ConfigFileName", configFileName_);
			return false;
		}
		if (!decodeIdentAccess(*identAccess)) {
			return false;
		}

		// decode sql access
		boost::optional<Config> sqlAccess = config.getChild("SQLAccess.Server");
		if (!sqlAccess) {
			Log(Error, "element missing in config file")
				.parameter("Element", "DBModel.OpcUaAccess.SQLAccess.Server")
				.parameter("ConfigFileName", configFileName_);
			return false;
		}
		if (!decodeSQLAccess(*sqlAccess)) {
			return false;
		}

		return true;
	}

	bool
	OpcUaAccessConfig::decodeNamespaceUris(Config& config)
	{
		// get Uri elements
		config.getValues("Uri", namespaceUris_);
		if (namespaceUris_.size() == 0) {
			Log(Error, "element missing in config file")
				.parameter("Element", "DBModel.OpcUaAccess.NamespaceUris.Uri")
				.parameter("ConfigFileName", configFileName_);
			return false;
		}

		return true;
	}

	bool
	OpcUaAccessConfig::decodeIdentAccess(Config& config)
	{
		// get ident access element
		boost::optional<Config> server = config.getChild("Server");
		if (!server) {
			Log(Error, "element missing in config file")
				.parameter("Element", "DBModel.OpcUaAccess.IdentAccess.Server")
				.parameter("ConfigFileName", configFileName_);
			return false;
		}

		// decode server section
		identAccess_.configFileName(configFileName_);
		identAccess_.elementPrefix("DBModel.OpcUaAccess.IdentAccess.Server");
		if (!identAccess_.decode(*server)) {
			return false;
		}

		// decode sql query section
		std::vector<Config> queryVec;
		config.getChilds("SQLQuerys.SQLQuery", queryVec);

		std::vector<Config>::iterator it;
		for (it = queryVec.begin(); it != queryVec.end(); it++) {
			std::string id;

			// get id attribute
			bool success = it->getConfigParameter("<xmlattr>.Id", id);
			if (!success) {
				Log(Error, "attribute missing in config file")
					.parameter("Element", "DBModel.OpcUaAccess.IdentAccess.SQLQuerys.SQLQuery")
					.parameter("Attribute", "Id")
					.parameter("ConfigFileName", configFileName_);
				return false;
			}

			// get sql query
			std::string sqlQuery = it->getValue();

			std::cout << " " << id << " " << sqlQuery << std::endl;
		}

		return true;
	}

	bool
	OpcUaAccessConfig::decodeSQLAccess(Config& config)
	{
		sqlAccess_.configFileName(configFileName_);
		sqlAccess_.elementPrefix("DBModel.OpcUaAccess.SQLAccess.Server");
		return sqlAccess_.decode(config);
	}

}
