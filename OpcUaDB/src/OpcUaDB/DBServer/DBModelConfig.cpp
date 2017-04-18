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
#include "OpcUaDB/DBServer/DBModelConfig.h"

using namespace OpcUaStackCore;

namespace OpcUaDB
{

	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------
	//
	// class DBModelConfig
	//
	// ------------------------------------------------------------------------
	// ------------------------------------------------------------------------
	DBModelConfig::DBModelConfig(void)
	: databaseConfig_()
	, opcUaAccessConfig_()
	{
	}

	DBModelConfig::~DBModelConfig(void)
	{
	}

	DatabaseConfig&
	DBModelConfig::databaseConfig(void)
	{
		return databaseConfig_;
	}

	OpcUaAccessConfig&
	DBModelConfig::opcUaAccessConfig(void)
	{
		return opcUaAccessConfig_;
	}

	bool
	DBModelConfig::decode(Config& config)
	{
		// get database configuration
		boost::optional<Config> child = config.getChild("Database");
		if (!child) {
			Log(Error, "element missing in config file")
				.parameter("Element", "DBModel.Database")
				.parameter("ConfigFileName", config.configFileName());
			return false;
		}
		if (!databaseConfig_.decode(*child)) {
			return false;
		}

		// get opc ua access configuration
		boost::optional<Config> accessConfig = config.getChild("OpcUaAccess");
		if (!child) {
			Log(Error, "element missing in config file")
				.parameter("Element", "DBModel.OpcUaAccess")
				.parameter("ConfigFileName", config.configFileName());
			return false;
		}
		opcUaAccessConfig_.configFileName(config.configFileName());
		if (!opcUaAccessConfig_.decode(*accessConfig)) {
			return false;
		}

		return true;
	}

}
