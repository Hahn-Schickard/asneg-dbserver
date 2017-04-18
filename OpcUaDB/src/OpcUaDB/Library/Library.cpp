/*
   Copyright 2015-2017 Kai Huebl (kai@huebl-sgh.de)

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
#include "OpcUaDB/Library/Library.h"
#include "OpcUaStackServer/ServiceSetApplication/ApplicationService.h"
#include "OpcUaStackServer/ServiceSetApplication/NodeReferenceApplication.h"
#include <iostream>
#include "BuildConfig.h"

namespace OpcUaDB
{

	Library::Library(void)
	: ApplicationIf()
	, configXmlManager_()
	, ioThread_()
	, dbServer_()
	{
	}

	Library::~Library(void)
	{
	}

	bool
	Library::startup(void)
	{
		Log(Debug, "Library::startup");

		//
		// create own thread
		//
		ioThread_ = constructSPtr<IOThread>();
		if (!ioThread_->startup()) return false;


		//
        // read database model configuration file
		//
		Config::SPtr config;
        if (!configXmlManager_.registerConfiguration(applicationInfo()->configFileName(), config)) {
        	Log(Error, "read DBModel configuration error")
        		.parameter("ErrorMessage", configXmlManager_.errorMessage())
        		.parameter("ConfigFileName", applicationInfo()->configFileName());
        	return false;
        }
        Log(Info, "read configuration file")
            .parameter("ConfigFileName", applicationInfo()->configFileName());


        //
        // decode configuration
        //
        boost::optional<Config> child = config->getChild("DBModel");
        if (!child) {
			Log(Error, "element missing in config file")
				.parameter("Element", "DBModel")
				.parameter("ConfigFileName", config->configFileName());
			return false;
        }
        if (!dbModelConfig_.decode(*child)) {
        	return false;
        }


        //
		// startup database server
        //
        dbServer_.applicationServiceIf(&service());
        dbServer_.dbModelConfig(&dbModelConfig_);
		if (!dbServer_.startup()) {
			return false;
		}

		Log(Info, "startup db server complete");
		return true;
	}

	bool
	Library::shutdown(void)
	{
		Log(Debug, "Library::shutdown");

		// shutdown database server
		if (!dbServer_.shutdown()) {
			return false;
		}
		return true;
	}

	std::string
	Library::version(void)
	{
		std::stringstream version;

		version << LIBRARY_VERSION_MAJOR << "." << LIBRARY_VERSION_MINOR << "." << LIBRARY_VERSION_PATCH;
		return version.str();
	}

}

extern "C" DLLEXPORT void  init(ApplicationIf** applicationIf) {
    *applicationIf = new OpcUaDB::Library();
}

