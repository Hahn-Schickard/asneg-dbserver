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

#ifndef __OpcUaDB_OpcUaAccessConfig_h__
#define __OpcUaDB_OpcUaAccessConfig_h__

#include "OpcUaStackCore/Base/ConfigXmlManager.h"
#include "OpcUaStackCore/ApplicationUtility/OpcUaReferenceConfig.h"

using namespace OpcUaStackCore;

namespace OpcUaDB
{

	class OpcUaAccessConfig
	{
	  public:
		typedef std::vector<std::string> NamespaceUris;

		OpcUaAccessConfig(void);
		~OpcUaAccessConfig(void);

    	void configFileName(const std::string& configFileName);
    	NamespaceUris& namespaceUris(void);
    	OpcUaReferenceConfig& identAccess(void);
    	OpcUaReferenceConfig& sqlAccess(void);

		bool decode(Config& config);

	  private:
		bool decodeNamespaceUris(Config& config);
		bool decodeIdentAccess(Config& config);
		bool decodeSQLAccess(Config& config);

		std::string configFileName_;
		NamespaceUris namespaceUris_;
    	OpcUaReferenceConfig identAccess_;
    	OpcUaReferenceConfig sqlAccess_;
	};

}

#endif
