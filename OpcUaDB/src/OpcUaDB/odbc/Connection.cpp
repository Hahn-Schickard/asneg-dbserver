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

#include <sql.h>
#include <sqlext.h>

#include "OpcUaDB/odbc/Connection.h"

namespace OpcUaDB
{

	Connection::Connection(void)
	{
	}

	Connection::~Connection(void)
	{
	}

	bool
	Connection::connect(void)
	{
		// FIXME: todo
		return true;
	}

	bool
	Connection::disconnect(void)
	{
		// FIXME: todo
		return true;
	}

}

#if 0
#include <stdio.h>



int main() {

    SQLHENV env;
    SQLHDBC dbc;
    long    res;

    // Environment
                // Allocation
        SQLAllocHandle( SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);

               // ODBC: Version: Set
        SQLSetEnvAttr( env, SQL_ATTR_ODBC_VERSION, (void*)SQL_OV_ODBC3, 0);

    // DBC: Allocate
        SQLAllocHandle( SQL_HANDLE_DBC, env, &dbc);

    // DBC: Connect
        res = SQLConnect( dbc, (SQLCHAR*) "MySQL_Test", SQL_NTS,
                               (SQLCHAR*) "scott", SQL_NTS,
                               (SQLCHAR*) "tiger", SQL_NTS);

        printf("RES: %i \n", res);

    //
        SQLDisconnect( dbc );
        SQLFreeHandle( SQL_HANDLE_DBC, dbc );
        SQLFreeHandle( SQL_HANDLE_ENV, env );

    printf("\n");
    return 0;
}
#endif
