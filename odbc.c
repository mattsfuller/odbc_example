/**
 * Author: Matt Fuller
 *
 * This is a basic test program I wrote while learning ODBC. It connects to a MySQL database I have running. 
 * I mostly followed the tutorial here:
 *             http://www.easysoft.com/developer/languages/c/odbc_tutorial.html
 */
#include <stdio.h>
#include <sql.h>
#include <sqlext.h>
#include <string.h>

void list_drivers();
void list_data_sources();
void list_tables();
void list_driver_information();

void mysql_connect();
void mysql_disconnect();

void print_results(SQLHSTMT stm);
void execute_query(char *query);

SQLHENV env;
SQLHDBC dbc;

int main() 
{
    mysql_connect(); 

    list_drivers();
    list_data_sources();
    list_driver_information();
    list_tables();

    execute_query("select * from junk");
    execute_query("select count(*), sum(a) from junk");
    execute_query("select a, a+3, 3.141592654 from junk");
    execute_query("select j1.a, j2.a from junk j1 natural join junk j2");

    mysql_disconnect();
}

/*
 * see Retrieving ODBC Diagnostics
 * for a definition of extract_error().
 */
static void extract_error(
    char *fn,
    SQLHANDLE handle,
    SQLSMALLINT type)
{
    SQLINTEGER i = 0;
    SQLINTEGER native;
    SQLCHAR state[ 7 ];
    SQLCHAR text[256];
    SQLSMALLINT len;
    SQLRETURN ret;
    fprintf(stderr,
            "\n"
            "The driver reported the following diagnostics whilst running "
            "%s\n\n",
            fn);
    do
    {
        ret = SQLGetDiagRec(type, handle, ++i, state, &native, text, sizeof(text), &len );
	if (SQL_SUCCEEDED(ret))
            printf("%s:%ld:%ld:%s\n", state, (long int)i, (long int)native, text);
    }
    while( ret == SQL_SUCCESS );
} 

void list_drivers() 
{
    char driver[256];
    char attr[256];
    SQLSMALLINT driver_ret;
    SQLSMALLINT attr_ret;
    SQLUSMALLINT direction;
    SQLRETURN ret;

    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0);

    printf("Drivers:\n");
    direction = SQL_FETCH_FIRST;
    while(SQL_SUCCEEDED(ret = SQLDrivers(env, direction,
                                         driver, sizeof(driver), &driver_ret,
                                         attr, sizeof(attr), &attr_ret))) {
        direction = SQL_FETCH_NEXT;
        printf("        %s - %s\n", driver, attr);
        if (ret == SQL_SUCCESS_WITH_INFO) printf("\tdata truncation\n");
    }
}

void list_data_sources()
{
    char dsn[256];
    char desc[256];
    SQLSMALLINT dsn_ret;
    SQLSMALLINT desc_ret;
    SQLUSMALLINT direction;
    SQLRETURN ret;

    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0);

    printf("Data Sources:\n");
    direction = SQL_FETCH_FIRST;
    while(SQL_SUCCEEDED(ret = SQLDataSources(env, direction,
                                             dsn, sizeof(dsn), &dsn_ret,
                                             desc, sizeof(desc), &desc_ret))) {
        direction = SQL_FETCH_NEXT;
        printf("        %s - %s\n", dsn, desc);
        if (ret == SQL_SUCCESS_WITH_INFO) printf("\tdata truncation\n");
    }
    printf("\n");
}

void list_driver_information()
{
    SQLCHAR dbms_name[256], dbms_ver[256];
    SQLUINTEGER getdata_support;
    SQLUSMALLINT max_concur_act;
    SQLSMALLINT string_len;

    /*
     *  Find something out about the driver.
     */
    SQLGetInfo(dbc, SQL_DBMS_NAME, (SQLPOINTER)dbms_name,
	       sizeof(dbms_name), NULL);
    SQLGetInfo(dbc, SQL_DBMS_VER, (SQLPOINTER)dbms_ver,
	       sizeof(dbms_ver), NULL);
    SQLGetInfo(dbc, SQL_GETDATA_EXTENSIONS, (SQLPOINTER)&getdata_support,
	       0, 0);
    SQLGetInfo(dbc, SQL_MAX_CONCURRENT_ACTIVITIES, &max_concur_act, 0, 0);
    
    printf("Driver Information:\n");
    printf("    DBMS Name: %s\n", dbms_name);
    printf("    DBMS Version: %s\n", dbms_ver);
    if (max_concur_act == 0) {
        printf("    SQL_MAX_CONCURRENT_ACTIVITIES - no limit or undefined\n");
    } else {
        printf("    SQL_MAX_CONCURRENT_ACTIVITIES = %u\n", max_concur_act);
    }
    if (getdata_support & SQL_GD_ANY_ORDER)
        printf("    SQLGetData - columns can be retrieved in any order\n");
    else
        printf("    SQLGetData - columns must be retrieved in order\n");
    if (getdata_support & SQL_GD_ANY_COLUMN)
        printf("    SQLGetData - can retrieve columns before last bound one\n");
    else
        printf("    SQLGetData - columns must be retrieved after last bound one\n");
    printf("\n");
}

void list_tables()
{
    SQLHSTMT stmt;

    /* Allocate a statement handle */
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    /* Retrieve a list of tables */
    SQLTables(stmt, NULL, 0, NULL, 0, NULL, 0, "TABLE", SQL_NTS);
    
    printf("Tables:\n");
    /* print the results */
    print_results(stmt);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);

    printf("\n");
}

void mysql_connect()
{
    SQLRETURN ret; /* ODBC API return status */
    SQLCHAR outstr[1024];
    SQLSMALLINT outstrlen;

    /* Allocate an environment handle */
    SQLAllocHandle(SQL_HANDLE_ENV, SQL_NULL_HANDLE, &env);
    /* We want ODBC 3 support */
    SQLSetEnvAttr(env, SQL_ATTR_ODBC_VERSION, (void *) SQL_OV_ODBC3, 0);
    /* Allocate a connection handle */
    SQLAllocHandle(SQL_HANDLE_DBC, env, &dbc);
    /* Connect to the DSN mydsn */
    ret = SQLDriverConnect(dbc, NULL, "DSN=MYSQL_TEST", SQL_NTS,
                           outstr, sizeof(outstr), &outstrlen,
                           SQL_DRIVER_COMPLETE);
    if (SQL_SUCCEEDED(ret)) {
        printf("Connected\n");
        printf("        Returned connection string was:\n\t%s\n", outstr);
        if (ret == SQL_SUCCESS_WITH_INFO) {
            printf("    Driver reported the following diagnostics\n");
            extract_error("SQLDriverConnect", dbc, SQL_HANDLE_DBC);
        }
 
    } else {
        fprintf(stderr, "Failed to connect\n");
        extract_error("SQLDriverConnect", dbc, SQL_HANDLE_DBC);
    }

    printf("\n");
}

void mysql_disconnect()
{
    /* disconnect from driver */
    SQLDisconnect(dbc);

    /* free up allocated handles */
    SQLFreeHandle(SQL_HANDLE_DBC, dbc);
    SQLFreeHandle(SQL_HANDLE_ENV, env);
}

void print_results(SQLHSTMT stmt)
{
    SQLRETURN ret; /* ODBC API return status */
    SQLSMALLINT columns; /* number of columns in result-set */
    int row = 0;

    /* How many columns are there */
    SQLNumResultCols(stmt, &columns);

    /* Loop through the rows in the result-set */
    while (SQL_SUCCEEDED(ret = SQLFetch(stmt))) {
        SQLUSMALLINT i;
        printf("Row %d\n", row++);
        /* Loop through the columns */
        for (i = 1; i <= columns; i++) {
            SQLLEN indicator;
            char buf[512];
            /* retrieve column data as a string */
            ret = SQLGetData(stmt, i, SQL_C_CHAR,
                             buf, sizeof(buf), &indicator);
            if (SQL_SUCCEEDED(ret)) {
                /* Handle null columns */
                if (indicator == SQL_NULL_DATA) strcpy(buf, "NULL");
                printf("  Column %u : %s\n", i, buf);
            }
        }
    }

    printf("\n");
}

void execute_query(char *query) 
{
    SQLHSTMT stmt;

    /* Allocate a statement handle */
    SQLAllocHandle(SQL_HANDLE_STMT, dbc, &stmt);
    
    /* execute query*/
    SQLExecDirect(stmt, query, SQL_NTS);
    
    printf("Query: %s\n", query);
    /* print the results */
    print_results(stmt);

    SQLFreeHandle(SQL_HANDLE_STMT, stmt);
}
