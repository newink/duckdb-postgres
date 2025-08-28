#pragma once

#ifdef USE_DYNAMIC_LIBPQ

#include <stdbool.h>
#include "libpq-fe.h"

// Function pointer types for libpq functions
typedef PGconn* (*PQconnectdb_t)(const char *conninfo);
typedef void (*PQfinish_t)(PGconn *conn);
typedef ConnStatusType (*PQstatus_t)(const PGconn *conn);
typedef char* (*PQerrorMessage_t)(const PGconn *conn);
typedef ExecStatusType (*PQresultStatus_t)(const PGresult *res);
typedef char* (*PQresultErrorMessage_t)(const PGresult *res);
typedef PGresult* (*PQexec_t)(PGconn *conn, const char *query);
typedef void (*PQclear_t)(PGresult *res);
typedef int (*PQntuples_t)(const PGresult *res);
typedef int (*PQnfields_t)(const PGresult *res);
typedef char* (*PQgetvalue_t)(const PGresult *res, int tup_num, int field_num);
typedef char* (*PQfname_t)(const PGresult *res, int field_num);
typedef Oid (*PQftype_t)(const PGresult *res, int field_num);
typedef int (*PQgetisnull_t)(const PGresult *res, int tup_num, int field_num);
typedef PQconninfoOption* (*PQconninfoParse_t)(const char *conninfo, char **errmsg);
typedef void (*PQconninfoFree_t)(PQconninfoOption *connOptions);
typedef char* (*PQuser_t)(const PGconn *conn);
typedef char* (*PQhost_t)(const PGconn *conn);
typedef char* (*PQport_t)(const PGconn *conn);
typedef char* (*PQdb_t)(const PGconn *conn);

// Global function pointers
extern PQconnectdb_t dynamic_PQconnectdb;
extern PQfinish_t dynamic_PQfinish;
extern PQstatus_t dynamic_PQstatus;
extern PQerrorMessage_t dynamic_PQerrorMessage;
extern PQresultStatus_t dynamic_PQresultStatus;
extern PQresultErrorMessage_t dynamic_PQresultErrorMessage;
extern PQexec_t dynamic_PQexec;
extern PQclear_t dynamic_PQclear;
extern PQntuples_t dynamic_PQntuples;
extern PQnfields_t dynamic_PQnfields;
extern PQgetvalue_t dynamic_PQgetvalue;
extern PQfname_t dynamic_PQfname;
extern PQftype_t dynamic_PQftype;
extern PQgetisnull_t dynamic_PQgetisnull;
extern PQconninfoParse_t dynamic_PQconninfoParse;
extern PQconninfoFree_t dynamic_PQconninfoFree;
extern PQuser_t dynamic_PQuser;
extern PQhost_t dynamic_PQhost;
extern PQport_t dynamic_PQport;
extern PQdb_t dynamic_PQdb;

// Macros to redirect libpq calls to dynamic function pointers
#define PQconnectdb dynamic_PQconnectdb
#define PQfinish dynamic_PQfinish
#define PQstatus dynamic_PQstatus
#define PQerrorMessage dynamic_PQerrorMessage
#define PQresultStatus dynamic_PQresultStatus
#define PQresultErrorMessage dynamic_PQresultErrorMessage
#define PQexec dynamic_PQexec
#define PQclear dynamic_PQclear
#define PQntuples dynamic_PQntuples
#define PQnfields dynamic_PQnfields
#define PQgetvalue dynamic_PQgetvalue
#define PQfname dynamic_PQfname
#define PQftype dynamic_PQftype
#define PQgetisnull dynamic_PQgetisnull
#define PQconninfoParse dynamic_PQconninfoParse
#define PQconninfoFree dynamic_PQconninfoFree
#define PQuser dynamic_PQuser
#define PQhost dynamic_PQhost
#define PQport dynamic_PQport
#define PQdb dynamic_PQdb

// Function to initialize dynamic libpq loading
bool libpq_dynamic_init(void);
void libpq_dynamic_cleanup(void);

#endif // USE_DYNAMIC_LIBPQ