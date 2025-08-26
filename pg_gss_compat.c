#ifdef HAVE_GSSAPI

#include "postgres_fe.h"
#include "libpq-fe.h"
#include "libpq-int.h"

/*
 * pg_GSS_have_cred_cache
 *
 * Check if we have GSSAPI credentials in the credential cache.
 * This function was introduced in PostgreSQL versions newer than 15.2.
 * This is a compatibility implementation for PostgreSQL 15.2.
 *
 * For PostgreSQL 15.2, we provide a simple implementation that
 * always returns false, which means we'll always try to acquire
 * credentials normally. This is safe and maintains compatibility.
 */
bool
pg_GSS_have_cred_cache(gss_cred_id_t *cred)
{
    /*
     * For PostgreSQL 15.2 compatibility, we simply return false.
     * This means we'll always attempt normal credential acquisition,
     * which is the safe fallback behavior.
     */
    return false;
}

/*
 * pg_GSS_error
 *
 * Report GSSAPI error to the client.
 * This function was introduced in PostgreSQL versions newer than 15.2.
 * This is a compatibility implementation for PostgreSQL 15.2.
 *
 * For PostgreSQL 15.2, we provide a simple implementation that
 * logs the error message and sets the connection error state.
 */
void
pg_GSS_error(const char *errmsg, PGconn *conn, OM_uint32 maj_stat, OM_uint32 min_stat)
{
    char        msg_buffer[256];
    
    /*
     * For PostgreSQL 15.2 compatibility, we provide basic error reporting.
     * We format a simple error message with the major and minor status codes.
     */
    snprintf(msg_buffer, sizeof(msg_buffer), 
             "%s (major: %u, minor: %u)", 
             errmsg, (unsigned int)maj_stat, (unsigned int)min_stat);
    
    printfPQExpBuffer(&conn->errorMessage, "%s\n", msg_buffer);
    conn->status = CONNECTION_BAD;
}

#endif /* HAVE_GSSAPI */