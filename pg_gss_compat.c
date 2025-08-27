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
    fprintf(stderr, "[DEBUG] pg_GSS_have_cred_cache: CALLED!\n");
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
    
    fprintf(stderr, "[DEBUG] pg_GSS_error: CALLED with message: %s\n", errmsg);
    
    snprintf(msg_buffer, sizeof(msg_buffer), 
             "%s (major: %u, minor: %u)", 
             errmsg, (unsigned int)maj_stat, (unsigned int)min_stat);
    
    printfPQExpBuffer(&conn->errorMessage, "%s\n", msg_buffer);
    conn->status = CONNECTION_BAD;
}

/*
 * pg_GSS_load_servicename
 *
 * Load the GSSAPI service name for authentication.
 * This function was introduced in PostgreSQL versions newer than 15.2.
 * This is a compatibility implementation for PostgreSQL 15.2.
 *
 * For PostgreSQL 15.2, we provide a simple implementation that
 * sets up the service name using the connection parameters.
 */
int
pg_GSS_load_servicename(PGconn *conn)
{
    char       *host = conn->pghost;
    char       *service = conn->krbsrvname;
    OM_uint32   maj_stat, min_stat;
    gss_buffer_desc temp_gbuf;
    char       *principal_name;
    
    /*
     * For PostgreSQL 15.2 compatibility, we provide basic service name loading.
     * This follows the same pattern as the original PostgreSQL implementation.
     */
    
    fprintf(stderr, "[DEBUG] pg_GSS_load_servicename: CALLED!\n");
    
    /* Use default service name if not specified */
    if (!service || service[0] == '\0')
        service = "postgres";
        
    /* Use localhost if no host specified */
    if (!host || host[0] == '\0')
        host = "localhost";
    
    /* Construct the principal name */
    principal_name = malloc(strlen(service) + strlen(host) + 2);
    if (!principal_name)
    {
        printfPQExpBuffer(&conn->errorMessage, "out of memory for GSS service name\n");
        return STATUS_ERROR;
    }
    
    sprintf(principal_name, "%s@%s", service, host);
    
    /* Convert to GSS name */
    temp_gbuf.value = principal_name;
    temp_gbuf.length = strlen(principal_name);
    
    maj_stat = gss_import_name(&min_stat, &temp_gbuf,
                               GSS_C_NT_HOSTBASED_SERVICE, &conn->gtarg_nam);
    
    free(principal_name);
    
    if (maj_stat != GSS_S_COMPLETE)
    {
        pg_GSS_error("GSSAPI service name import error", conn, maj_stat, min_stat);
        return STATUS_ERROR;
    }
    
    return STATUS_OK;
}

/*
 * pg_store_delegated_credential
 *
 * Store delegated GSSAPI credentials for later use.
 * This function was introduced in PostgreSQL versions newer than 15.2.
 * This is a compatibility implementation for PostgreSQL 15.2.
 *
 * For PostgreSQL 15.2, we provide a simple no-op implementation since
 * credential delegation storage is an advanced feature.
 */
void
pg_store_delegated_credential(gss_cred_id_t cred)
{
    /*
     * For PostgreSQL 15.2 compatibility, we provide a no-op implementation.
     * Credential delegation storage is an advanced feature that we can
     * safely skip for basic GSSAPI authentication functionality.
     */
    return;
}

#endif /* HAVE_GSSAPI */