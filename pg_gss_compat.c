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

#endif /* HAVE_GSSAPI */