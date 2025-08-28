#ifdef USE_DYNAMIC_LIBPQ

#include "libpq_dynamic.h"
#include <dlfcn.h>
#include <stdio.h>
#include <stdbool.h>

// Global handle for the dynamically loaded libpq
static void *libpq_handle = NULL;

// Global function pointers
PQconnectdb_t dynamic_PQconnectdb = NULL;
PQfinish_t dynamic_PQfinish = NULL;
PQstatus_t dynamic_PQstatus = NULL;
PQerrorMessage_t dynamic_PQerrorMessage = NULL;
PQresultStatus_t dynamic_PQresultStatus = NULL;
PQresultErrorMessage_t dynamic_PQresultErrorMessage = NULL;
PQexec_t dynamic_PQexec = NULL;
PQclear_t dynamic_PQclear = NULL;
PQntuples_t dynamic_PQntuples = NULL;
PQnfields_t dynamic_PQnfields = NULL;
PQgetvalue_t dynamic_PQgetvalue = NULL;
PQfname_t dynamic_PQfname = NULL;
PQftype_t dynamic_PQftype = NULL;
PQgetisnull_t dynamic_PQgetisnull = NULL;
PQconninfoParse_t dynamic_PQconninfoParse = NULL;
PQconninfoFree_t dynamic_PQconninfoFree = NULL;
PQuser_t dynamic_PQuser = NULL;
PQhost_t dynamic_PQhost = NULL;
PQport_t dynamic_PQport = NULL;
PQdb_t dynamic_PQdb = NULL;

// Library search paths for different systems
static const char* libpq_search_paths[] = {
    // Linux paths
    "libpq.so.5",           // Most common
    "libpq.so",             // Generic
    "/usr/lib64/libpq.so.5",
    "/usr/lib/libpq.so.5",
    "/usr/lib/x86_64-linux-gnu/libpq.so.5",
    "/usr/local/lib/libpq.so.5",
    // macOS paths  
    "libpq.dylib",
    "/opt/homebrew/lib/libpq.dylib",
    "/usr/local/lib/libpq.dylib",
    NULL
};

bool libpq_dynamic_init(void) {
    if (libpq_handle != NULL) {
        return true; // Already initialized
    }
    
    fprintf(stderr, "[DEBUG] libpq_dynamic_init: Starting dynamic libpq loading...\n");
    
    // Try different paths to find libpq
    for (int i = 0; libpq_search_paths[i] != NULL; i++) {
        fprintf(stderr, "[DEBUG] libpq_dynamic_init: Trying %s\n", libpq_search_paths[i]);
        
        libpq_handle = dlopen(libpq_search_paths[i], RTLD_LAZY);
        if (libpq_handle != NULL) {
            fprintf(stderr, "[DEBUG] libpq_dynamic_init: Successfully loaded %s\n", libpq_search_paths[i]);
            break;
        } else {
            fprintf(stderr, "[DEBUG] libpq_dynamic_init: Failed to load %s: %s\n", 
                   libpq_search_paths[i], dlerror());
        }
    }
    
    if (libpq_handle == NULL) {
        fprintf(stderr, "[ERROR] libpq_dynamic_init: Failed to load libpq from any location\n");
        return false;
    }
    
    // Clear any existing error
    dlerror();
    
    // Load function symbols
    #define LOAD_SYMBOL(name) \
        dynamic_##name = (name##_t) dlsym(libpq_handle, #name); \
        if (dynamic_##name == NULL) { \
            fprintf(stderr, "[ERROR] libpq_dynamic_init: Failed to load symbol " #name ": %s\n", dlerror()); \
            libpq_dynamic_cleanup(); \
            return false; \
        }
    
    LOAD_SYMBOL(PQconnectdb);
    LOAD_SYMBOL(PQfinish);
    LOAD_SYMBOL(PQstatus);
    LOAD_SYMBOL(PQerrorMessage);
    LOAD_SYMBOL(PQresultStatus);
    LOAD_SYMBOL(PQresultErrorMessage);
    LOAD_SYMBOL(PQexec);
    LOAD_SYMBOL(PQclear);
    LOAD_SYMBOL(PQntuples);
    LOAD_SYMBOL(PQnfields);
    LOAD_SYMBOL(PQgetvalue);
    LOAD_SYMBOL(PQfname);
    LOAD_SYMBOL(PQftype);
    LOAD_SYMBOL(PQgetisnull);
    LOAD_SYMBOL(PQconninfoParse);
    LOAD_SYMBOL(PQconninfoFree);
    LOAD_SYMBOL(PQuser);
    LOAD_SYMBOL(PQhost);
    LOAD_SYMBOL(PQport);
    LOAD_SYMBOL(PQdb);
    
    #undef LOAD_SYMBOL
    
    fprintf(stderr, "[DEBUG] libpq_dynamic_init: All libpq symbols loaded successfully\n");
    return true;
}

void libpq_dynamic_cleanup(void) {
    if (libpq_handle != NULL) {
        fprintf(stderr, "[DEBUG] libpq_dynamic_cleanup: Unloading dynamic libpq\n");
        dlclose(libpq_handle);
        libpq_handle = NULL;
        
        // Reset all function pointers
        dynamic_PQconnectdb = NULL;
        dynamic_PQfinish = NULL;
        dynamic_PQstatus = NULL;
        dynamic_PQerrorMessage = NULL;
        dynamic_PQresultStatus = NULL;
        dynamic_PQresultErrorMessage = NULL;
        dynamic_PQexec = NULL;
        dynamic_PQclear = NULL;
        dynamic_PQntuples = NULL;
        dynamic_PQnfields = NULL;
        dynamic_PQgetvalue = NULL;
        dynamic_PQfname = NULL;
        dynamic_PQftype = NULL;
        dynamic_PQgetisnull = NULL;
        dynamic_PQconninfoParse = NULL;
        dynamic_PQconninfoFree = NULL;
        dynamic_PQuser = NULL;
        dynamic_PQhost = NULL;
        dynamic_PQport = NULL;
        dynamic_PQdb = NULL;
    }
}

#endif // USE_DYNAMIC_LIBPQ