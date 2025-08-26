#!/bin/bash

# Install krb5 packages if we're in Docker environment
if [ "$LINUX_CI_IN_DOCKER" = "1" ]; then
    echo "Installing krb5 packages in Docker environment..."
    
    # Install both development and runtime packages
    apk add --no-cache krb5-dev krb5-libs krb5 2>/dev/null
    
    # Verify installation
    if [ -f "/usr/lib/libgssapi_krb5.so.2" ] || [ -f "/usr/lib/libgssapi_krb5.so" ]; then
        echo "krb5 packages installed successfully"
        ls -la /usr/lib/libgssapi* 2>/dev/null || true
        ls -la /usr/lib/*krb5* 2>/dev/null || true
    else
        echo "Warning: krb5 runtime libraries may not be available"
    fi
else
    echo "Not in Docker environment, skipping krb5 installation"
fi