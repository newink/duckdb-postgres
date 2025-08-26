#!/bin/bash

# Install krb5 packages if we're in Docker environment
if [ "$LINUX_CI_IN_DOCKER" = "1" ]; then
    echo "Installing krb5 packages in Docker environment..."
    
    # Install development, runtime, and static packages
    apk add --no-cache krb5-dev krb5-libs krb5 krb5-static 2>/dev/null
    
    # Verify installation
    echo "Checking for dynamic libraries..."
    ls -la /usr/lib/libgssapi* 2>/dev/null || echo "No libgssapi dynamic libraries found"
    echo "Checking for static libraries..."
    ls -la /usr/lib/libgssapi*.a /usr/lib/libkrb5*.a 2>/dev/null || echo "No static libraries found"
    
    if [ -f "/usr/lib/libgssapi_krb5.so.2" ] || [ -f "/usr/lib/libgssapi_krb5.a" ]; then
        echo "krb5 packages installed successfully"
    else
        echo "Warning: krb5 libraries may not be available"
    fi
else
    echo "Not in Docker environment, skipping krb5 installation"
fi