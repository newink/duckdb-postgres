#!/bin/bash

# Install krb5 packages if we're in Docker environment
if [ "$LINUX_CI_IN_DOCKER" = "1" ]; then
    echo "Installing krb5 packages in Docker environment..."
    
    # Detect package manager and install appropriate packages
    if command -v yum >/dev/null 2>&1; then
        echo "Detected yum package manager (RHEL/CentOS)"
        yum install -y krb5-devel krb5-libs krb5-workstation 2>/dev/null
    elif command -v apt-get >/dev/null 2>&1; then
        echo "Detected apt package manager (Debian/Ubuntu)"
        apt-get update && apt-get install -y libkrb5-dev libgssapi-krb5-2 krb5-user 2>/dev/null
    elif command -v apk >/dev/null 2>&1; then
        echo "Detected apk package manager (Alpine)"
        apk add --no-cache krb5-dev krb5-libs krb5 krb5-static 2>/dev/null
    else
        echo "No supported package manager found"
        exit 1
    fi
    
    # Verify installation - check multiple common library paths
    echo "Checking for dynamic libraries..."
    ls -la /usr/lib64/libgssapi* /usr/lib/libgssapi* /usr/local/lib/libgssapi* 2>/dev/null || echo "No libgssapi dynamic libraries found"
    echo "Checking for static libraries..."
    ls -la /usr/lib64/libgssapi*.a /usr/lib64/libkrb5*.a /usr/lib/libgssapi*.a /usr/lib/libkrb5*.a 2>/dev/null || echo "No static libraries found"
    
    # Check for successful installation in common paths
    if [ -f "/usr/lib64/libgssapi_krb5.so.2" ] || [ -f "/usr/lib/libgssapi_krb5.so.2" ] || \
       [ -f "/usr/lib64/libgssapi_krb5.a" ] || [ -f "/usr/lib/libgssapi_krb5.a" ] || \
       [ -f "/usr/local/lib/libgssapi_krb5.so" ]; then
        echo "krb5 packages installed successfully"
    else
        echo "Warning: krb5 libraries may not be available"
    fi
else
    echo "Not in Docker environment, skipping krb5 installation"
fi