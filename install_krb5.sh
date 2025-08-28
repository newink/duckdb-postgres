#!/usr/bin/env bash
set -euo pipefail

if [ "${LINUX_CI_IN_DOCKER:-0}" = "1" ]; then
    echo "Installing krb5 packages in Docker environment..."

    # Detect package manager and install appropriate packages
    if command -v apk >/dev/null 2>&1; then
        echo "Detected Alpine Linux - using apk"
        echo "Updating package index..."
        apk update
        echo "Available krb5 packages:"
        apk search krb5 || true
        echo "Installing krb5 packages..."
        apk add --no-cache krb5-dev krb5-libs krb5 2>/dev/null || \
        apk add --no-cache krb5-dev krb5 2>/dev/null || \
        apk add --no-cache libkrb5-dev libkrb5-3 2>/dev/null || \
        echo "Failed to install krb5 packages via apk"

    elif command -v apt-get >/dev/null 2>&1; then
        echo "Detected Debian/Ubuntu - using apt-get"
        apt-get update
        apt-get install -y libkrb5-dev libgssapi-krb5-2 krb5-user 2>/dev/null || \
        echo "Failed to install krb5 packages via apt"

    elif command -v yum >/dev/null 2>&1; then
        echo "Detected RHEL/CentOS - using yum"
        yum install -y krb5-devel krb5-libs krb5-workstation 2>/dev/null || \
        echo "Failed to install krb5 packages via yum"

    else
        echo "No supported package manager found (apk, apt-get, yum)"
    fi

    # Verify installation - check multiple library paths
    echo "Checking for GSSAPI/Kerberos libraries in common locations..."
    for libdir in /usr/lib /usr/lib64 /usr/local/lib /lib /lib64; do
        if [ -d "$libdir" ]; then
            echo "Checking $libdir:"
            ls -la $libdir/libgssapi* $libdir/libkrb5* 2>/dev/null || true
        fi
    done

    # Check for header files
    echo "Checking for header files..."
    find /usr/include -name "gssapi.h" -o -name "krb5.h" 2>/dev/null || \
        echo "No GSSAPI/Kerberos headers found"

    # Check pkg-config availability
    if command -v pkg-config >/dev/null 2>&1; then
        echo "Checking pkg-config for krb5:"
        pkg-config --exists krb5-gssapi && \
            echo "krb5-gssapi found via pkg-config" || \
            echo "krb5-gssapi not found via pkg-config"
        pkg-config --exists krb5 && \
            echo "krb5 found via pkg-config" || \
            echo "krb5 not found via pkg-config"
        pkg-config --exists mit-krb5-gssapi && \
            echo "mit-krb5-gssapi found via pkg-config" || \
            echo "mit-krb5-gssapi not found via pkg-config"
    fi

    if [ -f "/usr/lib/libgssapi_krb5.so.2" ] || \
       [ -f "/usr/lib/libgssapi_krb5.a" ]; then
        echo "krb5 packages installed successfully"
    else
        echo "krb5 installation may be incomplete"
    fi
fi