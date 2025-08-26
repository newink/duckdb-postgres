PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=postgres_scanner
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Pre-build step to install krb5-dev in Docker environment
configure_ci:
	@if [ "$$LINUX_CI_IN_DOCKER" = "1" ]; then \
		echo "Attempting to install krb5-dev in Docker environment..."; \
		apk add --no-cache krb5-dev 2>/dev/null || echo "Could not install krb5-dev, building without GSSAPI support"; \
	fi

# Override the release target to run configure_ci first
release: configure_ci
	$(MAKE) -f extension-ci-tools/makefiles/duckdb_extension.Makefile release

debug: configure_ci
	$(MAKE) -f extension-ci-tools/makefiles/duckdb_extension.Makefile debug

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile