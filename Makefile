PROJ_DIR := $(dir $(abspath $(lastword $(MAKEFILE_LIST))))

# Configuration of extension
EXT_NAME=postgres_scanner
EXT_CONFIG=${PROJ_DIR}extension_config.cmake

# Include the Makefile from extension-ci-tools
include extension-ci-tools/makefiles/duckdb_extension.Makefile

# Ensure krb5 packages are available before running tests
test_release: 
	@if [ "$$LINUX_CI_IN_DOCKER" = "1" ]; then \
		echo "Installing krb5 packages before tests..."; \
		$(PROJ_DIR)install_krb5.sh; \
	fi
	$(MAKE) -f extension-ci-tools/makefiles/duckdb_extension.Makefile test_release

test_debug:
	@if [ "$$LINUX_CI_IN_DOCKER" = "1" ]; then \
		echo "Installing krb5 packages before tests..."; \
		$(PROJ_DIR)install_krb5.sh; \
	fi
	$(MAKE) -f extension-ci-tools/makefiles/duckdb_extension.Makefile test_debug