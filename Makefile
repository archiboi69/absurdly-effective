.PHONY: format check test build pack docs serve-docs

# Format the published Effect package.
format:
	@cd sdks/effect && vp fmt

# Check the published Effect package.
check:
	@cd sdks/effect && vp lint && vp run type-check

# Test the published Effect package against stock Absurd.
test:
	@cd sdks/effect && vp test run

# Build the npm package.
build:
	@cd sdks/effect && vp run build

# Produce the npm tarball without publishing it.
pack:
	@cd sdks/effect && vp pm pack

ZENSICAL_VERSION ?= 0.0.21

# Build documentation site
docs:
	@uvx --from "zensical==$(ZENSICAL_VERSION)" zensical build
	@touch site/.nojekyll

# Serve documentation locally with live reload
serve-docs:
	@uvx --from "zensical==$(ZENSICAL_VERSION)" zensical serve
