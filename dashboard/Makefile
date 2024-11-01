# print out test coverage to sysout and into coverage directory. We add .PHONY
# to make sure that it always runs even if the coverage directory has been
# generated.
.PHONY: coverage
coverage: install-dev
	npm test -- --coverage

# Run all tests in src/ that end in .test.js or .spec.js. Outside of the CI this
# runs tests on all affected files that would be committed.
test: install-dev
	npm run test

clean:
	rm build/.install-dev

# format formats all HTML/CSS/JS/JSON/etc. files in-place.
format: install-dev
	npx prettier --write .

# returns a non-zero value if code is not formatted.
check-format: install-dev
	npx prettier --check .

install-dev: build/.install-dev

# install-dev installs all dashboard dependencies.
build/.install-dev: package.json package-lock.json
	npm install
	mkdir -p build && touch build/.install-dev
