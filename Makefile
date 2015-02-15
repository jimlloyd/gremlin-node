.PHONY: package lint test mocha mvn-test

default: package lint test

clean:
	mvn clean

package:
	mvn -DskipTests=true clean package

lint:
	node_modules/jshint/bin/jshint --verbose bin lib test

test: lint mvn-test mocha

mvn-test:
	mvn test

mocha: lint
	node_modules/mocha/bin/mocha --timeout 5s --reporter=spec --ui tdd
