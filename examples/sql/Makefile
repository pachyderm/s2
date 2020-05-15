.PHONY: run test integration-test conformance-test

run:
	go run main.go

test: integration-test conformance-test

integration-test:
	cd ../.. && make ./integration/venv
	cd ../../integration && . venv/bin/activate && \
		S3_ADDRESS=http://localhost:8080 S3_ACCESS_KEY=homer S3_SECRET_KEY=donuts \
		pytest test.py

conformance-test:
	../../conformance/conformance.py --s3tests-config=test/s3tests.yaml --ignore-config=test/ignore.conf --runs-dir=test/runs
