.PHONY: run integration-test conformance-test

run:
	go run main.go

integration-test:
	go test ./...

conformance-test:
	../conformance/conformance.py --s3tests-config=test/s3tests.yaml --ignore-config=test/ignore.conf --runs-dir=test/runs
