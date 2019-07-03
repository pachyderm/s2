.PHONY: run test

run:
	go run main.go

test:
	../conformance/conformance.py --s3tests-config=test/s3tests.yaml --ignore-config=test/ignore.conf --runs-dir=test/runs
