# s2 example

This example uses s2 to provide an S3-like API, with objects stored in-memory. It has a few shortcomings:

1) Data will be dropped when the process exits.
2) There are no optimizations, and quite a bit is implemented naively (e.g. the hash of files are recomputed every time it's necessary.)
3) Object listing has been purposefully simplified.
4) There is no support for versioning.

But it should be enough to demonstrate how to use s2.

## Tests

To run tests, start the server with `make run`. Then, in a separate shell, run `make test`. After the tests complete, stats will be printed out regarding which tests failed. Complete logs are available in `test/runs`.
