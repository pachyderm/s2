# s2 example

This example builds off of s2 and [gorm](http://gorm.io/) to provide an S3-like API, with objects stored in an in-memory sqlite instance. It has a few shortcomings:

1) Data will be dropped when the process exits.
2) Object listing has been purposefully simplified.
3) There is no support for versioning.

But it should be enough to demonstrate how to use s2.

## Tests

To run tests, start the server with `make run`. Then, in a separate shell, run `make test`. After the tests complete, stats will be printed out regarding which tests failed. Complete logs are available in `test/runs`.
