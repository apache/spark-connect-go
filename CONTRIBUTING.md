## Contributing to Spark

*Before opening a pull request*, review the
[Contributing to Spark guide](https://spark.apache.org/contributing.html).
It lists steps that are required before creating a PR. In particular, consider:

- Is the change important and ready enough to ask the community to spend time reviewing?
- Have you searched for existing, related JIRAs and pull requests?
- Is this a new feature that can stand alone as a [third party project](https://spark.apache.org/third-party-projects.html) ?
- Is the change being proposed clearly explained and motivated?

When you contribute code, you affirm that the contribution is your original work and that you
license the work to the project under the project's open source license. Whether or not you
state this explicitly, by submitting any copyrighted material via pull request, email, or
other means you agree to license the material under the project's open source license and
warrant that you have the legal authority to do so.


### Code Style and Checks

When submitting code we use a number of checks in our continous integration system to ensure
a consitent style and adherence to license rules. You can run these checks locally by running:

```bash
make check
```

This requires the following tools to be present in your PATH:

1. Java for checking license headers
2. `gofumpt` for formatting Go code
3. `golangci-lint` for linting Go code


### How to write tests

Please make sure that you have proper testing for the new code your adding. As part of the
code base we started to add mocks that allow you to simulate a lot of the necessary API
and don't require a running Spark instance.

`mock.ProtoClient` is a mock implementation of the `SparkConnectService_ExecutePlanClient`
interface which is the server-side stream of messages coming as a response from the server.

`testutils.NewConnectServiceClientMock` will create a mock client that implements the
`SparkConnectServiceClient` interface.

The combination of these two mocks allows you to test the client side of the code without
having to connect to Spark.

### What to contribute

We welcome contributions of all kinds to the `spark-connect-go` project. Some examples of
contributions are providing implementations of functionality that is missing in the Go
implementation. Some examples are, but are not limited to:

* Adding an existing feature of the DataFrame API in Golang.
* Adding support for a builtin function in the Spark API in Golang.
* Improving error handling in the client.

If you are unsure about whether a contribution is a good fit, feel free to open an issue
in the Apache Spark Jira.
