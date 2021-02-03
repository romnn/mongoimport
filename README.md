## Mongoimport

[![Build Status](https://github.com/romnn/mongoimport/workflows/test/badge.svg)](https://github.com/romnn/mongoimport/actions)
![GitHub](https://img.shields.io/github/license/romnn/mongoimport)
[![GoDoc](https://godoc.org/github.com/romnn/mongoimport?status.svg)](https://godoc.org/github.com/romnn/mongoimport)
[![Test Coverage](https://codecov.io/gh/romnn/mongoimport/branch/master/graph/badge.svg)](https://codecov.io/gh/romnn/mongoimport)
[![Release](https://img.shields.io/github/release/romnn/mongoimport)](https://github.com/romnn/mongoimport/releases/latest)
[![Docker Pulls](https://img.shields.io/docker/pulls/romnn/mongoimport)](https://hub.docker.com/r/romnn/mongoimport)

CLI and go library for importing data from CSV, JSON or XML files into MongoDB.

```bash
go run github.com/romnn/mongoimport/cmd/mongoimport --db-user=root --db-password=example csv <path-to-csv-files>
```
You can also download pre built binaries from the [releases](https://github.com/romnn/mongoimport/releases) page.

For a list of options, run
```bash
go run github.com/romnn/mongoimport/cmd/mongoimport csv --help
```

#### Usage as a library

Using the tool as a standalone CLI tool is great for quick loading of a few files. However, you might need more fine-grained control over what files are imported into which collection or perform additional pre/post processing (e.g. parsing timestamps). For this use case, we offer a very extensivle and modular API for configuring your imports.

```golang
import "github.com/romnn/mongoimport"

// example t.b.a
```

For more examples, see `examples/`.

#### Development

All commits are automatically built and tested via github actions. In order to pass the required checks it is strongly recommended to install the repositories pre commit hooks (assuming you are in the repositories root):
```bash
pip install pre-commit invoke bump2version
pre-commit install
```

The pre commit hooks will run a number of go tools. Try to run `pre-commit run --all-files` and check for missing tools. You might need to install some of those:
```bash
go get -u golang.org/x/lint/golint
go get -u github.com/fzipp/gocyclo
```
In order to be found, make sure to include `$GOPATH/bin` in your `$PATH` (`$GOPATH` has to be set of course like `export GOPATH="$HOME/go` for example).

Before committing, `pre-commit` is run to make sure all checks pass!
