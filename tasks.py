"""
Tasks for maintaining the project.
Execute 'invoke --list' for guidance on using Invoke
"""
import shutil
from ruamel.yaml import YAML
import pprint
import sys

from invoke import task
import webbrowser
from pathlib import Path

Path().expanduser()
yaml = YAML()

CMD_PKG = "github.com/romnnn/mongoimport/cmd/mongoimport"
PKG = "github.com/romnnn/mongoimport"

ROOT_DIR = Path(__file__).parent
BUILD_DIR = ROOT_DIR.joinpath("build")
TRAVIS_CONFIG_FILE = ROOT_DIR.joinpath(".travis.yml")


def _delete_file(file):
    try:
        file.unlink(missing_ok=True)
    except TypeError:
        # missing_ok argument added in 3.8
        try:
            file.unlink()
        except FileNotFoundError:
            pass


@task
def format(c):
    """Format code
    """
    c.run("pre-commit run go-fmt --all-files")
    c.run("pre-commit run go-imports --all-files")


@task
def test(c):
    """Run tests
    """
    c.run("env GO111MODULE=on go test -v -race ./...")


@task
def cyclo(c):
    """Check code complexity
    """
    c.run("pre-commit run go-cyclo --all-files")


@task
def lint(c):
    """Lint code
    """
    c.run("pre-commit run go-lint --all-files")
    c.run("pre-commit run go-vet --all-files")


@task
def install_hooks(c):
    """Install pre-commit hooks
    """
    c.run("pre-commit install")


@task
def pre_commit(c):
    """Run all pre-commit checks
    """
    c.run("pre-commit run --all-files")


@task(
    help=dict(
        publish="Publish the coverage result to codecov.io (default False)",
    ),
)
def coverage(c, publish=False):
    """Create coverage report
    """
    c.run("env GO111MODULE=on go test -v -race -coverprofile=coverage.txt -coverpkg=all -covermode=atomic ./...")
    if publish:
        # Publish the results via codecov
        c.run("bash <(curl -s https://codecov.io/bash)")


@task
def cc(c):
    """Build the project for all architectures
    """
    c.run(
        'gox -os="linux darwin windows" -arch="amd64" -output="build/{{.Dir}}-{{.OS}}-{{.Arch}}" -ldflags "-X main.Rev=`git rev-parse --short HEAD`" -verbose %s' % CMD_PKG)


@task
def build(c):
    """Build the project
    """
    c.run("pre-commit run go-build --all-files")


@task
def run(c):
    """Run the cmd target
    """
    options = sys.argv[3:]
    c.run(
        'go run {} {}'.format(CMD_PKG, " ".join(options)))


@task
def clean_build(c):
    """Clean up files from package building
    """
    c.run("rm -fr build/")


@task
def clean_coverage(c):
    """Clean up files from coverage measurement
    """
    c.run("find . -name 'coverage.txt' -exec rm -fr {} +")


@task(pre=[clean_build, clean_coverage])
def clean(c):
    """Runs all clean sub-tasks
    """
    pass
