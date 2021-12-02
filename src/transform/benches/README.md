# `mz-bench-transform`

The `mzbench-transform` package contains scripts for benchmarking the `transform` crate.

## Installation (`pyenv`)

Run the following commands only once on each host.

```bash
pyenv virtualenv 3.9.5 mzbench-transform
```

Run the following commands whenever the contents of this folder change.

```bash
# activate mzbench-transform environment
pyenv activate mzbench-transform
# install package in edit mode
pip install -e .
# install zsh shell completion 
mzbench-transform --install-completion zsh
rm -f ~/.zcompdump; compinit
```

## Run Benchmarks

```bash
mzbench-transform init # create schema
```