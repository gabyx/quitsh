### Development Guide

This guide documents our procedures and policies for project maintenance tasks,
including managing our conventions, pull/merge-requests, continuous integration,
releasing.

## Commit Convention

We use the
[conventional commits](https://www.conventionalcommits.org/en/v1.0.0/)
specification for commit messages and pull/merge-request titles.

<!-- Additional 'scopes' should be described here.-->
<!-- ### Scopes -->

## Regenerating the Watcher gRPC Code

The Go code in [`pkg/watcher/proto`](../pkg/watcher/proto) is generated from
`watcher.proto` and committed, so `go build` works without a protobuf toolchain.
After editing the schema, run:

```shell
just generate-proto
```

The recipe uses [`buf`](https://buf.build), which compiles protobuf without
`protoc`, and runs the code generator plugins through `go run`. Nothing needs to
be installed beyond Go itself.
