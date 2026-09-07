package watcher

// ProtocolVersion is the version of the watcher gRPC contract.
// A client refusing to talk to a differing server degrades to
// "every target is dirty".
const ProtocolVersion = "1"
