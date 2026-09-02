# Remote Expert Transfer v8

## Seam

Terrasync owns two deep entry-scoped interfaces:

- `serve_remote_source` revalidates one advertised `ObservedEntry`, negotiates the source-read
  ceiling, applies source-only QoS, streams the requested tail, and waits for the destination
  terminal.
- `receive_remote_destination` selects one session-local NDX, prepares or recovers the staged
  destination, durably registers its opaque recovery identity before requesting payload, writes a
  bounded stream, verifies it, applies the precompiled metadata plan to unpublished state,
  publishes `FinalDestination`, persists recovery completion, and returns one terminal.

Both interfaces use data-mover's expert source/destination halves. They never inspect a backend
kind, protocol handle, backend fact, recovery identity, or metadata encoding.

## Ordered protocol

```text
handshake
  -> opaque advertisement page(s) + advertisement terminal
  -> Destination.TransferRequested(ndx)
  -> Source.Offer(size, source_chunk, identity_key)
  -> Destination.PayloadRequested(durable_prefix, destination_chunk)
  -> Source.Payload(offset, bytes)*
  -> Source.Completed(full_size, full_blake3, identity_key, source_qos)
  -> Destination.Terminal(Completed | Failed | Cancelled)
```

`Offer` and destination requests use the existing FileList bidirectional stream. Payload and
source completion evidence use the same Data stream, so evidence cannot overtake bytes. The final
destination terminal uses AckProgress. A Receiver cannot request an NDX before Sender has revealed
FileList by sending advertisement; a transfer cannot run before the Control-stream handshake.

Protocol v8 adds the staged-metadata terminal phase and is intentionally incompatible with v7 and
earlier.

## Boundedness and chunk negotiation

The source emits chunks no larger than its backend/inflight offer. Terrasync zero-copy slices an
offered chunk when the destination negotiated a smaller limit. The destination limit is also
capped at the QUIC credit window, so one frame can never exceed the sender's admissible credit.

Payload enters a capacity-one destination channel before Receiver returns byte credit. The
connection ledger batches normal grants and flushes sub-threshold residual credit at each transfer
terminal; many small files therefore cannot leak the connection's available window.

## Recovery and failures

Only transfers larger than one negotiated chunk open application recovery persistence. A recovered
durable prefix is backend-observed, not caller-reported. The source hashes the complete logical file
while sending only bytes after that prefix. Recovery identity registration is acknowledged before
payload; the completed event is persisted only after publication.

Wire failures retain phase, side, source QoS, final-destination disposition, and one mutually
exclusive staged-state disposition. Backend-private errors and stage handles never cross the wire.
If a peer fails mid-payload, the local error retains both peer evidence and the owned data-mover
`TransferFailure`, so a recoverable destination stage is not lost. If recovery-completion
persistence fails after publication, the peer terminal explicitly reports that
`final_destination_changed` is true.

## Metadata gate

The destination already reconstructed the exact `ObservedEntry` from its opaque snapshot. It must
compile metadata semantics from those captured observations and an explicit target profile; it must
not re-read the source or switch on a protocol pair.

Terrasync compiles an immutable `MetadataPlan` before target I/O and passes it through
`RemoteExpertDestinationRequest`; the plan and metadata values do not travel on QUIC because the
destination already owns the exact stored observation. Data-mover verifies content, applies the
plan through the destination's stage-owned metadata seam, and only then publishes. Local mutates
the open staged file; NFS, CIFS, HDFS, and S3 resolve their private staged path/key and delegate to
their own metadata adapter. Terrasync never receives that private identity or calls a backend.

Application is fail-fast. A required family failure produces the `Metadata/Destination` terminal,
keeps `FinalDestination` unchanged, retains stage cleanup/recovery authority, and exposes the
partial family report through the data-mover failure. `VerifyOrSkip` is rejected at preflight when
the plan contains mutations, because retaining an existing content-equivalent object would discard
the metadata already applied to the new stage.

## Executable evidence

- capacity-one Local-to-Local transfer with unequal source/destination chunk ceilings;
- source QoS evidence round-trip;
- source-changed and cancellation fail-fast terminals;
- recovery registration and successful completion ordering;
- recovery completion failure after publication;
- mid-payload peer failure retaining a recoverable destination stage;
- the same Local-to-Local state machine over real loopback QUIC;
- stage-owned metadata success and fail-fast pre-publication failure;
- full app and transport regression suites.
