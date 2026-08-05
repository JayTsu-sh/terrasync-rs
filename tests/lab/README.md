# Terrasync integration lab

The lab is shared by `nfs-rs`, `data-mover-rs`, and `terrasync-rs`.

| Role | Management | Data | Services |
|---|---|---|---|
| Controller | 10.131.9.11 | 10.10.1.11 | GitHub Actions Runner |
| Source | 10.131.9.12 | 10.10.1.12 | NFSv3, NFSv4.1, RustFS |
| Destination | 10.131.9.13 | 10.10.1.13 | NFSv3, NFSv4.1, RustFS |
| Worker | 10.131.9.14 | 10.10.1.14 | RustFS, fault injection |

Every run must call `prepare-run.sh` with a unique `nightly-*` or `release-*`
identifier and call `cleanup-run.sh` from an `always()` step.

Management traffic uses `10.131.9.0/20`. Test data uses `10.10.1.0/24`.
Credentials are provisioned on the self-hosted runner and must not be committed.

The self-hosted runner uses the same environment as `data-mover-rs`. It must
provide `LAB_S3_ACCESS_KEY` and `LAB_S3_SECRET_KEY`, Python 3 with `boto3`, and
the `terrasync-lab` SSH key. Direct data-network traffic must bypass any system
HTTP proxy.

`run-e2e.sh` replaces the former protocol-specific Claude E2E skills with a
repeatable CI-owned suite. It runs the complete directed 4-by-4 synchronization
matrix across Local, NFSv3, NFSv4.1, and S3, verifies every destination by
SHA-256, and independently runs quick and full integrity checks. It also covers
same-backend incremental synchronization and the local filter contract. The
nightly workflow separately runs the real two-process QUIC tests.

The current lab has no SMB/CIFS endpoint. CIFS remains covered by unit and
integration tests, but is intentionally not claimed as a nightly physical-lab
backend. Add source and destination Samba services before adding CIFS to this
matrix.
