# Quiet Inkling STT gateway publication candidate

This is an audio-only export, not a deployed or approved service.
Build with Dockerfile.audio. Run `go test -tags audioonly ./gateway`.
The manifest hashes the reviewed source allowlist. No application source,
interpretation prompts, credentials, or journal corpus is included.

The workload requires owner-signed exact-release policy, verified downstream
TLS, single-use broker admission, bounded audio and explicit finalization.
A policy containing textIngress is rejected. Model selection is frozen for the
loaded policy: changing models requires a new approved signed policy and workload
restart, not a caller-supplied model name. Attestation must cover the GPU path.

Third-party license texts are included in the built image. Publication rights
and the final image, network configuration and runtime measurement must be reviewed
before deployment. This source bundle does not promote remote inference.
