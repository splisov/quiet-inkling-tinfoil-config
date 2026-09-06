# Quiet Inkling confidential inference staging

Public deployment configuration and release measurements for Quiet Inkling's
staging gateway. This repository does not contain the app source, gateway source,
private container image, credentials, signing private keys, or practitioner data.

## Status

Bootstrap only. No Tinfoil enclave or model release is approved or deployed by
this repository. The configuration file is intentionally absent until a private
registry image has a verified immutable manifest digest and the runtime/network
configuration has been reviewed. There are no placeholder image digests.

Both release workflows are gated by the repository variable
`STAGING_RELEASES_ENABLED`, which is unset. Do not enable it until configuration,
private-image access and release measurements have been reviewed. A GitHub
release is not permission to enable inference in the application.

## Deployment boundary

The intended CPU gateway verifies single-use admission, enforces bounded speech
operations, and connects to the approved Tinfoil speech model through an attested
connection. GPU transcription uses the separately billed hosted inference API.
Clients must verify an owner-approved exact gateway release before sending audio.

Only deployment settings, secret names, release workflows and this README belong
here. Secret values must remain in their designated secret stores. No live
privacy, accuracy or latency qualification is claimed by this bootstrap.

## Workflow provenance

Release workflows follow the process documented in the official
[Tinfoil Containers template](https://github.com/tinfoilsh/tinfoil-containers-template),
with a default-disabled staging gate. The upstream actions are pinned by commit.
