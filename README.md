# Quiet Inkling confidential STT staging

Public source and deployment measurements for the audio-only staging gateway.
The private Android app, text coordinator, interpretation prompts, corpus,
credentials and practitioner data are not published here.

`gateway/source-manifest.json` pins the explicit reviewed source export from
private app commit `4bdae95c`. Third-party notices accompany the source and image.
The published first-party source is available for inspection; no additional
open-source license grant is made by this staging publication.

The CPU gateway verifies single-use admission, limits speech operations, and
connects to the separately billed hosted speech model through verified TLS.
Clients require the owner's exact approved gateway release before sending audio.
A public image does not relax attestation, confinement or promotion checks.

Status: audio source published for staging. A source commit or container build
does not mean an enclave is deployed, approved or qualified. The Tinfoil release
workflows remain gated by `STAGING_RELEASES_ENABLED` until the configuration is
ready. Runtime secret values belong only in the designated secret stores.

The image publisher is manually dispatched and writes only the personal package
`ghcr.io/splisov/quiet-inkling-stt-public`. It uses this repository's temporary
Actions token, never a personal package token or another organization's registry.
