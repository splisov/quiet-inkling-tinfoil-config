# Quiet Inkling confidential STT staging

Public source and deployment measurements for the audio-only staging gateway.
The private Android app, text coordinator, interpretation prompts, corpus,
credentials and practitioner data are not published here.

`gateway/source-manifest.json` pins the reviewed audio-only source and tests.
The callback correction starts from the exact public v0.0.5 commit
`6505a72a229e5f6030a13a2cc405d3326c3d1df4`; every original manifest file was
verified before applying the narrow audio claim/renewal patch. The historical
private-export pointer did not identify that exact published tree. Third-party
notices accompany the source and image.
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

Audio claim callbacks have a 30-second maximum; renewal callbacks have a
60-second maximum clipped by existing paid authority and policy. Startup also
obeys the original capability and conservative lease deadlines. The existing
30-second-ahead byte forecast wakes one serialized renewal loop; only a valid
broker acknowledgment increases authority. Short recordings may reserve the
next 30-second quantum earlier. The roots, model pins, initial/maximum bytes,
lease length, clock margin and single provider connection remain bounded.
A source change or local image still requires measured-release approval and
installed-device connection, renewal and reconnect qualification.
