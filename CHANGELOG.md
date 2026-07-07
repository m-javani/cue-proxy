# Changelog

## [0.3.0] - 2026-07-07
### Changed
- **BREAKING**: Discard resolver and verifier components
- Simplified QUIC verifier implementation
- Update discovery mechanism to use cluster-based updates
- Replace test containers with Docker Compose

### Fixed
- Streamline verification process by removing redundant resolver/verifier layers
- Improve discovery reliability with cluster-aware updates

## [0.2.0] - 2026-07-01
### Changed
- Bump github.com/m-javani/cue dependency from v0.1.0 to v0.2.0
- Version now matches cue library version (0.2.0)

### Fixed
- Require proxy-id flag (no longer auto-generated)
- Validate proxy-id matches certificate identity (CN or SAN)
- Update CLI usage examples to include required proxy-id

### Security
- Prevent misconfiguration where proxy-id doesn't match certificate

## [0.1.0] - 2026-06-28
### Added
- Initial release