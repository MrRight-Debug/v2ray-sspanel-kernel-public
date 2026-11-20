# Deployment and Monitoring Templates

These files are sanitized templates. Replace every placeholder explicitly and
review the target paths before use.

No real panel URL, API key, node UUID, server address, or environment file is
included.

## Deployment

`deploy-binary.sh` validates the artifact hash and candidate
configuration, atomically replaces the target binary, restarts only the named
service, and verifies the running executable hash.

## Monitoring

`readonly-snapshot.sh` runs on the target host and prints a local
snapshot. Execute it through the existing administrative access path; the
template contains no SSH configuration or credentials.

