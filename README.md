# messaging-client

A thin client over Google Cloud Pub/Sub.

## Release

`messagingclient` publishes to PyPI by a one-click workflow — no manual tag or version edit.

- **Release:** Actions → **Publish to PyPI** → **Run workflow** → choose `part`
  (`major`/`minor`/`patch`), or `gh workflow run publish.yml -f part=patch`. It computes the next
  version from the latest `vX.Y.Z` tag, tags it, builds, and publishes to PyPI (OIDC trusted
  publishing).
- **Preview:** `dry-run=true` prints the next version without tagging or publishing.

The version is derived from the git tag by `setuptools_scm`; there is no version literal to bump.
