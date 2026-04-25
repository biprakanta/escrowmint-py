# Contributing

EscrowMint uses Conventional Commits so releases can be versioned and documented automatically.

## Commit Format

Use commit messages like:

- `fix: handle expired reservation reclaim`
- `feat: add chunk lease renew helper`
- `feat!: change reserve conflict semantics`

SemVer mapping:

- `fix:` -> patch release
- `feat:` -> minor release
- `!` or `BREAKING CHANGE:` -> major release

## Release Flow

1. Merge changes into `main` with Conventional Commit messages.
2. Release Please opens or updates a release PR.
3. Merging that release PR updates `CHANGELOG.md`, creates a Git tag, and creates the GitHub release notes.
4. The dedicated `release.yml` workflow verifies the tagged code and publishes the Python package to PyPI.

Prefer squash merges so the final commit title on `main` is a clean Conventional Commit.

Every user-facing change should land with a releasable Conventional Commit such as `feat:` or `fix:` so Release Please can keep the changelog, tag, GitHub release, and PyPI publication in sync.
