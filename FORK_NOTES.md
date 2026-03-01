# Fork Notes

This repository is a maintained fork of `byudaniel/redis-streams`.

## Goals

- Publish and support `@motivaventures/redis-streams` without waiting on upstream.
- Regularly pull useful upstream improvements.
- Upstream broadly useful fixes in parallel when possible.

## Current Intentional Divergence

- Package name/scope is `@motivaventures/redis-streams`.
- Publishing targets GitHub Packages for the `motivaventures` org.
- CI uses semantic-release to version, tag, release, and publish from `master`.

## Branch Roles

- `master`: Motiva release line (source of truth for published fork).
- `upstream-sync`: long-lived integration branch tracking `upstream/master`.
- `motiva/*`: feature/fix branches merged into `master`.
- `upstream/*`: optional branches used to open PRs back to upstream.

## Upstream Sync Cycle

1. `git fetch upstream origin`
2. `git checkout upstream-sync`
3. `git merge upstream/master`
4. Resolve conflicts and commit.
5. `git checkout master`
6. `git merge upstream-sync`
7. Run tests/build and release as needed.

## Upstream Contribution Cycle

For fixes that should go upstream:

1. Branch from `upstream/master` into `upstream/<topic>`.
2. Cherry-pick or reimplement the generic fix.
3. Push to fork and open PR targeting `byudaniel/redis-streams:master`.

This keeps Motiva releases independent while still collaborating upstream.
