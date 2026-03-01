# Fork Workflow

This workflow supports two parallel outcomes:

- fast Motiva releases from this fork, and
- optional PRs back to upstream.

## One-Time Local Setup

```bash
git remote -v
git remote set-url origin https://github.com/motivaventures/redis-streams.git
git remote add upstream https://github.com/byudaniel/redis-streams.git
git fetch origin upstream
git branch --track upstream-sync upstream/master
```

If `upstream-sync` already exists, skip the last command.

## Daily Development (Motiva)

```bash
git fetch origin upstream
git checkout master
git pull --ff-only origin master
git checkout -b motiva/<topic>

# make changes
git add .
git commit -m "<message>"
git push -u origin motiva/<topic>
```

Open PR to `motivaventures/redis-streams:master` and merge when ready.

## Sync Upstream Into Fork

```bash
git fetch upstream origin
git checkout upstream-sync
git merge upstream/master

# resolve conflicts if needed, then:
git push origin upstream-sync

git checkout master
git merge upstream-sync
git push origin master
```

## Send Fixes Back Upstream (Optional)

```bash
git fetch upstream origin
git checkout -b upstream/<topic> upstream/master

# cherry-pick or recreate generic fix
git cherry-pick <commit-sha>
git push -u origin upstream/<topic>

gh pr create --repo byudaniel/redis-streams --base master --head motivaventures:upstream/<topic>
```

## Release Fork Package

```bash
git checkout master
git pull --ff-only origin master
git push origin master
```

Publishing is handled by semantic-release in GitHub Actions.

- Trigger: push to `master` (or manual dispatch).
- Semantic versioning: inferred from Conventional Commit messages.
- Outputs: Git tag (`v*`), GitHub release notes, updated `CHANGELOG.md`, and package publish to GitHub Packages.

Commit message guidance for release automation:

- `fix: ...` -> patch release
- `feat: ...` -> minor release
- `feat!: ...` or `BREAKING CHANGE:` -> major release
