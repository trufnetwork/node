# Release Process

This page is for maintainers who publish a TN node release. Node operators read the result on the release page, and the [Node Upgrade Guide](node-upgrade-guide.md#is-the-upgrade-required) explains it to them.

## The Upgrade Line

The release notes open with one line that says whether node operators must install the release. `release.yaml` and `publish-node-image.yaml` check that line before they build anything. A release without it gets no binaries and no image, and the `latest` image tag does not move.

The line is exactly one of these four forms:

- Optional on both networks:
  ```text
  Upgrade: optional.
  ```
- Required on both networks:
  ```text
  Upgrade: required on mainnet from block <N> (about <YYYY-MM-DD HH:MM> UTC), required on testnet from block <M> (about <YYYY-MM-DD HH:MM> UTC).
  ```
- Required on mainnet only:
  ```text
  Upgrade: required on mainnet from block <N> (about <YYYY-MM-DD HH:MM> UTC), optional on testnet.
  ```
- Required on testnet only:
  ```text
  Upgrade: optional on mainnet, required on testnet from block <M> (about <YYYY-MM-DD HH:MM> UTC).
  ```

For example:

```text
Upgrade: required on mainnet from block 3300000 (about 2026-12-10 12:00 UTC), required on testnet from block 16300000 (about 2026-12-03 12:00 UTC).
```

The check is strict:

- The line is the first line of the notes, with no heading, blank line or formatting before or around it. The generated "What's Changed" list starts on the next line.
- Mainnet comes first, then testnet.
- A block is a whole number with no separators and no leading zero.
- The time is in UTC, written `YYYY-MM-DD HH:MM`. It is an estimate from the block rate; the block is what counts.
- A Windows line end (`\r\n`), which GitHub's editor can save, is accepted. Anything else around the line is not.

To check notes before you publish them:

```bash
scripts/release/check-upgrade-line.sh < notes.md
```

`scripts/release/test-check-upgrade-line.sh` tests the check against every form and a list of near misses. CI runs it.

## Optional or Required

A release is required on a network when a node that still runs the release before it would compute a different app hash for a block that network will produce. For example, the release:

- changes how blocks, transactions or actions run from a block height or a network parameter on, or
- adds a method that an action or a migration on that network will call.

A release that changes nothing a block computes, such as tooling, documentation or a fix outside block execution, is optional.

A required release must run every block before its block the same way as the release before it, so that operators can install it at any time before the block.

## Notice for a Required Release

- Publish a required release at least 7 days before its mainnet block, and state its testnet block on the same line.
- Pick each block from the measured block rate, as in [Pick the activation height](maa-rollout.md#pick-the-activation-height), and round the time.
- A migration that needs the new release, for example one whose actions call a method only that release has, is applied on a network only after the release's block on that network.
- Where the change waits on an on-chain network parameter, as `maa_exec` waits on `maa_activation_height`, a resolution can move its height later if operators need more time, as long as it passes before the earlier height (see [Re-scheduling](maa-rollout.md#re-scheduling)). Then edit the release notes to the new block and time. A height written into the code moves only with a new release.

## Where Release Notices Go Out

Release notices go out on the [release page](https://github.com/trufnetwork/node/releases). Operators get them by choosing **Watch → Custom → Releases** on this repository, or from the feed at <https://github.com/trufnetwork/node/releases.atom>.

## Publishing a Release

1. Draft the release from its tag, generate the notes, and put the upgrade line first.
2. Check the notes with `scripts/release/check-upgrade-line.sh`.
3. Publish. `release.yaml` builds the four `kwild` tarballs and uploads them to the release. `publish-node-image.yaml` pushes the image as `v<version>`, `<version>`, `sha-<short commit>` and `latest`.
4. If the line was missing or wrong, both workflows fail before they build. Edit the release to fix the line. `release.yaml` runs again on the edit and uploads the binaries. `publish-node-image.yaml` runs only when a release is published, so run **Publish Node Image** by hand on the release tag with **Also tag this build as latest**. A manual run has no release notes to read, so it skips the check.
5. After a required release, run **Build AMI** so that nodes launched from the AMI start on it.

Every edit of the release notes runs `release.yaml` again, which builds and uploads the binaries again.
