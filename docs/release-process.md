# Release Process

This page is for maintainers who publish a TN node release. Node operators read the result on the release page, and the [Node Upgrade Guide](node-upgrade-guide.md#is-the-upgrade-required) explains it to them.

## The Upgrade Line

The release notes open with one line that says whether node operators must install the release. `release.yaml` and `publish-node-image.yaml` check that line before they build anything. A release without it gets no binaries and no image, and the `latest` image tag does not move.

The **Draft release** form writes this line for you; see [Publishing a Release](#publishing-a-release).

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

- Publish a required release at least 7 days before its mainnet block, and state its testnet block on the same line. The **Draft release** form refuses a mainnet time less than 7 days away unless **Short notice** is ticked.
- Pick each block from the measured block rate, as in [Pick the activation height](maa-rollout.md#pick-the-activation-height), and round the time.
- A migration that needs the new release, for example one whose actions call a method only that release has, is applied on a network only after the release's block on that network.
- Where the change waits on an on-chain network parameter that the release adds, as `maa_exec` waits on `maa_activation_height`, the release's block is the earliest block in which the resolution that sets the parameter may pass, not the height that resolution sets. A node on an older release cannot read the parameter and halts once that resolution passes, whatever height it sets (see [Failure modes and rollback](maa-rollout.md#failure-modes-and-rollback)). If operators need more time, hold the resolution back and edit the release notes to the new block and time. After the resolution has passed, a follow-up resolution can still move the activation height later (see [Re-scheduling](maa-rollout.md#re-scheduling)), but that only delays the change on upgraded nodes and gives no more time to a node on an older release.
- A height written into the code moves only with a new release.

## Where Release Notices Go Out

Release notices go out on the [release page](https://github.com/trufnetwork/node/releases). Operators get them by choosing **Watch → Custom → Releases** on this repository, or from the feed at <https://github.com/trufnetwork/node/releases.atom>.

`release.yaml` also posts one message about each release to Telegram; see [The Telegram Announcement](#the-telegram-announcement).

## The Telegram Announcement

After `release.yaml` uploads the binaries, it posts one message to the Telegram chat and topic that the node alerts use. The repository secrets `TELEGRAM_BOT_TOKEN`, `TELEGRAM_CHAT_ID` and `TELEGRAM_TOPIC_ID` set the bot, the chat and the topic. The message is built from the upgrade line and the tag. For a required release, it names the networks, the block and its time on each, and the version that `kwild version` prints after the upgrade, and it links to the release page and to [Required In-Place Upgrade](node-upgrade-guide.md#required-in-place-upgrade). For an optional release, it says that the upgrade is optional and links to the release page.

The message goes out once per release. `release.yaml` reads the release's assets before it builds, and posts only when the release had none and its upload succeeded. That is the run of the publish or, when that run failed, the run of the edit that fixes the release. Later edits find the binaries in place and post nothing, and a manual run of the workflow posts nothing.

A failed post does not fail the release; the run shows a warning. To see the message, or to post it by hand, run the script with the tag, the release page URL and the notes:

```bash
scripts/release/announce-telegram.sh --dry-run v2.6.9 https://github.com/trufnetwork/node/releases/tag/v2.6.9 < notes.md
```

Without `--dry-run`, it posts the message to the chat in `TELEGRAM_CHAT_ID`, in the topic in `TELEGRAM_TOPIC_ID` when that is set, with the bot in `TELEGRAM_BOT_TOKEN`. Without the token or the chat id, as in a fork, it posts nothing and exits without an error. It refuses notes that `check-upgrade-line.sh` refuses.

`scripts/release/test-announce-telegram.sh` tests the messages and the post against a local fake Telegram. CI runs it.

## Publishing a Release

The normal way is the **Draft release** form. It writes the upgrade line, adds the notes that GitHub generates, and saves the release as a draft for you to review and publish. Writing the notes by hand is the fallback.

### With the Draft Release Form

1. On the repository's **Actions** tab, choose **Draft release**, then **Run workflow**. Leave **Use workflow from** on `main`.
2. Fill in the form:
   - **Tag**: the new release's tag, `vX.Y.Z`. No release, draft or tag may have it yet.
   - **Target**: the branch or commit to release, `main` by default. The draft is pinned to the commit that the target points at when the run starts, so a later merge does not slip into the release.
   - **Mainnet** and **Testnet**: `optional` or `required`; see [Optional or Required](#optional-or-required).
   - For each required network, its **block** and its **time**, in UTC as `YYYY-MM-DD HH:MM`; see [Notice for a Required Release](#notice-for-a-required-release). Leave both empty for an optional network. The run refuses a block or a time given for an optional network, so that nothing you typed is dropped.
   - **Summary**: optional. One line shown under the upgrade line, above the generated notes.
   - **Short notice**: tick it only for an urgent release whose mainnet time is less than 7 days away. The run then warns instead of refusing.
3. Click **Run workflow**. The run checks every answer before it creates anything and lists each problem it finds. It warns when a required block is not above that network's current height. It writes the notes: the upgrade line, then the summary when given, then the notes that GitHub generates since the previous release. It checks them with `check-upgrade-line.sh` and saves them as a draft release.
4. Open the run. Its summary shows the upgrade line, the Telegram message that the release will post, and the link to the draft.
5. Open the draft and review it. You can edit it, keeping the upgrade line first. Editing a draft starts no workflow.
6. Click **Publish release**. That starts the workflows in [What Publishing Starts](#what-publishing-starts). Publish soon after the run: the 7 days of notice count from the run, not from the publish.

The run stops at a draft because a release that a workflow creates with its `GITHUB_TOKEN` does not start other workflows. Your click on **Publish release** does, so the check, the binaries, the image and the Telegram announcement run as for a release written by hand.

`scripts/release/compose-release-notes.sh` checks the form and writes the upgrade line. `scripts/release/test-compose-release-notes.sh` tests it; CI runs it.

### By Hand

When the form cannot be used:

1. Draft the release from its tag, generate the notes, and put the upgrade line first.
2. Check the notes with `scripts/release/check-upgrade-line.sh`.
3. Publish.

### What Publishing Starts

1. `release.yaml` builds the four `kwild` tarballs, uploads them to the release and posts the Telegram announcement. `publish-node-image.yaml` pushes the image as `v<version>`, `<version>`, `sha-<short commit>` and `latest`.
2. If the line was missing or wrong, both workflows fail before they build. Edit the release to fix the line. `release.yaml` runs again on the edit, uploads the binaries and posts the announcement. `publish-node-image.yaml` runs only when a release is published, so run **Publish Node Image** by hand on the release tag with **Also tag this build as latest**. A manual run has no release notes to read, so it skips the check.
3. After a required release, run **Build AMI** so that nodes launched from the AMI start on it.

Every edit of the release notes runs `release.yaml` again, which builds and uploads the binaries again. It does not post the announcement again.
