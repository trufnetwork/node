#!/usr/bin/env python3
"""Generate .prod.sql override migrations for the mainnet bridge swap.

This script reads the test-flavored embedded migrations (which reference
the Hoodi testnet bridges `hoodi_tt` for TRUF and `hoodi_tt2` for USDC)
and produces sibling `.prod.sql` files containing CREATE OR REPLACE
overrides that target the mainnet bridges `eth_truf` and `eth_usdc`.

The embedded migration loader in `internal/migrations/migration.go`
explicitly skips files matching `*.prod.sql`, so the generated outputs
are NOT loaded automatically — they must be applied manually after the
embedded migrations run, e.g.::

    kwil-cli exec-sql --file <path> --sync \\
        --private-key $PRIVATE_KEY --provider $PROVIDER

Three transformation modes:

* `core` — the order-book SQL files (031, 032, 033, 037). For each
  action that mentions `hoodi_tt*`, substitute the bridge names and
  collapse `if $bridge = '<X>'` dispatch chains so only the `eth_usdc`
  branch remains. The `validate_bridge` AND-chain is also collapsed.

* `wrap` — the per-bridge ERC20 wrapper actions (erc20-bridge/001,
  004, 005). These define standalone `hoodi_tt_*` and `hoodi_tt2_*`
  actions; emit them with substituted names (`eth_truf_*`,
  `eth_usdc_*`) and skip the sepolia/ethereum siblings entirely.

* `fee` — the write-fee collection actions (001 create_streams,
  003 insert_records, 004 insert_taxonomy, 024 request_attestation).
  These hardcode `ethereum_bridge.balance/.transfer` for the TRUF
  fee. On mainnet `ethereum_bridge` is declared-but-empty, so emit a
  CREATE OR REPLACE override with `ethereum_bridge` → `eth_truf` to
  route fees through the production TRUF bridge instead.

Re-running the script regenerates the outputs deterministically. To
remove an override, delete the corresponding `.prod.sql`.
"""

from __future__ import annotations

import re
import sys
from pathlib import Path
from typing import Iterable

REPO_ROOT = Path(__file__).resolve().parent.parent
MIGRATIONS_DIR = REPO_ROOT / "internal" / "migrations"

# Per-bridge on-chain decimals. The ONLY place real decimals live for the
# mainnet override. The dev `.sql` migrations carry stub values appropriate
# for the embedded test bridges (all 18-decimal); on prod-generation we
# regenerate `get_bridge_units_per_dollar` entirely from this map so each
# bridge gets units_per_dollar = 10^decimals. To add a new bridge, add an
# entry here and ensure substitute_tokens maps a hoodi alias to its name
# (or extend substitute_tokens / MAINNET_BRIDGES).
BRIDGE_DECIMALS: dict[str, int] = {
    "eth_truf": 18,
    "eth_usdc": 6,
}

# (source_relpath, output_relpath, mode)
TARGETS: list[tuple[str, str, str]] = [
    ("031-order-book-vault.sql", "031-order-book-vault.prod.sql", "core"),
    ("032-order-book-actions.sql", "032-order-book-actions.prod.sql", "core"),
    ("033-order-book-settlement.sql", "033-order-book-settlement.prod.sql", "core"),
    ("037-order-book-validation.sql", "037-order-book-validation.prod.sql", "core"),
    ("038-order-book-queries.sql", "038-order-book-queries.prod.sql", "core"),
    (
        "erc20-bridge/001-actions.sql",
        "erc20-bridge/001-actions.prod.sql",
        "wrap",
    ),
    (
        "erc20-bridge/004-withdrawal-proof-action.sql",
        "erc20-bridge/004-withdrawal-proof-action.prod.sql",
        "wrap",
    ),
    (
        "erc20-bridge/005-history-actions.sql",
        "erc20-bridge/005-history-actions.prod.sql",
        "wrap",
    ),
    ("001-common-actions.sql", "001-common-actions.prod.sql", "fee"),
    ("003-primitive-insertion.sql", "003-primitive-insertion.prod.sql", "fee"),
    ("004-composed-taxonomy.sql", "004-composed-taxonomy.prod.sql", "fee"),
    ("024-attestation-actions.sql", "024-attestation-actions.prod.sql", "fee"),
]

HEADER_TEMPLATE = """\
-- =============================================================================
-- GENERATED FILE — DO NOT EDIT BY HAND
-- =============================================================================
-- Source : internal/migrations/{source}
-- Script : scripts/generate_prod_migrations.py
--
-- Manual-apply mainnet override. The embedded migration loader skips
-- *.prod.sql, so apply via:
--
--     kwil-cli exec-sql --file <this file> --sync \\
--         --private-key $PRIVATE_KEY --provider $PROVIDER
--
-- Prerequisite: erc20-bridge/000-extension.prod.sql must be applied
-- FIRST so the eth_truf and eth_usdc bridge instances exist.
-- =============================================================================

"""

ACTION_HEADER_RE = re.compile(
    r"^CREATE\s+OR\s+REPLACE\s+ACTION\s+(?P<name>\w+)",
    re.MULTILINE,
)


def _find_action_end(sql: str, header_start: int) -> int:
    """Return the index just past the closing `};` of the action that
    starts at `header_start`.

    Brace depth is tracked from the first `{` after the header. The
    action ends at the matching `}`; we then advance past the trailing
    `;` if present.
    """
    n = len(sql)
    i = header_start
    # Find the first `{` that opens the action body.
    while i < n and sql[i] != "{":
        i += 1
    if i >= n:
        raise ValueError(f"no opening brace after header at offset {header_start}")
    depth = 0
    while i < n:
        c = sql[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                # Past closing brace; consume optional `;` and trailing newline.
                i += 1
                if i < n and sql[i] == ";":
                    i += 1
                if i < n and sql[i] == "\n":
                    i += 1
                return i
        i += 1
    raise ValueError(f"unterminated action starting at offset {header_start}")


def split_actions(sql: str) -> list[tuple[str, str]]:
    """Return [(action_name, full_text)] for each top-level CREATE OR REPLACE ACTION.

    Non-action chunks (file-level comments, blank lines) are not
    returned — only callable actions, in source order.
    """
    actions: list[tuple[str, str]] = []
    for match in ACTION_HEADER_RE.finditer(sql):
        name = match.group("name")
        end = _find_action_end(sql, match.start())
        actions.append((name, sql[match.start():end]))
    return actions


def substitute_tokens(text: str) -> str:
    """Apply the bridge-name substitutions.

    Maps testnet / legacy bridges to their mainnet equivalents:
        hoodi_tt2       -> eth_usdc      (USDC collateral)
        hoodi_tt        -> eth_truf      (TRUF — fee/collateral)
        ethereum_bridge -> eth_truf      (legacy mainnet TRUF)

    Order matters: `hoodi_tt2` must be replaced before `hoodi_tt`,
    otherwise the second pass would corrupt the suffix.
    """
    text = text.replace("hoodi_tt2", "eth_usdc")
    text = text.replace("hoodi_tt", "eth_truf")
    text = text.replace("ethereum_bridge", "eth_truf")
    return text


# After substitute_tokens, the only mainnet collateral bridges we want to
# keep in dispatch chains are the ones declared in BRIDGE_DECIMALS. Anything
# else (sepolia_bridge, future testnet aliases) gets dropped.
# Used as a membership test only (`name in MAINNET_BRIDGES`); order is not
# load-bearing since the displayed branch order in collapse_dispatch comes
# from the source SQL, not from this tuple.
MAINNET_BRIDGES = tuple(BRIDGE_DECIMALS)

_BRANCH_HEADER_RE = re.compile(
    r"(?:if|else\s+if)\s+\$bridge\s*=\s*'(?P<name>\w+)'\s*\{",
)
_IF_BRANCH_HEADER_RE = re.compile(
    r"if\s+\$bridge\s*=\s*'(?P<name>\w+)'\s*\{",
)
_ELSE_HEADER_RE = re.compile(r"\s*else\s*\{")


def _scan_balanced_block(text: str, start: int) -> int:
    """Given an index pointing at an opening `{`, return index just past matching `}`."""
    if text[start] != "{":
        raise ValueError(f"expected '{{' at offset {start}, got {text[start]!r}")
    depth = 0
    i = start
    n = len(text)
    while i < n:
        c = text[i]
        if c == "{":
            depth += 1
        elif c == "}":
            depth -= 1
            if depth == 0:
                return i + 1
        i += 1
    raise ValueError(f"unbalanced braces from offset {start}")


def collapse_dispatch(action_text: str) -> str:
    """Walk each `if $bridge = '<X>' { ... } else if ... [else { ... }]`
    chain, keep only branches whose `<X>` is a mainnet bridge
    (eth_usdc, eth_truf), and drop the rest (sepolia_bridge etc.).

    Behavior summary:
      * Single mainnet branch in the chain  ->  inline body, prefix with a
        `if $bridge != '<X>' { ERROR }` guard if the original had a final
        `else { ERROR }`. Mirrors original silent-fall-through if no guard.
      * Two mainnet branches                ->  rebuild as
        `if … else if … [else { ERROR }]` listing only the mainnet bridges
        in the supported-list message.
      * No mainnet branches                 ->  drop the entire chain
        (shouldn't happen post-substitution, but safe).

    Multiple chains per action are handled iteratively.
    """
    out = []
    i = 0
    n = len(action_text)
    while i < n:
        # Find next dispatch CHAIN START (must begin with `if`, not `else if`).
        m = _IF_BRANCH_HEADER_RE.search(action_text, i)
        if not m:
            out.append(action_text[i:])
            break

        # Emit everything before the chain verbatim.
        out.append(action_text[i:m.start()])

        # Capture indentation of the `if` line for re-indentation.
        line_start = action_text.rfind("\n", 0, m.start()) + 1
        indent = action_text[line_start:m.start()]

        # Walk the chain, collecting (name, body_inner) for each branch.
        branches: list[tuple[str, str]] = []
        cursor = m.start()
        while cursor < n:
            bm = _BRANCH_HEADER_RE.match(action_text, cursor)
            if not bm:
                break
            name = bm.group("name")
            body_open = bm.end() - 1  # the `{`
            body_end = _scan_balanced_block(action_text, body_open)
            body_inner = action_text[body_open + 1:body_end - 1]
            branches.append((name, body_inner))
            cursor = body_end
            # Skip whitespace/newlines before the next `else …` or end of chain.
            while cursor < n and action_text[cursor] in " \t\n":
                cursor += 1
            # If next token isn't `else`, the chain ends here.
            if not action_text[cursor:cursor + 4].startswith("else"):
                break

        # Optional final `else { ... }`.
        had_final_else = False
        em = _ELSE_HEADER_RE.match(action_text, cursor)
        if em:
            had_final_else = True
            cursor = _scan_balanced_block(action_text, em.end() - 1)

        # Filter to mainnet branches, preserving source order.
        mainnet = [(name, body) for name, body in branches if name in MAINNET_BRIDGES]

        if not mainnet:
            # All branches were testnet — drop the whole chain.
            replacement = ""
        elif len(mainnet) == 1:
            # Single branch: inline (with optional guard).
            name, body_inner = mainnet[0]
            body = _redent_body(body_inner, indent)
            if had_final_else:
                replacement = (
                    f"if $bridge != '{name}' {{\n"
                    f"{indent}    ERROR('Invalid bridge. Supported: {name}');\n"
                    f"{indent}}}\n"
                    f"{indent}{body}"
                )
            else:
                replacement = body
        else:
            # Multiple mainnet branches: rebuild if/else-if chain.
            inner_indent = indent + "    "
            parts: list[str] = []
            for idx, (name, body_inner) in enumerate(mainnet):
                body = _redent_body(body_inner, inner_indent)
                if idx == 0:
                    parts.append(f"if $bridge = '{name}' {{")
                else:
                    parts.append(f"{indent}}} else if $bridge = '{name}' {{")
                parts.append(f"{inner_indent}{body}")
            if had_final_else:
                supported = ", ".join(name for name, _ in mainnet)
                parts.append(f"{indent}}} else {{")
                parts.append(f"{inner_indent}ERROR('Invalid bridge. Supported: {supported}');")
                parts.append(f"{indent}}}")
            else:
                parts.append(f"{indent}}}")
            replacement = "\n".join(parts)

        out.append(replacement)
        i = cursor
    return "".join(out)


def _redent_body(body: str, target_indent: str) -> str:
    """Strip the body's outer indentation and re-indent every line to
    `target_indent` (the indentation of the `if` keyword we're
    replacing). Leading and trailing blank lines are removed.
    """
    lines = body.splitlines()
    # Drop leading blank lines.
    while lines and not lines[0].strip():
        lines.pop(0)
    # Drop trailing blank lines.
    while lines and not lines[-1].strip():
        lines.pop()
    if not lines:
        return ""
    # Compute the smallest non-empty indentation in the body.
    min_indent = None
    for line in lines:
        if not line.strip():
            continue
        stripped_len = len(line) - len(line.lstrip(" "))
        if min_indent is None or stripped_len < min_indent:
            min_indent = stripped_len
    if min_indent is None:
        min_indent = 0
    out_lines = []
    for idx, line in enumerate(lines):
        if not line.strip():
            out_lines.append("")
            continue
        dedented = line[min_indent:]
        if idx == 0:
            # First line is appended after the existing target_indent.
            out_lines.append(dedented)
        else:
            out_lines.append(target_indent + dedented)
    return "\n".join(out_lines)


# After substitute_tokens, the validate_bridge AND chain reads:
#   $bridge != 'eth_usdc' AND $bridge != 'sepolia_bridge' AND $bridge != 'eth_truf'
# We want to drop the sepolia_bridge clause and update the matching ERROR
# string so the action accepts both mainnet bridges.
_VALIDATE_AND_CHAIN_RE = re.compile(
    r"\$bridge\s*!=\s*'eth_usdc'\s+AND\s*\n?\s*"
    r"\$bridge\s*!=\s*'sepolia_bridge'\s+AND\s*\n?\s*"
    r"\$bridge\s*!=\s*'eth_truf'",
)
_VALIDATE_AND_REPLACEMENT = (
    "$bridge != 'eth_usdc' AND\n"
    "       $bridge != 'eth_truf'"
)
_SUPPORTED_LIST_RE = re.compile(
    r"Supported:\s+eth_usdc,\s+sepolia_bridge,\s+eth_truf",
)


def collapse_validate_and_chain(text: str) -> str:
    """Collapse the three-clause `$bridge != …` predicate (used in
    `validate_bridge`) to drop the sepolia_bridge testnet clause, and
    shrink the matching "Supported:" ERROR list to the mainnet bridges
    only. Idempotent.
    """
    text = _VALIDATE_AND_CHAIN_RE.sub(_VALIDATE_AND_REPLACEMENT, text)
    text = _SUPPORTED_LIST_RE.sub("Supported: eth_usdc, eth_truf", text)
    return text


def regenerate_units_per_dollar() -> str:
    """Build the prod body of `get_bridge_units_per_dollar` from
    BRIDGE_DECIMALS. Replaces whatever the dev source contained — the
    dev branches are stubs that all return 10^18, but each prod bridge
    needs its own units_per_dollar = 10^decimals.

    Returns the full action text including the CREATE OR REPLACE header
    and trailing semicolon, ending with a single newline.
    """
    branches: list[str] = []
    for idx, (bridge, decimals) in enumerate(BRIDGE_DECIMALS.items()):
        units = "1" + "0" * decimals
        prefix = "if" if idx == 0 else "} else if"
        branches.append(
            f"    {prefix} $bridge = '{bridge}' {{\n"
            f"        RETURN '{units}'::NUMERIC(78, 0);"
        )
    body = "\n".join(branches) + "\n    }\n    ERROR('Unknown bridge: ' || $bridge);"
    return (
        "CREATE OR REPLACE ACTION get_bridge_units_per_dollar($bridge TEXT)\n"
        "    PUBLIC VIEW RETURNS (units_per_dollar NUMERIC(78, 0)) {\n"
        f"{body}\n"
        "};\n"
    )


def transform_core(source_sql: str) -> tuple[str, list[str]]:
    """Mode A. Emit each action whose body references `eth_truf`/`eth_usdc`
    after substitution, with dispatch chains collapsed.

    Special case: `get_bridge_units_per_dollar` is regenerated from
    BRIDGE_DECIMALS rather than rewritten via substitution — the dev
    branches are 18-decimal stubs but each prod bridge needs its actual
    decimals (eth_truf=18, eth_usdc=6). See regenerate_units_per_dollar.
    """
    parts: list[str] = []
    names: list[str] = []
    for name, raw in split_actions(source_sql):
        # `get_bridge_units_per_dollar` is regenerated regardless of whether
        # the dev source still mentions any bridge alias — its prod body
        # comes from BRIDGE_DECIMALS, not from textual substitution. Keep
        # this above the bridge-mention filter so a future refactor that
        # drops the alias from the helper body doesn't silently strip it
        # from the prod override.
        if name == "get_bridge_units_per_dollar":
            parts.append(regenerate_units_per_dollar())
            names.append(name)
            continue
        # Emit any action that references a bridge alias we substitute,
        # so e.g. `$bridge TEXT DEFAULT 'ethereum_bridge'` in get_user_collateral
        # becomes `'eth_truf'` in the prod override.
        if "hoodi_tt" not in raw and "ethereum_bridge" not in raw:
            continue
        substituted = substitute_tokens(raw)
        substituted = collapse_validate_and_chain(substituted)
        substituted = collapse_dispatch(substituted)
        parts.append(substituted.rstrip() + "\n")
        names.append(name)
    return "\n".join(parts), names


def transform_wrap(source_sql: str) -> tuple[str, list[str]]:
    """Mode B. Emit only `hoodi_tt_*` and `hoodi_tt2_*` actions, renamed."""
    parts: list[str] = []
    names: list[str] = []
    for name, raw in split_actions(source_sql):
        if not (name.startswith("hoodi_tt_") or name.startswith("hoodi_tt2_")):
            continue
        substituted = substitute_tokens(raw)
        # Rename in the action header only (substitute_tokens already did
        # this since the header contains the prefix).
        parts.append(substituted.rstrip() + "\n")
        names.append(substitute_tokens(name))
    return "\n".join(parts), names


def transform_fee(source_sql: str) -> tuple[str, list[str]]:
    """Mode C. Emit each action that calls `ethereum_bridge` directly
    (TRUF fee-collection pattern), with `ethereum_bridge` → `eth_truf`
    substitution.

    No if/else dispatch collapse — `ethereum_bridge` here appears as
    unconditional method calls inside fee-collection blocks, not as a
    branch in a bridge-dispatch chain. Order-book files where
    `ethereum_bridge` was a dispatch branch are handled by mode `core`
    instead, which drops those branches entirely.
    """
    parts: list[str] = []
    names: list[str] = []
    for name, raw in split_actions(source_sql):
        if "ethereum_bridge" not in raw:
            continue
        substituted = raw.replace("ethereum_bridge", "eth_truf")
        parts.append(substituted.rstrip() + "\n")
        names.append(name)
    return "\n".join(parts), names


def main(argv: Iterable[str] | None = None) -> int:
    print(f"reading from: {MIGRATIONS_DIR}")
    for source_rel, output_rel, mode in TARGETS:
        source_path = MIGRATIONS_DIR / source_rel
        output_path = MIGRATIONS_DIR / output_rel
        sql = source_path.read_text()
        if mode == "core":
            body, names = transform_core(sql)
        elif mode == "wrap":
            body, names = transform_wrap(sql)
        elif mode == "fee":
            body, names = transform_fee(sql)
        else:
            raise ValueError(f"unknown mode {mode!r}")
        if not body.strip():
            print(f"  SKIP {output_rel} (no hoodi references found)")
            continue
        output = HEADER_TEMPLATE.format(source=source_rel) + body
        output_path.write_text(output)
        print(f"  wrote {output_rel:<55} ({len(names)} action(s): {', '.join(names)})")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
