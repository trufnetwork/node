/**
 * Retired bridge actions
 *
 * mainnet_admin_bridge_tokens and sepolia_admin_bridge_tokens were defined in
 * erc20-bridge/001-actions.sql until #1162 replaced them with the per-bridge
 * *_bridge_tokens actions. Taking an action out of a file does not take it off
 * a network that already ran that file, so networks migrated before #1162 still
 * hold both.
 *
 * Neither can complete: each ends in a bridge() call with one argument
 * where the erc20 extension takes two (recipient, amount), and nothing in this
 * repository calls them. With no caller that could depend on them, they are
 * dropped here rather than by hand per environment. IF EXISTS makes this a
 * no-op on a network that never had them, and safe to apply on every deploy.
 */

DROP ACTION IF EXISTS mainnet_admin_bridge_tokens;
DROP ACTION IF EXISTS sepolia_admin_bridge_tokens;
