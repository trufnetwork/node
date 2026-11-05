/**
 * Nuon price feed setters
 *
 * nuon_set_base_constant, nuon_set_divisor_constant and nuon_set_source_stream
 * exist on mainnet but were never defined in this repository, so they cannot be
 * reviewed here, and nothing in this repository calls them. They are dropped
 * here. The nuon getters stay. IF EXISTS makes this a no-op on a network that
 * never had them, and safe to apply on every deploy.
 */

DROP ACTION IF EXISTS nuon_set_base_constant;
DROP ACTION IF EXISTS nuon_set_divisor_constant;
DROP ACTION IF EXISTS nuon_set_source_stream;
