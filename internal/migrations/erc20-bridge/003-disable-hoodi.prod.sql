-- Disable and remove Hoodi bridge instance (Test Token)
-- This migration removes the hoodi_tt extension to allow fresh deployment with new escrow
--
-- UNUSE will properly clean up the namespace even if the instance is already disabled.
-- Use this migration to rollback to a clean state before re-adding the hoodi_tt with new parameters.

UNUSE hoodi_tt;
