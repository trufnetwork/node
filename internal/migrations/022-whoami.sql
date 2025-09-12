-- whoami action to return the caller's wallet address, useful for debugging and verification.
CREATE OR REPLACE ACTION whoami() PUBLIC VIEW RETURNS TABLE(caller TEXT) {
  -- This action returns the caller's wallet address.
    RETURN @caller;
}