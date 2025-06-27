USE hello AS hello;

CREATE ACTION graceful_check() PUBLIC VIEW RETURNS (msg TEXT) {
    RETURN hello.greet();
}
