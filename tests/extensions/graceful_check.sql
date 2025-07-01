USE hello as hello;

CREATE ACTION graceful_check() PUBLIC VIEW RETURNS TABLE(msg TEXT) {
    FOR $row IN hello.greet() {
        RETURN NEXT $row.msg;
    }
}
