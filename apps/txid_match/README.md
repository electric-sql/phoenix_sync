# TxidMatch

Forces postgres's txid to wrap around in order to validate transaction id
matching.

On my machine I can force a wrap around in about 8 hours. Tweak the
`bumper_count` in `TxidMatch.Writer.Supervisor` to work with your machine.

Only tested/works on Linux.

    # create  a local postgres database with an in-memory data dir
    mix txid.postgres up

    # test with txid_current() as the txid function
    # returns 64-bit ids and fails
    mix txid "txid_current()"

    # test with txid_current() as the txid function
    # 32-bit ids and works
    mix txid "pg_current_xact_id()::xid"

    # stop the pg server and release the ramdisk
    mix txid.postgres down
