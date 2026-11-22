# Fuzz targets

[cargo-fuzz](https://github.com/rust-fuzz/cargo-fuzz) (libFuzzer) targets for the
parts of the relay that read data from outside. The package is not part of any
workspace, so normal builds are unchanged. It needs a nightly toolchain and
`cargo install cargo-fuzz`.

| Target | What it drives |
|---|---|
| `messages` | The websocket protocol messages in `src/protocol.rs` (`MessageToRelay` and `MessageToStreamer`) on arbitrary text, checking that anything accepted re-serializes and re-parses to the same message, plus the authentication hash on peer-supplied salt and challenge. The module is private, so the target includes the file by path instead of widening the library API. |
| `status_file` | `create_get_status_closure` with arbitrary `--status-file` contents. |

```sh
cargo install cargo-fuzz
mkdir -p fuzz/corpus/messages fuzz/corpus/status_file
cp fuzz/seeds/messages/* fuzz/corpus/messages/
cp fuzz/seeds/status_file/* fuzz/corpus/status_file/
cargo fuzz run messages -- -max_total_time=300 -max_len=2048
cargo fuzz run status_file -- -max_total_time=300 -max_len=2048
```

`fuzz/seeds/` holds a few hand-written inputs; copy them into the corpus first,
because libFuzzer writes new inputs into the corpus it is given. Generated
corpora, artifacts and build output are not committed. A crash is reproduced
with `cargo fuzz run <target> <artifact>` and should become a regression test.
