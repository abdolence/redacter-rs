# Vendored rule data

- `gitleaks.toml`: the default configuration of [gitleaks](https://github.com/gitleaks/gitleaks),
  commit `5f6ec56cfa6f8c67648d863582c7ae2f206d5c1c`, file `config/gitleaks.toml`. MIT licence,
  Copyright (c) 2019 Zachary Rice, in `gitleaks.LICENSE`. `../gitleaks/table.rs` is generated from
  it by `cargo test gitleaks_table_regenerate -- --ignored`.
- `noseyparker-examples.json`: the `examples` and `negative_examples` strings of the rules in
  `crates/noseyparker/data/default/builtin/rules` of
  [Nosey Parker](https://github.com/praetorian-inc/noseyparker), commit
  `2e6e7f36ce36619852532bbe698d8cb7a26d2da7`. Apache-2.0 licence in `noseyparker.LICENSE`, notice
  in `noseyparker.NOTICE`. Only the rule ids, names and these strings are kept. Each string is cut
  into pieces of 8 characters, so that no complete token is committed; the tests join the pieces
  again.
