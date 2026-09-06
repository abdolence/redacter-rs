# spike-ner

Offline NER spike: [rten](https://crates.io/crates/rten) 0.26 + the pure-Rust
[`tokenizers`](https://crates.io/crates/tokenizers) crate running a multilingual
token-classification model (`Davlan/distilbert-base-multilingual-cased-ner-hrl`,
ONNX export by `Xenova`).

Not part of the `redacter` crate: it has its own `Cargo.toml` and the published
crate only ships `src/**`.

## Fetch the model

Model files are gitignored. From this directory:

```sh
mkdir -p models && cd models
for f in onnx/model.onnx onnx/model_int8.onnx onnx/model_quantized.onnx tokenizer.json config.json; do
  curl -sL -o "$(basename "$f")" \
    "https://huggingface.co/Xenova/distilbert-base-multilingual-cased-ner-hrl/resolve/main/$f"
done
```

`model_quantized.onnx` (129 MB, uint8) is the recommended variant: same spans as
fp32 on the probes here, a quarter of the file size and a third of the RSS.

## Run

```sh
export CARGO_TARGET_DIR=/tmp/spike-ner-target   # keep out of the main crate's target dir
cargo build --release

"$CARGO_TARGET_DIR/release/spike-ner" \
  models/model_quantized.onnx models/tokenizer.json models/config.json \
  ../../test-fixtures/documents/customer-note.txt \
  ../../test-fixtures/documents/customers.csv \
  ../../test-fixtures/documents/customer.json \
  ../../test-fixtures/documents/customer-profile.html \
  fixtures/multilingual.txt \
  fixtures/false-positives-en.txt \
  fixtures/dates-en.txt
```

Each input prints `LABEL start..end score "text"` over byte offsets into the
original file, then a trailing `BENCH` line with model size, load time, the
median of 9 warm 512-token forward passes, and peak RSS (`VmHWM`).

Swap the first argument for `models/model.onnx` or `models/model_int8.onnx` to
compare variants.

To exercise the multi-window path, build an input longer than 510 tokens:

```sh
for i in $(seq 6); do cat fixtures/multilingual.txt; done > fixtures/long-multi.txt
```

## What it does

- `tokenizers::Tokenizer::from_file`, `encode(text, false)`; byte offsets and
  word ids come straight from the encoding.
- Sliding 512-token windows (510 body tokens) with a 64-token overlap; the first
  (more centred) prediction wins for tokens covered by two windows.
- Per-token argmax over the 9 logits, softmax score for the winner.
- Subword tokens collapse onto whole words (first subword's label wins, which is
  how the head is trained) before BIO merging, otherwise spans truncate mid-word
  (`Support Des` instead of `Support Desk`).

## rten notes

The graph declares `input_ids` and `attention_mask` as `tensor(int32)` and a
single `logits` output as `tensor(float)`, shape
`[batch_size, sequence_length, 9]`. The ONNX file says `int64`; rten narrows
i64 tensors to i32 at load time, so feed it `NdTensor<i32, 2>`. All three
variants load with `Model::load_file` on the `.onnx` directly, with no
unsupported operators.

## Known limits

- The head emits `PER`, `ORG` and `LOC` only. `config.json` lists `B-/I-DATE`,
  but the weights never predict them (`fixtures/dates-en.txt` returns nothing).
- No `EMAIL`, `PHONE`, `CREDIT_CARD`, `PASSPORT` or postcode classes; those need
  a separate regex/checksum pass.
- PDF and image fixtures are not covered here (no OCR in this spike).
