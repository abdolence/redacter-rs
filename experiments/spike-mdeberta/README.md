# Spike: mDeBERTa-v3 PII model on the rten runtime

Throwaway-but-kept experiment answering one question: can
[`Isotonic/mdeberta-v3-base_finetuned_ai4privacy_v2`](https://huggingface.co/Isotonic/mdeberta-v3-base_finetuned_ai4privacy_v2)
(multilingual PII token classification, 111 labels) be loaded and run by the
Rust `rten` 0.26 runtime, in fp32 and in dynamically quantized int8?

Not wired into the main crate. It has its own `Cargo.toml` with a bare
`[workspace]` table so cargo treats it as a standalone package.

## Findings

- **fp32 runs on stock rten 0.26, unmodified.** rten reads `.onnx` natively
  (default `onnx_format` feature); no `.rten` conversion step is needed.
- **rten is numerically correct.** Its logits match onnxruntime 1.29 to
  `8.2e-05` max absolute difference over logits spanning `[-9.74, 20.09]`,
  with 100% argmax agreement on every token.
- **int8 dynamic quantization is not viable for this checkpoint.** Per-tensor
  collapses every token to the `O` class; per-channel collapses every token to
  a single spurious class. **onnxruntime reproduces both failures**, so this is
  the quantization destroying a DeBERTa-v2 disentangled-attention model, not an
  rten bug. rten matches onnxruntime's broken output too.
- **Operator coverage is complete.** fp32 uses 38 distinct ops, int8 uses 41
  (adding `DynamicQuantizeLinear`, `MatMulInteger`, `DequantizeLinear`). All are
  in rten's ONNX registry. `list_ops.py` reports `Constant` as "missing" only
  because rten's loader folds `Constant` nodes into graph constants before the
  op registry is consulted (`rten-0.26.0/src/model/onnx_loader.rs`), so it is
  not a real gap.

Measured on this machine, sequence length 29, median of 5 warm runs:

| variant           | file size  | rten load | forward pass | peak RSS | output      |
|-------------------|-----------:|----------:|-------------:|---------:|-------------|
| fp32              | 1062.5 MiB |    246 ms |      41-47 ms|  1330 MiB| correct     |
| int8 per-tensor   |  322.1 MiB |    132 ms |        31 ms|   590 MiB| degenerate  |
| int8 per-channel  |  322.5 MiB |    132 ms |        31 ms|   590 MiB| degenerate  |

The model itself is also mediocre on the sample sentence even in fp32: it tags
the email correctly but misses "Sarah Connor" entirely and labels the phone
number `ZIPCODE`. That is the checkpoint's own behaviour, confirmed identical
under onnxruntime.

## Licensing

Blocking for commercial use. The model card declares `license: cc-by-nc-4.0`
(non-commercial), and the prose adds an ai4privacy dual license: free only for
individuals, non-profits, and for-profit entities with **at most 3 staff**;
anything larger "require[s] a company license which can be requested by
licensing@ai4privacy.com". It also prohibits distributing derivatives:
"It's not permissible to duplicate or alter Ai4Privacy's code with the
intention to distribute, sell, rent, license, re-license, or sublicense your
own derivative". The training dataset `ai4privacy/pii-masking-200k` carries the
same terms.

## Reproducing

Everything below writes to a scratch directory; nothing large is committed
(see `.gitignore`).

```sh
SCRATCH=/tmp/spike-mdeberta          # anywhere with ~2 GB free
mkdir -p "$SCRATCH/onnx"

# 1. Fetch the pre-existing ONNX export. The repo already ships onnx/, so no
#    optimum-cli export or torch install is needed.
for f in model.onnx tokenizer.json config.json tokenizer_config.json \
         special_tokens_map.json added_tokens.json; do
  curl -sL -o "$SCRATCH/onnx/$f" \
    "https://huggingface.co/Isotonic/mdeberta-v3-base_finetuned_ai4privacy_v2/resolve/main/onnx/$f"
done

# 2. Python env for the op listing and the quantizer.
python3 -m venv "$SCRATCH/.venv"
"$SCRATCH/.venv/bin/pip" install -r scripts/requirements.txt

# 3. List the operators the export uses, and diff against a runtime's op list.
#    Generate rten_ops.txt from the installed crate source:
grep -oP 'register_op!\(\K[A-Za-z0-9_]+' \
  ~/.cargo/registry/src/*/rten-0.26.0/src/op_registry/onnx_registry.rs \
  | sort -u > "$SCRATCH/rten_ops.txt"
"$SCRATCH/.venv/bin/python" scripts/list_ops.py "$SCRATCH/onnx/model.onnx" "$SCRATCH/rten_ops.txt"

# 4. Dynamic int8 quantization (add --per-channel for the other variant).
"$SCRATCH/.venv/bin/python" scripts/quantize.py \
  "$SCRATCH/onnx/model.onnx" "$SCRATCH/onnx/model.int8.onnx"

# 5. Load and run under rten. Keep a private target dir.
export CARGO_TARGET_DIR="$SCRATCH/target"
cargo build --release
"$SCRATCH/target/release/spike-mdeberta" "$SCRATCH/onnx/model.onnx"      "$SCRATCH/onnx"
"$SCRATCH/target/release/spike-mdeberta" "$SCRATCH/onnx/model.int8.onnx" "$SCRATCH/onnx"
```

Set `SPIKE_DUMP_LOGITS=<path>` to dump the raw f32 logits, which is how the
comparison against onnxruntime above was made.
