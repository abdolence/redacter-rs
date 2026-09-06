"""Dynamically quantize an ONNX model's weights to int8.

Usage: python quantize.py <model.onnx> <model.int8.onnx> [--per-channel]

Plain per-tensor dynamic quantization destroys this particular DeBERTa-v2
checkpoint (every token collapses to the "O" class); `--per-channel` keeps a
separate scale per output channel and is the first thing to try.
"""

import sys

from onnxruntime.quantization import QuantType, quantize_dynamic


def main() -> int:
    args = [a for a in sys.argv[1:] if not a.startswith("--")]
    per_channel = "--per-channel" in sys.argv[1:]
    if len(args) != 2:
        print(__doc__, file=sys.stderr)
        return 2

    quantize_dynamic(
        model_input=args[0],
        model_output=args[1],
        weight_type=QuantType.QInt8,
        per_channel=per_channel,
    )
    print(f"wrote {args[1]} (per_channel={per_channel})")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
