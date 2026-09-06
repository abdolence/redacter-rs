"""Print the distinct ONNX operator types used by a model.

Usage: python list_ops.py <model.onnx> [rten_ops.txt]

With a second argument (a newline-separated list of operator types the target
runtime supports) it also prints which of the model's operators are missing.
"""

import sys

import onnx


def main() -> int:
    if len(sys.argv) < 2:
        print(__doc__, file=sys.stderr)
        return 2

    model = onnx.load(sys.argv[1], load_external_data=False)

    def walk(graph):
        for node in graph.node:
            yield node
            for attr in node.attribute:
                if attr.g.ByteSize():
                    yield from walk(attr.g)
                for sub in attr.graphs:
                    yield from walk(sub)

    ops = sorted({n.op_type for n in walk(model.graph)})
    print(f"opset: {[(i.domain or 'ai.onnx', i.version) for i in model.opset_import]}")
    print(f"{len(ops)} distinct operators:")
    print(" ".join(ops))

    if len(sys.argv) > 2:
        with open(sys.argv[2]) as fh:
            supported = {line.strip() for line in fh if line.strip()}
        missing = [op for op in ops if op not in supported]
        print(f"\nmissing from runtime op list: {missing or 'none'}")

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
