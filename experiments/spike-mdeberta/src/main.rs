//! Spike: can rten load and run the mdeberta-v3 ai4privacy PII token classifier?
//!
//! Usage: spike-mdeberta <model.onnx> <assets_dir>
//!
//! `assets_dir` must contain `tokenizer.json` and `config.json` from the same
//! model repo. Prints the load result, the per-token predicted labels for a
//! fixed sample sentence, the median warm forward-pass latency and peak RSS.

use std::time::Instant;

use anyhow::{Context, Result, anyhow};
use rten::Model;
use rten_tensor::prelude::*;
use rten_tensor::NdTensor;

const SAMPLE: &str =
    "My name is Sarah Connor, you can email me at sarah.connor@example.com or call 555-0142.";

/// Peak resident set size in MiB, read from `/proc/self/status` (`VmHWM`).
fn peak_rss_mib() -> Option<f64> {
    let status = std::fs::read_to_string("/proc/self/status").ok()?;
    let line = status.lines().find(|l| l.starts_with("VmHWM:"))?;
    let kib: f64 = line.split_whitespace().nth(1)?.parse().ok()?;
    Some(kib / 1024.0)
}

fn load_id2label(config_path: &str) -> Result<Vec<String>> {
    let raw = std::fs::read_to_string(config_path)
        .with_context(|| format!("reading {config_path}"))?;
    let cfg: serde_json::Value = serde_json::from_str(&raw)?;
    let map = cfg
        .get("id2label")
        .and_then(|v| v.as_object())
        .ok_or_else(|| anyhow!("config.json has no id2label object"))?;

    let mut labels = vec![String::new(); map.len()];
    for (k, v) in map {
        let idx: usize = k.parse().context("id2label key is not an integer")?;
        let name = v.as_str().ok_or_else(|| anyhow!("id2label value not a string"))?;
        *labels
            .get_mut(idx)
            .ok_or_else(|| anyhow!("id2label index {idx} out of range"))? = name.to_string();
    }
    Ok(labels)
}

fn main() -> Result<()> {
    let mut args = std::env::args().skip(1);
    let model_path = args.next().ok_or_else(|| anyhow!("usage: <model.onnx> <assets_dir>"))?;
    let assets = args.next().ok_or_else(|| anyhow!("usage: <model.onnx> <assets_dir>"))?;

    let bytes = std::fs::metadata(&model_path)?.len();
    println!("model:  {model_path}");
    println!("size:   {} bytes ({:.1} MiB)", bytes, bytes as f64 / (1024.0 * 1024.0));

    // (1) Load the ONNX graph directly. rten 0.26 reads .onnx natively via the
    // default `onnx_format` feature; no .rten conversion step is involved.
    let t0 = Instant::now();
    let model = match Model::load_file(&model_path) {
        Ok(m) => {
            println!("load:   OK in {:.2?}", t0.elapsed());
            m
        }
        Err(e) => {
            println!("load:   FAILED after {:.2?}", t0.elapsed());
            println!("error:  {e}");
            println!("debug:  {e:?}");
            return Ok(());
        }
    };

    let inputs: Vec<_> = model
        .input_ids()
        .iter()
        .filter_map(|&id| model.node_info(id).and_then(|n| n.name().map(str::to_string)))
        .collect();
    let outputs: Vec<_> = model
        .output_ids()
        .iter()
        .filter_map(|&id| model.node_info(id).and_then(|n| n.name().map(str::to_string)))
        .collect();
    println!("inputs: {inputs:?}");
    println!("output: {outputs:?}");

    // (2) Tokenizer. mDeBERTa-v3 uses a SentencePiece Unigram model; check the
    // `tokenizers` crate loads it and that offsets come back as byte ranges.
    let tok_path = format!("{assets}/tokenizer.json");
    let tokenizer = tokenizers::Tokenizer::from_file(&tok_path)
        .map_err(|e| anyhow!("tokenizer load failed: {e}"))?;
    println!("tok:    OK ({tok_path})");

    let enc = tokenizer
        .encode(SAMPLE, true)
        .map_err(|e| anyhow!("encode failed: {e}"))?;
    let ids: Vec<i32> = enc.get_ids().iter().map(|&i| i as i32).collect();
    let mask: Vec<i32> = enc.get_attention_mask().iter().map(|&i| i as i32).collect();
    let offsets = enc.get_offsets();
    let toks = enc.get_tokens();
    let n = ids.len();
    println!("tokens: {n}");
    println!(
        "offset: first non-special = {:?} -> {:?}",
        offsets.get(1),
        offsets.get(1).and_then(|&(s, e)| SAMPLE.get(s..e)),
    );

    let labels = load_id2label(&format!("{assets}/config.json"))?;
    println!("labels: {}", labels.len());

    let id_input = model.node_id("input_ids")?;
    let mask_input = model.node_id("attention_mask")?;
    let logits_out = model.node_id("logits")?;

    let run_once = || -> Result<Vec<f32>> {
        let id_t = NdTensor::from_data([1, n], ids.clone());
        let mask_t = NdTensor::from_data([1, n], mask.clone());
        let [out] = model.run_n(
            vec![(id_input, id_t.view().into()), (mask_input, mask_t.view().into())],
            [logits_out],
            None,
        )?;
        let (shape, data) = out.into_shape_vec::<f32, 3>()?;
        if shape[1] != n {
            return Err(anyhow!("unexpected logits shape {shape:?}"));
        }
        Ok(data)
    };

    // (3) One correctness pass, then 5 warm runs for a median latency.
    let logits = run_once()?;
    let n_labels = labels.len();
    println!("\npredicted labels (non-O only):");
    let mut any = false;
    for i in 0..n {
        let row = &logits[i * n_labels..(i + 1) * n_labels];
        let (best, _) = row
            .iter()
            .enumerate()
            .max_by(|a, b| a.1.total_cmp(b.1))
            .expect("non-empty label row");
        if labels[best] != "O" {
            any = true;
            let (s, e) = offsets[i];
            println!(
                "  {:>2} {:<16} {:<20} span={:?}",
                i,
                toks[i],
                labels[best],
                SAMPLE.get(s..e).unwrap_or(""),
            );
        }
    }
    if !any {
        println!("  (none)");
    }

    // Optional: dump raw logits so the output can be diffed against onnxruntime.
    if let Ok(dump) = std::env::var("SPIKE_DUMP_LOGITS") {
        let mut buf = Vec::with_capacity(logits.len() * 4);
        for v in &logits {
            buf.extend_from_slice(&v.to_le_bytes());
        }
        std::fs::write(&dump, buf)?;
        println!("dumped: {} f32 logits -> {dump}", logits.len());
    }

    let mut times = Vec::new();
    for _ in 0..5 {
        let t = Instant::now();
        run_once()?;
        times.push(t.elapsed());
    }
    times.sort();
    println!("\nlatency: median {:.2?} over 5 warm runs (seq_len={n})", times[2]);
    println!("min/max: {:.2?} / {:.2?}", times[0], times[4]);
    match peak_rss_mib() {
        Some(m) => println!("peakRSS: {m:.0} MiB"),
        None => println!("peakRSS: unavailable"),
    }
    Ok(())
}
