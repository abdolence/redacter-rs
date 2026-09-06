//! Offline NER spike: rten + HuggingFace tokenizers, token-classification.
//!
//! Usage:
//!   spike-ner <model.onnx> <tokenizer.json> <config.json> [input files...]

use std::path::Path;
use std::time::Instant;

use anyhow::{anyhow, Context, Result};
use rten::{Model, NodeId};
use rten_tensor::prelude::*;
use rten_tensor::NdTensor;
use tokenizers::Tokenizer;

const WINDOW: usize = 512; // model max_position_embeddings
const OVERLAP: usize = 64;

#[derive(Debug, Clone)]
struct Span {
    label: String,
    start: usize,
    end: usize,
    score: f32,
}

struct Ner {
    model: Model,
    tokenizer: Tokenizer,
    id2label: Vec<String>,
    cls: u32,
    sep: u32,
    input_ids: NodeId,
    attention_mask: NodeId,
    output: NodeId,
}

impl Ner {
    fn load(model_path: &Path, tok_path: &Path, cfg_path: &Path) -> Result<(Self, f64)> {
        let t0 = Instant::now();
        let model = Model::load_file(model_path)
            .map_err(|e| anyhow!("rten failed to load {}: {e}", model_path.display()))?;
        let load_ms = t0.elapsed().as_secs_f64() * 1000.0;

        let tokenizer = Tokenizer::from_file(tok_path).map_err(|e| anyhow!("tokenizer: {e}"))?;

        let cfg: serde_json::Value =
            serde_json::from_slice(&std::fs::read(cfg_path).context("read config.json")?)?;
        let map = cfg["id2label"]
            .as_object()
            .ok_or_else(|| anyhow!("config.json has no id2label object"))?;
        let mut id2label = vec![String::new(); map.len()];
        for (k, v) in map {
            let i: usize = k.parse()?;
            id2label[i] = v.as_str().unwrap_or("O").to_string();
        }

        // Resolve graph node ids by name, printing what the model actually declares.
        let named = |name: &str| -> Option<NodeId> { model.find_node(name) };
        let input_ids = named("input_ids").ok_or_else(|| anyhow!("no `input_ids` input"))?;
        let attention_mask =
            named("attention_mask").ok_or_else(|| anyhow!("no `attention_mask` input"))?;
        let output = *model
            .output_ids()
            .first()
            .ok_or_else(|| anyhow!("model has no outputs"))?;

        let cls = tokenizer
            .token_to_id("[CLS]")
            .ok_or_else(|| anyhow!("no [CLS]"))?;
        let sep = tokenizer
            .token_to_id("[SEP]")
            .ok_or_else(|| anyhow!("no [SEP]"))?;

        Ok((
            Ner {
                model,
                tokenizer,
                id2label,
                cls,
                sep,
                input_ids,
                attention_mask,
                output,
            },
            load_ms,
        ))
    }

    fn describe_nodes(&self) {
        println!("-- graph nodes --");
        for (kind, ids) in [
            ("input", self.model.input_ids()),
            ("output", self.model.output_ids()),
        ] {
            for &id in ids {
                if let Some(info) = self.model.node_info(id) {
                    println!(
                        "  {kind}: name={:?} dtype={:?} shape={:?}",
                        info.name(),
                        info.dtype(),
                        info.shape()
                    );
                }
            }
        }
    }

    /// Run one window of token ids, returning [seq_len][num_labels] logits.
    fn run_window(&self, ids: &[i32]) -> Result<NdTensor<f32, 2>> {
        let n = ids.len();
        let input = NdTensor::from_data([1, n], ids.to_vec());
        let mask = NdTensor::from_data([1, n], vec![1i32; n]);
        let [out] = self
            .model
            .run_n(
                vec![
                    (self.input_ids, input.view().into()),
                    (self.attention_mask, mask.view().into()),
                ],
                [self.output],
                None,
            )
            .map_err(|e| anyhow!("rten run: {e}"))?;
        let out: NdTensor<f32, 3> = out.try_into().map_err(|e| anyhow!("output cast: {e:?}"))?;
        let [_b, s, l] = out.shape();
        Ok(NdTensor::from_data([s, l], out.to_vec()))
    }

    /// Tag `text`, returning merged BIO spans over the original byte offsets.
    fn tag(&self, text: &str) -> Result<(Vec<Span>, Vec<f64>)> {
        let enc = self
            .tokenizer
            .encode(text, false)
            .map_err(|e| anyhow!("encode: {e}"))?;
        let ids: Vec<u32> = enc.get_ids().to_vec();
        let offsets: Vec<(usize, usize)> = enc.get_offsets().to_vec();
        let word_ids: Vec<Option<u32>> = enc.get_word_ids().to_vec();

        let body = WINDOW - 2; // room for [CLS] / [SEP]
        let stride = body.saturating_sub(OVERLAP);

        // (label_id, score) per source token; later windows only fill gaps.
        let mut tags: Vec<Option<(usize, f32)>> = vec![None; ids.len()];
        let mut window_ms: Vec<f64> = Vec::new();

        let mut start = 0usize;
        loop {
            let end = (start + body).min(ids.len());
            let mut win = Vec::with_capacity(end - start + 2);
            win.push(self.cls as i32);
            win.extend(ids[start..end].iter().map(|&i| i as i32));
            win.push(self.sep as i32);

            let t = Instant::now();
            let logits = self.run_window(&win)?;
            window_ms.push(t.elapsed().as_secs_f64() * 1000.0);

            for (k, slot) in tags[start..end].iter_mut().enumerate() {
                let row = logits.slice(k + 1); // skip [CLS]
                let (best, &max) = row
                    .iter()
                    .enumerate()
                    .max_by(|a, b| a.1.partial_cmp(b.1).unwrap())
                    .unwrap();
                let sum: f32 = row.iter().map(|v| (v - max).exp()).sum();
                let score = 1.0 / sum;
                // Keep the first (most centred) prediction for overlapped tokens.
                if slot.is_none() {
                    *slot = Some((best, score));
                }
            }

            if end == ids.len() {
                break;
            }
            start += stride;
        }

        Ok((self.merge(text, &offsets, &word_ids, &tags), window_ms))
    }

    /// Collapse subword tokens onto whole words (first-subword label wins, which
    /// is how token-classification heads are trained), then merge BIO into spans.
    fn merge(
        &self,
        text: &str,
        offsets: &[(usize, usize)],
        word_ids: &[Option<u32>],
        tags: &[Option<(usize, f32)>],
    ) -> Vec<Span> {
        // (label_id, score, byte_start, byte_end) per word.
        let mut words: Vec<(usize, f32, usize, usize)> = Vec::new();
        let mut prev_word: Option<u32> = None;
        for (i, tag) in tags.iter().enumerate() {
            let Some((id, score)) = *tag else { continue };
            let (s, e) = offsets[i];
            if s == e {
                continue;
            }
            let word = word_ids.get(i).copied().flatten();
            let same_word = word.is_some() && word == prev_word;
            prev_word = word;
            if same_word {
                if let Some(last) = words.last_mut() {
                    last.3 = e; // extend the word, keep the first subword's label
                    continue;
                }
            }
            words.push((id, score, s, e));
        }

        let mut spans: Vec<Span> = Vec::new();
        for &(id, score, s, e) in &words {
            let label = &self.id2label[id];
            if label == "O" {
                continue;
            }
            let (prefix, entity) = label.split_at(2);
            let entity = entity.to_string();
            let continues = matches!(spans.last(), Some(last)
                if last.label == entity
                    && prefix == "I-"
                    // contiguous or separated only by whitespace/punctuation glue
                    && text[last.end..s].trim().is_empty());
            if continues {
                let last = spans.last_mut().unwrap();
                last.end = e;
                last.score = last.score.min(score);
            } else {
                spans.push(Span {
                    label: entity,
                    start: s,
                    end: e,
                    score,
                });
            }
        }
        spans
    }
}

fn peak_rss_mb() -> f64 {
    std::fs::read_to_string("/proc/self/status")
        .ok()
        .and_then(|s| {
            s.lines()
                .find(|l| l.starts_with("VmHWM:"))
                .and_then(|l| l.split_whitespace().nth(1)?.parse::<f64>().ok())
        })
        .map(|kb| kb / 1024.0)
        .unwrap_or(f64::NAN)
}

fn median(v: &mut [f64]) -> f64 {
    v.sort_by(|a, b| a.partial_cmp(b).unwrap());
    if v.is_empty() {
        return f64::NAN;
    }
    v[v.len() / 2]
}

fn main() -> Result<()> {
    let args: Vec<String> = std::env::args().collect();
    if args.len() < 5 {
        eprintln!("usage: spike-ner <model.onnx> <tokenizer.json> <config.json> <input>...");
        std::process::exit(2);
    }
    let model_path = Path::new(&args[1]);
    let size_mb = std::fs::metadata(model_path)?.len() as f64 / (1024.0 * 1024.0);

    let (ner, load_ms) = Ner::load(model_path, Path::new(&args[2]), Path::new(&args[3]))?;
    println!(
        "model={} size={:.1}MB load={:.0}ms labels={:?}",
        model_path.file_name().unwrap().to_string_lossy(),
        size_mb,
        load_ms,
        ner.id2label
    );
    ner.describe_nodes();

    let mut all_window_ms: Vec<f64> = Vec::new();
    for path in &args[4..] {
        let text = std::fs::read_to_string(path).with_context(|| format!("read {path}"))?;
        println!("\n===== {path} ({} bytes) =====", text.len());
        let (spans, mut ms) = ner.tag(&text)?;
        for s in &spans {
            println!(
                "  {:<5} {:>4}..{:<4} {:.2}  {:?}",
                s.label,
                s.start,
                s.end,
                s.score,
                &text[s.start..s.end]
            );
        }
        if spans.is_empty() {
            println!("  (no entities)");
        }
        println!("  windows={} first_run_ms={:.1}", ms.len(), median(&mut ms));
    }

    // Warm latency benchmark on a full 512-token window.
    let filler = "Der Kunde Hans Müller aus der Hauptstraße 12 in Berlin arbeitet bei Siemens AG. ";
    let long: String = filler.repeat(200);
    let enc = ner
        .tokenizer
        .encode(long.as_str(), false)
        .map_err(|e| anyhow!("{e}"))?;
    let mut win: Vec<i32> = vec![ner.cls as i32];
    win.extend(enc.get_ids()[..WINDOW - 2].iter().map(|&i| i as i32));
    win.push(ner.sep as i32);
    for _ in 0..2 {
        ner.run_window(&win)?; // warm up
    }
    let mut runs: Vec<f64> = Vec::new();
    for _ in 0..9 {
        let t = Instant::now();
        ner.run_window(&win)?;
        runs.push(t.elapsed().as_secs_f64() * 1000.0);
    }
    all_window_ms.extend(runs.iter().copied());
    println!(
        "\nBENCH model={} size_mb={:.1} load_ms={:.0} window512_median_ms={:.1} \
         min={:.1} max={:.1} n=9 peak_rss_mb={:.0} threads={}",
        model_path.file_name().unwrap().to_string_lossy(),
        size_mb,
        load_ms,
        median(&mut runs),
        all_window_ms.iter().cloned().fold(f64::MAX, f64::min),
        all_window_ms.iter().cloned().fold(0.0, f64::max),
        peak_rss_mb(),
        std::thread::available_parallelism().map(|n| n.get()).unwrap_or(0),
    );
    Ok(())
}
