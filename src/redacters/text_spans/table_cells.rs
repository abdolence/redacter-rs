use super::{apply_redaction, Finding, REDACTED};
use crate::redacters::Detection;
use crate::AppResult;

/// The findings in one table cell, with offsets into the cell. Only a blank cell is skipped,
/// so that digit-only values such as phone and card numbers reach the model; a provider that
/// can rule out more cells does so before calling this.
///
/// The cell is scored with its column header in front of it, `"city: Madrid"` rather than
/// `"Madrid"`: a model reads a one-word cell better as a sentence. The prefix is scoring
/// context only: `shift_into_cell` maps the findings back onto the cell, so no header text
/// can reach a redacted cell or a detection.
#[cfg(any(
    feature = "local-ner",
    feature = "local-gliner",
    feature = "local-credential-ml"
))]
pub fn find_in_cell<F, E>(
    headers: &[String],
    column: usize,
    cell: &str,
    find_in: &mut F,
) -> Result<Vec<Finding>, E>
where
    F: FnMut(&str) -> Result<Vec<Finding>, E>,
{
    if cell.trim().is_empty() {
        return Ok(Vec::new());
    }
    // A row wider than the header list, or an empty header, leaves the cell alone.
    let prefix = match headers.get(column) {
        Some(header) if !header.is_empty() => format!("{header}: "),
        _ => String::new(),
    };
    let scored = if prefix.is_empty() {
        find_in(cell)?
    } else {
        find_in(&format!("{prefix}{cell}"))?
    };
    Ok(shift_into_cell(scored, prefix.len()))
}

/// Redacts every table cell that `find_in(column, cell)` reports a finding in, leaving
/// the rest byte for byte, and returns the findings as detections located in their cell of the
/// rows given. The closure decides which cells reach the model, so a provider that can rule
/// out cells (a name model skips a cell with no letter) wraps [`find_in_cell`] instead of
/// passing it directly.
pub fn redact_rows<F>(
    rows: Vec<Vec<String>>,
    mut find_in: F,
) -> AppResult<(Vec<Vec<String>>, Vec<Detection>)>
where
    F: FnMut(usize, &str) -> AppResult<Vec<Finding>>,
{
    let mut detections = Vec::new();
    let mut redacted_rows = Vec::with_capacity(rows.len());
    for (row_index, row) in rows.into_iter().enumerate() {
        let mut redacted_row = Vec::with_capacity(row.len());
        for (column, cell) in row.into_iter().enumerate() {
            let in_cell = find_in(column, &cell)?;
            if in_cell.is_empty() {
                redacted_row.push(cell);
                continue;
            }
            for finding in &in_cell {
                detections.push(Detection::in_table_cell(
                    row_index,
                    column,
                    &cell,
                    finding.start..finding.end,
                    finding.rule.clone().into(),
                    finding.score,
                )?);
            }
            redacted_row.push(apply_redaction(&cell, &in_cell, REDACTED));
        }
        redacted_rows.push(redacted_row);
    }
    Ok((redacted_rows, detections))
}

/// Maps findings on `"{prefix}{cell}"` back onto `cell`. A finding that ends inside the
/// prefix is the model matching the header itself and is dropped; one that crosses the
/// boundary keeps only the part that falls in the cell. `prefix_len` is a byte length and the
/// cell starts exactly there, so every clamped offset stays on a character boundary.
#[cfg(any(
    feature = "local-ner",
    feature = "local-gliner",
    feature = "local-credential-ml"
))]
fn shift_into_cell(findings: Vec<Finding>, prefix_len: usize) -> Vec<Finding> {
    findings
        .into_iter()
        .filter(|finding| finding.end > prefix_len)
        .map(|mut finding| {
            finding.start = finding.start.max(prefix_len) - prefix_len;
            finding.end -= prefix_len;
            finding
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::redacters::text_spans::RuleName;
    use crate::redacters::{DetectionLabel, DetectionLocation};

    fn finding(start: usize, end: usize, rule: &str) -> Finding {
        Finding {
            start,
            end,
            rule: RuleName::new(rule),
            score: None,
        }
    }

    #[test]
    fn each_redacted_cell_is_returned_as_a_detection_in_its_cell() {
        let rows = vec![
            vec!["Ana Lima".to_string(), "Oslo".to_string()],
            vec!["no name".to_string(), "Ana Lima, Oslo".to_string()],
        ];

        let (redacted, detections) = redact_rows(rows, |column, cell| {
            Ok(match (column, cell) {
                (0, "Ana Lima") => vec![finding(0, 8, "person")],
                (1, "Ana Lima, Oslo") => vec![finding(0, 8, "person"), finding(10, 14, "city")],
                _ => Vec::new(),
            })
        })
        .unwrap();

        assert_eq!(
            redacted,
            vec![
                vec!["[REDACTED]", "Oslo"],
                vec!["no name", "[REDACTED], [REDACTED]"],
            ]
        );
        let placed: Vec<_> = detections
            .iter()
            .map(|detection| {
                (
                    detection.label.clone(),
                    detection.location,
                    detection.value.as_str(),
                )
            })
            .collect();
        assert_eq!(
            placed,
            vec![
                (
                    DetectionLabel::new("person"),
                    DetectionLocation::TableCell {
                        row: 1,
                        column: 1,
                        start: 0,
                        end: 8
                    },
                    "Ana Lima"
                ),
                (
                    DetectionLabel::new("person"),
                    DetectionLocation::TableCell {
                        row: 2,
                        column: 2,
                        start: 0,
                        end: 8
                    },
                    "Ana Lima"
                ),
                (
                    DetectionLabel::new("city"),
                    DetectionLocation::TableCell {
                        row: 2,
                        column: 2,
                        start: 10,
                        end: 14
                    },
                    "Oslo"
                ),
            ]
        );
    }

    #[test]
    fn a_table_with_nothing_found_returns_no_detections() {
        let rows = vec![vec!["Oslo".to_string()]];
        let (redacted, detections) = redact_rows(rows.clone(), |_, _| Ok(Vec::new())).unwrap();
        assert_eq!(redacted, rows);
        assert!(detections.is_empty());
    }
}
