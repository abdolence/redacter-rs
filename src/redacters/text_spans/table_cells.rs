use super::{apply_redaction, Finding, REDACTED};

/// The findings in one table cell, with offsets into the cell. Only a blank cell is skipped,
/// so that digit-only values such as phone and card numbers reach the model; a provider that
/// can rule out more cells does so before calling this.
///
/// The cell is scored with its column header in front of it, `"city: Madrid"` rather than
/// `"Madrid"`: a model reads a one-word cell better as a sentence. The prefix is scoring
/// context only: `shift_into_cell` maps the findings back onto the cell, so no header text
/// can reach a redacted cell or a detection.
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
/// the rest byte for byte, and counts the findings. The closure decides which cells reach
/// the model, so a provider that can rule out cells (a name model skips a cell with no
/// letter) wraps [`find_in_cell`] instead of passing it directly.
pub fn redact_rows<F, E>(
    rows: Vec<Vec<String>>,
    mut find_in: F,
) -> Result<(Vec<Vec<String>>, usize), E>
where
    F: FnMut(usize, &str) -> Result<Vec<Finding>, E>,
{
    let mut findings = 0usize;
    let mut redacted_rows = Vec::with_capacity(rows.len());
    for row in rows {
        let mut redacted_row = Vec::with_capacity(row.len());
        for (column, cell) in row.into_iter().enumerate() {
            let in_cell = find_in(column, &cell)?;
            if in_cell.is_empty() {
                redacted_row.push(cell);
                continue;
            }
            findings += in_cell.len();
            redacted_row.push(apply_redaction(&cell, &in_cell, REDACTED));
        }
        redacted_rows.push(redacted_row);
    }
    Ok((redacted_rows, findings))
}

/// Maps findings on `"{prefix}{cell}"` back onto `cell`. A finding that ends inside the
/// prefix is the model matching the header itself and is dropped; one that crosses the
/// boundary keeps only the part that falls in the cell. `prefix_len` is a byte length and the
/// cell starts exactly there, so every clamped offset stays on a character boundary.
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
