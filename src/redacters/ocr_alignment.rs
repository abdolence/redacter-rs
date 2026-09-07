//! Aligns the words an OCR pass produced against the text a redacter chain returned, so the
//! image redacter can box exactly the words that changed instead of every word that no longer
//! appears anywhere in a document-wide set (which misses a word that is also part of an
//! unredacted span elsewhere on the page, and mishandles partial or repeated-word matches).

/// Marks which of `original`'s words were changed by redaction, by aligning `original` against
/// `redacted` with a longest-common-subsequence match on whitespace-separated tokens.
///
/// The result has exactly `original.len()` entries; `result[i]` is `true` when `original[i]`
/// was not matched by an identical token in `redacted`, at the position the alignment lines up
/// with it. This correctly marks:
/// - a single word replaced by a token (e.g. `[REDACTED]`),
/// - several consecutive words collapsed into one replacement token,
/// - a partial replacement inside a word (`Astrid` -> `[REDACTED]rid`), because the token no
///   longer matches the original word at all,
/// - only the occurrence of a repeated word that was actually redacted, because the LCS keeps
///   the earliest alignment that preserves order rather than matching by value alone.
///
/// `redacted` is tokenised on any run of whitespace, so joining OCR words with newlines between
/// lines (rather than plain spaces) does not affect the alignment.
pub fn words_to_redact(original: &[String], redacted: &str) -> Vec<bool> {
    let redacted_tokens: Vec<&str> = redacted.split_whitespace().collect();
    let n = original.len();
    let m = redacted_tokens.len();

    // dp[i][j] = length of the LCS of original[i..] and redacted_tokens[j..].
    let mut dp = vec![vec![0usize; m + 1]; n + 1];
    for i in (0..n).rev() {
        for j in (0..m).rev() {
            dp[i][j] = if original[i] == redacted_tokens[j] {
                dp[i + 1][j + 1] + 1
            } else {
                dp[i + 1][j].max(dp[i][j + 1])
            };
        }
    }

    // Walk the table forward, following the same tie-break the table was filled with, to find
    // which original indices participate in the LCS: those are the words left untouched.
    let mut matched = vec![false; n];
    let (mut i, mut j) = (0, 0);
    while i < n && j < m {
        if original[i] == redacted_tokens[j] {
            matched[i] = true;
            i += 1;
            j += 1;
        } else if dp[i + 1][j] >= dp[i][j + 1] {
            i += 1;
        } else {
            j += 1;
        }
    }

    matched.into_iter().map(|is_matched| !is_matched).collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn words(text: &str) -> Vec<String> {
        text.split_whitespace().map(str::to_string).collect()
    }

    #[test]
    fn no_change_marks_nothing() {
        let original = words("the quick brown fox");
        let to_redact = words_to_redact(&original, "the quick brown fox");
        assert_eq!(to_redact, vec![false, false, false, false]);
    }

    #[test]
    fn everything_replaced_marks_everything() {
        let original = words("the quick brown fox");
        let to_redact = words_to_redact(&original, "[REDACTED] [REDACTED] [REDACTED] [REDACTED]");
        assert_eq!(to_redact, vec![true, true, true, true]);
    }

    #[test]
    fn one_word_replaced_marks_only_that_word() {
        let original = words("call John today");
        let to_redact = words_to_redact(&original, "call [REDACTED] today");
        assert_eq!(to_redact, vec![false, true, false]);
    }

    #[test]
    fn three_consecutive_words_collapsed_into_one_token_marks_all_three() {
        let original = words("call John Michael Smith today");
        let to_redact = words_to_redact(&original, "call [REDACTED] today");
        assert_eq!(to_redact, vec![false, true, true, true, false]);
    }

    #[test]
    fn a_repeated_word_is_marked_only_at_the_redacted_occurrence() {
        let original = words("foo bar foo");
        // Only the second "foo" was redacted; the first is untouched.
        let to_redact = words_to_redact(&original, "foo bar [REDACTED]");
        assert_eq!(to_redact, vec![false, false, true]);
    }

    #[test]
    fn a_partial_replacement_inside_a_word_marks_that_word() {
        let original = words("hello Astrid goodbye");
        // A rule matched only "Ast" inside the word, leaving "rid" stuck to the token.
        let to_redact = words_to_redact(&original, "hello [REDACTED]rid goodbye");
        assert_eq!(to_redact, vec![false, true, false]);
    }

    #[test]
    fn a_redacter_that_inserts_a_space_does_not_panic_on_token_count_mismatch() {
        let original = words("email me at john@example.com now");
        // Suppose a redacter rewrites the email into two tokens instead of one.
        let to_redact = words_to_redact(&original, "email me at [REDACTED] [SPAN] now");
        assert_eq!(to_redact, vec![false, false, false, true, false]);
    }
}
