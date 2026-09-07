//! The built-in rule table. Rows are matched independently and merged by `RuleSet`, so
//! declaration order only decides which name a finding gets when two rules match exactly
//! the same span (the first declared wins).

use super::super::validators;
use super::{Example, RuleGroup, RuleSpec};

pub(super) fn builtin_rules() -> &'static [RuleSpec] {
    RULES
}

const RULES: &[RuleSpec] = &[
    RuleSpec {
        name: "email",
        group: RuleGroup::Email,
        capture_group: 0,
        validator: None,
        keyword: false,
        description: "Email address",
        example: Example::Synthetic("john.smith@example.com"),
        pattern: r"(?i)\b[a-z0-9._%+-]+@(?:[a-z0-9-]+\.)+[a-z]{2,}\b",
    },
    RuleSpec {
        name: "phone-international",
        group: RuleGroup::Phone,
        capture_group: 1,
        validator: Some(("8 to 15 varied digits", validators::phone_digits)),
        keyword: false,
        description: "Phone number with a `+` or `00` country code, 8 to 15 digits",
        example: Example::Synthetic("+44 20 7946 0958"),
        // The leading `(?:^|[^\w.+-])` is a left boundary `\b` cannot express: without it the
        // `+` of a semver build suffix (`1.2.3+20240115`) starts a match. Only group 1 is
        // redacted so the character before the number survives.
        pattern: r"(?:^|[^\w.+-])((?:\+|00)[1-9]\d{0,2}[ .-]?(?:\(\d{1,4}\)[ .-]?)?\d(?:[ .-]?\d){6,12})\b",
    },
    RuleSpec {
        name: "phone-national",
        group: RuleGroup::Phone,
        capture_group: 0,
        validator: Some(("9 to 15 varied digits", validators::phone_national_digits)),
        keyword: false,
        description: "National phone number: an area code and 7 digits, or a trunk `0` and 9 to 12 digits in groups",
        example: Example::Synthetic("020 7946 0958"),
        // NANP-style area code plus 3-4, or a trunk `0` followed by 2 to 5 groups of digits
        // (UK `020 7946 0958`, FR `01 23 45 67 89`, DE `030 901820`). The trunk form repeats
        // one separator rather than mixing them, so a run of dates (`2024-04-30 2024-05-01`)
        // cannot be joined into one number; the validator enforces the 9-digit minimum that
        // keeps `03.04.2024` and other 8-digit shapes out. Hyphen-separated groups need three
        // digits because 3-2-4 with hyphens is the US SSN shape, whose own rule owns it.
        pattern: r"(?:\(\d{2,4}\)|\b\d{2,4})[ .-]\d{3}[ .-]\d{3,4}\b|\b0\d{1,4}(?:(?: \d{2,8}){1,4}|(?:\.\d{2,8}){1,4}|(?:-\d{3,8}){1,4})\b",
    },
    RuleSpec {
        name: "iban",
        group: RuleGroup::Iban,
        capture_group: 0,
        validator: Some(("ISO 7064 mod 97-10 and national length", validators::iban)),
        keyword: false,
        description: "International bank account number",
        example: Example::Synthetic("GB82 WEST 1234 5698 7654 32"),
        pattern: r"\b[A-Z]{2}\d{2}(?: ?[A-Z0-9]){11,30}\b",
    },
    RuleSpec {
        name: "payment-card",
        group: RuleGroup::PaymentCard,
        capture_group: 0,
        validator: Some(("Luhn", validators::luhn)),
        keyword: false,
        description: "Payment card number of 13 to 19 digits, grouped 4-4-4-4 or Amex 4-6-5",
        example: Example::Synthetic("4111 1111 1111 1111"),
        // Separators are only allowed where cards actually group them (4-4-4-4 with an
        // optional 3-digit tail, or Amex 4-6-5); a separator between any two digits made
        // pairs of dates such as `2024-04-30 2024-05-01` a card whenever Luhn passed. An
        // unbroken run must start with a card major industry digit, which keeps epoch
        // milliseconds (13 digits starting with 1 until 2033) out.
        pattern: r"\b(?:\d{4}[ -]\d{4}[ -]\d{4}[ -]\d{4}(?:[ -]\d{3})?|\d{4}[ -]\d{6}[ -]\d{5}|[3-6]\d{12,18})\b",
    },
    RuleSpec {
        name: "ipv4",
        group: RuleGroup::Network,
        capture_group: 0,
        validator: Some(("address parse", validators::ipv4)),
        keyword: false,
        description: "IPv4 address",
        example: Example::Synthetic("192.168.0.1"),
        pattern: r"\b\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\b",
    },
    RuleSpec {
        name: "ipv6",
        group: RuleGroup::Network,
        capture_group: 0,
        validator: Some(("address parse", validators::ipv6)),
        keyword: false,
        description: "IPv6 address",
        example: Example::Synthetic("2001:db8::1"),
        pattern: r"(?i)\b(?:[0-9a-f]{0,4}:){2,7}[0-9a-f]{1,4}\b",
    },
    RuleSpec {
        name: "mac-address",
        group: RuleGroup::Network,
        capture_group: 0,
        validator: None,
        keyword: false,
        description: "MAC address, colon or hyphen separated",
        example: Example::Synthetic("00:1A:2B:3C:4D:5E"),
        pattern: r"(?i)\b(?:[0-9a-f]{2}[:-]){5}[0-9a-f]{2}\b",
    },
    RuleSpec {
        name: "url-credentials",
        group: RuleGroup::Url,
        capture_group: 1,
        validator: None,
        keyword: false,
        description: "`user:password` embedded in a URL; only the credentials are redacted",
        example: Example::Synthetic("https://alice:s3cret@example.com/path"),
        pattern: r"\b[a-zA-Z][a-zA-Z0-9+.-]*://([^\s/@:]+:[^\s/@]+)@",
    },
    RuleSpec {
        name: "aws-access-key",
        group: RuleGroup::Secrets,
        capture_group: 0,
        validator: None,
        keyword: false,
        description: "AWS access key id (`AKIA` or `ASIA` and 16 characters)",
        example: Example::Synthetic("AKIAIOSFODNN7EXAMPLE"),
        pattern: r"\b(?:AKIA|ASIA)[A-Z0-9]{16}\b",
    },
    RuleSpec {
        name: "aws-secret-key",
        group: RuleGroup::Secrets,
        capture_group: 1,
        validator: None,
        keyword: true,
        description: "AWS secret access key: 40 characters within 20 characters of the words `aws` and `secret`",
        example: Example::Synthetic("aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY"),
        // Tighter than the spec, which asks only for a 40-character value near the word
        // `secret`: any `secret=<40 chars>` matches base64-looking build hashes and generic
        // application secrets, so `aws` is required nearby as well.
        pattern: r#"(?i)aws.{0,20}?secret.{0,20}?[=:'"\s]\s*([A-Za-z0-9/+=]{40})(?:[^A-Za-z0-9/+=]|$)"#,
    },
    RuleSpec {
        name: "gcp-api-key",
        group: RuleGroup::Secrets,
        capture_group: 1,
        validator: None,
        keyword: false,
        description: "Google API key (`AIza` and 35 characters)",
        example: Example::Synthetic("AIzaSyA1234567890abcdefghijklmnopqrstuv"),
        pattern: r"\b(AIza[0-9A-Za-z_-]{35})(?:[^0-9A-Za-z_-]|$)",
    },
    RuleSpec {
        name: "github-token",
        group: RuleGroup::Secrets,
        capture_group: 0,
        validator: None,
        keyword: false,
        description: "GitHub token (`ghp_`, `gho_`, `ghu_`, `ghs_`, `ghr_` or `github_pat_`)",
        example: Example::Synthetic("ghp_abcdefghijklmnopqrstuvwxyz0123456789"),
        pattern: r"\b(?:(?:ghp|gho|ghu|ghs|ghr)_[A-Za-z0-9]{36,}|github_pat_[A-Za-z0-9_]{22,})\b",
    },
    RuleSpec {
        name: "slack-token",
        group: RuleGroup::Secrets,
        capture_group: 0,
        validator: None,
        keyword: false,
        description: "Slack token (`xoxa-`, `xoxb-`, `xoxp-`, `xoxr-`)",
        example: Example::Synthetic("xoxb-123456789012-abcdefghij"),
        pattern: r"\bxox[abpr]-[A-Za-z0-9-]{10,}\b",
    },
    RuleSpec {
        name: "stripe-key",
        group: RuleGroup::Secrets,
        capture_group: 0,
        validator: None,
        keyword: false,
        description: "Stripe live secret or restricted key (`sk_live_`, `rk_live_`)",
        example: Example::Synthetic("sk_live_abcdefghijklmnop"),
        pattern: r"\b(?:sk|rk)_live_[A-Za-z0-9]{16,}\b",
    },
    RuleSpec {
        name: "jwt",
        group: RuleGroup::Secrets,
        capture_group: 0,
        validator: None,
        keyword: false,
        description: "JSON Web Token: three base64url parts, the first starting with `eyJ`",
        example: Example::Synthetic("eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.abc123def456"),
        pattern: r"\beyJ[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\.[A-Za-z0-9_-]{5,}\b",
    },
    RuleSpec {
        name: "authorization-header",
        group: RuleGroup::Secrets,
        capture_group: 1,
        validator: None,
        keyword: true,
        description: "Value of an `Authorization: Bearer` or `Basic` header",
        example: Example::Synthetic("Authorization: Bearer abc.def.ghi"),
        pattern: r"(?i)\bauthorization\s*:\s*(?:bearer|basic)\s+([A-Za-z0-9._~+/=-]+)",
    },
    RuleSpec {
        name: "private-key-block",
        group: RuleGroup::Secrets,
        capture_group: 0,
        validator: None,
        keyword: false,
        description: "PEM or PGP private key block from `BEGIN` to `END`",
        example: Example::Synthetic("-----BEGIN RSA PRIVATE KEY-----\nMIIB\n-----END RSA PRIVATE KEY-----"),
        // `BLOCK` is optional so armoured PGP keys (`BEGIN PGP PRIVATE KEY BLOCK`) match too.
        pattern: r"(?s)-----BEGIN [A-Z ]*PRIVATE KEY(?: BLOCK)?-----.*?-----END [A-Z ]*PRIVATE KEY(?: BLOCK)?-----",
    },
    RuleSpec {
        name: "us-ssn",
        group: RuleGroup::UsIdentifiers,
        capture_group: 0,
        validator: Some(("SSA area and group rules", validators::us_ssn_or_itin)),
        keyword: false,
        description: "US Social Security number `AAA-GG-SSSS` or ITIN",
        example: Example::Synthetic("123-45-6789"),
        pattern: r"\b\d{3}-\d{2}-\d{4}\b",
    },
    RuleSpec {
        name: "us-passport",
        group: RuleGroup::UsIdentifiers,
        capture_group: 1,
        validator: None,
        keyword: true,
        description: "Passport number of 8 or 9 digits within 20 characters of the word passport",
        example: Example::Synthetic("Passport number: 123456789"),
        pattern: r"(?i)\bpassport\b.{0,20}?\b([A-Z]?\d{8,9})\b",
    },
    RuleSpec {
        name: "eu-vat",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some((
            "per-country check (mod 11,10 / mod 97 / Luhn / eleven test)",
            validators::eu_vat,
        )),
        keyword: false,
        description: "VAT number of DE, FR, IT, ES, NL, BE or GB",
        example: Example::Synthetic("DE136695976"),
        pattern: r"\b(?:DE\d{9}|FR[A-Z0-9]{2}\d{9}|IT\d{11}|ES[A-Z0-9]\d{7}[A-Z0-9]|NL\d{9}B\d{2}|BE0\d{9}|GB\d{9}(?:\d{3})?)\b",
    },
    RuleSpec {
        name: "spanish-dni-nie",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod 23 control letter", validators::spanish_dni_nie)),
        keyword: false,
        description: "Spanish DNI (8 digits and a letter) or NIE (X, Y or Z, 7 digits and a letter)",
        example: Example::Synthetic("12345678Z"),
        pattern: r"\b(?:\d{8}|[XYZ]\d{7})[A-Z]\b",
    },
    RuleSpec {
        name: "italian-codice-fiscale",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("control character", validators::italian_codice_fiscale)),
        keyword: false,
        description: "Italian codice fiscale, 16 characters",
        example: Example::Synthetic("RSSMRA85M01H501Q"),
        pattern: r"\b[A-Z]{6}\d{2}[A-EHLMPRST]\d{2}[A-Z]\d{3}[A-Z]\b",
    },
    RuleSpec {
        name: "dutch-bsn",
        group: RuleGroup::EuIdentifiers,
        capture_group: 1,
        validator: Some(("eleven test", validators::dutch_bsn)),
        keyword: true,
        description: "Dutch BSN, 9 digits within 20 characters of `bsn`, `burgerservicenummer` or `sofinummer`",
        example: Example::Synthetic("BSN 111222333"),
        pattern: r"(?i)\b(?:bsn|burgerservicenummer|sofinummer|sofi-nummer)\b.{0,20}?\b(\d{9})\b",
    },
    RuleSpec {
        name: "uk-nino",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("prefix and suffix rules", validators::uk_nino)),
        keyword: false,
        description: "UK National Insurance number `AB 12 34 56 C`",
        example: Example::Synthetic("AB 12 34 56 C"),
        pattern: r"\b[A-Z]{2} ?\d{2} ?\d{2} ?\d{2} ?[A-D]\b",
    },
    RuleSpec {
        name: "french-nir",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod 97 key", validators::french_nir)),
        keyword: false,
        description: "French NIR (social security number), 15 digits including the key",
        example: Example::Synthetic("2 69 05 49 588 157 80"),
        pattern: r"\b[12] ?\d{2} ?(?:0[1-9]|1[0-2]|20) ?(?:\d{2}|2[AB]) ?\d{3} ?\d{3} ?\d{2}\b",
    },
    RuleSpec {
        name: "german-steuer-id",
        group: RuleGroup::EuIdentifiers,
        capture_group: 1,
        validator: Some(("ISO 7064 MOD 11,10", validators::german_steuer_id)),
        keyword: true,
        description: "German Steuer-ID, 11 digits within 20 characters of `Steuer-ID`, `IdNr` or `tax id`",
        example: Example::Synthetic("Steuer-ID 86095742719"),
        pattern: r"(?i)\b(?:steuer-?id|steuer-?idnr|idnr|steueridentifikationsnummer|steuerliche identifikationsnummer|tax id)\b.{0,20}?\b([1-9]\d{10})\b",
    },
    RuleSpec {
        name: "swedish-personnummer",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("Luhn and date", validators::swedish_personnummer)),
        keyword: false,
        description: "Swedish personnummer or samordningsnummer, `YYMMDD-NNNC` or `YYYYMMDD-NNNC`",
        example: Example::Synthetic("811218-9876"),
        // The century is two digits: `19811218-9876` is `19` + `811218` + `-9876`.
        pattern: r"\b(?:18|19|20)?\d{6}[-+]?\d{4}\b",
    },
    RuleSpec {
        name: "norwegian-fodselsnummer",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("two mod-11 control digits and date", validators::norwegian_fodselsnummer)),
        keyword: false,
        description: "Norwegian fødselsnummer or D-number, 11 digits `DDMMYY IIIKK`",
        example: Example::Synthetic("01019012480"),
        pattern: r"\b\d{6} ?\d{5}\b",
    },
    RuleSpec {
        name: "danish-cpr",
        group: RuleGroup::EuIdentifiers,
        capture_group: 1,
        validator: Some(("date", validators::danish_cpr)),
        keyword: true,
        description: "Danish CPR number `DDMMYY-SSSS` within 20 characters of `cpr` or `personnummer`",
        example: Example::Synthetic("CPR-nr. 010203-1234"),
        pattern: r"(?i)\b(?:cpr|personnummer)\b.{0,20}?\b(\d{6}-?\d{4})\b",
    },
    RuleSpec {
        name: "finnish-hetu",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod-31 control character and date", validators::finnish_hetu)),
        keyword: false,
        description: "Finnish henkilötunnus `DDMMYYCZZZQ` with its century marker",
        // https://dvv.fi/en/personal-identity-code, the "Anna Suomalainen" worked example.
        example: Example::Published("131052-308T"),
        pattern: r"\b\d{6}[-+A-FU-Y]\d{3}[0-9A-FHJ-NPR-Y]\b",
    },
    RuleSpec {
        name: "icelandic-kennitala",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod-11 control digit and date", validators::icelandic_kennitala)),
        keyword: false,
        description: "Icelandic kennitala of a person, `DDMMYY-NNCM`",
        // https://en.wikipedia.org/wiki/Icelandic_identification_number
        example: Example::Published("120174-3399"),
        pattern: r"\b\d{6}-?\d{4}\b",
    },
    RuleSpec {
        name: "baltic-personal-code",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod-11 control digit and date", validators::baltic_personal_code)),
        keyword: false,
        description: "Estonian isikukood or Lithuanian asmens kodas, 11 digits `GYYMMDDSSSC`",
        // https://et.wikipedia.org/wiki/Isikukood, the worked checksum example.
        example: Example::Published("37605030299"),
        pattern: r"\b[1-6]\d{10}\b",
    },
    RuleSpec {
        name: "latvian-personas-kods",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod-11 control digit, date on the pre-2017 form", validators::latvian_personas_kods)),
        keyword: false,
        description: "Latvian personas kods, `DDMMYY-CSSSK` or the post-2017 `32SSSS-SSSSK`",
        example: Example::Synthetic("161175-19997"),
        pattern: r"\b\d{6}-?\d{5}\b",
    },
    RuleSpec {
        name: "polish-pesel",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod-10 control digit and date", validators::polish_pesel)),
        keyword: false,
        description: "Polish PESEL, 11 digits with the century encoded in the month",
        example: Example::Synthetic("44051401359"),
        pattern: r"\b\d{11}\b",
    },
    RuleSpec {
        name: "czech-slovak-rodne-cislo",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("divisible by 11 and date", validators::czech_slovak_rodne_cislo)),
        keyword: false,
        description: "Czech or Slovak rodné číslo, `YYMMDD/SSSC` (10 digits, issued since 1954)",
        example: Example::Synthetic("780123/3540"),
        pattern: r"\b\d{6}/?\d{4}\b",
    },
];

#[cfg(test)]
mod tests {
    use super::super::test_support::{all_rules, assert_redacts, assert_untouched};

    /// Strings no built-in rule may touch: order and epoch numbers, timestamps, versions,
    /// amounts, dates without a birth keyword, and checksum-failing neighbours.
    pub(super) const CONTROLS: &[&str] = &[
        "order 1234567890",
        "epoch 1700000000",
        "ref 12345678901",
        "ts 1725700000004",
        "id 9876543210987",
        "build v1.2.3+20240115",
        "sum 1.234.567,89",
        "am 03.04.2024",
        "on 2024-04-30",
        "GB82 WEST 1234 5698 7654 33",
        "4111 1111 1111 1112",
        "uid 123456789012",
    ];

    #[test]
    fn controls_are_kept() {
        for input in CONTROLS {
            assert_untouched(input);
        }
    }

    #[test]
    fn nordic_baltic_polish_and_czech_identifiers_are_redacted() {
        for (input, expected) in [
            ("personnummer 811218-9876", "personnummer [REDACTED]"),
            ("pnr 198112189876", "pnr [REDACTED]"),
            (
                "samordningsnummer 811278-9873",
                "samordningsnummer [REDACTED]",
            ),
            ("fnr 01019012480", "fnr [REDACTED]"),
            ("d-nummer 41019012393", "d-nummer [REDACTED]"),
            ("CPR-nr. 010203-1234", "CPR-nr. [REDACTED]"),
            ("personnummer: 0102031234", "personnummer: [REDACTED]"),
            ("hetu 131052-308T", "hetu [REDACTED]"),
            ("hetu 010594Y9021", "hetu [REDACTED]"),
            ("kennitala 120174-3399", "kennitala [REDACTED]"),
            ("isikukood 37605030299", "isikukood [REDACTED]"),
            ("asmens kodas 33309240064", "asmens kodas [REDACTED]"),
            ("personas kods 161175-19997", "personas kods [REDACTED]"),
            ("personas kods 320000-12340", "personas kods [REDACTED]"),
            ("PESEL 44051401359", "PESEL [REDACTED]"),
            ("rodné číslo 780123/3540", "rodné číslo [REDACTED]"),
            ("rodné číslo 7801233540", "rodné číslo [REDACTED]"),
        ] {
            assert_redacts(input, expected);
        }
    }

    #[test]
    fn nordic_baltic_polish_and_czech_lookalikes_are_kept() {
        for input in [
            "811218-9877",     // Swedish Luhn fails
            "8113189875",      // Luhn passes, month 13
            "15068512334",     // Norwegian second control digit wrong
            "ref 010203-1235", // Danish shape without a keyword (and no other checksum)
            "131052-308U",     // Finnish control character wrong
            "120174-3389",     // Icelandic control digit wrong
            "39001011238",     // Baltic control digit wrong
            "010190-11232",    // Latvian control digit wrong
            "44051401358",     // PESEL control digit wrong
            "900101/1238",     // rodné číslo not divisible by 11
        ] {
            assert_untouched(input);
        }
    }

    #[test]
    fn same_span_matched_by_two_rules_redacts_once_and_deterministically() {
        // "010101-0189" is a 10-digit shape that is simultaneously a valid Swedish
        // personnummer (Luhn over all 10 digits, month 01, day 01) and a valid Icelandic
        // kennitala (day 01, month 01, year 1901, mod-11 control digit 8): both rules'
        // patterns match the identical span, so the merge must keep exactly one finding,
        // and which rule's name wins must not change between runs.
        let text = "id 010101-0189 done";
        let (first_text, first_findings) = all_rules().redact(text);
        let (second_text, second_findings) = all_rules().redact(text);
        assert_eq!(first_text, "id [REDACTED] done");
        assert_eq!(first_findings.len(), 1, "{first_findings:?}");
        assert_eq!(first_text, second_text);
        assert_eq!(first_findings, second_findings);
    }
}
