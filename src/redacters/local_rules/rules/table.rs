//! The built-in rule table. Rows are matched independently and merged by `RuleSet`, so
//! declaration order only decides which name a finding gets when two rules match exactly
//! the same span (the first declared wins).

use super::super::validators;
use super::{Example, RuleGroup, RuleSpec};

pub(super) fn builtin_rules() -> &'static [RuleSpec] {
    RULES
}

/// Keywords that anchor the driving licence rules, in the languages the passport rule covers.
macro_rules! driving_licence_keywords {
    () => {
        r"driving licen[cs]e|driver'?s licen[cs]e|driver licen[cs]e|f[üu]hrerschein|fuehrerschein|permis de conduire|licencia de conducir|carnet de conducir|patente di guida|rijbewijs|carta de condu[çc][ãa]o|prawo jazdy|k[öo]rkort"
    };
}

/// Keywords that anchor the birth-date rule: en, de, fr, es, it, nl, pt, pl, sv. `né le`
/// needs its accent, because `ne le` is ordinary French.
macro_rules! birth_keywords {
    () => {
        r"born(?: on)?|d\.?o\.?b|date[ _-]of[ _-]birth|birth[ _-]?date|birthday|geburtsdatum|geboren(?: am| op)?|né(?:e|\(e\))? le|date de naissance|fecha de nacimiento|nacid[oa] el|data di nascita|nat[oa] il|geboortedatum|data de nascimento|nascid[oa] em|data urodzenia|urodzon[ya]|f[öo]dd|f[öo]delsedatum"
    };
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
        name: "passport",
        group: RuleGroup::UsIdentifiers,
        capture_group: 1,
        validator: None,
        keyword: true,
        description: "Passport number (6 to 9 digits with up to 3 letters, or the French `12AB34567` shape) within 20 characters of the word passport in en, de, fr, es, it, nl, pt, pl or sv",
        example: Example::Synthetic("Passport no. X1234567"),
        pattern: r"(?i)\b(?:passport|passeport|pasaporte|passaporto|paspoort|passaporte|paszport|reisepass|pass-?n(?:r|ummer))\b.{0,20}?\b([A-Z]{0,3}\d{6,9}|\d{2}[A-Z]{2}\d{5})\b",
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
        // The "Anna Suomalainen" worked example.
        example: Example::Published("131052-308T", "https://dvv.fi/en/personal-identity-code"),
        pattern: r"\b\d{6}[-+A-FU-Y]\d{3}[0-9A-FHJ-NPR-Y]\b",
    },
    RuleSpec {
        name: "icelandic-kennitala",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod-11 control digit and date", validators::icelandic_kennitala)),
        keyword: false,
        description: "Icelandic kennitala of a person, `DDMMYY-NNCM`",
        example: Example::Published(
            "120174-3399",
            "https://en.wikipedia.org/wiki/Icelandic_identification_number",
        ),
        pattern: r"\b\d{6}-?\d{4}\b",
    },
    RuleSpec {
        name: "baltic-personal-code",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod-11 control digit and date", validators::baltic_personal_code)),
        keyword: false,
        description: "Estonian isikukood or Lithuanian asmens kodas, 11 digits `GYYMMDDSSSC`",
        // The worked checksum example.
        example: Example::Published("37605030299", "https://et.wikipedia.org/wiki/Isikukood"),
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
    RuleSpec {
        name: "portuguese-nif",
        group: RuleGroup::EuIdentifiers,
        capture_group: 1,
        validator: Some(("mod-11 check digit", validators::portuguese_nif)),
        keyword: true,
        description: "Portuguese NIF, 9 digits within 20 characters of `nif`, `nipc` or `contribuinte`",
        example: Example::Synthetic("NIF 123456789"),
        pattern: r"(?i)\b(?:nif|nipc|contribuinte)\b.{0,20}?\b(\d{3} ?\d{3} ?\d{3})\b",
    },
    RuleSpec {
        name: "irish-pps",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod-23 check letter", validators::irish_pps)),
        keyword: false,
        description: "Irish PPS number, 7 digits and one or two letters",
        // The worked example.
        example: Example::Published(
            "1234567FA",
            "https://en.wikipedia.org/wiki/Personal_Public_Service_Number",
        ),
        pattern: r"\b\d{7}[A-W][A-IW]?\b",
    },
    RuleSpec {
        name: "swiss-ahv",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("EAN-13 check digit", validators::swiss_ahv)),
        keyword: false,
        description: "Swiss AHV/AVS number `756.NNNN.NNNN.NC`",
        example: Example::Synthetic("756.9217.0769.85"),
        pattern: r"\b756\.?\d{4}\.?\d{4}\.?\d{2}\b",
    },
    RuleSpec {
        name: "austrian-svnr",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod-11 check digit and date", validators::austrian_svnr)),
        keyword: false,
        description: "Austrian Sozialversicherungsnummer `LLLP DDMMYY`",
        example: Example::Synthetic("1237 010180"),
        pattern: r"\b[1-9]\d{3} ?\d{6}\b",
    },
    RuleSpec {
        name: "belgian-national-number",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod-97 check and date", validators::belgian_national_number)),
        keyword: false,
        description: "Belgian national number `YY.MM.DD-SSS.CC`",
        example: Example::Synthetic("85.07.30-033.28"),
        pattern: r"\b\d{2}\.?\d{2}\.?\d{2}[-.]?\d{3}\.?\d{2}\b",
    },
    RuleSpec {
        name: "luxembourg-matricule",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("Luhn and Verhoeff check digits and date", validators::luxembourg_matricule)),
        keyword: false,
        description: "Luxembourg matricule, 13 digits starting with the birth date `YYYYMMDD`",
        example: Example::Synthetic("1893120105732"),
        pattern: r"\b(?:18|19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{5}\b",
    },
    RuleSpec {
        name: "hungarian-personal-number",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod-11 check digit and date", validators::hungarian_personal_number)),
        keyword: false,
        description: "Hungarian személyi szám, 11 digits `M YYMMDD SSSC`",
        example: Example::Synthetic("1 900101 1249"),
        pattern: r"\b[1-8][ -]?\d{6}[ -]?\d{4}\b",
    },
    RuleSpec {
        name: "hungarian-taj",
        group: RuleGroup::EuIdentifiers,
        capture_group: 1,
        validator: Some(("mod-10 check digit", validators::hungarian_taj)),
        keyword: true,
        description: "Hungarian TAJ number, 9 digits within 20 characters of `TAJ`",
        example: Example::Synthetic("TAJ szám: 123 456 788"),
        pattern: r"(?i)\b(?:taj(?:[ -]?sz[áa]m)?|t[áa]rsadalombiztos[íi]t[áa]si)\b.{0,20}?\b(\d{3}[ -]?\d{3}[ -]?\d{3})\b",
    },
    RuleSpec {
        name: "greek-amka",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("Luhn and date", validators::greek_amka)),
        keyword: false,
        description: "Greek AMKA, 11 digits starting with the birth date `DDMMYY`",
        example: Example::Synthetic("01019012341"),
        pattern: r"\b\d{11}\b",
    },
    RuleSpec {
        name: "greek-afm",
        group: RuleGroup::EuIdentifiers,
        capture_group: 1,
        validator: Some(("mod-11 check digit", validators::greek_afm)),
        keyword: true,
        description: "Greek AFM (tax number), 9 digits within 20 characters of `AFM` or `ΑΦΜ`",
        example: Example::Synthetic("ΑΦΜ: 090000045"),
        pattern: r"(?i)\b(?:afm|α\.?φ\.?μ\.?)\b.{0,20}?\b(\d{9})\b",
    },
    RuleSpec {
        name: "bulgarian-egn",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod-11 check digit and date", validators::bulgarian_egn)),
        keyword: false,
        description: "Bulgarian EGN, 10 digits starting with the birth date `YYMMDD`",
        example: Example::Synthetic("6101057509"),
        pattern: r"\b\d{10}\b",
    },
    RuleSpec {
        name: "croatian-oib",
        group: RuleGroup::EuIdentifiers,
        capture_group: 1,
        validator: Some(("ISO 7064 MOD 11,10", validators::croatian_oib)),
        keyword: true,
        description: "Croatian OIB, 11 digits within 20 characters of `OIB`",
        example: Example::Synthetic("OIB: 69435151530"),
        pattern: r"(?i)\boib\b.{0,20}?\b(\d{11})\b",
    },
    RuleSpec {
        name: "jmbg",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod-11 check digit and date", validators::jmbg)),
        keyword: false,
        description: "JMBG of Serbia, Bosnia and Herzegovina, Montenegro and North Macedonia, and the Slovenian EMŠO, 13 digits",
        // The worked example (first male baby registered in Slovenia on 1 January 2006).
        example: Example::Published(
            "0101006500006",
            "https://en.wikipedia.org/wiki/Unique_Master_Citizen_Number",
        ),
        pattern: r"\b\d{13}\b",
    },
    RuleSpec {
        name: "romanian-cnp",
        group: RuleGroup::EuIdentifiers,
        capture_group: 0,
        validator: Some(("mod-11 check digit, date and county", validators::romanian_cnp)),
        keyword: false,
        description: "Romanian CNP, 13 digits `S YYMMDD JJ NNN C`",
        example: Example::Synthetic("1900101123457"),
        pattern: r"\b[1-9]\d{12}\b",
    },
    RuleSpec {
        name: "turkish-tckn",
        group: RuleGroup::EuIdentifiers,
        capture_group: 1,
        validator: Some(("two mod-10 check digits", validators::turkish_tckn)),
        keyword: true,
        description: "Turkish TCKN, 11 digits within 20 characters of `TCKN` or `T.C. Kimlik No`",
        example: Example::Synthetic("T.C. Kimlik No: 10000000146"),
        pattern: r"(?i)\b(?:tckn|t\.?c\.? ?kimlik(?: no| numaras[ıi])?|kimlik (?:no|numaras[ıi]))\b.{0,20}?\b([1-9]\d{10})\b",
    },
    RuleSpec {
        name: "albanian-nid",
        group: RuleGroup::EuIdentifiers,
        capture_group: 1,
        validator: Some(("date", validators::albanian_nid)),
        keyword: true,
        description: "Albanian NID (letter, 8 digits, letter) within 20 characters of `NID` or `numri personal`",
        example: Example::Synthetic("NID: I05101999Q"),
        pattern: r"(?i)\b(?:nid|numri (?:personal|i identitetit)|id personale)\b.{0,20}?\b([A-M]\d{8}[A-W])\b",
    },
    RuleSpec {
        name: "maltese-id",
        group: RuleGroup::EuIdentifiers,
        capture_group: 1,
        validator: None,
        keyword: true,
        description: "Maltese identity card number (7 digits and a letter) within 20 characters of `ID card`",
        example: Example::Synthetic("ID card no. 0123456M"),
        pattern: r"(?i)\b(?:id(?:entity)? card|karta tal-identit[àa]|maltese id|id number)\b.{0,20}?\b(\d{7}[MGAPLHBZ])\b",
    },
    RuleSpec {
        name: "canadian-sin",
        group: RuleGroup::WorldIdentifiers,
        capture_group: 1,
        validator: Some(("Luhn", validators::canadian_sin)),
        keyword: true,
        description: "Canadian SIN, 9 digits within 20 characters of `SIN`, `NAS` or `social insurance`",
        // "a fictitious, but valid, SIN".
        example: Example::Published(
            "SIN: 046 454 286",
            "https://en.wikipedia.org/wiki/Social_Insurance_Number",
        ),
        // `SIN` and `NAS` are case-sensitive on purpose: `sin` is an English word.
        pattern: r"\b(?:SIN|NAS|(?i:social insurance(?: number)?|num[ée]ro d'assurance sociale|assurance sociale))\b.{0,20}?\b(\d{3}[ -]?\d{3}[ -]?\d{3})\b",
    },
    RuleSpec {
        name: "australian-tfn",
        group: RuleGroup::WorldIdentifiers,
        capture_group: 1,
        validator: Some(("mod-11 weighted sum", validators::australian_tfn)),
        keyword: true,
        description: "Australian TFN, 9 digits within 20 characters of `TFN` or `tax file number`",
        example: Example::Synthetic("TFN 123 456 782"),
        pattern: r"(?i)\b(?:tfn|tax file (?:number|no))\b.{0,20}?\b(\d{3}[ -]?\d{3}[ -]?\d{3})\b",
    },
    RuleSpec {
        name: "brazilian-cpf",
        group: RuleGroup::WorldIdentifiers,
        capture_group: 0,
        validator: Some(("two mod-11 check digits", validators::brazilian_cpf)),
        keyword: false,
        description: "Brazilian CPF in its written form `NNN.NNN.NNN-NN`",
        example: Example::Synthetic("111.444.777-35"),
        pattern: r"\b\d{3}\.\d{3}\.\d{3}-\d{2}\b",
    },
    RuleSpec {
        name: "brazilian-cpf-plain",
        group: RuleGroup::WorldIdentifiers,
        capture_group: 1,
        validator: Some(("two mod-11 check digits", validators::brazilian_cpf)),
        keyword: true,
        description: "Brazilian CPF as 11 bare digits within 20 characters of `CPF`",
        example: Example::Synthetic("CPF: 12345678909"),
        pattern: r"(?i)\bcpf\b.{0,20}?\b(\d{11})\b",
    },
    RuleSpec {
        name: "indian-aadhaar",
        group: RuleGroup::WorldIdentifiers,
        capture_group: 1,
        validator: Some(("Verhoeff", validators::indian_aadhaar)),
        keyword: true,
        description: "Indian Aadhaar, 12 digits within 20 characters of `Aadhaar` or `UIDAI`",
        example: Example::Synthetic("Aadhaar 9999 4105 7058"),
        pattern: r"(?i)\b(?:aadha?ar|uidai)\b.{0,20}?\b([2-9]\d{3} ?\d{4} ?\d{4})\b",
    },
    RuleSpec {
        name: "south-african-id",
        group: RuleGroup::WorldIdentifiers,
        capture_group: 0,
        validator: Some(("Luhn, date and citizenship digit", validators::south_african_id)),
        keyword: false,
        description: "South African ID number, 13 digits starting with the birth date `YYMMDD`",
        example: Example::Synthetic("8001015009087"),
        pattern: r"\b\d{13}\b",
    },
    RuleSpec {
        name: "chinese-resident-id",
        group: RuleGroup::WorldIdentifiers,
        capture_group: 0,
        validator: Some(("ISO 7064 MOD 11-2 and date", validators::chinese_resident_id)),
        keyword: false,
        description: "Chinese resident identity number, 18 characters with the birth date `YYYYMMDD`",
        example: Example::Synthetic("11010519491231002X"),
        pattern: r"\b[1-9]\d{5}(?:18|19|20)\d{2}(?:0[1-9]|1[0-2])(?:0[1-9]|[12]\d|3[01])\d{3}[0-9X]\b",
    },
    RuleSpec {
        name: "us-driving-licence",
        group: RuleGroup::UsIdentifiers,
        capture_group: 1,
        validator: None,
        keyword: true,
        description: "US driver's license number (7 to 9 digits with an optional letter, or a letter and 11 to 14 digits) within 20 characters of a driving licence keyword or `DL#`",
        example: Example::Synthetic("Driver's License: A1234567"),
        // `dl ?#` cannot end on `\b` (both `#` and the space are non-word), hence the split.
        pattern: concat!(r"(?i)(?:\b(?:", driving_licence_keywords!(), r"|dl ?(?:no|number))\b|\bdl ?#).{0,20}?\b([A-Z]?\d{7,9}|[A-Z]\d{11,14})\b"),
    },
    RuleSpec {
        name: "uk-driving-licence",
        group: RuleGroup::EuIdentifiers,
        capture_group: 1,
        validator: Some(("date fields", validators::uk_driving_licence)),
        keyword: true,
        description: "UK driving licence number (16 characters encoding the birth date) within 20 characters of a driving licence keyword or `DVLA`",
        // No citable source gives a full worked number for this format; built from the
        // published field layout (surname, then date of birth, then initials).
        example: Example::Synthetic("Driving licence: MORGA657054SM9IJ"),
        pattern: concat!(r"(?i)\b(?:", driving_licence_keywords!(), r"|dvla)\b.{0,20}?\b([A-Z9]{5}\d{6}[A-Z9]{2}\d[A-Z0-9]{2})\b"),
    },
    RuleSpec {
        name: "uk-postcode",
        group: RuleGroup::Postcodes,
        capture_group: 1,
        validator: None,
        keyword: false,
        description: "UK postcode in upper case, with the letters allowed in each position",
        // Allocated to Buckingham Palace.
        example: Example::Published(
            "SW1A 1AA",
            "https://en.wikipedia.org/wiki/Postcodes_in_the_United_Kingdom",
        ),
        // `\b` alone lets the postcode shape match inside an identifier such as
        // `SKU-SW1A1AA-2024` (`-` is not a word character, so `\b` sits right there); the
        // regex crate has no lookaround, so the surrounding non-identifier character is
        // matched and consumed instead, and only group 1 is the finding. This means two
        // postcodes separated by exactly one space will not both match (the shared space
        // can only be one match's trailing context or the other's leading context, not
        // both); two characters of separation (`, `) are enough for both to match.
        pattern: r"(?:^|[^\w/_-])(GIR ?0AA|[A-PR-UWYZ](?:[0-9]{1,2}|[A-HK-Y][0-9]{1,2}|[0-9][A-HJKPSTUW]|[A-HK-Y][0-9][ABEHMNPRVWXY]) ?[0-9][ABD-HJLNP-UW-Z]{2})(?:[^\w/_-]|$)",
    },
    RuleSpec {
        name: "irish-eircode",
        group: RuleGroup::Postcodes,
        capture_group: 1,
        validator: None,
        keyword: false,
        description: "Irish Eircode: a routing key and a four-character unique identifier",
        example: Example::Synthetic("D02 X285"),
        // See `uk-postcode` for why the surrounding character is matched instead of using
        // lookaround, and its limitation for two identifiers separated by a single space.
        pattern: r"(?:^|[^\w/_-])((?:D6W|[AC-FHKNPRTV-Y]\d{2}) ?[AC-FHKNPRTV-Y0-9]{4})(?:[^\w/_-]|$)",
    },
    RuleSpec {
        name: "canadian-postal-code",
        group: RuleGroup::Postcodes,
        capture_group: 1,
        validator: None,
        keyword: false,
        description: "Canadian postal code `A1A 1A1` without the letters Canada Post excludes",
        // Canada Post's own headquarters building in Ottawa.
        example: Example::Published(
            "K1A 0B1",
            "https://en.wikipedia.org/wiki/Postal_codes_in_Canada",
        ),
        // See `uk-postcode` for why the surrounding character is matched instead of using
        // lookaround, and its limitation for two identifiers separated by a single space.
        pattern: r"(?:^|[^\w/_-])([ABCEGHJ-NPRSTVXY]\d[ABCEGHJ-NPRSTV-Z] ?\d[ABCEGHJ-NPRSTV-Z]\d)(?:[^\w/_-]|$)",
    },
    RuleSpec {
        name: "birth-date",
        group: RuleGroup::BirthDates,
        capture_group: 1,
        validator: Some(("calendar date in 1900..=today's year", validators::birth_date)),
        keyword: true,
        description: "Date of birth (ISO, dotted or slashed with either day-first or month-first order tried, or a month name in en, de, fr, es, it, nl, pt, pl, sv) within 30 characters of a birth keyword in those languages",
        example: Example::Synthetic("Date of birth: 14 March 1985"),
        // The 30 characters may span a newline (a form puts the label on its own line) but
        // not a sentence terminator, so a keyword followed by an unrelated sentence and then
        // an unrelated date does not match; the optional `\.?` right after the keyword's own
        // `\b` absorbs a trailing abbreviation period (`D.O.B.`) so it is not mistaken for a
        // sentence end by the window that follows. The month is any word of 3 to 12 Latin
        // letters; the validator checks it against the month table, and `\p{L}` is not used
        // because under `(?i)` it pushes the compiled size past `REGEX_SIZE_LIMIT`.
        pattern: concat!(r"(?i)\b(?:", birth_keywords!(), r")\b\.?[^.!?]{0,30}?\b(\d{4}[-./]\d{2}[-./]\d{2}|\d{1,2}[-./]\d{1,2}[-./]\d{4}|\d{1,2}(?:st|nd|rd|th|er)?\.?(?: de)? [a-zÀ-ſ]{3,12}\.?(?: de)?,? \d{4}|[a-zÀ-ſ]{3,12}\.? \d{1,2}(?:st|nd|rd|th)?,? \d{4})\b"),
    },
];

#[cfg(test)]
mod tests {
    use super::super::super::validators::MONTHS;
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
    fn rest_of_europe_identifiers_are_redacted() {
        for (input, expected) in [
            ("NIF 123456789", "NIF [REDACTED]"),
            (
                "n.º de contribuinte: 212 345 672",
                "n.º de contribuinte: [REDACTED]",
            ),
            ("PPS 1234567FA", "PPS [REDACTED]"),
            ("PPSN 7654321G", "PPSN [REDACTED]"),
            ("AHV 756.9217.0769.85", "AHV [REDACTED]"),
            ("AHV 7561234123413", "AHV [REDACTED]"),
            ("SVNR 1237 010180", "SVNR [REDACTED]"),
            (
                "rijksregisternummer 85.07.30-033.28",
                "rijksregisternummer [REDACTED]",
            ),
            ("numéro national 85073003328", "numéro national [REDACTED]"),
            ("matricule 1893120105732", "matricule [REDACTED]"),
            ("személyi szám 1 900101 1249", "személyi szám [REDACTED]"),
            ("TAJ szám: 123 456 788", "TAJ szám: [REDACTED]"),
            ("ΑΜΚΑ 01019012341", "ΑΜΚΑ [REDACTED]"),
            ("ΑΦΜ: 090000045", "ΑΦΜ: [REDACTED]"),
            ("AFM 123456783", "AFM [REDACTED]"),
            ("ЕГН 6101057509", "ЕГН [REDACTED]"),
            ("OIB: 69435151530", "OIB: [REDACTED]"),
            ("EMŠO 0101006500006", "EMŠO [REDACTED]"),
            ("CNP 1900101123457", "CNP [REDACTED]"),
            ("T.C. Kimlik No: 10000000146", "T.C. Kimlik No: [REDACTED]"),
            ("TCKN 12345678950", "TCKN [REDACTED]"),
            ("NID: I05101999Q", "NID: [REDACTED]"),
            ("ID card no. 0123456M", "ID card no. [REDACTED]"),
        ] {
            assert_redacts(input, expected);
        }
    }

    #[test]
    fn world_identifiers_are_redacted() {
        for (input, expected) in [
            ("SIN: 046 454 286", "SIN: [REDACTED]"),
            (
                "Social Insurance Number 123-456-782",
                "Social Insurance Number [REDACTED]",
            ),
            ("TFN 123 456 782", "TFN [REDACTED]"),
            ("Tax File No. 876543210", "Tax File No. [REDACTED]"),
            ("CPF 111.444.777-35", "CPF [REDACTED]"),
            ("111.444.777-35", "[REDACTED]"),
            ("cpf: 12345678909", "cpf: [REDACTED]"),
            ("Aadhaar 9999 4105 7058", "Aadhaar [REDACTED]"),
            ("UIDAI 234567890124", "UIDAI [REDACTED]"),
            ("ID 8001015009087", "ID [REDACTED]"),
            ("身份证 11010519491231002X", "身份证 [REDACTED]"),
        ] {
            assert_redacts(input, expected);
        }
    }

    #[test]
    fn rest_of_europe_and_world_lookalikes_are_kept() {
        for input in [
            "ref 123456789",     // NIF, AFM, SIN, TFN shapes without a keyword
            "1234567FB",         // PPS second letter changes the check
            "756.1234.1234.14",  // AHV check digit wrong
            "1237 010181",       // SVNR check digit wrong
            "85.07.30-033.29",   // Belgian check wrong
            "1990010112386",     // Luxembourg Verhoeff digit wrong
            "19001011248",       // Hungarian check wrong
            "TAJ 123456789",     // TAJ check wrong
            "01019012342",       // AMKA Luhn wrong
            "AFM 123456784",     // AFM check wrong
            "9001011237",        // EGN check wrong
            "OIB 12345678904",   // OIB check wrong
            "0101985501230",     // JMBG check wrong
            "1900101123458",     // CNP check wrong
            "TCKN 12345678951",  // TCKN check wrong
            "NID I05132999Q",    // Albanian day 32
            "ID card 12345678M", // Maltese id has 7 digits
            "SIN 123456783",     // SIN Luhn wrong
            "TFN 876543211",     // TFN check wrong
            // Not "111.444.777-36": phone-national's own varied-digit pattern independently
            // matches "111.444.777" (a valid grouped phone shape) even though the CPF check
            // fails, so that value is redacted by a different, correctly-firing rule. A body
            // of one repeated digit fails phone-national's variety check too.
            "222.222.222-99",         // CPF check wrong
            "cpf 12345678900",        // CPF check wrong
            "ref 12345678909",        // bare CPF without a keyword
            "Aadhaar 2345 6789 0125", // Verhoeff wrong
            "9001015009087",          // South African Luhn wrong
            "110105199001011233",     // Chinese check wrong
        ] {
            assert_untouched(input);
        }
    }

    #[test]
    fn postcodes_are_redacted() {
        for (input, expected) in [
            ("London NW1 6XE", "London [REDACTED]"),
            ("SW1A 1AA", "[REDACTED]"),
            ("EC1A1BB", "[REDACTED]"),
            ("GIR 0AA", "[REDACTED]"),
            ("\"postcode\": \"NW1 6XE\"", "\"postcode\": \"[REDACTED]\""),
            ("Dublin D02 X285", "Dublin [REDACTED]"),
            ("Eircode D6W 1234", "Eircode [REDACTED]"),
            ("Cork T12Y0AN", "Cork [REDACTED]"),
            ("Ottawa K1A 0B1", "Ottawa [REDACTED]"),
            ("M5V 3L9", "[REDACTED]"),
        ] {
            assert_redacts(input, expected);
        }
    }

    #[test]
    fn birth_dates_need_a_keyword_within_thirty_characters() {
        for (input, expected) in [
            ("Date of birth: 14 March 1985", "Date of birth: [REDACTED]"),
            (
                "\"date_of_birth\": \"1985-03-14\"",
                "\"date_of_birth\": \"[REDACTED]\"",
            ),
            (
                "<th>Date of birth</th><td>14 March 1985</td>",
                "<th>Date of birth</th><td>[REDACTED]</td>",
            ),
            ("born on March 14, 1985", "born on [REDACTED]"),
            ("DOB 03/14/1985", "DOB [REDACTED]"),
            ("D.O.B. 14/03/1985", "D.O.B. [REDACTED]"),
            ("Geburtsdatum: 14.03.1985", "Geburtsdatum: [REDACTED]"),
            ("geboren am 14. März 1985", "geboren am [REDACTED]"),
            ("née le 14 mars 1985", "née le [REDACTED]"),
            ("né le 1er janvier 1985", "né le [REDACTED]"),
            (
                "fecha de nacimiento: 14 de marzo de 1985",
                "fecha de nacimiento: [REDACTED]",
            ),
            (
                "data di nascita 14 marzo 1985",
                "data di nascita [REDACTED]",
            ),
            ("geboortedatum 14 maart 1985", "geboortedatum [REDACTED]"),
            (
                "data de nascimento 14 de março de 1985",
                "data de nascimento [REDACTED]",
            ),
            (
                "data urodzenia: 14 marca 1985",
                "data urodzenia: [REDACTED]",
            ),
            ("född 1985-03-14", "född [REDACTED]"),
            (
                "Date of birth:\n14 March 1985",
                "Date of birth:\n[REDACTED]",
            ),
            ("born 14 Sept. 1985", "born [REDACTED]"),
        ] {
            assert_redacts(input, expected);
        }
    }

    #[test]
    fn every_month_name_in_the_table_is_matched_by_the_birth_date_pattern() {
        for (name, _) in MONTHS {
            assert_redacts(&format!("born 14 {name} 1985"), "born [REDACTED]");
            let capitalised: String = name
                .chars()
                .enumerate()
                .map(|(i, c)| {
                    if i == 0 {
                        c.to_uppercase().next().unwrap_or(c)
                    } else {
                        c
                    }
                })
                .collect();
            assert_redacts(&format!("born {capitalised} 14, 1985"), "born [REDACTED]");
        }
    }

    #[test]
    fn passports_and_driving_licences_need_a_keyword() {
        for (input, expected) in [
            ("Passport number: 123456789", "Passport number: [REDACTED]"),
            (
                "Passport no.</th><td>X1234567",
                "Passport no.</th><td>[REDACTED]",
            ),
            ("Passport X1234567", "Passport [REDACTED]"),
            ("Reisepass Nr. C01234567", "Reisepass Nr. [REDACTED]"),
            ("passeport 12AB34567", "passeport [REDACTED]"),
            ("pasaporte AAB123456", "pasaporte [REDACTED]"),
            ("paszport ZS1234567", "paszport [REDACTED]"),
            (
                "Driving licence: MORGA657054SM9IJ",
                "Driving licence: [REDACTED]",
            ),
            (
                "driver's license MORGA657054SM9IJ",
                "driver's license [REDACTED]",
            ),
            ("Führerschein MORGA657054SM9IJ", "Führerschein [REDACTED]"),
            ("Driver's License: A1234567", "Driver's License: [REDACTED]"),
            (
                "driver license no. 123456789",
                "driver license no. [REDACTED]",
            ),
            ("DL# 12345678", "DL# [REDACTED]"),
            (
                "licencia de conducir 12345678",
                "licencia de conducir [REDACTED]",
            ),
            (
                "permis de conduire 12345678",
                "permis de conduire [REDACTED]",
            ),
            ("DL no L123456789012", "DL no [REDACTED]"),
        ] {
            assert_redacts(input, expected);
        }
    }

    #[test]
    fn postcode_date_passport_and_licence_lookalikes_are_kept() {
        for input in [
            "nw1 6xe",                         // lowercase postcode
            "NW1 6XC",                         // C is not a unit letter
            "D02 X28",                         // Eircode identifier too short
            "D1A 0B1",                         // D is not a Canadian first letter
            "The invoice dated 14 March 1985", // no birth keyword
            "born in 1985",                    // a year is not a date
            "born in London",
            "ne le 14 mars 1985",               // `ne le` is not `né le`
            "Date of birth: 31.04.1985",        // no such day
            "Date of birth: 14 March 1899",     // before the year window
            "reference 123456789",              // no passport keyword
            "passport valid until 2030",        // 4 digits
            "driver's license 12345",           // too short
            "ref MORGA657054SM9IJ",             // no licence keyword
            "driving licence MORGA657354SM9IJ", // day 35
        ] {
            assert_untouched(input);
        }
    }

    #[test]
    fn passport_and_driving_licence_values_are_case_insensitive() {
        for (input, expected) in [
            ("Passport ab1234567", "Passport [REDACTED]"),
            (
                "driving licence: morga657054sm9ij",
                "driving licence: [REDACTED]",
            ),
            ("dl# a1234567", "dl# [REDACTED]"),
            ("id card no. 0123456m", "id card no. [REDACTED]"), // carried over from Task 3
        ] {
            assert_redacts(input, expected);
        }
    }

    #[test]
    fn birth_date_keyword_window_does_not_cross_a_sentence_boundary() {
        // The window between the keyword and the date must not swallow an unrelated date in
        // the next sentence, but a newline (a form's label on its own line) and a period
        // after the date itself must still be fine.
        assert_untouched("born in Paris.\nInvoice date: 2024-01-05");
        assert_redacts("Date of birth:\n14.03.1985", "Date of birth:\n[REDACTED]");
        assert_redacts("Born 14 March 1985.", "Born [REDACTED].");
    }

    #[test]
    fn postcode_context_excludes_surrounding_identifier_characters() {
        for input in ["SKU-SW1A1AA-2024", "/path/SW1A1AA/"] {
            assert_untouched(input);
        }
        for (input, expected) in [
            ("London SW1A 1AA.", "London [REDACTED]."),
            ("Dublin D02 X285,", "Dublin [REDACTED],"),
            ("Toronto M5V 3L9", "Toronto [REDACTED]"),
            // Two postcodes separated by two characters (", ") both match: the first
            // match's trailing context consumes the comma, leaving the space free as the
            // second match's own leading context. Two postcodes separated by a single
            // space would not both match, since the shared space can only serve as
            // trailing context for the first or leading context for the second, not both;
            // this is a known limitation of the no-lookaround boundary trick.
            ("SW1A 1AA, EC1A 1BB", "[REDACTED], [REDACTED]"),
        ] {
            assert_redacts(input, expected);
        }
    }

    #[test]
    fn birth_date_numeric_day_and_month_are_each_tried_in_both_orders() {
        for (input, expected) in [
            ("DOB 13/04/1985", "DOB [REDACTED]"), // valid only day-first (month 13 invalid)
            ("DOB 04/13/1985", "DOB [REDACTED]"), // valid only month-first (day 13, month 04)
        ] {
            assert_redacts(input, expected);
        }
        assert_untouched("DOB 13/13/1985"); // invalid either way
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
