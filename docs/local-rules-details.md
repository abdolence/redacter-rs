# Local rules details

Coverage, value grammar and known limits of the built-in rules of the `local-rules` redacter. The rules themselves, with their checks, keywords and examples, are listed in [local-rules.md](local-rules.md), and the redacter is described in the [README](../README.md#local-rules-redacter).

## Identifiers

The `eu-identifiers` group holds the national personal numbers of Albania, Austria, Belgium, Bosnia and Herzegovina, Bulgaria, Croatia, Czechia, Denmark, Estonia, Finland, France, Germany (Steuer-ID), Greece (AMKA and AFM), Hungary (személyi szám and TAJ), Iceland, Ireland, Italy, Latvia, Lithuania, Luxembourg, Malta, Montenegro, the Netherlands, North Macedonia, Norway, Poland, Portugal, Romania, Serbia, Slovakia, Slovenia, Spain, Sweden, Switzerland, Turkey and the United Kingdom (NINO and driving licence), plus EU VAT numbers.

`eu-identifiers` covers the whole of Europe including the Nordics, Baltics, Balkans, Switzerland, Turkey
and the UK. Every rule with its check, whether it needs a context word, and an example is listed in
[local-rules.md](local-rules.md). Each identifier is verified with its published check digit
and, where the number embeds one, its birth date. Not covered, because the country publishes no format
or no check digit for its personal number: Cyprus, Liechtenstein, Monaco, Andorra, San Marino, Vatican
City and Kosovo; Ukraine, Moldova and Belarus are out of scope.

Identifiers whose check digit alone would accept too many ordinary numbers (Danish CPR, Portuguese NIF,
Greek AFM, Hungarian TAJ, Croatian OIB, Turkish TCKN, a bare Brazilian CPF, Canadian SIN, Australian TFN,
Indian Aadhaar) and shapes without a check (passports, driving licences, dates of birth, Albanian and
Maltese ids) are only redacted within 20 characters (30 for dates of birth) of a context word such as
`CPR`, `NIF`, `passport` or `date of birth`, in the local language. The keyword has to be in the same
text unit: a CSV cell is redacted on its own, so a column of bare CPFs under a `cpf` header is not found
while the written `NNN.NNN.NNN-NN` form is.

## Secrets

A password or key written as `key=value`, `key: value` or a JSON field is redacted when the key ends in a word such as
`password`, `secret`, `token`, `api_key` or `private_key`, and the value has at least 8 characters with a digit or one
of `!@#%^&*+=?~` among them. The key name stays. A quoted value ends at the next quote of the kind that opened it,
single, double or backtick, and may hold spaces, commas, semicolons, the other two quote kinds, escaped quotes such as
`\"` and, in a single-quoted value, the SQL and YAML doubled quote `''`. JSON written inside a JSON string, where the
quotes are `\"`, is redacted as well. A value in braces directly after the `=`, such as the ODBC `Pwd={Sup3r;S3cret1}`,
is redacted and the braces stay. An unquoted value ends at a space, comma, semicolon, quote, `<`, a closing bracket
(`)`, `]`, `}` or `>`) or an `&` that starts a `name=` pair, so in `?token=abc123def456&user=alice` only the token is
redacted.

Some values are taken for references or placeholders and left alone:

- a quoted value starting with whitespace, `<`, `$`, `%`, `*`, `[` or `{`;
- an unquoted value starting with `$`, `%`, `*`, `&`, `:`, `<`, `>`, `(`, `)`, `[`, `]`, `{` or `}`;
- an unquoted value that is a dotted or `::` path of identifiers, such as `process.env.API_KEY_V2` or
  `crate::signing::private_key::Key`, unless one segment has more than 32 characters or every segment mixes letters
  and digits, as in `abcdefgh1234.ijklmnop5678`.

The `generic-api-key` rule, taken from gitleaks, reports a random-looking value after a key name that holds `key`,
`secret`, `token`, `password`, `auth`, `access`, `api`, `credential` or `creds`, as in `license_key: vsgm-6vh4-hwnx-zkxt`.
Only blanks may stand between the key name and the separator (`=`, `:`, `=>` and similar), so aligned assignments are
found, and a comma separates only before the quote that opens the value, as in
`os.getenv("SIGNING_KEY", "Zq8Lw3Tn5VxB7mRc")`. Gitleaks also lets further words follow the key name and accepts a bare
comma, and so reports prose such as `the key, ChaCha20-Poly1305.` or `the key algorithm: KEY_ALG_RSA_1024`.

Known misses: a value of letters only, such as `password: my-secret-pass`; a password starting with a listed character,
such as `password=$ecretPa55word`; an unquoted password written as a dotted path, such as `password: john.smith2024`;
a password written in a sentence; a `generic-api-key` value after a label of several words, such as
`API Key ID: Zq8Lw3Tn5VxB7mRc`, `Client Secret Value: Zq8Lw3Tn5VxB7mRc` or `{"API Key ID": "Zq8Lw3Tn5VxB7mRc"}`; one
after a bare comma, such as `signing_key,Zq8Lw3Tn5VxB7mRc`; one on a line of its own after the comma, as in a multi-line
`os.getenv(` call; and a quoted value that does not close on its line, such as `password = "Sup3rS3cret1` at a cut-off
line end, a YAML `>-` block or a `"""` triple-quoted string.

Known over-redactions: a value that names a secret instead of holding it, such as the Terraform
`secret = "ip2location-token"` that points to a Secret Manager secret, and any other literal with a digit or a symbol
after such a key, such as `token: v2-beta-channel`; and a file path after such a key, such as
`PWD=/home/runner/work/app2` or `secret: /run/secrets/db_password1`, because one base64 secret in 64 starts with `/`.
The `generic-api-key` rule also reports a cipher or identifier name directly after a key name and a separator, such as
`the cipher key: ChaCha20-Poly1305`.

## Matching on shape

Matching on shape alone over-redacts in places. The known cases: a number of 13 to 19 digits starting
with 3 to 6 that passes the Luhn check is taken for a payment card, which also catches the rarer case of
a 13-digit EAN barcode starting with 3 to 6 whose own check digit happens to pass Luhn too
(`--local-rules-disable payment-card` is the lever for either); a national phone number written with
a leading `0` is any 9 to 12 digits in 2 to 5 groups, which also fits some reference numbers; any 6 to 9
digit number within 20 characters of the word `passport` is taken for a passport number; and national
identifiers without a keyword are accepted on their check digit and embedded date alone, so a bare
10-digit run of digits has about a 2% chance and an 11-digit run about 1.5% of passing one of the
checksummed rules. For a document that is mostly numbers, `--local-rules-disable eu-identifiers` (or
disabling the specific groups that do not apply) is the lever to pull.
