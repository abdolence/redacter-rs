# Local rules reference

Every built-in rule of the `local-rules` redacter, grouped as the `--local-rules` and `--local-rules-disable` options see them. A rule with a validator only redacts values that pass the named check; a rule with a keyword only redacts values within a few characters of a context word such as `passport` or `NIF`. A synthetic example is made up for the tests, with check digits computed from the published formula; a published example is a real worked value from the cited public specification or vendor documentation. Generated from the rule table by `cargo test rules_reference_regenerate -- --ignored`; do not edit by hand.

| Group | Rules | Purpose |
|---|---|---|
| `email` | 1 | Email addresses |
| `phone` | 2 | Phone numbers, international and national forms |
| `payment-card` | 1 | Payment card numbers |
| `iban` | 1 | International bank account numbers |
| `network` | 3 | IPv4, IPv6 and MAC addresses |
| `url` | 1 | Credentials embedded in URLs |
| `secrets` | 9 | API keys, tokens, authorization headers and private keys |
| `us-identifiers` | 3 | US identifiers: SSN and ITIN, passports, driver's licenses |
| `eu-identifiers` | 34 | European identifiers: national personal numbers, VAT numbers, UK driving licences |
| `world-identifiers` | 7 | Identifiers of Canada, Australia, Brazil, India, South Africa and China |
| `postcodes` | 3 | UK, Irish and Canadian postcodes |
| `birth-dates` | 1 | Dates of birth next to a birth keyword |
| `custom` | user-defined | User-defined regex and dictionary rules, always enabled |

## `email`

Email addresses.

| Rule | Description | Validator | Keyword | Example |
|---|---|---|---|---|
| `email` | Email address | no | no | `john.smith@example.com` (synthetic) |

## `phone`

Phone numbers, international and national forms.

| Rule | Description | Validator | Keyword | Example |
|---|---|---|---|---|
| `phone-international` | Phone number with a `+` or `00` country code, 8 to 15 digits | yes (8 to 15 varied digits) | no | `+44 20 7946 0958` (synthetic) |
| `phone-national` | National phone number: an area code and 7 digits, or a trunk `0` and 9 to 12 digits in groups | yes (9 to 15 varied digits) | no | `020 7946 0958` (synthetic) |

## `payment-card`

Payment card numbers.

| Rule | Description | Validator | Keyword | Example |
|---|---|---|---|---|
| `payment-card` | Payment card number of 13 to 19 digits, grouped 4-4-4-4 or Amex 4-6-5 | yes (Luhn) | no | `4111 1111 1111 1111` (synthetic) |

## `iban`

International bank account numbers.

| Rule | Description | Validator | Keyword | Example |
|---|---|---|---|---|
| `iban` | International bank account number | yes (ISO 7064 mod 97-10 and national length) | no | `GB82 WEST 1234 5698 7654 32` (synthetic) |

## `network`

IPv4, IPv6 and MAC addresses.

| Rule | Description | Validator | Keyword | Example |
|---|---|---|---|---|
| `ipv4` | IPv4 address | yes (address parse) | no | `192.168.0.1` (synthetic) |
| `ipv6` | IPv6 address | yes (address parse) | no | `2001:db8::1` (synthetic) |
| `mac-address` | MAC address, colon or hyphen separated | no | no | `00:1A:2B:3C:4D:5E` (synthetic) |

## `url`

Credentials embedded in URLs.

| Rule | Description | Validator | Keyword | Example |
|---|---|---|---|---|
| `url-credentials` | `user:password` embedded in a URL; only the credentials are redacted | no | no | `https://alice:s3cret@example.com/path` (synthetic) |

## `secrets`

API keys, tokens, authorization headers and private keys.

| Rule | Description | Validator | Keyword | Example |
|---|---|---|---|---|
| `aws-access-key` | AWS access key id (`AKIA` or `ASIA` and 16 characters) | no | no | `AKIAIOSFODNN7EXAMPLE` (synthetic) |
| `aws-secret-key` | AWS secret access key: 40 characters within 20 characters of the words `aws` and `secret` | no | yes | `aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY` (synthetic) |
| `gcp-api-key` | Google API key (`AIza` and 35 characters) | no | no | `AIzaSyA1234567890abcdefghijklmnopqrstuv` (synthetic) |
| `github-token` | GitHub token (`ghp_`, `gho_`, `ghu_`, `ghs_`, `ghr_` or `github_pat_`) | no | no | `ghp_abcdefghijklmnopqrstuvwxyz0123456789` (synthetic) |
| `slack-token` | Slack token (`xoxa-`, `xoxb-`, `xoxp-`, `xoxr-`) | no | no | `xoxb-123456789012-abcdefghij` (synthetic) |
| `stripe-key` | Stripe live secret or restricted key (`sk_live_`, `rk_live_`) | no | no | `sk_live_abcdefghijklmnop` (synthetic) |
| `jwt` | JSON Web Token: three base64url parts, the first starting with `eyJ` | no | no | `eyJhbGciOiJIUzI1NiJ9.eyJzdWIiOiIxMjM0In0.abc123def456` (synthetic) |
| `authorization-header` | Value of an `Authorization: Bearer` or `Basic` header | no | yes | `Authorization: Bearer abc.def.ghi` (synthetic) |
| `private-key-block` | PEM or PGP private key block from `BEGIN` to `END` | no | no | `-----BEGIN RSA PRIVATE KEY-----\nMIIB\n-----END RSA PRIVATE KEY-----` (synthetic) |

## `us-identifiers`

US identifiers: SSN and ITIN, passports, driver's licenses.

| Rule | Description | Validator | Keyword | Example |
|---|---|---|---|---|
| `us-ssn` | US Social Security number `AAA-GG-SSSS` or ITIN | yes (SSA area and group rules) | no | `123-45-6789` (synthetic) |
| `passport` | Passport number (6 to 9 digits with up to 3 letters, or the French `12AB34567` shape) within 20 characters of the word passport in en, de, fr, es, it, nl, pt, pl or sv | no | yes | `Passport no. X1234567` (synthetic) |
| `us-driving-licence` | US driver's license number (7 to 9 digits with an optional letter, or a letter and 11 to 14 digits) within 20 characters of a driving licence keyword or `DL#` | no | yes | `Driver's License: A1234567` (synthetic) |

## `eu-identifiers`

European identifiers: national personal numbers, VAT numbers, UK driving licences.

| Rule | Description | Validator | Keyword | Example |
|---|---|---|---|---|
| `eu-vat` | VAT number of DE, FR, IT, ES, NL, BE or GB | yes (per-country check (mod 11,10 / mod 97 / Luhn / eleven test)) | no | `DE136695976` (synthetic) |
| `spanish-dni-nie` | Spanish DNI (8 digits and a letter) or NIE (X, Y or Z, 7 digits and a letter) | yes (mod 23 control letter) | no | `12345678Z` (synthetic) |
| `italian-codice-fiscale` | Italian codice fiscale, 16 characters | yes (control character) | no | `RSSMRA85M01H501Q` (synthetic) |
| `dutch-bsn` | Dutch BSN, 9 digits within 20 characters of `bsn`, `burgerservicenummer` or `sofinummer` | yes (eleven test) | yes | `BSN 111222333` (synthetic) |
| `uk-nino` | UK National Insurance number `AB 12 34 56 C` | yes (prefix and suffix rules) | no | `AB 12 34 56 C` (synthetic) |
| `french-nir` | French NIR (social security number), 15 digits including the key | yes (mod 97 key) | no | `2 69 05 49 588 157 80` (synthetic) |
| `german-steuer-id` | German Steuer-ID, 11 digits within 20 characters of `Steuer-ID`, `IdNr` or `tax id` | yes (ISO 7064 MOD 11,10) | yes | `Steuer-ID 86095742719` (synthetic) |
| `swedish-personnummer` | Swedish personnummer or samordningsnummer, `YYMMDD-NNNC` or `YYYYMMDD-NNNC` | yes (Luhn and date) | no | `811218-9876` (synthetic) |
| `norwegian-fodselsnummer` | Norwegian fødselsnummer or D-number, 11 digits `DDMMYY IIIKK` | yes (two mod-11 control digits and date) | no | `01019012480` (synthetic) |
| `danish-cpr` | Danish CPR number `DDMMYY-SSSS` within 20 characters of `cpr` or `personnummer` | yes (date) | yes | `CPR-nr. 010203-1234` (synthetic) |
| `finnish-hetu` | Finnish henkilötunnus `DDMMYYCZZZQ` with its century marker | yes (mod-31 control character and date) | no | `131052-308T` (published, [source](https://dvv.fi/en/personal-identity-code)) |
| `icelandic-kennitala` | Icelandic kennitala of a person, `DDMMYY-NNCM` | yes (mod-11 control digit and date) | no | `120174-3399` (published, [source](https://en.wikipedia.org/wiki/Icelandic_identification_number)) |
| `baltic-personal-code` | Estonian isikukood or Lithuanian asmens kodas, 11 digits `GYYMMDDSSSC` | yes (mod-11 control digit and date) | no | `37605030299` (published, [source](https://et.wikipedia.org/wiki/Isikukood)) |
| `latvian-personas-kods` | Latvian personas kods, `DDMMYY-CSSSK` or the post-2017 `32SSSS-SSSSK` | yes (mod-11 control digit, date on the pre-2017 form) | no | `161175-19997` (synthetic) |
| `polish-pesel` | Polish PESEL, 11 digits with the century encoded in the month: 1900-1999 (month 1-12) or 2000-2099 (month 21-32) | yes (mod-10 control digit and date) | no | `44051401359` (synthetic) |
| `czech-slovak-rodne-cislo` | Czech or Slovak rodné číslo, `YYMMDD/SSSC` (10 digits, issued since 1954) | yes (divisible by 11 and date) | no | `780123/3540` (synthetic) |
| `portuguese-nif` | Portuguese NIF, 9 digits within 20 characters of `nif`, `nipc` or `contribuinte` | yes (mod-11 check digit) | yes | `NIF 123456789` (synthetic) |
| `irish-pps` | Irish PPS number, 7 digits and one or two letters | yes (mod-23 check letter) | no | `1234567FA` (published, [source](https://en.wikipedia.org/wiki/Personal_Public_Service_Number)) |
| `swiss-ahv` | Swiss AHV/AVS number `756.NNNN.NNNN.NC` | yes (EAN-13 check digit) | no | `756.9217.0769.85` (synthetic) |
| `austrian-svnr` | Austrian Sozialversicherungsnummer `LLLP DDMMYY` | yes (mod-11 check digit and date) | no | `1237 010180` (synthetic) |
| `belgian-national-number` | Belgian national number `YY.MM.DD-SSS.CC` | yes (mod-97 check and date) | no | `85.07.30-033.28` (synthetic) |
| `luxembourg-matricule` | Luxembourg matricule, 13 digits starting with the birth date `YYYYMMDD` | yes (Luhn and Verhoeff check digits and date) | no | `1893120105732` (synthetic) |
| `hungarian-personal-number` | Hungarian személyi szám, 11 digits `M YYMMDD SSSC` | yes (mod-11 check digit and date) | no | `1 900101 1249` (synthetic) |
| `hungarian-taj` | Hungarian TAJ number, 9 digits within 20 characters of `TAJ` | yes (mod-10 check digit) | yes | `TAJ szám: 123 456 788` (synthetic) |
| `greek-amka` | Greek AMKA, 11 digits starting with the birth date `DDMMYY` | yes (Luhn and date) | no | `01019012341` (synthetic) |
| `greek-afm` | Greek AFM (tax number), 9 digits within 20 characters of `AFM` or `ΑΦΜ` | yes (mod-11 check digit) | yes | `ΑΦΜ: 090000045` (synthetic) |
| `bulgarian-egn` | Bulgarian EGN, 10 digits starting with the birth date `YYMMDD`, accepted for 1900-2099 | yes (mod-11 check digit and date) | no | `6101057509` (synthetic) |
| `croatian-oib` | Croatian OIB, 11 digits within 20 characters of `OIB` | yes (ISO 7064 MOD 11,10) | yes | `OIB: 69435151530` (synthetic) |
| `jmbg` | JMBG of Serbia, Bosnia and Herzegovina, Montenegro and North Macedonia, and the Slovenian EMŠO, 13 digits | yes (mod-11 check digit and date) | no | `0101006500006` (published, [source](https://en.wikipedia.org/wiki/Unique_Master_Citizen_Number)) |
| `romanian-cnp` | Romanian CNP, 13 digits `S YYMMDD JJ NNN C` | yes (mod-11 check digit, date and county) | no | `1900101123457` (synthetic) |
| `turkish-tckn` | Turkish TCKN, 11 digits within 20 characters of `TCKN` or `T.C. Kimlik No` | yes (two mod-10 check digits) | yes | `T.C. Kimlik No: 10000000146` (synthetic) |
| `albanian-nid` | Albanian NID (letter, 8 digits, letter) within 20 characters of `NID` or `numri personal` | yes (date) | yes | `NID: I05101999Q` (synthetic) |
| `maltese-id` | Maltese identity card number (7 digits and a letter) within 20 characters of `ID card` | no | yes | `ID card no. 0123456M` (synthetic) |
| `uk-driving-licence` | UK driving licence number (16 characters encoding the birth date) within 20 characters of a driving licence keyword or `DVLA` | yes (date fields) | yes | `Driving licence: MORGA657054SM9IJ` (synthetic) |

## `world-identifiers`

Identifiers of Canada, Australia, Brazil, India, South Africa and China.

| Rule | Description | Validator | Keyword | Example |
|---|---|---|---|---|
| `canadian-sin` | Canadian SIN, 9 digits within 20 characters of `SIN`, `NAS` or `social insurance` | yes (Luhn) | yes | `SIN: 046 454 286` (published, [source](https://en.wikipedia.org/wiki/Social_Insurance_Number)) |
| `australian-tfn` | Australian TFN, 9 digits within 20 characters of `TFN` or `tax file number` | yes (mod-11 weighted sum) | yes | `TFN 123 456 782` (synthetic) |
| `brazilian-cpf` | Brazilian CPF in its written form `NNN.NNN.NNN-NN` | yes (two mod-11 check digits) | no | `111.444.777-35` (synthetic) |
| `brazilian-cpf-plain` | Brazilian CPF as 11 bare digits within 20 characters of `CPF` | yes (two mod-11 check digits) | yes | `CPF: 12345678909` (synthetic) |
| `indian-aadhaar` | Indian Aadhaar, 12 digits within 20 characters of `Aadhaar` or `UIDAI` | yes (Verhoeff) | yes | `Aadhaar 9999 4105 7058` (synthetic) |
| `south-african-id` | South African ID number, 13 digits starting with the birth date `YYMMDD` | yes (Luhn, date and citizenship digit) | no | `8001015009087` (synthetic) |
| `chinese-resident-id` | Chinese resident identity number, 18 characters with the birth date `YYYYMMDD` | yes (ISO 7064 MOD 11-2 and date) | no | `11010519491231002X` (synthetic) |

## `postcodes`

UK, Irish and Canadian postcodes.

| Rule | Description | Validator | Keyword | Example |
|---|---|---|---|---|
| `uk-postcode` | UK postcode in upper case, with the letters allowed in each position | yes (inward code is not a storage-capacity abbreviation) | no | `SW1A 1AA` (published, [source](https://en.wikipedia.org/wiki/Postcodes_in_the_United_Kingdom)) |
| `irish-eircode` | Irish Eircode: a routing key and a four-character unique identifier | yes (at least one letter in the unique identifier) | no | `D02 X285` (synthetic) |
| `canadian-postal-code` | Canadian postal code `A1A 1A1` without the letters Canada Post excludes | no | no | `K1A 0B1` (published, [source](https://en.wikipedia.org/wiki/Postal_codes_in_Canada)) |

## `birth-dates`

Dates of birth next to a birth keyword.

| Rule | Description | Validator | Keyword | Example |
|---|---|---|---|---|
| `birth-date` | Date of birth (ISO, dotted or slashed with either day-first or month-first order tried, or a month name in en, de, fr, es, it, nl, pt, pl, sv) within 30 characters of a birth keyword in those languages | yes (calendar date in 1900..=today's year) | yes | `Date of birth: 14 March 1985` (synthetic) |
