# Local rules reference

Every built-in rule of the `local-rules` redacter, grouped as the `--local-rules` and `--local-rules-disable` options see them. A rule with a validator only redacts values that pass the named check; a rule with a keyword only redacts values within a few characters of a context word such as `passport` or `NIF`. A synthetic example is made up for the tests, with check digits computed from the published formula; a published example is a real worked value from the cited public specification or vendor documentation. `redacter scan -d local-rules` reports the same matches instead of redacting them, labelled with the rule name from the first column of the tables below. Generated from the rule table by `cargo test rules_reference_regenerate -- --ignored`; do not edit by hand.

| Group | Rules | Purpose |
|---|---|---|
| `email` | 1 | Email addresses |
| `phone` | 2 | Phone numbers, international and national forms |
| `payment-card` | 1 | Payment card numbers |
| `iban` | 1 | International bank account numbers |
| `network` | 3 | IPv4, IPv6 and MAC addresses |
| `url` | 1 | Credentials embedded in URLs |
| `secrets` | 217 | Passwords and keys in `key=value` lines, vendor API keys and tokens, authorization headers and private keys |
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

Passwords and keys in `key=value` lines, vendor API keys and tokens, authorization headers and private keys.

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
| `secret-assignment` | Unquoted value of a `password`, `secret`, `token` or `api_key` style key written as `key=value` or `key: value`, up to the next space, comma, semicolon, quote, `<`, closing bracket or `&` followed by a `name=` | yes (8 characters or more with a digit or one of `!@#%^&*+=?~`, not a dotted or `::` identifier path) | yes | `DB_PASSWORD=Sup3rS3cret!` (synthetic) |
| `secret-assignment-double-quoted` | Double-quoted value of a `password`, `secret`, `token` or `api_key` style key, 8 characters or more up to the closing quote; `\"` stays inside the value | yes (digit or one of `!@#%^&*+=?~` in the value) | yes | `password = "My pass; 1234"` (synthetic) |
| `secret-assignment-single-quoted` | Single-quoted value of a `password`, `secret`, `token` or `api_key` style key, 8 characters or more up to the closing quote; `\'` and the SQL and YAML `''` stay inside the value | yes (digit or one of `!@#%^&*+=?~` in the value) | yes | `password = 'My pass; 1234'` (synthetic) |
| `secret-assignment-backtick-quoted` | Backtick-quoted value of a `password`, `secret`, `token` or `api_key` style key, 8 characters or more up to the closing backtick | yes (digit or one of `!@#%^&*+=?~` in the value) | yes | `` password = `My pass; 1234` `` (synthetic) |
| `secret-assignment-escaped-double-quoted` | Value of a `password`, `secret`, `token` or `api_key` style key in JSON inside a string, where the quotes are written `\"`, 8 characters or more up to the closing `\"` | yes (digit or one of `!@#%^&*+=?~` in the value) | yes | `{\"password\":\"My pass; 1234\"}` (synthetic) |
| `secret-assignment-braced` | Value in braces directly after the `=` of a `password`, `secret`, `token` or `api_key` style key, the way an ODBC connection string quotes a password that holds `;`, 7 characters or more | yes (digit or one of `!@#%^&*+=?~` in the value) | yes | `Pwd={Sup3r;S3cret1}` (synthetic) |
| `openai-key` | OpenAI API key (`sk-proj-`, `sk-svcacct-`, `sk-admin-`, or the legacy form containing `T3BlbkFJ`) | no | no | `sk-proj-abcdefghij0123456789ABCDEFGHIJ` (synthetic) |
| `anthropic-key` | Anthropic API or admin key (`sk-ant-api` or `sk-ant-admin`, two digits and 20 characters or more) | no | no | `sk-ant-api03-abcdefghij0123456789ABCDEFGHIJ` (synthetic) |
| `gitlab-token` | GitLab personal access token (`glpat-` and 20 characters or more) | no | no | `glpat-abcdefghij0123456789` (synthetic) |
| `npm-token` | npm access token (`npm_` and 36 characters) | no | no | `npm_abcdefghijklmnopqrstuvwxyz0123456789` (synthetic) |
| `slack-webhook` | Slack webhook URL (`hooks.slack.com/services/`, `workflows/` or `triggers/` and the secret path) | no | no | `hooks.slack.com/services/T0000AAAA/B0000BBBB/abcdEFGHijklMNOPqrstUVWX` (synthetic) |
| `sendgrid-key` | SendGrid API key (`SG.`, 20 characters or more, `.`, 40 characters or more) | no | no | `SG.abcdefghijklmnopqrst.abcdefghijklmnopqrstuvwxyz0123456789ABCD` (synthetic) |
| `huggingface-token` | Hugging Face access token (`hf_` and 30 letters or digits or more) | no | no | `hf_abcdefghijklmnopqrstuvwxyzABCD` (synthetic) |
| `discord-bot-token` | Discord bot token: three base64url parts of 24 to 26, 6 and 27 or more characters, the first starting with `M`, `N` or `O` | no | no | `MTAwMDAwMDAwMDAwMDAwMDAw.G1aB2c.abcdefghijklmnopqrstuvwxyz0` (synthetic) |

### Rules ported from gitleaks

The rules below are the default rules of [gitleaks](https://github.com/gitleaks/gitleaks) at commit `5f6ec56cfa6f8c67648d863582c7ae2f206d5c1c`, MIT licence, Copyright (c) 2019 Zachary Rice. A rule runs only on text that holds one of its keywords, in any case. A secret is redacted when its Shannon entropy, in bits per character, reaches the entropy column, and no allowlist of the file or of the rule matches it. The descriptions are the ones gitleaks gives. A rule that a hand-written rule above already matches is not ported. Generated by `cargo test gitleaks_table_regenerate -- --ignored`.

| Rule | Description | Entropy | Keywords |
|---|---|---|---|
| `1password-secret-key` | Uncovered a possible 1Password secret key, potentially compromising access to secrets in vaults | 3.8 | `a3-` |
| `1password-service-account-token` | Uncovered a possible 1Password service account token, potentially compromising access to secrets in vaults | 4 | `ops_` |
| `adafruit-api-key` | Identified a potential Adafruit API Key, which could lead to unauthorized access to Adafruit services and sensitive data exposure | no | `adafruit` |
| `adobe-client-id` | Detected a pattern that resembles an Adobe OAuth Web Client ID, posing a risk of compromised Adobe integrations and data breaches | 2 | `adobe` |
| `adobe-client-secret` | Discovered a potential Adobe Client Secret, which, if exposed, could allow unauthorized Adobe service access and data manipulation | 2 | `p8e-` |
| `age-secret-key` | Discovered a potential Age encryption tool secret key, risking data decryption and unauthorized access to sensitive information | no | `age-secret-key-1` |
| `airtable-api-key` | Uncovered a possible Airtable API Key, potentially compromising database access and leading to data leakage or alteration | no | `airtable` |
| `airtable-personnal-access-token` | Uncovered a possible Airtable Personal AccessToken, potentially compromising database access and leading to data leakage or alteration | no | `airtable` |
| `algolia-api-key` | Identified an Algolia API Key, which could result in unauthorized search operations and data exposure on Algolia-managed platforms | no | `algolia` |
| `alibaba-access-key-id` | Detected an Alibaba Cloud AccessKey ID, posing a risk of unauthorized cloud resource access and potential data compromise | 2 | `ltai` |
| `alibaba-secret-key` | Discovered a potential Alibaba Cloud Secret Key, potentially allowing unauthorized operations and data access within Alibaba Cloud | 2 | `alibaba` |
| `artifactory-api-key` | Detected an Artifactory api key, posing a risk unauthorized access to the central repository | 4.5 | `akcp` |
| `artifactory-reference-token` | Detected an Artifactory reference token, posing a risk of impersonation and unauthorized access to the central repository | 4.5 | `cmvmd` |
| `asana-client-id` | Discovered a potential Asana Client ID, risking unauthorized access to Asana projects and sensitive task information | no | `asana` |
| `asana-client-secret` | Identified an Asana Client Secret, which could lead to compromised project management integrity and unauthorized access | no | `asana` |
| `atlassian-api-token` | Detected an Atlassian API token, posing a threat to project management and collaboration tool security and data confidentiality | 3.5 | `atlassian`, `confluence`, `jira`, `atatt3` |
| `authress-service-client-access-key` | Uncovered a possible Authress Service Client Access Key, which may compromise access control services and sensitive data | 2 | `sc_`, `ext_`, `scauth_`, `authress_` |
| `aws-access-token` | Identified a pattern that may indicate AWS credentials, risking unauthorized cloud resource access and data breaches on AWS platforms | 3 | `a3t`, `akia`, `asia`, `abia`, `acca` |
| `aws-amazon-bedrock-api-key-long-lived` | Identified a pattern that may indicate long-lived Amazon Bedrock API keys, risking unauthorized Amazon Bedrock usage | 3 | `absk` |
| `aws-amazon-bedrock-api-key-short-lived` | Identified a pattern that may indicate short-lived Amazon Bedrock API keys, risking unauthorized Amazon Bedrock usage | 3 | `bedrock-api-key-` |
| `azure-ad-client-secret` | Azure AD Client Secret | 3 | `q~` |
| `beamer-api-token` | Detected a Beamer API token, potentially compromising content management and exposing sensitive notifications and updates | no | `beamer` |
| `bitbucket-client-id` | Discovered a potential Bitbucket Client ID, risking unauthorized repository access and potential codebase exposure | no | `bitbucket` |
| `bitbucket-client-secret` | Discovered a potential Bitbucket Client Secret, posing a risk of compromised code repositories and unauthorized access | no | `bitbucket` |
| `bittrex-access-key` | Identified a Bittrex Access Key, which could lead to unauthorized access to cryptocurrency trading accounts and financial loss | no | `bittrex` |
| `bittrex-secret-key` | Detected a Bittrex Secret Key, potentially compromising cryptocurrency transactions and financial security | no | `bittrex` |
| `cisco-meraki-api-key` | Cisco Meraki is a cloud-managed IT solution that provides networking, security, and device management through an easy-to-use interface | 3 | `meraki` |
| `clickhouse-cloud-api-secret-key` | Identified a pattern that may indicate clickhouse cloud API secret key, risking unauthorized clickhouse cloud api access and data breaches on ClickHouse Cloud platforms | 3 | `4b1d` |
| `clojars-api-token` | Uncovered a possible Clojars API token, risking unauthorized access to Clojure libraries and potential code manipulation | 2 | `clojars_` |
| `cloudflare-api-key` | Detected a Cloudflare API Key, potentially compromising cloud application deployments and operational security | 2 | `cloudflare` |
| `cloudflare-global-api-key` | Detected a Cloudflare Global API Key, potentially compromising cloud application deployments and operational security | 2 | `cloudflare` |
| `cloudflare-origin-ca-key` | Detected a Cloudflare Origin CA Key, potentially compromising cloud application deployments and operational security | 2 | `cloudflare`, `v1.0-` |
| `codecov-access-token` | Found a pattern resembling a Codecov Access Token, posing a risk of unauthorized access to code coverage reports and sensitive data | no | `codecov` |
| `cohere-api-token` | Identified a Cohere Token, posing a risk of unauthorized access to AI services and data manipulation | 4 | `cohere`, `co_api_key` |
| `coinbase-access-token` | Detected a Coinbase Access Token, posing a risk of unauthorized access to cryptocurrency accounts and financial transactions | no | `coinbase` |
| `confluent-access-token` | Identified a Confluent Access Token, which could compromise access to streaming data platforms and sensitive data flow | no | `confluent` |
| `confluent-secret-key` | Found a Confluent Secret Key, potentially risking unauthorized operations and data access within Confluent services | no | `confluent` |
| `contentful-delivery-api-token` | Discovered a Contentful delivery API token, posing a risk to content management systems and data integrity | no | `contentful` |
| `curl-auth-user` | Discovered a potential basic authorization token provided in a curl command, which could compromise the curl accessed resource | 2 | `curl` |
| `databricks-api-token` | Uncovered a Databricks API token, which may compromise big data analytics platforms and sensitive data processing | 3 | `dapi` |
| `datadog-access-token` | Detected a Datadog Access Token, potentially risking monitoring and analytics data exposure and manipulation | no | `datadog` |
| `defined-networking-api-token` | Identified a Defined Networking API token, which could lead to unauthorized network operations and data breaches | no | `dnkey` |
| `digitalocean-access-token` | Found a DigitalOcean OAuth Access Token, risking unauthorized cloud resource access and data compromise | 3 | `doo_v1_` |
| `digitalocean-pat` | Discovered a DigitalOcean Personal Access Token, posing a threat to cloud infrastructure security and data privacy | 3 | `dop_v1_` |
| `digitalocean-refresh-token` | Uncovered a DigitalOcean OAuth Refresh Token, which could allow prolonged unauthorized access and resource manipulation | no | `dor_v1_` |
| `discord-api-token` | Detected a Discord API key, potentially compromising communication channels and user data privacy on Discord | no | `discord` |
| `discord-client-id` | Identified a Discord client ID, which may lead to unauthorized integrations and data exposure in Discord applications | 2 | `discord` |
| `discord-client-secret` | Discovered a potential Discord client secret, risking compromised Discord bot integrations and data leaks | 2 | `discord` |
| `doppler-api-token` | Discovered a Doppler API token, posing a risk to environment and secrets management security | 2 | `dp.pt.` |
| `droneci-access-token` | Detected a Droneci Access Token, potentially compromising continuous integration and deployment workflows | no | `droneci` |
| `dropbox-api-token` | Identified a Dropbox API secret, which could lead to unauthorized file access and data breaches in Dropbox storage | no | `dropbox` |
| `dropbox-long-lived-api-token` | Found a Dropbox long-lived API token, risking prolonged unauthorized access to cloud storage and sensitive data | no | `dropbox` |
| `dropbox-short-lived-api-token` | Discovered a Dropbox short-lived API token, posing a risk of temporary but potentially harmful data access and manipulation | no | `dropbox` |
| `duffel-api-token` | Uncovered a Duffel API token, which may compromise travel platform integrations and sensitive customer data | 2 | `duffel_` |
| `dynatrace-api-token` | Detected a Dynatrace API token, potentially risking application performance monitoring and data exposure | 4 | `dt0c01.` |
| `easypost-api-token` | Identified an EasyPost API token, which could lead to unauthorized postal and shipment service access and data exposure | 2 | `ezak` |
| `easypost-test-api-token` | Detected an EasyPost test API token, risking exposure of test environments and potentially sensitive shipment data | 2 | `eztk` |
| `etsy-access-token` | Found an Etsy Access Token, potentially compromising Etsy shop management and customer data | 3 | `etsy` |
| `facebook-access-token` | Discovered a Facebook Access Token, posing a risk of unauthorized access to Facebook accounts and personal data exposure | 3 | `facebook` |
| `facebook-page-access-token` | Discovered a Facebook Page Access Token, posing a risk of unauthorized access to Facebook accounts and personal data exposure | 4 | `eaam`, `eaac` |
| `facebook-secret` | Discovered a Facebook Application secret, posing a risk of unauthorized access to Facebook accounts and personal data exposure | 3 | `facebook` |
| `fastly-api-token` | Uncovered a Fastly API key, which may compromise CDN and edge cloud services, leading to content delivery and security issues | no | `fastly` |
| `finicity-api-token` | Detected a Finicity API token, potentially risking financial data access and unauthorized financial operations | no | `finicity` |
| `finicity-client-secret` | Identified a Finicity Client Secret, which could lead to compromised financial service integrations and data breaches | no | `finicity` |
| `finnhub-access-token` | Found a Finnhub Access Token, risking unauthorized access to financial market data and analytics | no | `finnhub` |
| `flickr-access-token` | Discovered a Flickr Access Token, posing a risk of unauthorized photo management and potential data leakage | no | `flickr` |
| `flutterwave-encryption-key` | Uncovered a Flutterwave Encryption Key, which may compromise payment processing and sensitive financial information | 2 | `flwseck_test` |
| `flutterwave-public-key` | Detected a Finicity Public Key, potentially exposing public cryptographic operations and integrations | 2 | `flwpubk_test` |
| `flutterwave-secret-key` | Identified a Flutterwave Secret Key, risking unauthorized financial transactions and data breaches | 2 | `flwseck_test` |
| `flyio-access-token` | Uncovered a Fly.io API key | 4 | `fo1_`, `fm1`, `fm2_` |
| `frameio-api-token` | Found a Frame.io API token, potentially compromising video collaboration and project management | no | `fio-u-` |
| `freshbooks-access-token` | Discovered a Freshbooks Access Token, posing a risk to accounting software access and sensitive financial data exposure | no | `freshbooks` |
| `generic-api-key` | Detected a Generic API Key, potentially exposing access to various services and sensitive operations | 3.5 | `access`, `api`, `auth`, `key`, `credential`, `creds`, `passwd`, `password`, `secret`, `token` |
| `gitlab-cicd-job-token` | Identified a GitLab CI/CD Job Token, potential access to projects and some APIs on behalf of a user while the CI job is running | 3 | `glcbt-` |
| `gitlab-deploy-token` | Identified a GitLab Deploy Token, risking access to repositories, packages and containers with write access | 3 | `gldt-` |
| `gitlab-feature-flag-client-token` | Identified a GitLab feature flag client token, risks exposing user lists and features flags used by an application | 3 | `glffct-` |
| `gitlab-feed-token` | Identified a GitLab feed token, risking exposure of user data | 3 | `glft-` |
| `gitlab-incoming-mail-token` | Identified a GitLab incoming mail token, risking manipulation of data sent by mail | 3 | `glimt-` |
| `gitlab-kubernetes-agent-token` | Identified a GitLab Kubernetes Agent token, risking access to repos and registry of projects connected via agent | 3 | `glagent-` |
| `gitlab-oauth-app-secret` | Identified a GitLab OIDC Application Secret, risking access to apps using GitLab as authentication provider | 3 | `gloas-` |
| `gitlab-pat-routable` | Identified a GitLab Personal Access Token (routable), risking unauthorized access to GitLab repositories and codebase exposure | 4 | `glpat-` |
| `gitlab-ptt` | Found a GitLab Pipeline Trigger Token, potentially compromising continuous integration workflows and project security | 3 | `glptt-` |
| `gitlab-rrt` | Discovered a GitLab Runner Registration Token, posing a risk to CI/CD pipeline integrity and unauthorized access | 3 | `gr1348941` |
| `gitlab-runner-authentication-token` | Discovered a GitLab Runner Authentication Token, posing a risk to CI/CD pipeline integrity and unauthorized access | 3 | `glrt-` |
| `gitlab-runner-authentication-token-routable` | Discovered a GitLab Runner Authentication Token (Routable), posing a risk to CI/CD pipeline integrity and unauthorized access | 4 | `glrt-` |
| `gitlab-scim-token` | Discovered a GitLab SCIM Token, posing a risk to unauthorized access for a organization or instance | 3 | `glsoat-` |
| `gitlab-session-cookie` | Discovered a GitLab Session Cookie, posing a risk to unauthorized access to a user account | 3 | `_gitlab_session=` |
| `gitter-access-token` | Uncovered a Gitter Access Token, which may lead to unauthorized access to chat and communication services | no | `gitter` |
| `gocardless-api-token` | Detected a GoCardless API token, potentially risking unauthorized direct debit payment operations and financial data exposure | no | `live_`, `gocardless` |
| `grafana-api-key` | Identified a Grafana API key, which could compromise monitoring dashboards and sensitive data analytics | 3 | `eyjrijoi` |
| `grafana-cloud-api-token` | Found a Grafana cloud API token, risking unauthorized access to cloud-based monitoring services and data exposure | 3 | `glc_` |
| `grafana-service-account-token` | Discovered a Grafana service account token, posing a risk of compromised monitoring services and data integrity | 3 | `glsa_` |
| `harness-api-key` | Identified a Harness Access Token (PAT or SAT), risking unauthorized access to a Harness account | no | `pat.`, `sat.` |
| `hashicorp-tf-api-token` | Uncovered a HashiCorp Terraform user/org API token, which may lead to unauthorized infrastructure management and security breaches | 3.5 | `atlasv1` |
| `heroku-api-key` | Detected a Heroku API Key, potentially compromising cloud application deployments and operational security | no | `heroku` |
| `heroku-api-key-v2` | Detected a Heroku API Key, potentially compromising cloud application deployments and operational security | 4 | `hrku-aa` |
| `hubspot-api-key` | Found a HubSpot API Token, posing a risk to CRM data integrity and unauthorized marketing operations | no | `hubspot` |
| `huggingface-organization-api-token` | Uncovered a Hugging Face Organization API token, potentially compromising AI organization accounts and associated data | 2 | `api_org_` |
| `infracost-api-token` | Detected an Infracost API Token, risking unauthorized access to cloud cost estimation tools and financial data | 3 | `ico-` |
| `intercom-api-key` | Identified an Intercom API Token, which could compromise customer communication channels and data privacy | no | `intercom` |
| `intra42-client-secret` | Found a Intra42 client secret, which could lead to unauthorized access to the 42School API and sensitive data | 3 | `intra`, `s-s4t2ud-`, `s-s4t2af-` |
| `jfrog-api-key` | Found a JFrog API Key, posing a risk of unauthorized access to software artifact repositories and build pipelines | no | `jfrog`, `artifactory`, `bintray`, `xray` |
| `jfrog-identity-token` | Discovered a JFrog Identity Token, potentially compromising access to JFrog services and sensitive software artifacts | no | `jfrog`, `artifactory`, `bintray`, `xray` |
| `jwt-base64` | Detected a Base64-encoded JSON Web Token, posing a risk of exposing encoded authentication and data exchange information | 2 | `zxlk` |
| `kraken-access-token` | Identified a Kraken Access Token, potentially compromising cryptocurrency trading accounts and financial security | no | `kraken` |
| `kucoin-access-token` | Found a Kucoin Access Token, risking unauthorized access to cryptocurrency exchange services and transactions | no | `kucoin` |
| `kucoin-secret-key` | Discovered a Kucoin Secret Key, which could lead to compromised cryptocurrency operations and financial data breaches | no | `kucoin` |
| `launchdarkly-access-token` | Uncovered a Launchdarkly Access Token, potentially compromising feature flag management and application functionality | no | `launchdarkly` |
| `linear-api-key` | Detected a Linear API Token, posing a risk to project management tools and sensitive task data | 2 | `lin_api_` |
| `linear-client-secret` | Identified a Linear Client Secret, which may compromise secure integrations and sensitive project management data | 2 | `linear` |
| `linkedin-client-id` | Found a LinkedIn Client ID, risking unauthorized access to LinkedIn integrations and professional data exposure | 2 | `linkedin`, `linked_in`, `linked-in` |
| `linkedin-client-secret` | Discovered a LinkedIn Client secret, potentially compromising LinkedIn application integrations and user data | 2 | `linkedin`, `linked_in`, `linked-in` |
| `lob-api-key` | Uncovered a Lob API Key, which could lead to unauthorized access to mailing and address verification services | no | `test_`, `live_` |
| `lob-pub-api-key` | Detected a Lob Publishable API Key, posing a risk of exposing mail and print service integrations | no | `test_pub`, `live_pub`, `_pub` |
| `looker-client-id` | Found a Looker Client ID, risking unauthorized access to a Looker account and exposing sensitive data | no | `looker` |
| `looker-client-secret` | Found a Looker Client Secret, risking unauthorized access to a Looker account and exposing sensitive data | no | `looker` |
| `mailgun-private-api-token` | Found a Mailgun private API token, risking unauthorized email service operations and data breaches | no | `mailgun` |
| `mailgun-pub-key` | Discovered a Mailgun public validation key, which could expose email verification processes and associated data | no | `mailgun` |
| `mailgun-signing-key` | Uncovered a Mailgun webhook signing key, potentially compromising email automation and data integrity | no | `mailgun` |
| `mapbox-api-token` | Detected a MapBox API token, posing a risk to geospatial services and sensitive location data exposure | no | `mapbox` |
| `mattermost-access-token` | Identified a Mattermost Access Token, which may compromise team communication channels and data privacy | no | `mattermost` |
| `maxmind-license-key` | Discovered a potential MaxMind license key | 4 | `_mmk` |
| `messagebird-api-token` | Found a MessageBird API token, risking unauthorized access to communication platforms and message data | no | `messagebird`, `message-bird`, `message_bird` |
| `messagebird-client-id` | Discovered a MessageBird client ID, potentially compromising API integrations and sensitive communication data | no | `messagebird`, `message-bird`, `message_bird` |
| `microsoft-teams-webhook` | Uncovered a Microsoft Teams Webhook, which could lead to unauthorized access to team collaboration tools and data leaks | no | `webhook.office.com`, `webhookb2`, `incomingwebhook` |
| `netlify-access-token` | Detected a Netlify Access Token, potentially compromising web hosting services and site management | no | `netlify` |
| `new-relic-browser-api-token` | Identified a New Relic ingest browser API token, risking unauthorized access to application performance data and analytics | no | `nrjs-` |
| `new-relic-insert-key` | Discovered a New Relic insight insert key, compromising data injection into the platform | no | `nrii-` |
| `new-relic-user-api-id` | Found a New Relic user API ID, posing a risk to application monitoring services and data integrity | no | `new-relic`, `newrelic`, `new_relic` |
| `new-relic-user-api-key` | Discovered a New Relic user API Key, which could lead to compromised application insights and performance monitoring | no | `nrak` |
| `notion-api-token` | Notion API token | 4 | `ntn_` |
| `nytimes-access-token` | Detected a Nytimes Access Token, risking unauthorized access to New York Times APIs and content services | no | `nytimes`, `new-york-times`, `newyorktimes` |
| `octopus-deploy-api-key` | Discovered a potential Octopus Deploy API key, risking application deployments and operational security | 3 | `api-` |
| `okta-access-token` | Identified an Okta Access Token, which may compromise identity management services and user authentication data | 4 | `okta` |
| `perplexity-api-key` | Detected a Perplexity API key, which could lead to unauthorized access to Perplexity AI services and data exposure | 4 | `pplx-` |
| `plaid-api-token` | Discovered a Plaid API Token, potentially compromising financial data aggregation and banking services | no | `plaid` |
| `plaid-client-id` | Uncovered a Plaid Client ID, which could lead to unauthorized financial service integrations and data breaches | 3.5 | `plaid` |
| `plaid-secret-key` | Detected a Plaid Secret key, risking unauthorized access to financial accounts and sensitive transaction data | 3.5 | `plaid` |
| `planetscale-api-token` | Identified a PlanetScale API token, potentially compromising database management and operations | 3 | `pscale_tkn_` |
| `planetscale-oauth-token` | Found a PlanetScale OAuth token, posing a risk to database access control and sensitive data integrity | 3 | `pscale_oauth_` |
| `planetscale-password` | Discovered a PlanetScale password, which could lead to unauthorized database operations and data breaches | 3 | `pscale_pw_` |
| `postman-api-token` | Uncovered a Postman API token, potentially compromising API testing and development workflows | 3 | `pmak-` |
| `prefect-api-token` | Detected a Prefect API token, risking unauthorized access to workflow management and automation services | 2 | `pnu_` |
| `privateai-api-token` | Identified a PrivateAI Token, posing a risk of unauthorized access to AI services and data manipulation | 3 | `privateai`, `private_ai`, `private-ai` |
| `pulumi-api-token` | Found a Pulumi API token, posing a risk to infrastructure as code services and cloud resource management | 2 | `pul-` |
| `pypi-upload-token` | Discovered a PyPI upload token, potentially compromising Python package distribution and repository integrity | 3 | `pypi-ageichlwas5vcmc` |
| `rapidapi-access-token` | Uncovered a RapidAPI Access Token, which could lead to unauthorized access to various APIs and data services | no | `rapidapi` |
| `readme-api-token` | Detected a Readme API token, risking unauthorized documentation management and content exposure | 2 | `rdme_` |
| `rubygems-api-token` | Identified a Rubygem API token, potentially compromising Ruby library distribution and package management | 2 | `rubygems_` |
| `scalingo-api-token` | Found a Scalingo API token, posing a risk to cloud platform services and application deployment security | 2 | `tk-us-` |
| `sendbird-access-id` | Discovered a Sendbird Access ID, which could compromise chat and messaging platform integrations | no | `sendbird` |
| `sendbird-access-token` | Uncovered a Sendbird Access Token, potentially risking unauthorized access to communication services and user data | no | `sendbird` |
| `sendinblue-api-token` | Identified a Sendinblue API token, which may compromise email marketing services and subscriber data privacy | 2 | `xkeysib-` |
| `sentry-access-token` | Found a Sentry.io Access Token (old format), risking unauthorized access to error tracking services and sensitive application data | 3 | `sentry` |
| `sentry-org-token` | Found a Sentry.io Organization Token, risking unauthorized access to error tracking services and sensitive application data | 4.5 | `sntrys_eyjpyxqio` |
| `sentry-user-token` | Found a Sentry.io User Token, risking unauthorized access to error tracking services and sensitive application data | 3.5 | `sntryu_` |
| `settlemint-application-access-token` | Found a Settlemint Application Access Token | 3 | `sm_aat` |
| `settlemint-personal-access-token` | Found a Settlemint Personal Access Token | 3 | `sm_pat` |
| `settlemint-service-access-token` | Found a Settlemint Service Access Token | 3 | `sm_sat` |
| `shippo-api-token` | Discovered a Shippo API token, potentially compromising shipping services and customer order data | 2 | `shippo_` |
| `shopify-access-token` | Uncovered a Shopify access token, which could lead to unauthorized e-commerce platform access and data breaches | 2 | `shpat_` |
| `shopify-custom-access-token` | Detected a Shopify custom access token, potentially compromising custom app integrations and e-commerce data security | 2 | `shpca_` |
| `shopify-private-app-access-token` | Identified a Shopify private app access token, risking unauthorized access to private app data and store operations | 2 | `shppa_` |
| `shopify-shared-secret` | Found a Shopify shared secret, posing a risk to application authentication and e-commerce platform security | 2 | `shpss_` |
| `sidekiq-secret` | Discovered a Sidekiq Secret, which could lead to compromised background job processing and application data breaches | no | `bundle_enterprise__contribsys__com`, `bundle_gems__contribsys__com` |
| `sidekiq-sensitive-url` | Uncovered a Sidekiq Sensitive URL, potentially exposing internal job queues and sensitive operation details | no | `gems.contribsys.com`, `enterprise.contribsys.com` |
| `slack-app-token` | Detected a Slack App-level token, risking unauthorized access to Slack applications and workspace data | 2 | `xapp` |
| `slack-config-access-token` | Found a Slack Configuration access token, posing a risk to workspace configuration and sensitive data access | 2 | `xoxe.xoxb-`, `xoxe.xoxp-` |
| `slack-config-refresh-token` | Discovered a Slack Configuration refresh token, potentially allowing prolonged unauthorized access to configuration settings | 2 | `xoxe-` |
| `slack-legacy-token` | Detected a Slack Legacy token, risking unauthorized access to older Slack integrations and user data | 2 | `xoxo`, `xoxs` |
| `snyk-api-token` | Uncovered a Snyk API token, potentially compromising software vulnerability scanning and code security | no | `snyk` |
| `sonar-api-token` | Uncovered a Sonar API token, potentially compromising software vulnerability scanning and code security | no | `sonar` |
| `sourcegraph-access-token` | Sourcegraph is a code search and navigation engine | 3 | `sgp_`, `sourcegraph` |
| `square-access-token` | Detected a Square Access Token, risking unauthorized payment processing and financial transaction exposure | 2 | `sq0atp-`, `eaaa` |
| `squarespace-access-token` | Identified a Squarespace Access Token, which may compromise website management and content control on Squarespace | no | `squarespace` |
| `stripe-access-token` | Found a Stripe Access Token, posing a risk to payment processing services and sensitive financial data | 2 | `sk_test`, `sk_live`, `sk_prod`, `rk_test`, `rk_live`, `rk_prod` |
| `sumologic-access-id` | Discovered a SumoLogic Access ID, potentially compromising log management services and data analytics integrity | 3 | `sumo` |
| `sumologic-access-token` | Uncovered a SumoLogic Access Token, which could lead to unauthorized access to log data and analytics insights | 3 | `sumo` |
| `telegram-bot-api-token` | Detected a Telegram Bot API Token, risking unauthorized bot operations and message interception on Telegram | no | `telegr` |
| `travisci-access-token` | Identified a Travis CI Access Token, potentially compromising continuous integration services and codebase security | no | `travis` |
| `twilio-api-key` | Found a Twilio API Key, posing a risk to communication services and sensitive customer interaction data | 3 | `sk` |
| `twitch-api-token` | Discovered a Twitch API token, which could compromise streaming services and account integrations | no | `twitch` |
| `twitter-access-secret` | Uncovered a Twitter Access Secret, potentially risking unauthorized Twitter integrations and data breaches | no | `twitter` |
| `twitter-access-token` | Detected a Twitter Access Token, posing a risk of unauthorized account operations and social media data exposure | no | `twitter` |
| `twitter-api-key` | Identified a Twitter API Key, which may compromise Twitter application integrations and user data security | no | `twitter` |
| `twitter-api-secret` | Found a Twitter API Secret, risking the security of Twitter app integrations and sensitive data access | no | `twitter` |
| `twitter-bearer-token` | Discovered a Twitter Bearer Token, potentially compromising API access and data retrieval from Twitter | no | `twitter` |
| `typeform-api-token` | Uncovered a Typeform API token, which could lead to unauthorized survey management and data collection | no | `tfp_` |
| `vault-batch-token` | Detected a Vault Batch Token, risking unauthorized access to secret management services and sensitive data | 4 | `hvb.` |
| `vault-service-token` | Identified a Vault Service Token, potentially compromising infrastructure security and access to sensitive credentials | 3.5 | `hvs.`, `s.` |
| `yandex-access-token` | Found a Yandex Access Token, posing a risk to Yandex service integrations and user data privacy | no | `yandex` |
| `yandex-api-key` | Discovered a Yandex API Key, which could lead to unauthorized access to Yandex services and data manipulation | no | `yandex` |
| `yandex-aws-access-token` | Uncovered a Yandex AWS Access Token, potentially compromising cloud resource access and data security on Yandex Cloud | no | `yandex` |
| `zendesk-secret-key` | Detected a Zendesk Secret Key, risking unauthorized access to customer support services and sensitive ticketing data | no | `zendesk` |

### Gitleaks rules not ported

- `anthropic-admin-api-key`: covered by the hand-written `anthropic-key`;
- `anthropic-api-key`: covered by the hand-written `anthropic-key`;
- `curl-auth-header`: needs Unicode classes whose compiled size is over the regex size limit;
- `freemius-secret-key`: applies only to files whose name matches a path pattern, which a text scan does not see;
- `gcp-api-key`: covered by the hand-written `gcp-api-key`;
- `github-app-token`: covered by the hand-written `github-token`;
- `github-fine-grained-pat`: covered by the hand-written `github-token`;
- `github-oauth`: covered by the hand-written `github-token`;
- `github-pat`: covered by the hand-written `github-token`;
- `github-refresh-token`: covered by the hand-written `github-token`;
- `gitlab-pat`: covered by the hand-written `gitlab-token`;
- `hashicorp-tf-password`: applies only to files whose name matches a path pattern, which a text scan does not see;
- `huggingface-access-token`: covered by the hand-written `huggingface-token`;
- `jwt`: covered by the hand-written `jwt`;
- `kubernetes-secret-yaml`: applies only to files whose name matches a path pattern, which a text scan does not see;
- `mailchimp-api-key`: needs Unicode classes whose compiled size is over the regex size limit;
- `npm-access-token`: covered by the hand-written `npm-token`;
- `nuget-config-password`: applies only to files whose name matches a path pattern, which a text scan does not see;
- `openai-api-key`: covered by the hand-written `openai-key`;
- `openshift-user-token`: needs Unicode classes whose compiled size is over the regex size limit;
- `pkcs12-file`: applies only to files whose name matches a path pattern, which a text scan does not see;
- `private-key`: covered by the hand-written `private-key-block`;
- `sendgrid-api-token`: covered by the hand-written `sendgrid-key`;
- `slack-bot-token`: covered by the hand-written `slack-token`;
- `slack-legacy-bot-token`: covered by the hand-written `slack-token`;
- `slack-legacy-workspace-token`: covered by the hand-written `slack-token`;
- `slack-user-token`: covered by the hand-written `slack-token`;
- `slack-webhook-url`: covered by the hand-written `slack-webhook`;

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
