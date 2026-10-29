#!/bin/sh
# Installs the redacter command line tool from its GitHub release.
#
#   curl -fsSL https://raw.githubusercontent.com/abdolence/redacter-rs/master/install.sh | sh
#
# REDACTER_INSTALL_DIR  target directory, default $HOME/.local/bin
# REDACTER_VERSION      release tag such as v0.21.0, default is the latest release
#
# See the "Installation" section of the README for details.
#
# The whole script is wrapped in main and called on the last line, so a download
# cut short in the middle of a pipe to sh never runs a partial script.

set -eu

REPO="abdolence/redacter-rs"
API_URL="https://api.github.com/repos/$REPO/releases"
DOWNLOAD_URL="https://github.com/$REPO/releases/download"

TMP_DIR=""

die() {
    printf 'error: %s\n' "$*" >&2
    exit 1
}

cleanup() {
    if [ -n "$TMP_DIR" ]; then
        rm -rf "$TMP_DIR"
    fi
}

asset_for() {
    case "$1-$2" in
        Darwin-arm64) printf 'redacter-Darwin-aarch64.tar.gz\n' ;;
        Darwin-x86_64) printf 'redacter-Darwin-x86_64.tar.gz\n' ;;
        Linux-x86_64 | Linux-amd64) printf 'redacter-Linux-x86_64.tar.gz\n' ;;
        Linux-aarch64 | Linux-arm64) printf 'redacter-Linux-aarch64.tar.gz\n' ;;
        Linux-armv6l | Linux-armv7l) printf 'redacter-Linux-arm-musl.tar.gz\n' ;;
        FreeBSD-amd64 | FreeBSD-x86_64) printf 'redacter-FreeBSD-x86_64.tar.gz\n' ;;
        *) return 1 ;;
    esac
}

# Prints the machine name to pick the archive by. A shell started under Rosetta
# on Apple silicon reports x86_64, and the native build is the better choice there.
# A 32-bit userland on a 64-bit ARM kernel reports aarch64 and can only run the
# 32-bit static build.
detect_machine() {
    machine=$(uname -m)
    case "$(uname -s)-$machine" in
        Darwin-x86_64)
            if [ "$(sysctl -n hw.optional.arm64 2>/dev/null || true)" = "1" ]; then
                machine="arm64"
            fi
            ;;
        Linux-aarch64 | Linux-arm64)
            if [ "$(getconf LONG_BIT 2>/dev/null || true)" = "32" ]; then
                machine="armv7l"
            fi
            ;;
    esac
    printf '%s\n' "$machine"
}

# The glibc archives cannot run on musl systems such as Alpine.
is_musl() {
    ldd --version 2>&1 | grep -qi musl
}

download() {
    if command -v curl >/dev/null 2>&1; then
        curl -fsSL "$1" -o "$2"
    elif command -v wget >/dev/null 2>&1; then
        wget -q -O "$2" "$1"
    else
        die "neither curl nor wget is installed"
    fi
}

# The GitHub API answers with pretty-printed JSON, and jq may not be installed.
# Newlines are removed first so one sed expression sees the whole document.
release_tag() {
    tr -d '\n' | sed -n 's/.*"tag_name": *"\([^"]*\)".*/\1/p'
}

# asset_digest ASSET: the SHA-256 GitHub records for ASSET, read from the release JSON on
# standard input. The assets are split into one line per object, so the name and the
# digest are matched within the same asset whatever order the keys come in, and an
# asset whose digest is null never picks up the digest of its neighbour.
asset_digest() {
    tr -d '\n' |
        sed 's/}, *{/\
/g' |
        grep "\"name\": *\"$1\"" |
        grep -o '"digest": *"sha256:[0-9a-fA-F]\{64\}"' |
        head -n 1 |
        sed 's/.*sha256:\([0-9a-fA-F]*\)".*/\1/' |
        tr 'A-F' 'a-f'
}

sha256_of() {
    if command -v sha256sum >/dev/null 2>&1; then
        sha256sum "$1" | cut -d ' ' -f 1
    elif command -v shasum >/dev/null 2>&1; then
        shasum -a 256 "$1" | cut -d ' ' -f 1
    elif command -v sha256 >/dev/null 2>&1; then
        sha256 -q "$1"
    else
        return 1
    fi
}

# verify_sha256 FILE EXPECTED: fails unless FILE hashes to EXPECTED.
verify_sha256() {
    actual=$(sha256_of "$1") || die "cannot compute SHA-256, install sha256sum, shasum or sha256"
    if [ "$actual" != "$2" ]; then
        die "SHA-256 mismatch for $(basename "$1"): expected $2, got $actual. Nothing was installed"
    fi
}

main() {
    umask 022

    os=$(uname -s)
    machine=$(detect_machine)
    asset=$(asset_for "$os" "$machine") ||
        die "unsupported platform $os $machine. Supported: macOS (arm64, x86_64), Linux (x86_64, aarch64, armv6l, armv7l), FreeBSD (x86_64).
On Windows use install.ps1. Other platforms: build from source with 'cargo install redacter', or look for an archive at https://github.com/$REPO/releases"
    case "$asset" in
        redacter-Linux-x86_64.tar.gz | redacter-Linux-aarch64.tar.gz)
            if is_musl; then
                die "this system uses musl libc and the $os $machine archives need glibc. Build from source with 'cargo install redacter'"
            fi
            ;;
    esac

    install_dir="${REDACTER_INSTALL_DIR:-}"
    if [ -z "$install_dir" ]; then
        [ -n "${HOME:-}" ] || die "HOME is not set, set REDACTER_INSTALL_DIR"
        install_dir="$HOME/.local/bin"
    fi
    install_dir="${install_dir%/}"

    version="${REDACTER_VERSION:-}"
    case "$version" in
        "") release_url="$API_URL/latest" ;;
        *[!A-Za-z0-9._+-]* | [!v]* | v | v[!0-9]*) die "REDACTER_VERSION must be a release tag such as v0.21.0, got '$version'" ;;
        *) release_url="$API_URL/tags/$version" ;;
    esac

    TMP_DIR=$(mktemp -d "${TMPDIR:-/tmp}/redacter-install.XXXXXX") || die "cannot create a temporary directory"
    trap cleanup EXIT
    trap 'exit 130' INT
    trap 'exit 143' HUP TERM

    release_json="$TMP_DIR/release.json"
    download "$release_url" "$release_json" ||
        die "cannot fetch release information from $release_url. Check REDACTER_VERSION, and that the GitHub API rate limit is not exhausted"

    tag=$(release_tag <"$release_json")
    case "$tag" in
        v[0-9]*) ;;
        *) die "$release_url did not return a redacter release" ;;
    esac

    expected=$(asset_digest "$asset" <"$release_json")
    [ -n "$expected" ] || die "release $tag has no SHA-256 digest for $asset, refusing to install it"

    printf 'Installing redacter %s (%s) to %s\n' "$tag" "$asset" "$install_dir"
    download "$DOWNLOAD_URL/$tag/$asset" "$TMP_DIR/$asset" || die "cannot download $DOWNLOAD_URL/$tag/$asset"
    verify_sha256 "$TMP_DIR/$asset" "$expected"

    mkdir "$TMP_DIR/extract"
    tar -xzf "$TMP_DIR/$asset" -C "$TMP_DIR/extract"
    [ -f "$TMP_DIR/extract/redacter" ] || die "$asset does not contain a redacter binary"

    mkdir -p "$install_dir"
    # The new binary is staged beside the old one and run before it replaces it, so a binary
    # that cannot run here leaves an existing installation alone. Replacing through a rename
    # also lets an upgrade succeed while the old binary is running.
    target="$install_dir/redacter"
    cp "$TMP_DIR/extract/redacter" "$target.new"
    chmod 755 "$target.new"
    if ! banner=$("$target.new" --help 2>&1); then
        rm -f "$target.new"
        die "$asset does not run on this system: $banner
Nothing was installed. Build from source with 'cargo install redacter'"
    fi
    mv -f "$target.new" "$target"

    printf 'Installed %s: %s\n' "$target" "$(printf '%s\n' "$banner" | head -n 1)"

    case ":${PATH:-}:" in
        *":$install_dir:"*) ;;
        *)
            # The literal $PATH is what the user is told to type.
            # shellcheck disable=SC2016
            printf '\n%s is not on your PATH. Add it with:\n  export PATH="%s:$PATH"\n' "$install_dir" "$install_dir"
            ;;
    esac
}

main "$@"
