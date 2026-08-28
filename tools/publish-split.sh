#!/bin/sh
set -eu

# Publish the monorepo to the split insanai repositories. The local
# repository stays a monorepo; this script derives each public history:
#
#   zaxonlite/            -> git@github.com:insanai/zaxonlite.git
#   zaxonlite/src/cli_ui/ -> git@github.com:insanai/zaxon-cli-ui.git
#   docs/                 -> git@github.com:insanai/zxdocs.git
#   everything else       -> git@github.com:insanai/paxos-zig.git
#
# Subtree splits and the path filter are deterministic, so repeated runs
# produce fast-forward pushes. Requires git-filter-repo for paxos-zig.
#
# zaxonlite is the one split that is NOT byte-identical to its subtree:
# the monorepo manifest pins paxos as the sibling path "..", which a
# standalone `zig fetch` consumer cannot resolve. The split therefore
# carries exactly one manifest-pin commit on top of the subtree,
# replacing the path with the released paxos-zig archive URL + content
# hash. The commit's author, committer, message, and dates are fixed
# (dates reuse the split tip's author date), so reruns reproduce the
# identical SHA and pushes stay fast-forward. Tags are still a manual
# release step: tag the rewritten split tip locally and push it to the
# split remote (plain vX.Y.Z, matching the existing scheme).

cd "$(git rev-parse --show-toplevel)"
remote_base="git@github.com:insanai"

# The released paxos-zig archive the published zaxonlite manifest pins.
# Recompute the hash with `zig fetch <url>` when bumping the release.
paxos_release_url="https://github.com/insanai/paxos-zig/archive/refs/tags/v0.6.0.tar.gz"
paxos_release_hash="paxos-0.6.0-_zKMfQnOAwBE8VPidSXbFEGc2NVkYfwgyiqmrL4IRZ6B"

force=""
for arg in "$@"; do
    case "$arg" in
        -f|--force) force="--force" ;;
    esac
done

publish_subtree() {
    prefix="$1"
    repo="$2"
    branch="split-publish/${repo}"
    git branch -D "$branch" >/dev/null 2>&1 || true
    git subtree split --prefix="$prefix" -b "$branch" >/dev/null
    git push $force "${remote_base}/${repo}.git" "refs/heads/${branch}:refs/heads/main"
    git branch -D "$branch" >/dev/null
}

# zaxonlite: subtree split plus the deterministic manifest-pin commit
# described in the header. Runs after the workdir/trap setup below.
publish_zaxonlite() {
    branch="split-publish/zaxonlite"
    git branch -D "$branch" >/dev/null 2>&1 || true
    git subtree split --prefix=zaxonlite -b "$branch" >/dev/null
    git worktree add --quiet "$workdir/zaxonlite" "$branch"
    (
        cd "$workdir/zaxonlite"
        sed -i.bak \
            's|\.paxos = \.{ \.path = "\.\." }|.paxos = .{ .url = "'"$paxos_release_url"'", .hash = "'"$paxos_release_hash"'" }|' \
            build.zig.zon
        rm build.zig.zon.bak
        if ! grep -q "$paxos_release_hash" build.zig.zon; then
            echo "error: .paxos manifest rewrite did not apply" >&2
            exit 1
        fi
        tip_date="$(git log -1 --format=%aI HEAD)"
        GIT_AUTHOR_NAME="publish-split" GIT_AUTHOR_EMAIL="publish@insan.ai" \
        GIT_AUTHOR_DATE="$tip_date" \
        GIT_COMMITTER_NAME="publish-split" GIT_COMMITTER_EMAIL="publish@insan.ai" \
        GIT_COMMITTER_DATE="$tip_date" \
            git commit -qam "publish: pin paxos to the released archive"
    )
    git push $force "${remote_base}/zaxonlite.git" "refs/heads/${branch}:refs/heads/main"
    git worktree remove --force "$workdir/zaxonlite"
    git branch -D "$branch" >/dev/null
}

# paxos-zig is the root minus the split directories. git subtree cannot
# exclude paths, so filter a throwaway clone and push its history. It is
# pushed FIRST: the other repos build against paxos-zig main, so their
# push-triggered CI must already see the matching library revision.
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
# --no-local: git-filter-repo refuses hardlinked local clones.
git clone --quiet --no-local "$(pwd)" "$workdir/paxos-zig"
(
    cd "$workdir/paxos-zig"
    git filter-repo --quiet --invert-paths --path zaxonlite --path docs
    git push $force "${remote_base}/paxos-zig.git" refs/heads/main:refs/heads/main
)

publish_zaxonlite
publish_subtree zaxonlite/src/cli_ui zaxon-cli-ui
publish_subtree docs zxdocs
