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

cd "$(git rev-parse --show-toplevel)"
remote_base="git@github.com:insanai"

publish_subtree() {
    prefix="$1"
    repo="$2"
    branch="split-publish/${repo}"
    git branch -D "$branch" >/dev/null 2>&1 || true
    git subtree split --prefix="$prefix" -b "$branch" >/dev/null
    git push "${remote_base}/${repo}.git" "refs/heads/${branch}:refs/heads/main"
    git branch -D "$branch" >/dev/null
}

publish_subtree zaxonlite zaxonlite
publish_subtree zaxonlite/src/cli_ui zaxon-cli-ui
publish_subtree docs zxdocs

# paxos-zig is the root minus the split directories. git subtree cannot
# exclude paths, so filter a throwaway clone and push its history.
workdir="$(mktemp -d)"
trap 'rm -rf "$workdir"' EXIT
git clone --quiet "$(pwd)" "$workdir/paxos-zig"
(
    cd "$workdir/paxos-zig"
    git filter-repo --quiet --invert-paths --path zaxonlite --path docs
    git push "${remote_base}/paxos-zig.git" refs/heads/main:refs/heads/main
)
