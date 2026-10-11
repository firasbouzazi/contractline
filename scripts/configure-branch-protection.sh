#!/usr/bin/env bash
# Applies the protection rules for `main` described in docs/adr/0004.
# Requires the GitHub CLI (`gh`) authenticated as a repository admin.
#
# Usage: scripts/configure-branch-protection.sh [owner/repo]
set -euo pipefail

repo="${1:-$(gh repo view --json nameWithOwner --jq .nameWithOwner)}"

# Required check names must match the job names in .github/workflows/ci.yml.
gh api --method PUT "repos/${repo}/branches/main/protection" --input - <<'JSON'
{
  "required_status_checks": {
    "strict": true,
    "contexts": ["checks (3.11)", "checks (3.12)"]
  },
  "enforce_admins": false,
  "required_pull_request_reviews": {
    "required_approving_review_count": 1,
    "dismiss_stale_reviews": true,
    "require_code_owner_reviews": true
  },
  "restrictions": null,
  "required_linear_history": true,
  "allow_force_pushes": false,
  "allow_deletions": false,
  "required_conversation_resolution": true
}
JSON

gh api --method PATCH "repos/${repo}" \
  -F allow_squash_merge=true \
  -F allow_merge_commit=false \
  -F allow_rebase_merge=false \
  -F delete_branch_on_merge=true \
  >/dev/null

echo "Branch protection applied to ${repo}@main"
