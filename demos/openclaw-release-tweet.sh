#!/usr/bin/env bash
set -euo pipefail

# OpenClaw release tweet demo (Lobster)
#
# What it does:
# - Reads commits since a ref (defaults to HEAD~30 or STATE_REF if set)
# - Reads top section of CHANGELOG.md
# - Uses llm_task.invoke to generate ONE tweet in the requested style
# - Shows an approval gate
# - Optionally posts via bird CLI
#
# Prereqs:
# - OpenClaw repo checked out locally (default: ../openclaw)
# - jq installed
# - Clawdbot/Moltbot gateway reachable for llm_task.invoke via env:
#     export CLAWD_URL='http://127.0.0.1:19700/tools/invoke'
#     export CLAWD_TOKEN='<token>'
# - bird CLI installed+authed if POST=true
#
# Usage:
#   STYLE=sassy POST=false ./demos/openclaw-release-tweet.sh
#   STYLE=professional POST=false ./demos/openclaw-release-tweet.sh
#   STYLE=drybread POST=true ./demos/openclaw-release-tweet.sh

STYLE=${STYLE:-professional} # sassy|professional|drybread
POST=${POST:-false}          # true|false
REPO_DIR=${REPO_DIR:-../openclaw}
SINCE_REF=${SINCE_REF:-}
MAX_COMMITS=${MAX_COMMITS:-30}
LINK=${LINK:-https://openclaw.dev}

if [[ ! -d "$REPO_DIR/.git" ]]; then
  echo "Repo not found: $REPO_DIR (expected a git repo). Set REPO_DIR=..." >&2
  exit 1
fi

if ! command -v jq >/dev/null 2>&1; then
  echo "jq is required" >&2
  exit 1
fi

if [[ "$STYLE" != "sassy" && "$STYLE" != "professional" && "$STYLE" != "drybread" ]]; then
  echo "STYLE must be one of: sassy|professional|drybread" >&2
  exit 1
fi

# Determine since ref
if [[ -z "$SINCE_REF" ]]; then
  SINCE_REF="HEAD~${MAX_COMMITS}"
fi

COMMITS=$(cd "$REPO_DIR" && git log --no-merges --pretty=format:'%h %s (%an)' "$SINCE_REF..HEAD" | head -n 80 | jq -R -s -c 'split("\n") | map(select(length>0))')

CHANGELOG=$(cd "$REPO_DIR" && node - <<'NODE'
const fs = require('fs');
const t = fs.readFileSync('CHANGELOG.md','utf8');
const parts = t.split(/\n## /);
if (parts.length > 1) {
  const top = ('## ' + parts[1]).split(/\n## /)[0];
  process.stdout.write(top.slice(0, 6000));
} else {
  process.stdout.write(t.slice(0, 6000));
}
NODE
)

CONTEXT=$(jq -n --arg style "$STYLE" --arg since "$SINCE_REF" --arg link "$LINK" --argjson commits "$COMMITS" --arg changelog "$CHANGELOG" '{style:$style,since:$since,link:$link,commits:$commits,changelog:$changelog}')

PIPELINE=$(cat <<'EOF'
exec --json --shell 'printf "%s" "$CONTEXT"'
| llm_task.invoke --schema '{"type":"object","properties":{"tweet":{"type":"string"}},"required":["tweet"],"additionalProperties":false}' --prompt "You are writing a release tweet for OpenClaw.\n\nInput JSON has: style (sassy|professional|drybread), since, link, commits[], changelog.\n\nWrite ONE tweet in the requested style.\nConstraints:\n- <= 260 chars\n- Include the link exactly once\n- Do not hallucinate features\n- No hashtags unless truly helpful (max 1)\n\nReturn JSON: {\"tweet\":\"...\"}\n\nINPUT:\n{{.}}"
| approve --prompt 'Post this tweet?'
EOF
)

# Render pipeline with env substitutions via bash (CONTEXT)
export CONTEXT

OUT=$(node bin/lobster.js run --mode tool "$PIPELINE")

# If needs approval, show prompt + preview
STATUS=$(echo "$OUT" | jq -r '.status')

if [[ "$STATUS" == "needs_approval" ]]; then
  PROMPT=$(echo "$OUT" | jq -r '.requiresApproval.prompt')
  echo "\n--- APPROVAL ---\n$PROMPT\n" >&2

  if [[ "$POST" == "true" ]]; then
    # Approve and resume, then post via bird.
    TOKEN=$(echo "$OUT" | jq -r '.requiresApproval.resumeToken')
    DONE=$(node bin/lobster.js resume --token "$TOKEN" --approve yes)
    TWEET=$(echo "$DONE" | jq -r '.output[0].tweet')
    echo "\nPosting via bird...\n" >&2
    bird post --text "$TWEET"
    echo "\nDone.\n" >&2
  else
    echo "(Dry-run) Not posting. Set POST=true to actually post." >&2
  fi
  exit 0
fi

if [[ "$STATUS" == "ok" ]]; then
  echo "$OUT" | jq -r '.output[0].tweet'
  exit 0
fi

echo "$OUT" | jq '.'
exit 1
