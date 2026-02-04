# Lobster demos

## OpenClaw release tweet

A stage-friendly demo: read OpenClaw commits + changelog and generate a release tweet in one of three styles, with an approval gate and optional posting.

File: `demos/openclaw-release-tweet.sh`

### Prereqs
- OpenClaw repo checked out locally (default path: `../openclaw`)
- `jq`
- A running Clawdbot/Moltbot Gateway for `llm_task.invoke`:
  - `CLAWD_URL` e.g. `http://127.0.0.1:19700/tools/invoke`
  - `CLAWD_TOKEN` bearer token
- Optional: `bird` CLI installed + authenticated (only if `POST=true`)

### Run
```bash
cd lobster
STYLE=sassy POST=false ./demos/openclaw-release-tweet.sh
STYLE=professional POST=false ./demos/openclaw-release-tweet.sh
STYLE=drybread POST=true ./demos/openclaw-release-tweet.sh
```

### Notes
- By default it uses commits from `HEAD~30..HEAD`. Override with `SINCE_REF=<tag-or-sha>`.
- Set `LINK=https://...` to point at your release notes / site.
