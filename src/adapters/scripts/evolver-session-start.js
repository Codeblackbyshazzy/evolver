#!/usr/bin/env node
// evolver-session-start.js
// Reads recent evolution memory and injects it as context for the agent session.
// Input: stdin JSON (session context). Output: stdout JSON with agent_message.

const fs = require('fs');
const path = require('path');
const os = require('os');

const { findEvolverRoot, findMemoryGraph } = require('./_runtimePaths');
const { filterRelevantOutcomes } = require('./_memoryFiltering');

function readLastN(filePath, n) {
  try {
    const content = fs.readFileSync(filePath, 'utf8');
    const lines = content.trim().split('\n').filter(Boolean);
    return lines.slice(-n).map(line => {
      try { return JSON.parse(line); } catch { return null; }
    }).filter(Boolean);
  } catch { return []; }
}

function formatOutcome(entry) {
  const status = entry.outcome ? entry.outcome.status : 'unknown';
  const score = entry.outcome && entry.outcome.score != null ? entry.outcome.score : '?';
  const note = entry.outcome && entry.outcome.note ? entry.outcome.note : '';
  const signals = Array.isArray(entry.signals) ? entry.signals.slice(0, 3).join(', ') : '';
  const ts = entry.timestamp ? entry.timestamp.slice(0, 10) : '';
  const icon = status === 'success' ? '+' : status === 'failed' ? '-' : '?';
  return `[${icon}] ${ts} score=${score} signals=[${signals}] ${note}`.slice(0, 200);
}

// Dedup guard: on platforms like Kiro, the sessionStart-equivalent event
// (`promptSubmit`) fires on every user message in a session. Without this
// guard, recent memory would be re-injected on every prompt. We key the
// dedup on (platform, cwd) with a short TTL so a fresh agent session within
// the same workspace still gets the injection, but mid-session prompts do
// not. Cursor/Claude Code/Codex have true sessionStart events and should
// bypass this check (controlled by EVOLVER_SESSION_START_DEDUP env var,
// which the Kiro adapter sets on the hook command line implicitly via the
// runtime environment, and other adapters leave unset).
function getDedupStatePath() {
  const dir = process.env.EVOLVER_SESSION_STATE_DIR
    || path.join(os.homedir(), '.evolver');
  try { fs.mkdirSync(dir, { recursive: true }); } catch { /* ignore */ }
  return path.join(dir, 'session-start-state.json');
}

function shouldSkipInjection() {
  // Only apply dedup when explicitly enabled (set by Kiro adapter) OR when
  // we detect a per-prompt-firing platform via PROMPT_SUBMIT heuristic in
  // stdin. The stdin is drained in main(), so we rely on env flag here.
  const dedupEnabled = String(process.env.EVOLVER_SESSION_START_DEDUP || '').toLowerCase() === '1'
    || String(process.env.EVOLVER_SESSION_START_DEDUP || '').toLowerCase() === 'true';
  if (!dedupEnabled) return false;

  const ttlMs = Number(process.env.EVOLVER_SESSION_START_DEDUP_TTL_MS) || (30 * 60 * 1000);
  const key = process.cwd();
  const statePath = getDedupStatePath();

  let state = {};
  try {
    if (fs.existsSync(statePath)) {
      state = JSON.parse(fs.readFileSync(statePath, 'utf8')) || {};
    }
  } catch { state = {}; }

  const now = Date.now();
  const last = state[key];
  if (typeof last === 'number' && now - last < ttlMs) {
    return true;
  }

  state[key] = now;
  try {
    for (const k of Object.keys(state)) {
      if (typeof state[k] !== 'number' || now - state[k] > 24 * 60 * 60 * 1000) {
        delete state[k];
      }
    }
    const tmp = statePath + '.tmp';
    fs.writeFileSync(tmp, JSON.stringify(state), 'utf8');
    fs.renameSync(tmp, statePath);
  } catch { /* best-effort */ }

  return false;
}

function main() {
  if (shouldSkipInjection()) {
    process.stdout.write(JSON.stringify({}));
    return;
  }

  const evolverRoot = findEvolverRoot();
  const graphPath = findMemoryGraph(evolverRoot);

  if (!graphPath) {
    process.stdout.write(JSON.stringify({}));
    return;
  }

  const entries = readLastN(graphPath, 5);
  const filtered = filterRelevantOutcomes(entries);

  if (filtered.length === 0) {
    process.stdout.write(JSON.stringify({}));
    return;
  }

  const successCount = filtered.filter(e => e.outcome && e.outcome.status === 'success').length;
  const failCount = filtered.filter(e => e.outcome && e.outcome.status === 'failed').length;

  const lines = filtered.map(formatOutcome);
  const summary = [
    `[Evolution Memory] Recent ${filtered.length} outcomes (${successCount} success, ${failCount} failed):`,
    ...lines,
    '',
    'Use successful approaches. Avoid repeating failed patterns.',
  ].join('\n');

  process.stdout.write(JSON.stringify({
    agent_message: summary,
    additionalContext: summary,
  }));
}

main();
