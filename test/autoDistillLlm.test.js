'use strict';

const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawn } = require('node:child_process');

const FAKE = path.resolve(__dirname, 'fixtures/fakeClaudeDistill.js');

function fakeSpawn(mode, counter) {
  return (bin, args, opts) => { if (counter) counter.n++; return spawn(process.execPath, [FAKE], { ...opts, env: { ...(opts && opts.env), FAKE_CLAUDE_MODE: mode } }); };
}

let tmpRoot, prevEnv;
const ENV = [
  'EVOLVER_REPO_ROOT', 'GEP_ASSETS_DIR', 'EVOLVER_AUTO_DISTILL_LLM', 'DISTILLER_MIN_CAPSULES',
  'EVOLVE_DISTILL_TIMEOUT_MS', 'EVOLVE_DISTILL_VALIDATION_TIMEOUT_MS', 'EVOLVER_AUTO_DISTILL_LLM_PUBLISH',
  'SKILL_AUTO_PUBLISH', 'A2A_HUB_URL', 'EVOLVER_AUTO_DISTILL_LLM_COOLDOWN_MS',
  'EVOLVER_AUTO_DISTILL_LLM_MAX_ATTEMPTS', 'EVOLVER_AUTO_DISTILL_LLM_HASH_CAP', 'SKILL_DISTILLER',
];

function seedCapsules(assetsDir, n, tag) {
  fs.mkdirSync(assetsDir, { recursive: true });
  const capsules = [];
  for (let i = 0; i < n; i++) {
    capsules.push({
      type: 'Capsule', id: `capsule_${tag || 'seed'}_${i}`, gene: 'gene_seed_source',
      trigger: ['seed_a', 'seed_b'], summary: `seed ${i}`,
      outcome: { status: 'success', score: 0.9 }, blast_radius: { files: 1, lines: 3 },
    });
  }
  fs.writeFileSync(path.join(assetsDir, 'capsules.json'), JSON.stringify({ version: 1, capsules }));
  fs.writeFileSync(path.join(assetsDir, 'genes.json'), JSON.stringify({ version: 1, genes: [] }));
  fs.writeFileSync(path.join(assetsDir, 'events.jsonl'), '');
}

function freshModule() {
  for (const m of ['paths', 'skillDistiller', 'assetStore', 'autoDistillLlm', 'execBridge', 'policyCheck']) {
    try { delete require.cache[require.resolve('../src/gep/' + m)]; } catch (_) {}
  }
  return require('../src/gep/autoDistillLlm');
}

beforeEach(() => {
  prevEnv = {}; for (const k of ENV) prevEnv[k] = process.env[k];
  tmpRoot = fs.mkdtempSync(path.join(os.tmpdir(), 'p3sm-'));
  process.env.EVOLVER_REPO_ROOT = tmpRoot;
  process.env.DISTILLER_MIN_CAPSULES = '3';
  process.env.A2A_HUB_URL = '';
  process.env.SKILL_AUTO_PUBLISH = '0';
  seedCapsules(path.join(tmpRoot, '.evolver', 'gep'), 5);
});
afterEach(() => {
  for (const k of ENV) { if (prevEnv[k] === undefined) delete process.env[k]; else process.env[k] = prevEnv[k]; }
  try { fs.rmSync(tmpRoot, { recursive: true, force: true }); } catch (_) {}
});

function genesOnDisk() {
  try { return JSON.parse(fs.readFileSync(path.join(tmpRoot, '.evolver', 'gep', 'genes.json'), 'utf8')).genes || []; }
  catch (_) { return []; }
}
function byHash() {
  try { return ((JSON.parse(fs.readFileSync(path.join(tmpRoot, 'memory', 'distiller_state.json'), 'utf8')).p3_llm) || {}).by_hash || {}; }
  catch (_) { return {}; }
}

describe('autoDistillLlm — pure helpers', () => {
  const m = require('../src/gep/autoDistillLlm');
  it('normalizeValidation drops blocked+heavy, keeps light', () => {
    const r = m.normalizeValidation({ validation: ['node --version', 'node -e "x"', 'node scripts/validate-suite.js', 'node --test test/a.test.js'] });
    assert.deepEqual(r.gene.validation, ['node --version']);
  });
  it('jaccardDuplicate flags near-dup, ignores same id', () => {
    assert.equal(m.jaccardDuplicate({ id: 'n', signals_match: ['a', 'b', 'c', 'd'] }, [{ id: 'o', signals_match: ['a', 'b', 'c', 'e'] }], 0.5), 'o');
    assert.equal(m.jaccardDuplicate({ id: 'n', signals_match: ['x'] }, [{ id: 'o', signals_match: ['a'] }], 0.8), null);
  });
});

describe('autoDistillLlm — _p3Decide state machine (pure)', () => {
  const m = require('../src/gep/autoDistillLlm');
  const now = 1_000_000_000;
  it('C8: enforced rec -> idempotent skip in both modes', () => {
    const rec = { enforced_at: 'x' };
    assert.equal(m._p3Decide('shadow', rec, now), 'enforced_idempotent_skip');
    assert.equal(m._p3Decide('enforce', rec, now), 'enforced_idempotent_skip');
  });
  it('C1+C2: shadowed rec -> shadow skips, enforce spawns', () => {
    const rec = { shadowed_at: 'x', enforced_at: null, failed_attempts: 0 };
    assert.equal(m._p3Decide('shadow', rec, now), 'shadow_idempotent_skip'); // C1
    assert.equal(m._p3Decide('enforce', rec, now), 'spawn');                 // C2/C3
  });
  it('fresh rec -> spawn', () => {
    assert.equal(m._p3Decide('enforce', null, now), 'spawn');
    assert.equal(m._p3Decide('shadow', null, now), 'spawn');
  });
  it('C4: failed within cooldown -> p3_cooldown; exhausted -> failed_exhausted', () => {
    process.env.EVOLVER_AUTO_DISTILL_LLM_COOLDOWN_MS = '100000';
    const mm = freshModule();
    const cooling = { failed_attempts: 1, last_attempt_at: new Date(now - 50).toISOString() };
    assert.equal(mm._p3Decide('enforce', cooling, now), 'p3_cooldown');
    const exhausted = { failed_attempts: 3, last_attempt_at: new Date(now - 1e9).toISOString() };
    assert.equal(mm._p3Decide('enforce', exhausted, now), 'failed_exhausted');
  });
  it('C5: failed but cooled (under max) -> spawn', () => {
    process.env.EVOLVER_AUTO_DISTILL_LLM_COOLDOWN_MS = '100';
    const mm = freshModule();
    const cooled = { failed_attempts: 1, last_attempt_at: new Date(now - 100000).toISOString() };
    assert.equal(mm._p3Decide('enforce', cooled, now), 'spawn');
  });
});

describe('autoDistillLlm — flow (fake claude seam)', () => {
  it('off mode: never spawns', async () => {
    const m = freshModule(); const c = { n: 0 };
    const r = await m.autoDistillLlm({ mode: 'off', spawnFn: fakeSpawn('good_gene', c) });
    assert.equal(r.reason, 'disabled'); assert.equal(c.n, 0);
  });

  it('enforce fresh: spawn -> green -> upsert + by_hash.enforced_at', async () => {
    const m = freshModule();
    const r = await m.autoDistillLlm({ mode: 'enforce', spawnFn: fakeSpawn('good_gene') });
    assert.equal(r.ok, true, 'reason=' + r.reason);
    assert.ok(genesOnDisk().some((g) => g.id === r.gene.id));
    const recs = Object.values(byHash());
    assert.ok(recs.length === 1 && recs[0].enforced_at, 'enforced_at recorded');
  });

  it('C8: enforce twice same data -> 2nd is enforced_idempotent_skip, no 2nd spawn', async () => {
    const m = freshModule();
    await m.autoDistillLlm({ mode: 'enforce', spawnFn: fakeSpawn('good_gene') });
    const c = { n: 0 };
    const r2 = await m.autoDistillLlm({ mode: 'enforce', spawnFn: fakeSpawn('good_gene', c) });
    assert.equal(r2.reason, 'enforced_idempotent_skip');
    assert.equal(c.n, 0, 'no spawn on already-enforced data');
  });

  it('C1: shadow twice same data -> 2nd is shadow_idempotent_skip', async () => {
    const m = freshModule();
    await m.autoDistillLlm({ mode: 'shadow', spawnFn: fakeSpawn('good_gene') });
    const c = { n: 0 };
    const r2 = await m.autoDistillLlm({ mode: 'shadow', spawnFn: fakeSpawn('good_gene', c) });
    assert.equal(r2.reason, 'shadow_idempotent_skip');
    assert.equal(c.n, 0);
  });

  it('C2 (the headline regression): shadow then enforce SAME data -> enforce upserts', async () => {
    const m = freshModule();
    const s = await m.autoDistillLlm({ mode: 'shadow', spawnFn: fakeSpawn('good_gene') });
    assert.equal(s.reason, 'shadow_logged');
    assert.equal(genesOnDisk().length, 0, 'shadow did not upsert');
    const e = await m.autoDistillLlm({ mode: 'enforce', spawnFn: fakeSpawn('good_gene') });
    assert.equal(e.ok, true, 'enforce after shadow on same data must upsert (reason=' + e.reason + ')');
    assert.ok(genesOnDisk().some((g) => g.id === e.gene.id));
  });

  it('deterministic failure bumps failed_attempts; transient does not', async () => {
    const m = freshModule();
    process.env.EVOLVER_AUTO_DISTILL_LLM_COOLDOWN_MS = '0'; // no cooldown so we can re-spawn
    const r1 = await m.autoDistillLlm({ mode: 'enforce', spawnFn: fakeSpawn('no_gene') });
    assert.equal(r1.reason, 'no_gene_in_response');
    assert.equal(Object.values(byHash())[0].failed_attempts, 1, 'deterministic failure bumps count');
  });

  it('C4: failure then immediate retry within cooldown -> p3_cooldown (no spawn)', async () => {
    const m = freshModule();
    process.env.EVOLVER_AUTO_DISTILL_LLM_COOLDOWN_MS = '600000';
    await m.autoDistillLlm({ mode: 'enforce', spawnFn: fakeSpawn('no_gene') });
    const c = { n: 0 };
    const r2 = await m.autoDistillLlm({ mode: 'enforce', spawnFn: fakeSpawn('good_gene', c) });
    assert.equal(r2.reason, 'p3_cooldown');
    assert.equal(c.n, 0, 'cooldown blocks immediate re-spawn on same failed data');
  });

  it('nonzero exit -> rejected, no upsert, request cleaned', async () => {
    const m = freshModule();
    const r = await m.autoDistillLlm({ mode: 'enforce', spawnFn: fakeSpawn('nonzero_exit_with_gene') });
    assert.equal(r.reason, 'claude_nonzero_exit');
    assert.equal(genesOnDisk().length, 0);
    const sd = require('../src/gep/skillDistiller');
    assert.ok(!fs.existsSync(sd.distillRequestPath()), 'transient cleans its own request');
  });

  it('C6: a FOREIGN fresh request -> inflight_request, no spawn, request preserved', async () => {
    const m = freshModule();
    const sd = require('../src/gep/skillDistiller');
    const rp = sd.distillRequestPath();
    fs.mkdirSync(path.dirname(rp), { recursive: true });
    fs.writeFileSync(rp, JSON.stringify({ type: 'DistillationRequest', owner: 'manual', created_at: new Date().toISOString(), data_hash: 'someother' }));
    const c = { n: 0 };
    const r = await m.autoDistillLlm({ mode: 'enforce', spawnFn: fakeSpawn('good_gene', c) });
    assert.equal(r.reason, 'inflight_request');
    assert.equal(c.n, 0);
    assert.ok(fs.existsSync(rp), 'foreign request preserved');
  });

  it('insufficient data -> insufficient_data', async () => {
    process.env.DISTILLER_MIN_CAPSULES = '999';
    const m = freshModule();
    const r = await m.autoDistillLlm({ mode: 'enforce', spawnFn: fakeSpawn('good_gene') });
    assert.equal(r.reason, 'insufficient_data');
  });

  it('SKILL_DISTILLER=false -> not_ready', async () => {
    process.env.SKILL_DISTILLER = 'false';
    const m = freshModule();
    const r = await m.autoDistillLlm({ mode: 'enforce', spawnFn: fakeSpawn('good_gene') });
    assert.equal(r.reason, 'not_ready');
  });

  it('unbounded-state guard: by_hash capped at HASH_CAP, enforced survives', async () => {
    process.env.EVOLVER_AUTO_DISTILL_LLM_HASH_CAP = '2';
    process.env.EVOLVER_AUTO_DISTILL_LLM_COOLDOWN_MS = '0';
    const m = freshModule();
    // drive several distinct hashes by changing the capsule set each round
    for (let round = 0; round < 4; round++) {
      seedCapsules(path.join(tmpRoot, '.evolver', 'gep'), 5 + round, 'r' + round);
      await m.autoDistillLlm({ mode: 'shadow', spawnFn: fakeSpawn('good_gene') });
    }
    assert.ok(Object.keys(byHash()).length <= 2, 'by_hash capped: ' + Object.keys(byHash()).length);
  });
});
