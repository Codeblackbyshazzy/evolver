// Tests for getNodeId resolution chain:
//   1. A2A_NODE_ID env (with format validation, warn on malformed but still use)
//   2. Persisted ~/.evomap/node_id (accepts 12-32 hex)
//   3. Project-local .evomap_node_id fallback
//   4. Mailbox node_id fallback, copied to the canonical persisted file
//   5. Random fallback (12 hex), persisted on first call
//
// Regression targets:
//   - NODE_ID_RE must accept 16-hex hub-issued IDs (was stuck at /{12}$/)
//   - When persisted file has valid 16-hex ID, do NOT overwrite with fallback
//   - Two installs with identical device fingerprint must produce different
//     IDs on first run (clone-collision fix, #71)
const { describe, it, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

function freshRequire(id) {
  delete require.cache[require.resolve(id)];
  return require(id);
}

function waitForFile(file, timeoutMs = 5_000) {
  const startedAt = Date.now();
  return new Promise((resolve, reject) => {
    function poll() {
      if (fs.existsSync(file)) return resolve();
      if (Date.now() - startedAt > timeoutMs) {
        return reject(new Error(`timed out waiting for ${path.basename(file)}`));
      }
      setTimeout(poll, 10);
    }
    poll();
  });
}

function waitForChild(child) {
  return new Promise((resolve, reject) => {
    child.once('error', reject);
    child.once('exit', (code, signal) => resolve({ code, signal }));
  });
}

function withTempHome(run) {
  const tmpHome = fs.mkdtempSync(path.join(os.tmpdir(), 'evomap-nodeid-'));
  const originalHome = process.env.HOME;
  const originalUserProfile = process.env.USERPROFILE;
  const originalEvolverHome = process.env.EVOLVER_HOME;
  process.env.HOME = tmpHome;
  process.env.USERPROFILE = tmpHome;
  process.env.EVOLVER_HOME = path.join(tmpHome, '.evomap');

  // Also hide the project-local .evomap_node_id fallback so a pre-existing
  // file from a real run (where evolver was exercised in this checkout)
  // does not short-circuit _loadPersistedNodeId() and bypass the tmpHome
  // persist path under test. We stash it and restore in finally.
  const LOCAL_NODE_ID_FILE = path.resolve(__dirname, '..', '.evomap_node_id');
  const LOCAL_STASH = LOCAL_NODE_ID_FILE + '.test-stash';
  let stashed = false;
  try {
    if (fs.existsSync(LOCAL_NODE_ID_FILE)) {
      fs.renameSync(LOCAL_NODE_ID_FILE, LOCAL_STASH);
      stashed = true;
    }
  } catch { /* best effort */ }

  try {
    return run(tmpHome);
  } finally {
    if (originalHome === undefined) delete process.env.HOME;
    else process.env.HOME = originalHome;
    if (originalUserProfile === undefined) delete process.env.USERPROFILE;
    else process.env.USERPROFILE = originalUserProfile;
    if (originalEvolverHome === undefined) delete process.env.EVOLVER_HOME;
    else process.env.EVOLVER_HOME = originalEvolverHome;
    try { fs.rmSync(tmpHome, { recursive: true, force: true }); } catch {}
    if (stashed) {
      try {
        // Clean any spurious node_id written during the test so restore wins.
        if (fs.existsSync(LOCAL_NODE_ID_FILE)) fs.rmSync(LOCAL_NODE_ID_FILE);
        fs.renameSync(LOCAL_STASH, LOCAL_NODE_ID_FILE);
      } catch { /* best effort */ }
    } else {
      // No pre-existing file, but test may have written one via the
      // LOCAL_NODE_ID_FILE fallback. Clean it up so we leave no trace.
      try { if (fs.existsSync(LOCAL_NODE_ID_FILE)) fs.rmSync(LOCAL_NODE_ID_FILE); } catch {}
    }
  }
}

describe('getNodeId resolution', () => {
  let savedEnv;

  beforeEach(() => {
    savedEnv = {
      A2A_NODE_ID: process.env.A2A_NODE_ID,
      AGENT_NAME: process.env.AGENT_NAME,
      EVOMAP_DEVICE_ID: process.env.EVOMAP_DEVICE_ID,
    };
    delete process.env.A2A_NODE_ID;
    delete process.env.AGENT_NAME;
    delete process.env.EVOMAP_DEVICE_ID;
  });

  afterEach(() => {
    for (const k of Object.keys(savedEnv)) {
      if (savedEnv[k] === undefined) delete process.env[k];
      else process.env[k] = savedEnv[k];
    }
  });

  it('returns A2A_NODE_ID env verbatim when format is valid (12 hex)', () => {
    process.env.A2A_NODE_ID = 'node_abcdef012345';
    const { getNodeId } = freshRequire('../src/gep/a2aProtocol');
    assert.equal(getNodeId(), 'node_abcdef012345');
  });

  it('returns A2A_NODE_ID env verbatim when format is valid (16 hex, hub-issued)', () => {
    process.env.A2A_NODE_ID = 'node_71c0a711a894cbf3';
    const { getNodeId } = freshRequire('../src/gep/a2aProtocol');
    assert.equal(getNodeId(), 'node_71c0a711a894cbf3');
  });

  it('accepts A2A_NODE_ID env with odd format but warns (does not crash)', () => {
    process.env.A2A_NODE_ID = 'test-node';
    const warns = [];
    const origWarn = console.warn;
    console.warn = (msg) => warns.push(String(msg));
    try {
      const { getNodeId } = freshRequire('../src/gep/a2aProtocol');
      assert.equal(getNodeId(), 'test-node');
      assert.ok(warns.some((m) => m.includes('unexpected format')), 'should warn');
    } finally {
      console.warn = origWarn;
    }
  });

  it('loads persisted 12-hex node_id from ~/.evomap/node_id', () => {
    withTempHome((tmpHome) => {
      const dir = path.join(tmpHome, '.evomap');
      fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
      fs.writeFileSync(path.join(dir, 'node_id'), 'node_112233445566', 'utf8');
      const { getNodeId } = freshRequire('../src/gep/a2aProtocol');
      assert.equal(getNodeId(), 'node_112233445566');
    });
  });

  it('loads persisted 16-hex node_id from ~/.evomap/node_id (hub-issued format, regression fix)', () => {
    withTempHome((tmpHome) => {
      const dir = path.join(tmpHome, '.evomap');
      fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
      fs.writeFileSync(path.join(dir, 'node_id'), 'node_71c0a711a894cbf3', 'utf8');
      const { getNodeId } = freshRequire('../src/gep/a2aProtocol');
      assert.equal(getNodeId(), 'node_71c0a711a894cbf3',
        'Must not discard valid 16-hex node_id and regenerate a 12-hex fallback');
    });
  });

  it('reuses a valid mailbox-only node_id and persists it for normal execution', () => {
    withTempHome((tmpHome) => {
      const evomapDir = path.join(tmpHome, '.evomap');
      const mailboxDir = path.join(evomapDir, 'mailbox');
      fs.mkdirSync(mailboxDir, { recursive: true });
      fs.writeFileSync(path.join(mailboxDir, 'state.json'), JSON.stringify({
        node_id: 'node_eeeeeeeeeeee',
        node_secret: 'secret-for-existing-node',
      }), 'utf8');

      const { getNodeId } = freshRequire('../src/gep/a2aProtocol');
      assert.equal(getNodeId(), 'node_eeeeeeeeeeee');

      const persistedPath = path.join(evomapDir, 'node_id');
      assert.equal(fs.readFileSync(persistedPath, 'utf8'), 'node_eeeeeeeeeeee');
    });
  });

  it('adopts a different canonical winner when mailbox persistence loses the create race', () => {
    withTempHome((tmpHome) => {
      const nodeA = 'node_aaaaaaaaaaaa';
      const nodeB = 'node_bbbbbbbbbbbb';
      const evomapDir = path.join(tmpHome, '.evomap');
      const mailboxDir = path.join(evomapDir, 'mailbox');
      const nodeIdPath = path.join(evomapDir, 'node_id');
      fs.mkdirSync(mailboxDir, { recursive: true });
      fs.writeFileSync(path.join(mailboxDir, 'state.json'), JSON.stringify({ node_id: nodeA }), 'utf8');

      const originalOpenSync = fs.openSync;
      let injectedWinner = false;
      fs.openSync = function (file, flags) {
        if (!injectedWinner && file === nodeIdPath && flags === 'wx') {
          injectedWinner = true;
          const winnerFd = originalOpenSync.call(fs, nodeIdPath, 'wx', 0o600);
          try {
            fs.writeSync(winnerFd, nodeB);
            fs.fsyncSync(winnerFd);
          } finally {
            fs.closeSync(winnerFd);
          }
          const err = new Error('simulated competing node_id winner');
          err.code = 'EEXIST';
          throw err;
        }
        return originalOpenSync.apply(this, arguments);
      };

      try {
        const { getNodeId } = freshRequire('../src/gep/a2aProtocol');
        assert.equal(getNodeId(), nodeB, 'loser must adopt the on-disk winner');
        assert.equal(getNodeId(), nodeB, 'winner must remain cached in-process');
        assert.equal(fs.readFileSync(nodeIdPath, 'utf8'), nodeB);
      } finally {
        fs.openSync = originalOpenSync;
      }
    });
  });

  it('serializes an empty canonical claim window and never writes a loser fallback identity', async () => {
    const tmpHome = fs.mkdtempSync(path.join(os.tmpdir(), 'evomap-nodeid-claim-race-'));
    const evomapDir = path.join(tmpHome, '.evomap');
    const mailboxDir = path.join(evomapDir, 'mailbox');
    const nodeIdPath = path.join(evomapDir, 'node_id');
    const readyFile = path.join(tmpHome, 'claim-ready');
    const attemptedFile = path.join(tmpHome, 'loser-attempted');
    const releaseFile = path.join(tmpHome, 'release-winner');
    const resultFile = path.join(tmpHome, 'loser-result');
    const winnerId = 'node_aaaaaaaaaaaa';
    const loserId = 'node_bbbbbbbbbbbb';
    const lockModule = require.resolve('../src/canonicalIdentityLock');
    const protocolModule = require.resolve('../src/gep/a2aProtocol');
    const localFallback = path.resolve(__dirname, '..', '.evomap_node_id');
    const localFallbackBefore = fs.existsSync(localFallback)
      ? fs.readFileSync(localFallback)
      : null;
    fs.mkdirSync(mailboxDir, { recursive: true });
    fs.writeFileSync(path.join(mailboxDir, 'state.json'), JSON.stringify({ node_id: loserId }), 'utf8');

    const childEnv = { ...process.env, HOME: tmpHome, USERPROFILE: tmpHome, EVOLVER_HOME: evomapDir };
    delete childEnv.A2A_NODE_ID;
    const winnerScript = [
      "const fs = require('fs');",
      `const { acquireCanonicalIdentityLock } = require(${JSON.stringify(lockModule)});`,
      `const release = acquireCanonicalIdentityLock(${JSON.stringify(nodeIdPath)});`,
      `const fd = fs.openSync(${JSON.stringify(nodeIdPath)}, 'wx', 0o600);`,
      `fs.writeFileSync(${JSON.stringify(readyFile)}, '1');`,
      `const deadline = Date.now() + 10_000;`,
      `while (!fs.existsSync(${JSON.stringify(releaseFile)}) && Date.now() < deadline) {`,
      '  Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);',
      '}',
      `if (!fs.existsSync(${JSON.stringify(releaseFile)})) throw new Error('release signal timeout');`,
      `fs.writeFileSync(fd, ${JSON.stringify(winnerId)});`,
      'fs.fsyncSync(fd);',
      'fs.closeSync(fd);',
      'release();',
    ].join('\n');
    const loserScript = [
      "const fs = require('fs');",
      `fs.writeFileSync(${JSON.stringify(attemptedFile)}, '1');`,
      `const { getNodeId } = require(${JSON.stringify(protocolModule)});`,
      `fs.writeFileSync(${JSON.stringify(resultFile)}, getNodeId());`,
    ].join('\n');

    let winner = null;
    let loser = null;
    try {
      winner = spawn(process.execPath, ['-e', winnerScript], { env: childEnv, stdio: 'ignore' });
      const winnerExitPromise = waitForChild(winner);
      await waitForFile(readyFile);
      loser = spawn(process.execPath, ['-e', loserScript], { env: childEnv, stdio: 'ignore' });
      const loserExitPromise = waitForChild(loser);
      await waitForFile(attemptedFile);
      fs.writeFileSync(releaseFile, '1');

      assert.deepEqual(await winnerExitPromise, { code: 0, signal: null });
      assert.deepEqual(await loserExitPromise, { code: 0, signal: null });
      assert.equal(fs.readFileSync(nodeIdPath, 'utf8'), winnerId);
      assert.equal(fs.readFileSync(resultFile, 'utf8'), winnerId, 'loser must adopt the canonical winner');

      const localFallbackAfter = fs.existsSync(localFallback)
        ? fs.readFileSync(localFallback)
        : null;
      assert.deepEqual(localFallbackAfter, localFallbackBefore, 'loser must not write install-local fallback');

      const restart = spawn(process.execPath, ['-e', [
        `const { getNodeId } = require(${JSON.stringify(protocolModule)});`,
        'process.stdout.write(getNodeId());',
      ].join('\n')], { env: childEnv, stdio: ['ignore', 'pipe', 'ignore'] });
      let restartOutput = '';
      restart.stdout.setEncoding('utf8');
      restart.stdout.on('data', (chunk) => { restartOutput += chunk; });
      assert.deepEqual(await waitForChild(restart), { code: 0, signal: null });
      assert.equal(restartOutput, winnerId, 'restart must keep the same single winner');
    } finally {
      if (winner && winner.exitCode === null) winner.kill();
      if (loser && loser.exitCode === null) loser.kill();
      fs.rmSync(tmpHome, { recursive: true, force: true });
    }
  });

  it('ignores a malformed mailbox node_id and generates a fresh identity', () => {
    withTempHome((tmpHome) => {
      const evomapDir = path.join(tmpHome, '.evomap');
      const mailboxDir = path.join(evomapDir, 'mailbox');
      fs.mkdirSync(mailboxDir, { recursive: true });
      fs.writeFileSync(path.join(mailboxDir, 'state.json'), JSON.stringify({
        node_id: 'not-a-valid-id',
        node_secret: 'secret-for-invalid-node',
      }), 'utf8');

      const { getNodeId } = freshRequire('../src/gep/a2aProtocol');
      const id = getNodeId();
      assert.match(id, /^node_[a-f0-9]{12}$/);
      assert.notEqual(id, 'not-a-valid-id');
      assert.equal(fs.readFileSync(path.join(evomapDir, 'node_id'), 'utf8'), id);
    });
  });

  it('rejects obviously malformed persisted value and falls back to a fresh random id', () => {
    withTempHome((tmpHome) => {
      const dir = path.join(tmpHome, '.evomap');
      const nodeIdFile = path.join(dir, 'node_id');
      fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
      fs.writeFileSync(nodeIdFile, 'not-a-valid-id', 'utf8');
      const { getNodeId } = freshRequire('../src/gep/a2aProtocol');
      const id = getNodeId();
      assert.match(id, /^node_[a-f0-9]{12}$/, 'fallback should be 12-hex');
      assert.equal(fs.readFileSync(nodeIdFile, 'utf8'), id, 'stale invalid marker should be repaired under the lock');
      assert.equal(freshRequire('../src/gep/a2aProtocol').getNodeId(), id, 'restart must keep the repaired identity');
    });
  });

  it('clears orphan canonical credentials before claiming a fresh node_id', () => {
    withTempHome((tmpHome) => {
      const dir = path.join(tmpHome, '.evomap');
      const nodeIdFile = path.join(dir, 'node_id');
      fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
      fs.writeFileSync(path.join(dir, 'node_secret'), 'a'.repeat(64), 'utf8');
      fs.writeFileSync(path.join(dir, 'node_secret_version'), '7', 'utf8');
      fs.writeFileSync(path.join(dir, 'node_secret_source'), 'hub_rotate', 'utf8');

      const originalOpenSync = fs.openSync;
      let checkedBeforeClaim = false;
      fs.openSync = function (file, flags) {
        if (file === nodeIdFile && flags === 'wx') {
          checkedBeforeClaim = true;
          assert.equal(fs.existsSync(path.join(dir, 'node_secret')), false);
          assert.equal(fs.existsSync(path.join(dir, 'node_secret_version')), false);
          assert.equal(fs.existsSync(path.join(dir, 'node_secret_source')), false);
        }
        return originalOpenSync.apply(this, arguments);
      };
      try {
        const { getNodeId } = freshRequire('../src/gep/a2aProtocol');
        const id = getNodeId();
        assert.match(id, /^node_[a-f0-9]{12}$/);
        assert.equal(fs.readFileSync(nodeIdFile, 'utf8'), id);
        assert.equal(checkedBeforeClaim, true, 'orphan credentials must be gone before node_id is visible');
      } finally {
        fs.openSync = originalOpenSync;
      }
    });
  });

  it('quarantines orphan canonical credentials when cleanup fails', () => {
    withTempHome((tmpHome) => {
      const dir = path.join(tmpHome, '.evomap');
      const nodeIdFile = path.join(dir, 'node_id');
      const secretFile = path.join(dir, 'node_secret');
      fs.mkdirSync(dir, { recursive: true, mode: 0o700 });
      fs.writeFileSync(secretFile, 'a'.repeat(64), 'utf8');

      const originalUnlinkSync = fs.unlinkSync;
      fs.unlinkSync = function (file) {
        if (file === secretFile) {
          const err = new Error('simulated cleanup failure');
          err.code = 'EACCES';
          throw err;
        }
        return originalUnlinkSync.apply(this, arguments);
      };
      try {
        const a2a = freshRequire('../src/gep/a2aProtocol');
        assert.throws(() => a2a.getNodeId(), (err) => err && err.code === 'NODE_ID_PERSIST_FAILED');
        assert.equal(fs.readFileSync(nodeIdFile, 'utf8'), 'invalid');
        assert.equal(fs.existsSync(secretFile), true);
        assert.deepEqual(a2a.getHubNodeCredentialsReadOnly(), { secret: null, version: null });
      } finally {
        fs.unlinkSync = originalUnlinkSync;
      }
    });
  });

  it('clears and binds an ownerless mailbox tuple before publishing a fresh identity', () => {
    withTempHome((tmpHome) => {
      const dir = path.join(tmpHome, '.evomap');
      const mailboxDir = path.join(dir, 'mailbox');
      const stateFile = path.join(mailboxDir, 'state.json');
      fs.mkdirSync(mailboxDir, { recursive: true, mode: 0o700 });
      fs.writeFileSync(stateFile, JSON.stringify({
        node_secret: 'b'.repeat(64),
        node_secret_version: '9',
        node_secret_source: 'hub_rotate',
      }), 'utf8');

      const a2a = freshRequire('../src/gep/a2aProtocol');
      const id = a2a.getNodeId();
      const state = JSON.parse(fs.readFileSync(stateFile, 'utf8'));
      assert.match(id, /^node_[a-f0-9]{12}$/);
      assert.equal(state.node_id, id);
      assert.equal(state.node_secret, '');
      assert.equal(state.node_secret_version, '');
      assert.equal(state.node_secret_source, '');
      assert.deepEqual(a2a.getHubNodeCredentialsReadOnly(id), { secret: null, version: null });

      const restarted = freshRequire('../src/gep/a2aProtocol');
      assert.equal(restarted.getNodeId(), id);
      assert.deepEqual(restarted.getHubNodeCredentialsReadOnly(id), { secret: null, version: null });
    });
  });

  it('fails closed on canonical lock timeout and never caches a mailbox fallback', () => {
    withTempHome((tmpHome) => {
      const dir = path.join(tmpHome, '.evomap');
      const mailboxDir = path.join(dir, 'mailbox');
      const nodeIdFile = path.join(dir, 'node_id');
      const mailboxId = 'node_bbbbbbbbbbbb';
      fs.mkdirSync(mailboxDir, { recursive: true, mode: 0o700 });
      fs.writeFileSync(path.join(mailboxDir, 'state.json'), JSON.stringify({ node_id: mailboxId }), 'utf8');

      const lock = freshRequire('../src/canonicalIdentityLock');
      lock._setCanonicalIdentityLockTimingForTesting({ waitMs: 1, timeoutMs: 25 });
      const release = lock.acquireCanonicalIdentityLock(nodeIdFile);
      const a2a = freshRequire('../src/gep/a2aProtocol');
      try {
        assert.throws(() => a2a.getNodeId(), (err) => err && err.code === 'CANONICAL_IDENTITY_LOCK_TIMEOUT');
        assert.equal(fs.existsSync(nodeIdFile), false);
      } finally {
        release();
        lock._resetCanonicalIdentityLockTimingForTesting();
      }

      assert.equal(a2a.getNodeId(), mailboxId, 'the timed-out mailbox identity must not have been cached');
      assert.equal(fs.readFileSync(nodeIdFile, 'utf8'), mailboxId);
    });
  });

  it('fallback writes 12-hex node_id to ~/.evomap/node_id and is stable across repeated calls', () => {
    withTempHome((tmpHome) => {
      const mod1 = freshRequire('../src/gep/a2aProtocol');
      const first = mod1.getNodeId();
      assert.match(first, /^node_[a-f0-9]{12}$/);

      const persistedPath = path.join(tmpHome, '.evomap', 'node_id');
      assert.ok(fs.existsSync(persistedPath), 'fallback should persist node_id');
      assert.equal(fs.readFileSync(persistedPath, 'utf8').trim(), first);

      const mod2 = freshRequire('../src/gep/a2aProtocol');
      assert.equal(mod2.getNodeId(), first, 'second process should reuse persisted id');
    });
  });

  it('two installs with identical device fingerprint produce different node ids (clone-collision fix)', () => {
    // Simulate: a container image is built and cloned to two hosts before
    // evolver has ever run. Both hosts inherit the same hostname / MAC /
    // agent name / cwd. The deterministic hash would have collided; the
    // random fallback must not.
    const ids = [];
    for (let i = 0; i < 2; i++) {
      withTempHome(() => {
        process.env.EVOMAP_DEVICE_ID = 'a'.repeat(32);
        process.env.AGENT_NAME = 'default';
        const { getNodeId } = freshRequire('../src/gep/a2aProtocol');
        ids.push(getNodeId());
      });
    }
    assert.match(ids[0], /^node_[a-f0-9]{12}$/);
    assert.match(ids[1], /^node_[a-f0-9]{12}$/);
    assert.notEqual(ids[0], ids[1], 'identical fingerprint must not yield identical node id');
  });
});
