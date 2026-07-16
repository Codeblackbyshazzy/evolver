'use strict';

// Regression coverage for the proxy node_id unification fix (PR #188
// follow-up). In EVOMAP_PROXY=1 deployments, the proxy LifecycleManager
// handles heartbeats while the a2aProtocol heartbeat thread is dormant.
// Prior to this fix the proxy minted its own node_id and stored it ONLY
// in MailboxStore state.json. The legacy `~/.evomap/node_id` file —
// which `_shortNodeIdForStatePath` in src/gep/a2aProtocol.js consults
// when choosing the per-node `force_update_last.<suffix>.json` path —
// stayed empty (or, worse, retained a stale id from a previous
// non-proxy run).
//
// Outcome:
//   - Proxy-only deployments: the state-file suffix collapsed to 'anon'
//     for every node sharing one EVOLVER_HOME, masking upgrade
//     attribution across nodes.
//   - Mixed-mode deployments: the state-file suffix was derived from
//     the legacy file's STALE id while the heartbeat body.node_id used
//     the proxy's CURRENT id — the hub attributed upgrade rows to the
//     wrong node.
//
// The fix unifies the two identities: after a successful hello() the
// LifecycleManager writes its node_id to the same legacy file path the
// a2aProtocol writer uses, so `_shortNodeIdForStatePath` returns a
// suffix derived from the SAME id the heartbeat body declares.
//
// Defense-in-depth: `_shortNodeIdForStatePath` also now requires the
// 8-char slice to match /^[a-f0-9]{8}$/, otherwise it falls through to
// 'anon' rather than emit a suffix that could escape EVOLVER_HOME via
// path traversal (e.g. a corrupted legacy file containing
// `node_../etc`).

const test = require('node:test');
const assert = require('node:assert');
const fs = require('fs');
const os = require('os');
const path = require('path');
const { spawn } = require('child_process');

// hubFetch refuses to route through global.fetch for https://example.test
// unless EVOMAP_HUB_ALLOW_INSECURE=1. Tests below stub global.fetch.
const _origInsecure = process.env.EVOMAP_HUB_ALLOW_INSECURE;
process.env.EVOMAP_HUB_ALLOW_INSECURE = '1';

// A2A_NODE_SECRET is asserted-present by _buildHeaders; pin a dummy
// value so the manager can build the Authorization header.
if (!process.env.A2A_NODE_SECRET) {
  process.env.A2A_NODE_SECRET = 'a'.repeat(64);
}

const { LifecycleManager } = require('../src/proxy/lifecycle/manager');
const a2aProtocol = require('../src/gep/a2aProtocol');
const {
  _shortNodeIdForStatePathForTesting,
  _resetCachedNodeIdForTesting,
  _persistLastUpdateStateForTesting,
  _getLastUpdateStatePathForTesting,
  _resetLastUpdateStateForTesting,
  _resetHubNodeSecretStateForTesting,
} = a2aProtocol._testing;

test.after(() => {
  if (_origInsecure === undefined) delete process.env.EVOMAP_HUB_ALLOW_INSECURE;
  else process.env.EVOMAP_HUB_ALLOW_INSECURE = _origInsecure;
});

// Two valid ids per NODE_ID_RE; both pass the regex so any precedence
// disagreement (store vs legacy) shows up as an id mismatch on the wire.
const STORE_ID = 'node_973fad206a3846f7';
const LEGACY_ID = 'node_abcdef0123456789';

function makeFakeEvomapDir(setupFn) {
  const dir = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-nodeid-unify-'));
  if (setupFn) setupFn(dir);
  return dir;
}

function makeStore(initial = {}) {
  const state = { ...initial };
  return {
    getState: (k) => (state[k] !== undefined ? state[k] : null),
    setState: (k, v) => { state[k] = v; },
    countPending: () => 0,
    writeInbound: () => {},
    writeInboundBatch: () => {},
    _state: state,
  };
}

function silentLogger() {
  return { log: () => {}, warn: () => {}, error: () => {}, info: () => {}, debug: () => {} };
}

function mockFetch(responseFactory) {
  const calls = [];
  const fn = async (url, opts) => {
    calls.push({ url, opts });
    return responseFactory(calls.length, opts);
  };
  fn.calls = calls;
  return fn;
}

function responseFromJson({ status = 200, json = {} } = {}) {
  return {
    ok: status >= 200 && status < 300,
    status,
    headers: { get: () => null },
    json: async () => json,
    text: async () => JSON.stringify(json),
  };
}

// Drive EVOLVER_HOME so paths.getEvomapDir() routes to the fake dir.
// Resetting _cachedNodeId before each case prevents a previous test's
// getNodeId() invocation from leaking the cached id across the require
// cache — otherwise `_shortNodeIdForStatePath` would short-circuit on
// the cache before consulting the legacy file we are testing.
async function withFakeEvomapDir(dir, body) {
  const _origHome = process.env.EVOLVER_HOME;
  const _origFetch = global.fetch;
  process.env.EVOLVER_HOME = dir;
  _resetCachedNodeIdForTesting();
  try {
    return await body();
  } finally {
    if (_origHome === undefined) delete process.env.EVOLVER_HOME;
    else process.env.EVOLVER_HOME = _origHome;
    global.fetch = _origFetch;
    _resetCachedNodeIdForTesting();
    _resetLastUpdateStateForTesting();
    fs.rmSync(dir, { recursive: true, force: true });
  }
}

test('proxy hello on a fresh install (no legacy file, no store) writes the minted id to ~/.evomap/node_id', async () => {
  const dir = makeFakeEvomapDir();
  await withFakeEvomapDir(dir, async () => {
    const legacyFile = path.join(dir, 'node_id');
    assert.ok(!fs.existsSync(legacyFile), 'precondition: legacy file absent');

    global.fetch = mockFetch(() => responseFromJson({
      status: 200,
      json: { payload: { status: 'acknowledged' } },
    }));

    const store = makeStore();
    const mgr = new LifecycleManager({ hubUrl: 'https://example.test', store, logger: silentLogger() });
    const result = await mgr.hello();

    assert.strictEqual(result.ok, true);
    assert.match(result.nodeId, /^node_[a-f0-9]{12}$/, 'minted id has the expected shape');
    assert.ok(fs.existsSync(legacyFile),
      'fix: after hello() the proxy must seed ~/.evomap/node_id so a2aProtocol can derive the same suffix');
    const written = fs.readFileSync(legacyFile, 'utf8').trim();
    assert.strictEqual(written, result.nodeId,
      'legacy file must match the id the proxy minted and sent on the wire');
    assert.strictEqual(store.getState('node_id'), result.nodeId,
      'store must also be primed with the minted id');
  });
});

test('proxy hello when ~/.evomap/node_id already has a valid legacy id reuses it AND keeps the file intact', async () => {
  const dir = makeFakeEvomapDir((evomap) => {
    fs.writeFileSync(path.join(evomap, 'node_id'), LEGACY_ID, { mode: 0o600 });
  });
  await withFakeEvomapDir(dir, async () => {
    const legacyFile = path.join(dir, 'node_id');

    let observedSenderId = null;
    global.fetch = mockFetch((_n, opts) => {
      try { observedSenderId = JSON.parse(opts.body).sender_id; } catch { /* ignore */ }
      return responseFromJson({ status: 200, json: { payload: { status: 'acknowledged' } } });
    });

    const store = makeStore(); // store is empty -> legacy fallback applies
    const mgr = new LifecycleManager({ hubUrl: 'https://example.test', store, logger: silentLogger() });
    const result = await mgr.hello();

    assert.strictEqual(result.ok, true);
    assert.strictEqual(result.nodeId, LEGACY_ID,
      'hello must reuse the legacy id (existing fallback behaviour)');
    assert.strictEqual(observedSenderId, LEGACY_ID,
      'sender_id on the wire must match the legacy id');
    assert.strictEqual(fs.readFileSync(legacyFile, 'utf8').trim(), LEGACY_ID,
      'legacy file must be untouched on the idempotent path');
  });
});

test('proxy hello in MIXED MODE (store has its own id, legacy file holds a DIFFERENT id) overwrites the legacy file with the store id', async () => {
  // This is the bug scenario: a previous non-proxy run left LEGACY_ID in
  // ~/.evomap/node_id, then the user wiped state.json and the proxy
  // minted STORE_ID. Without the fix, the proxy heartbeats with
  // body.node_id=STORE_ID while `_shortNodeIdForStatePath` would prefer
  // the in-process _cachedNodeId (or fall through to LEGACY_ID via the
  // legacy file) — the upgrade row is attributed to LEGACY_ID, not
  // STORE_ID.
  const dir = makeFakeEvomapDir((evomap) => {
    fs.writeFileSync(path.join(evomap, 'node_id'), LEGACY_ID, { mode: 0o600 });
  });
  await withFakeEvomapDir(dir, async () => {
    const legacyFile = path.join(dir, 'node_id');

    let observedSenderId = null;
    global.fetch = mockFetch((_n, opts) => {
      try { observedSenderId = JSON.parse(opts.body).sender_id; } catch { /* ignore */ }
      return responseFromJson({ status: 200, json: { payload: { status: 'acknowledged' } } });
    });

    const store = makeStore({ node_id: STORE_ID });
    const mgr = new LifecycleManager({ hubUrl: 'https://example.test', store, logger: silentLogger() });
    const result = await mgr.hello();

    assert.strictEqual(result.nodeId, STORE_ID, 'store wins over legacy (existing precedence)');
    assert.strictEqual(observedSenderId, STORE_ID, 'wire id matches store');
    // The post-fix assertion: legacy file must now equal STORE_ID.
    assert.strictEqual(fs.readFileSync(legacyFile, 'utf8').trim(), STORE_ID,
      'fix: legacy file must be overwritten with the store id to avoid suffix drift');
  });
});

test('proxy store-wins transition clears canonical credentials before node A becomes node B', async () => {
  const credentialFiles = {
    node_secret: 'a'.repeat(64),
    node_secret_version: '7',
    node_secret_source: 'hub_rotate',
    node_secret_env_suppressed: 'sha256:' + 'b'.repeat(64),
  };
  const dir = makeFakeEvomapDir((evomap) => {
    fs.writeFileSync(path.join(evomap, 'node_id'), LEGACY_ID, { mode: 0o600 });
    for (const [name, value] of Object.entries(credentialFiles)) {
      fs.writeFileSync(path.join(evomap, name), value, { mode: 0o600 });
    }
  });
  await withFakeEvomapDir(dir, async () => {
    const savedEnv = {
      A2A_NODE_SECRET: process.env.A2A_NODE_SECRET,
      EVOMAP_NODE_SECRET: process.env.EVOMAP_NODE_SECRET,
      A2A_NODE_SECRET_VERSION: process.env.A2A_NODE_SECRET_VERSION,
      EVOMAP_NODE_SECRET_VERSION: process.env.EVOMAP_NODE_SECRET_VERSION,
      A2A_HUB_TOKEN: process.env.A2A_HUB_TOKEN,
      A2A_HUB_URL: process.env.A2A_HUB_URL,
    };
    try {
      for (const key of Object.keys(savedEnv)) delete process.env[key];
      process.env.A2A_HUB_URL = 'https://example.test';
      _resetHubNodeSecretStateForTesting();

      const store = makeStore({ node_id: STORE_ID });
      new LifecycleManager({ hubUrl: 'https://example.test', store, logger: silentLogger() });

      assert.strictEqual(fs.readFileSync(path.join(dir, 'node_id'), 'utf8'), STORE_ID);
      for (const name of Object.keys(credentialFiles)) {
        assert.strictEqual(fs.existsSync(path.join(dir, name)), false, `${name} must not cross the identity transition`);
      }
      assert.deepStrictEqual(a2aProtocol.getHubNodeCredentialsReadOnly(STORE_ID), {
        secret: null,
        version: null,
      });

      let observedHeartbeat = null;
      global.fetch = mockFetch((_n, opts) => {
        observedHeartbeat = { headers: opts.headers, body: JSON.parse(opts.body) };
        return responseFromJson({ status: 200, json: { status: 'ok' } });
      });
      _resetCachedNodeIdForTesting();
      const result = await a2aProtocol.sendHeartbeat();

      assert.strictEqual(result.ok, true);
      assert.strictEqual(observedHeartbeat.body.node_id, STORE_ID);
      assert.strictEqual(observedHeartbeat.headers.Authorization, undefined);
      assert.strictEqual(observedHeartbeat.headers['X-EvoMap-Node-Secret-Version'], undefined);
      assert.strictEqual(observedHeartbeat.body.node_secret_version, undefined);
    } finally {
      _resetHubNodeSecretStateForTesting();
      for (const [key, value] of Object.entries(savedEnv)) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  });
});

test('proxy store-wins clears orphan credentials when canonical owner is missing or invalid', async (t) => {
  for (const scenario of [
    { name: 'missing node_id', nodeId: null },
    { name: 'invalid node_id', nodeId: 'corrupt-node-id' },
  ]) {
    await t.test(scenario.name, async () => {
      const credentialFiles = {
        node_secret: 'a'.repeat(64),
        node_secret_version: '99',
        node_secret_source: 'hub_rotate',
        node_secret_env_suppressed: 'sha256:' + 'b'.repeat(64),
      };
      const dir = makeFakeEvomapDir((evomap) => {
        if (scenario.nodeId) {
          fs.writeFileSync(path.join(evomap, 'node_id'), scenario.nodeId, { mode: 0o600 });
        }
        for (const [name, value] of Object.entries(credentialFiles)) {
          fs.writeFileSync(path.join(evomap, name), value, { mode: 0o600 });
        }
      });
      await withFakeEvomapDir(dir, async () => {
        const savedEnv = {
          A2A_NODE_SECRET: process.env.A2A_NODE_SECRET,
          EVOMAP_NODE_SECRET: process.env.EVOMAP_NODE_SECRET,
          A2A_NODE_SECRET_VERSION: process.env.A2A_NODE_SECRET_VERSION,
          EVOMAP_NODE_SECRET_VERSION: process.env.EVOMAP_NODE_SECRET_VERSION,
          A2A_HUB_TOKEN: process.env.A2A_HUB_TOKEN,
          A2A_HUB_URL: process.env.A2A_HUB_URL,
        };
        try {
          for (const key of Object.keys(savedEnv)) delete process.env[key];
          process.env.A2A_HUB_URL = 'https://example.test';
          _resetHubNodeSecretStateForTesting();

          const store = makeStore({ node_id: STORE_ID });
          new LifecycleManager({ hubUrl: 'https://example.test', store, logger: silentLogger() });

          assert.strictEqual(fs.readFileSync(path.join(dir, 'node_id'), 'utf8'), STORE_ID);
          for (const name of Object.keys(credentialFiles)) {
            assert.strictEqual(fs.existsSync(path.join(dir, name)), false, `${name} must be removed`);
          }
          assert.deepStrictEqual(a2aProtocol.getHubNodeCredentialsReadOnly(STORE_ID), {
            secret: null,
            version: null,
          });

          let observedHeartbeat = null;
          global.fetch = mockFetch((_n, opts) => {
            observedHeartbeat = { headers: opts.headers, body: JSON.parse(opts.body) };
            return responseFromJson({ status: 200, json: { status: 'ok' } });
          });
          _resetCachedNodeIdForTesting();
          const result = await a2aProtocol.sendHeartbeat();

          assert.strictEqual(result.ok, true);
          assert.strictEqual(observedHeartbeat.body.node_id, STORE_ID);
          assert.strictEqual(observedHeartbeat.headers.Authorization, undefined);
          assert.strictEqual(observedHeartbeat.headers['X-EvoMap-Node-Secret-Version'], undefined);
          assert.strictEqual(observedHeartbeat.body.node_secret_version, undefined);
        } finally {
          _resetHubNodeSecretStateForTesting();
          for (const [key, value] of Object.entries(savedEnv)) {
            if (value === undefined) delete process.env[key];
            else process.env[key] = value;
          }
        }
      });
    });
  }
});

test('proxy orphan cleanup failure never restores an ownerless secret as node B', async () => {
  const orphanSecret = 'a'.repeat(64);
  const dir = makeFakeEvomapDir((evomap) => {
    fs.writeFileSync(path.join(evomap, 'node_secret'), orphanSecret, { mode: 0o600 });
    fs.writeFileSync(path.join(evomap, 'node_secret_version'), '99', { mode: 0o600 });
    fs.writeFileSync(path.join(evomap, 'node_secret_source'), 'hub_rotate', { mode: 0o600 });
  });
  await withFakeEvomapDir(dir, async () => {
    const savedEnv = {
      A2A_NODE_SECRET: process.env.A2A_NODE_SECRET,
      EVOMAP_NODE_SECRET: process.env.EVOMAP_NODE_SECRET,
      A2A_NODE_SECRET_VERSION: process.env.A2A_NODE_SECRET_VERSION,
      EVOMAP_NODE_SECRET_VERSION: process.env.EVOMAP_NODE_SECRET_VERSION,
      A2A_HUB_TOKEN: process.env.A2A_HUB_TOKEN,
    };
    for (const key of Object.keys(savedEnv)) delete process.env[key];
    const secretFile = path.join(dir, 'node_secret');
    const originalUnlinkSync = fs.unlinkSync;
    fs.unlinkSync = function (file) {
      if (file === secretFile) {
        const err = new Error('simulated orphan credential cleanup failure');
        err.code = 'EACCES';
        throw err;
      }
      return originalUnlinkSync.apply(this, arguments);
    };
    try {
      try {
        const store = makeStore({ node_id: STORE_ID });
        new LifecycleManager({ hubUrl: 'https://example.test', store, logger: silentLogger() });
      } finally {
        fs.unlinkSync = originalUnlinkSync;
      }

      assert.notStrictEqual(fs.readFileSync(path.join(dir, 'node_id'), 'utf8'), STORE_ID);
      assert.strictEqual(fs.readFileSync(secretFile, 'utf8'), orphanSecret);
      assert.deepStrictEqual(a2aProtocol.getHubNodeCredentialsReadOnly(STORE_ID), {
        secret: null,
        version: null,
      });
    } finally {
      fs.unlinkSync = originalUnlinkSync;
      for (const [key, value] of Object.entries(savedEnv)) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  });
});

test('proxy same-id persistence preserves the complete canonical credential tuple', async () => {
  const credentialFiles = {
    node_secret: 'c'.repeat(64),
    node_secret_version: '11',
    node_secret_source: 'hub_rotate',
    node_secret_env_suppressed: 'sha256:' + 'd'.repeat(64),
  };
  const dir = makeFakeEvomapDir((evomap) => {
    fs.writeFileSync(path.join(evomap, 'node_id'), STORE_ID, { mode: 0o600 });
    for (const [name, value] of Object.entries(credentialFiles)) {
      fs.writeFileSync(path.join(evomap, name), value, { mode: 0o600 });
    }
  });
  await withFakeEvomapDir(dir, async () => {
    const store = makeStore({ node_id: STORE_ID });
    new LifecycleManager({ hubUrl: 'https://example.test', store, logger: silentLogger() });

    assert.strictEqual(fs.readFileSync(path.join(dir, 'node_id'), 'utf8'), STORE_ID);
    for (const [name, value] of Object.entries(credentialFiles)) {
      assert.strictEqual(fs.readFileSync(path.join(dir, name), 'utf8'), value);
    }
  });
});

test('proxy transition restores the exact node A snapshot when the post-switch credential clear fails', async () => {
  const originalCredentials = {
    node_secret: 'e'.repeat(64),
    node_secret_version: '13',
    node_secret_source: 'hub_rotate',
  };
  const dir = makeFakeEvomapDir((evomap) => {
    fs.writeFileSync(path.join(evomap, 'node_id'), LEGACY_ID, { mode: 0o600 });
    for (const [name, value] of Object.entries(originalCredentials)) {
      fs.writeFileSync(path.join(evomap, name), value, { mode: 0o600 });
    }
  });
  await withFakeEvomapDir(dir, async () => {
    const nodeIdFile = path.join(dir, 'node_id');
    const secretFile = path.join(dir, 'node_secret');
    const suppressionFile = path.join(dir, 'node_secret_env_suppressed');
    const racingSecret = 'f'.repeat(64);
    const originalRenameSync = fs.renameSync;
    const originalUnlinkSync = fs.unlinkSync;
    let injectedAfterSwitch = false;
    let rejectedPostSwitchClear = false;

    fs.renameSync = function (source, target) {
      const result = originalRenameSync.apply(this, arguments);
      if (
        !injectedAfterSwitch &&
        target === nodeIdFile &&
        fs.readFileSync(nodeIdFile, 'utf8') === STORE_ID
      ) {
        injectedAfterSwitch = true;
        fs.writeFileSync(secretFile, racingSecret, { mode: 0o600 });
        fs.writeFileSync(suppressionFile, 'sha256:' + 'a'.repeat(64), { mode: 0o600 });
      }
      return result;
    };
    fs.unlinkSync = function (file) {
      if (
        injectedAfterSwitch &&
        !rejectedPostSwitchClear &&
        file === secretFile &&
        fs.readFileSync(secretFile, 'utf8') === racingSecret
      ) {
        rejectedPostSwitchClear = true;
        const err = new Error('simulated post-switch credential clear failure');
        err.code = 'EACCES';
        throw err;
      }
      return originalUnlinkSync.apply(this, arguments);
    };

    try {
      const store = makeStore({ node_id: STORE_ID });
      new LifecycleManager({ hubUrl: 'https://example.test', store, logger: silentLogger() });
    } finally {
      fs.renameSync = originalRenameSync;
      fs.unlinkSync = originalUnlinkSync;
    }

    assert.strictEqual(injectedAfterSwitch, true, 'test must inject a write after node B is committed');
    assert.strictEqual(rejectedPostSwitchClear, true, 'test must reject the second credential clear');
    assert.strictEqual(fs.readFileSync(nodeIdFile, 'utf8'), LEGACY_ID);
    for (const [name, value] of Object.entries(originalCredentials)) {
      assert.strictEqual(fs.readFileSync(path.join(dir, name), 'utf8'), value);
    }
    assert.strictEqual(fs.existsSync(suppressionFile), false, 'snapshot-absent sibling must stay absent');
  });
});

test('canonical tuple lock makes an old node A writer finish before proxy switches to node B', async () => {
  const rotatedA = 'c'.repeat(64);
  const dir = makeFakeEvomapDir((evomap) => {
    fs.writeFileSync(path.join(evomap, 'node_id'), LEGACY_ID, { mode: 0o600 });
  });
  await withFakeEvomapDir(dir, async () => {
    const savedEnv = {
      A2A_NODE_ID: process.env.A2A_NODE_ID,
      A2A_NODE_SECRET: process.env.A2A_NODE_SECRET,
      EVOMAP_NODE_SECRET: process.env.EVOMAP_NODE_SECRET,
      A2A_NODE_SECRET_VERSION: process.env.A2A_NODE_SECRET_VERSION,
      EVOMAP_NODE_SECRET_VERSION: process.env.EVOMAP_NODE_SECRET_VERSION,
      A2A_HUB_TOKEN: process.env.A2A_HUB_TOKEN,
      A2A_HUB_URL: process.env.A2A_HUB_URL,
    };
    const attemptedFile = path.join(dir, 'manager-attempted');
    const completedFile = path.join(dir, 'manager-completed');
    const managerPath = require.resolve('../src/proxy/lifecycle/manager');
    const secretFile = path.join(dir, 'node_secret');
    const originalRenameSync = fs.renameSync;
    let child = null;
    let childCompletedBeforeRename = false;
    try {
      for (const key of Object.keys(savedEnv)) delete process.env[key];
      process.env.A2A_NODE_ID = LEGACY_ID;
      process.env.A2A_NODE_SECRET = 'a'.repeat(64);
      process.env.A2A_NODE_SECRET_VERSION = '7';
      process.env.A2A_HUB_URL = 'https://example.test';
      _resetHubNodeSecretStateForTesting();
      _resetCachedNodeIdForTesting();
      global.fetch = async () => responseFromJson({
        status: 200,
        json: {
          payload: {
            status: 'acknowledged',
            node_secret: rotatedA,
            node_secret_version: 9,
            your_node_id: LEGACY_ID,
          },
        },
      });

      fs.renameSync = function (source, target) {
        if (!child && target === secretFile) {
          const childScript = [
            "const fs = require('fs');",
            `const { LifecycleManager } = require(${JSON.stringify(managerPath)});`,
            `fs.writeFileSync(${JSON.stringify(attemptedFile)}, '1');`,
            `const state = { node_id: ${JSON.stringify(STORE_ID)} };`,
            'const store = {',
            '  getState: (key) => state[key] === undefined ? null : state[key],',
            '  setState: (key, value) => { state[key] = value; },',
            '  countPending: () => 0, writeInbound: () => {}, writeInboundBatch: () => {},',
            '};',
            'const logger = { log: () => {}, warn: () => {}, error: () => {}, info: () => {}, debug: () => {} };',
            "new LifecycleManager({ hubUrl: 'https://example.test', store, logger });",
            `fs.writeFileSync(${JSON.stringify(completedFile)}, '1');`,
          ].join('\n');
          const childEnv = { ...process.env, EVOLVER_HOME: dir };
          delete childEnv.A2A_NODE_ID;
          delete childEnv.A2A_NODE_SECRET;
          delete childEnv.EVOMAP_NODE_SECRET;
          delete childEnv.A2A_NODE_SECRET_VERSION;
          delete childEnv.EVOMAP_NODE_SECRET_VERSION;
          delete childEnv.A2A_HUB_TOKEN;
          child = spawn(process.execPath, ['-e', childScript], {
            env: childEnv,
            stdio: 'ignore',
          });
          const attemptDeadline = Date.now() + 5_000;
          while (!fs.existsSync(attemptedFile) && Date.now() < attemptDeadline) {
            Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
          }
          assert.strictEqual(fs.existsSync(attemptedFile), true, 'child must reach manager construction');
          const blockedDeadline = Date.now() + 500;
          while (!fs.existsSync(completedFile) && Date.now() < blockedDeadline) {
            Atomics.wait(new Int32Array(new SharedArrayBuffer(4)), 0, 0, 10);
          }
          childCompletedBeforeRename = fs.existsSync(completedFile);
        }
        return originalRenameSync.apply(this, arguments);
      };

      const hello = await a2aProtocol.sendHelloToHub();
      assert.strictEqual(hello.ok, true);
    } finally {
      fs.renameSync = originalRenameSync;
    }

    assert.ok(child, 'test must start the proxy transition process');
    const childExit = await new Promise((resolve, reject) => {
      child.once('error', reject);
      child.once('exit', (code, signal) => resolve({ code, signal }));
    });
    assert.deepStrictEqual(childExit, { code: 0, signal: null });
    assert.strictEqual(childCompletedBeforeRename, false, 'manager must wait while node A owns the tuple lock');

    try {
      for (const key of Object.keys(savedEnv)) delete process.env[key];
      process.env.A2A_HUB_URL = 'https://example.test';
      _resetHubNodeSecretStateForTesting();
      _resetCachedNodeIdForTesting();
      assert.strictEqual(fs.readFileSync(path.join(dir, 'node_id'), 'utf8'), STORE_ID);
      for (const name of [
        'node_secret',
        'node_secret_version',
        'node_secret_source',
        'node_secret_env_suppressed',
      ]) {
        assert.strictEqual(fs.existsSync(path.join(dir, name)), false, `${name} must be absent after node B wins`);
      }
      assert.deepStrictEqual(a2aProtocol.getHubNodeCredentialsReadOnly(STORE_ID), {
        secret: null,
        version: null,
      });

      let observedHeartbeat = null;
      global.fetch = mockFetch((_n, opts) => {
        observedHeartbeat = { headers: opts.headers, body: JSON.parse(opts.body) };
        return responseFromJson({ status: 200, json: { status: 'ok' } });
      });
      const heartbeat = await a2aProtocol.sendHeartbeat();
      assert.strictEqual(heartbeat.ok, true, JSON.stringify(heartbeat));
      assert.strictEqual(observedHeartbeat.body.node_id, STORE_ID);
      assert.strictEqual(observedHeartbeat.headers.Authorization, undefined);
      assert.strictEqual(observedHeartbeat.headers['X-EvoMap-Node-Secret-Version'], undefined);
      assert.strictEqual(observedHeartbeat.body.node_secret_version, undefined);
    } finally {
      _resetHubNodeSecretStateForTesting();
      _resetCachedNodeIdForTesting();
      for (const [key, value] of Object.entries(savedEnv)) {
        if (value === undefined) delete process.env[key];
        else process.env[key] = value;
      }
    }
  });
});

test('proxy heartbeat write of force_update_last.<suffix>.json uses a suffix derived from the SAME id as this.nodeId', async () => {
  // End-to-end assertion: the canonical bug surface. After hello, we
  // seed a pending last_update; the next heartbeat reads the state
  // file via `_getLastUpdateStatePath`, which builds its name from
  // `_shortNodeIdForStatePath()`. The suffix must equal the first 8
  // hex chars of `this.nodeId` — anything else is suffix drift.
  const dir = makeFakeEvomapDir();
  await withFakeEvomapDir(dir, async () => {
    global.fetch = mockFetch(() => responseFromJson({
      status: 200,
      json: { payload: { status: 'acknowledged' } },
    }));

    const store = makeStore();
    const mgr = new LifecycleManager({ hubUrl: 'https://example.test', store, logger: silentLogger() });
    const helloResult = await mgr.hello();
    assert.strictEqual(helloResult.ok, true);

    const proxyId = helloResult.nodeId; // e.g. node_abc123def456
    const expectedSuffix = proxyId.replace(/^node_/, '').slice(0, 8);
    assert.match(expectedSuffix, /^[a-f0-9]{8}$/);

    // Now build a state path the way the heartbeat would, and verify
    // the suffix is derived from the proxy's minted id (not 'anon',
    // not a stale legacy id).
    const statePath = _getLastUpdateStatePathForTesting();
    const expectedName = `force_update_last.${expectedSuffix}.json`;
    assert.strictEqual(path.basename(statePath), expectedName,
      'state-file suffix must be derived from the proxy node_id, not "anon"');

    // Round-trip a write+read to prove the file is reachable under the
    // unified suffix and matches mgr.nodeId on the wire.
    _persistLastUpdateStateForTesting({
      to_version: '1.88.0',
      from_version: '1.87.0',
      status: 'success',
      finished_at: Date.now(),
    });
    assert.ok(fs.existsSync(statePath), 'persisted state file must land at the unified path');
    assert.strictEqual(mgr.nodeId, proxyId, 'this.nodeId must equal the id whose hex seeded the suffix');
  });
});

test('_shortNodeIdForStatePath regex guard: a corrupted legacy file (path-traversal payload) falls through to "anon"', async () => {
  // Defense-in-depth: without the regex gate, a corrupt or hostile
  // legacy file like `node_../etc/passwd` would yield a suffix like
  // `../etc/p`, which path.join() in `_getLastUpdateStatePath` would
  // happily resolve OUTSIDE EVOLVER_HOME. The fix requires the 8-char
  // slice to match /^[a-f0-9]{8}$/ — otherwise fall back to the safe
  // 'anon' bucket rather than emit a traversal path.
  const dir = makeFakeEvomapDir((evomap) => {
    fs.writeFileSync(path.join(evomap, 'node_id'), 'node_../etc/passwd', { mode: 0o600 });
  });
  await withFakeEvomapDir(dir, async () => {
    const suffix = _shortNodeIdForStatePathForTesting();
    assert.strictEqual(suffix, 'anon',
      'corrupted legacy id must NOT propagate into the state-file path');
    // The constructed path stays inside EVOLVER_HOME.
    const statePath = _getLastUpdateStatePathForTesting();
    assert.strictEqual(path.dirname(statePath), dir,
      'state-file path must remain rooted at EVOLVER_HOME, never escape via traversal');
    assert.strictEqual(path.basename(statePath), 'force_update_last.anon.json');
  });
});

test('_shortNodeIdForStatePath regex guard: an uppercase-hex legacy file also falls through to "anon"', async () => {
  // NODE_ID_RE itself is case-insensitive on reads, but the persisted
  // canonical form is lowercase. An uppercase suffix would still index
  // the same hub-side row, but our local invariant is lowercase-only —
  // pin the regex to lowercase to keep the suffix space deterministic.
  const dir = makeFakeEvomapDir((evomap) => {
    fs.writeFileSync(path.join(evomap, 'node_id'), 'node_ABCDEF0123456789', { mode: 0o600 });
  });
  await withFakeEvomapDir(dir, async () => {
    const suffix = _shortNodeIdForStatePathForTesting();
    assert.strictEqual(suffix, 'anon',
      'non-lowercase hex must not produce a different suffix bucket');
  });
});

// ---------------------------------------------------------------------------
// H4 regression: pre-hello reportForceUpdateOutcome must NOT land at
// force_update_last.anon.json. Persist-on-construction fix.
// ---------------------------------------------------------------------------
//
// Bug: in proxy mode `_cachedNodeId` (a2aProtocol module-scope) is never set
// (only getNodeId() sets it, proxy never calls getNodeId), and the legacy
// file was only written AFTER hello() succeeded. If a 426 first-heartbeat
// or an enrich.js force_update path called reportForceUpdateOutcome BEFORE
// hello() returned, `_shortNodeIdForStatePath` saw neither cache nor file
// and fell through to 'anon'. The state file landed at
// `force_update_last.anon.json`. On restart, hello() wrote the real id;
// the next heartbeat read `force_update_last.<8hex>.json`; the anon file
// was orphaned and the outcome was permanently lost from the hub's view.
//
// Fix: LifecycleManager constructor now calls _persistLegacyNodeId as soon
// as the in-memory node_id is known (from MailboxStore), respecting the
// NODE_ID_RE guard. These tests pin that behaviour.

test('H4: LifecycleManager construction persists store node_id to legacy file BEFORE any hello() call', async () => {
  // Pre-condition: store already has a node_id (typical post-first-boot
  // restart), legacy file is empty. Without the fix, the legacy file
  // stays empty until hello() returns — opening the pre-hello window
  // where `_shortNodeIdForStatePath` falls through to 'anon'.
  const dir = makeFakeEvomapDir();
  await withFakeEvomapDir(dir, async () => {
    const legacyFile = path.join(dir, 'node_id');
    assert.ok(!fs.existsSync(legacyFile), 'precondition: legacy file absent');

    const store = makeStore({ node_id: STORE_ID });
    // Construct only — do NOT call hello() or heartbeat().
    // eslint-disable-next-line no-new
    new LifecycleManager({ hubUrl: 'https://example.test', store, logger: silentLogger() });

    assert.ok(fs.existsSync(legacyFile),
      'fix: constructor must seed ~/.evomap/node_id immediately, not wait for hello()');
    assert.strictEqual(fs.readFileSync(legacyFile, 'utf8').trim(), STORE_ID,
      'legacy file content must equal the store node_id');
  });
});

test('H4: pre-hello _shortNodeIdForStatePath returns the correct hex8 suffix (not "anon")', async () => {
  // The behavioural assertion behind the fix: after construction but
  // BEFORE any hello() call, the a2aProtocol module-level helper that
  // picks the per-node state-file suffix must already see the right id.
  const dir = makeFakeEvomapDir();
  await withFakeEvomapDir(dir, async () => {
    const store = makeStore({ node_id: STORE_ID });
    // eslint-disable-next-line no-new
    new LifecycleManager({ hubUrl: 'https://example.test', store, logger: silentLogger() });

    const suffix = _shortNodeIdForStatePathForTesting();
    const expected = STORE_ID.replace(/^node_/, '').slice(0, 8);
    assert.strictEqual(suffix, expected,
      'pre-hello suffix must come from the constructed manager\'s store node_id, not fall through to anon');
    assert.notStrictEqual(suffix, 'anon',
      'fix: pre-hello path must never land force_update outcomes in the anon bucket');
  });
});

test('H4: pre-hello reportForceUpdateOutcome lands the state file at the unified hex8 suffix', async () => {
  // End-to-end replay of the orphan path: store has node_id; construct
  // LifecycleManager (which seeds the legacy file); simulate the
  // pre-hello reportForceUpdateOutcome by writing through the same
  // helper a2aProtocol uses internally. The path the state file lands
  // at must match the per-node hex8 — not anon — so the next heartbeat
  // can find and forward it instead of orphaning it.
  const dir = makeFakeEvomapDir();
  await withFakeEvomapDir(dir, async () => {
    const store = makeStore({ node_id: STORE_ID });
    // eslint-disable-next-line no-new
    new LifecycleManager({ hubUrl: 'https://example.test', store, logger: silentLogger() });

    // Simulate the pre-hello force_update outcome write. In production
    // this is reportForceUpdateOutcome()->_persistLastUpdateState(); we
    // use the test hook to keep the assertion focused on the path.
    _persistLastUpdateStateForTesting({
      to_version: '1.88.0',
      from_version: '1.87.0',
      status: 'success',
      finished_at: Date.now(),
    });

    const statePath = _getLastUpdateStatePathForTesting();
    const expectedHex = STORE_ID.replace(/^node_/, '').slice(0, 8);
    const expectedName = `force_update_last.${expectedHex}.json`;
    assert.strictEqual(path.basename(statePath), expectedName,
      'fix: pre-hello state file must use the unified per-node suffix, not "anon"');
    assert.ok(fs.existsSync(statePath),
      'state file must be present at the unified path so the next heartbeat collects it');
    // Anti-assertion: the orphan path the bug used to write to must NOT exist.
    const orphanPath = path.join(dir, 'force_update_last.anon.json');
    assert.ok(!fs.existsSync(orphanPath),
      'force_update_last.anon.json must not exist — the bug used to orphan outcomes here');
  });
});

test('H4 regex guard: constructor does NOT persist a malformed store node_id', async () => {
  // Defense-in-depth: the early persist path must honour NODE_ID_RE for
  // the same reason hello()'s persist call does. A corrupted store
  // value (state.json was edited or partially written) must not poison
  // the legacy file with garbage that `_shortNodeIdForStatePath` would
  // later treat as authoritative.
  const dir = makeFakeEvomapDir();
  await withFakeEvomapDir(dir, async () => {
    const legacyFile = path.join(dir, 'node_id');
    const store = makeStore({ node_id: 'not_a_valid_node_id' });
    // eslint-disable-next-line no-new
    new LifecycleManager({ hubUrl: 'https://example.test', store, logger: silentLogger() });

    assert.ok(!fs.existsSync(legacyFile),
      'malformed store node_id must be rejected by NODE_ID_RE — legacy file stays absent');
  });
});
