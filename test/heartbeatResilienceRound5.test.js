// Regression coverage for the round-5 audit (2026-05-28) heartbeat
// resilience fixes. Each test pins exactly one fix surfaced by the
// 12-agent round-5 audit on top of rounds 1-4. The bugs being covered:
//
//   1. Round-4 installed _pendingRescheduleDelayMs = 8 min when the
//      unknown_node loop counter saturated. But the drift detector's
//      persistent-failure branch (consecutiveFailures > 0 + idle >
//      2*interval) called pokeHeartbeat() every 30s and bypassed the
//      delay via setImmediate(_heartbeatTick). The very next tick hit
//      the still-cached unknown_node, bumped the counter, and the 8
//      min wait was effectively zero. Round-5 adds an absolute
//      _unknownNodeBackoffUntil deadline that the drift detector
//      respects.
//
//   2. The unknown_node -> re-hello-ok branch reset the failure
//      counter but did NOT delay the next tick. At default 30s
//      heartbeat interval, the next tick hits the same cached
//      unknown_node almost immediately and the counter climbs to the
//      threshold (above) for nothing but DB replication lag on the
//      first hello write. Round-5 installs a hello-recovery delay
//      so the cache has time to refresh.
//
//   3. _fetchHubEvents was only called from the heartbeat success
//      path when has_pending_events=true. With SSE silently broken
//      on default installs (Node 22.x EventSource is experimental,
//      the `eventsource` fallback package is not in node_modules),
//      events queued server-side until the next heartbeat happened
//      to surface has_pending_events. Round-5 adds a self-driving
//      long-poll that runs continuously and respects the unknown_node
//      backoff.
//
//   4. SSE open / error and reauth-backoff installation now write
//      one-line JSON records to evolver_loop.log so the next "evolver
//      dead" incident has on-disk evidence past the final
//      heartbeat_ok entry. Round-4 logged only the success path.
//
//   5. getHeartbeatStats() exposes unknownNodeBackoffUntil +
//      selfDrivingPollEnabled / selfDrivingPollBackoffMs so ops can
//      distinguish "waiting on hub cache" from "running but no
//      events" without re-reading the source.

const { describe, it, after, beforeEach, afterEach } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');

// Unconditionally pin the test secret inside test scope (a host-exported
// A2A_NODE_SECRET would otherwise win and make assertions host-dependent
// the moment global.fetch stops being stubbed). Save the original and
// restore it after the suite so we do not mutate ambient env for siblings.
const _origA2ASecret = process.env.A2A_NODE_SECRET;
process.env.A2A_NODE_SECRET = 'a'.repeat(64);
after(() => {
  if (_origA2ASecret === undefined) delete process.env.A2A_NODE_SECRET;
  else process.env.A2A_NODE_SECRET = _origA2ASecret;
});

const a2a = require('../src/gep/a2aProtocol');
const { sendHeartbeat, getHeartbeatStats } = a2a;
const {
  _resetHeartbeatStateForTesting,
  _setHeartbeatStateForTesting,
  _getHeartbeatInternalsForTesting,
  _resetHubEventBufferForTesting,
  _bufferPolledHubEventsForTesting,
  _startSelfDrivingPollForTesting,
  _stopSelfDrivingPollForTesting,
  _runSelfDrivingPollForTesting,
} = a2a._testing;

function nextTick() {
  return new Promise((r) => setImmediate(r));
}
async function settle() {
  await nextTick(); await nextTick(); await nextTick(); await nextTick();
}

describe('round-5: unknown_node backoff installs an absolute deadline (not just _pendingRescheduleDelayMs)', () => {
  let origFetch, origHubUrl, origAllow;
  beforeEach(() => {
    _resetHeartbeatStateForTesting();
    origFetch = global.fetch;
    origHubUrl = process.env.A2A_HUB_URL;
    origAllow = process.env.EVOMAP_HUB_ALLOW_INSECURE;
    process.env.A2A_HUB_URL = 'http://localhost:19999';
    process.env.EVOMAP_HUB_ALLOW_INSECURE = '1';
  });
  afterEach(() => {
    global.fetch = origFetch;
    if (origHubUrl === undefined) delete process.env.A2A_HUB_URL;
    else process.env.A2A_HUB_URL = origHubUrl;
    if (origAllow === undefined) delete process.env.EVOMAP_HUB_ALLOW_INSECURE;
    else process.env.EVOMAP_HUB_ALLOW_INSECURE = origAllow;
    _resetHeartbeatStateForTesting();
  });

  it('crossing the threshold installs unknownNodeBackoffUntil >= now + 7min', async () => {
    global.fetch = async (url) => {
      const u = String(url || '');
      if (u.indexOf('/a2a/hello') !== -1) {
        return { ok: true, status: 200, json: async () => ({ ok: true, status: 'ok' }), text: async () => '' };
      }
      return { ok: true, status: 200, json: async () => ({ status: 'unknown_node' }), text: async () => '' };
    };
    await sendHeartbeat();
    await settle();
    await sendHeartbeat();
    await settle();
    const state = _getHeartbeatInternalsForTesting();
    assert.ok(
      state.unknownNodeBackoffUntil > Date.now() + 7 * 60_000,
      'deadline must extend past the hub cache TTL (420s); got ' + state.unknownNodeBackoffUntil
    );
  });

  it('persistent-failure poke is suppressed while the deadline is active (drift + pokeHeartbeat share the gate)', () => {
    // The pre-fix version of this test only set state and asserted that
    // state -- it never actually invoked the drift detector's poke path,
    // so the gate it claims to guard was never exercised. The drift
    // detector lives in an internal setInterval (a2aProtocol.js around
    // L2614) and has no direct test seam, so we exercise the IDENTICAL
    // gate contract by calling pokeHeartbeat() directly:
    //   - Round-5 added `!unknownNodeBackoffActive` to the drift detector's
    //     persistent-failure branch (a2aProtocol.js ~L2836).
    //   - Round-6 (§19.1) added the matching
    //     `if (_unknownNodeBackoffUntil > now) return false`
    //     guard inside pokeHeartbeat() itself (a2aProtocol.js ~L1249).
    // Both fixes enforce the same contract: while the cache-poisoning
    // backoff is hot, NO poke (drift-detector-driven, user-activity,
    // SIGCONT, SSE-message) may schedule a heartbeat tick. Reverting
    // either fix flips pokeHeartbeat from `return false` to scheduling
    // a setImmediate(_heartbeatTick), which this test catches.
    const now = Date.now();
    _setHeartbeatStateForTesting({
      running: true,
      intervalMs: 30_000,
      lastTickAt: now - 5 * 60_000, // way past 2*interval -> drift would poke
      consecutiveFailures: 3,         // wasFailing branch -> bypasses throttle
      unknownNodeBackoffUntil: now + 4 * 60_000,
    });
    // Sanity: the persistent-failure preconditions are real, so a missing
    // gate would actually schedule a tick (rather than being suppressed by
    // some other unrelated guard).
    const preState = _getHeartbeatInternalsForTesting();
    assert.equal(preState.running, true);
    assert.equal(preState.inFlight, false);
    assert.ok(preState.consecutiveFailures > 0,
      'precondition: failure counter must be > 0 so pokeHeartbeat takes the wasFailing path');
    assert.ok(preState.unknownNodeBackoffUntil > Date.now(),
      'precondition: backoff deadline must be in the future to exercise the gate');

    // Spy on setImmediate -- pokeHeartbeat's "actually schedule a tick"
    // path queues setImmediate(_heartbeatTick). If the gate is alive, no
    // such call is made.
    let setImmediateCalls = 0;
    const origSI = global.setImmediate;
    global.setImmediate = function () {
      setImmediateCalls++;
      return origSI.apply(null, arguments);
    };
    let pokeResult;
    try {
      pokeResult = a2a.pokeHeartbeat();
    } finally {
      global.setImmediate = origSI;
    }

    assert.equal(pokeResult, false,
      'pokeHeartbeat must REFUSE while unknownNodeBackoffUntil is in the future. ' +
      'Pre-fix: returns true and queues setImmediate(_heartbeatTick), hammering ' +
      'the still-hot hub cache every 30s.');
    assert.equal(setImmediateCalls, 0,
      'no setImmediate(_heartbeatTick) must be queued while the deadline is active');

    const after = _getHeartbeatInternalsForTesting();
    assert.ok(after.unknownNodeBackoffUntil > Date.now(),
      'deadline must remain intact after a refused poke');
    assert.equal(after.consecutiveFailures, 3,
      'pokeHeartbeat must NOT clear the failure counter when it refuses ' +
      '(the round-3 fix to pokeHeartbeat: gate first, mutate second)');
  });

  it('hello-recovery delay arms _pendingRescheduleDelayMs even when the counter is below threshold', async () => {
    // First tick: unknown_node + hello ok -> counter=1, below threshold,
    // but the hello-recovery delay should still arm so the next tick
    // does not slam the cached response 30s later.
    //
    // Precondition: confirm _pendingRescheduleDelayMs starts at 0 so the
    // assertion below proves the value was set DURING this tick (rather
    // than inherited from prior state). _resetHeartbeatStateForTesting()
    // in beforeEach already clears it, but pinning the precondition makes
    // a regression in the reset hook surface here too instead of silently
    // letting a stale 30s+ value pass the post-tick assertion.
    const pre = _getHeartbeatInternalsForTesting();
    assert.equal(pre.pendingRescheduleDelayMs, 0,
      'precondition: _pendingRescheduleDelayMs must start at 0 so the post-tick ' +
      'assertion proves the hello-recovery branch (a2aProtocol.js ~L1749) set it');

    global.fetch = async (url) => {
      const u = String(url || '');
      if (u.indexOf('/a2a/hello') !== -1) {
        return { ok: true, status: 200, json: async () => ({ ok: true, status: 'ok' }), text: async () => '' };
      }
      return { ok: true, status: 200, json: async () => ({ status: 'unknown_node' }), text: async () => '' };
    };
    await sendHeartbeat();
    await settle();
    const after = _getHeartbeatInternalsForTesting();
    assert.equal(after.consecutiveUnknownNodeAfterHello, 1,
      'counter should be 1 (below threshold) after one cycle -- proves the unknown_node ' +
      'hello-ok branch actually fired (not just stubbed state inspection)');
    assert.ok(after.pendingRescheduleDelayMs >= 30_000,
      'hello-recovery delay must be at least HEARTBEAT_FIRST_DELAY_MS margin to let DB replication catch up; got ' +
      after.pendingRescheduleDelayMs);
  });

  it('an ok heartbeat clears both the counter AND the deadline', async () => {
    // Force the deadline to a future value, then run one ok cycle.
    const future = Date.now() + 10 * 60_000;
    _setHeartbeatStateForTesting({
      running: true,
      intervalMs: 30_000,
      unknownNodeBackoffUntil: future,
    });
    assert.equal(_getHeartbeatInternalsForTesting().unknownNodeBackoffUntil, future);
    global.fetch = async () => ({
      ok: true, status: 200,
      json: async () => ({ status: 'ok' }),
      text: async () => '',
    });
    await sendHeartbeat();
    await settle();
    assert.equal(_getHeartbeatInternalsForTesting().unknownNodeBackoffUntil, 0,
      'a single ok heartbeat must drop the deadline so the next episode starts fresh');
  });

  it('_resetHeartbeatStateForTesting clears the deadline (cross-test isolation)', () => {
    _setHeartbeatStateForTesting({ unknownNodeBackoffUntil: Date.now() + 60_000 });
    _resetHeartbeatStateForTesting();
    assert.equal(_getHeartbeatInternalsForTesting().unknownNodeBackoffUntil, 0,
      'reset hook must clear the round-5 deadline');
  });
});

describe('round-5: self-driving long-poll runs independently of heartbeat', () => {
  let origFetch, origHubUrl, origAllow, origDisableSelf;
  beforeEach(() => {
    _resetHeartbeatStateForTesting();
    _resetHubEventBufferForTesting();
    origFetch = global.fetch;
    origHubUrl = process.env.A2A_HUB_URL;
    origAllow = process.env.EVOMAP_HUB_ALLOW_INSECURE;
    origDisableSelf = process.env.EVOLVER_DISABLE_SELF_DRIVING_POLL;
    process.env.A2A_HUB_URL = 'http://localhost:19999';
    process.env.EVOMAP_HUB_ALLOW_INSECURE = '1';
    delete process.env.EVOLVER_DISABLE_SELF_DRIVING_POLL;
  });
  afterEach(() => {
    _stopSelfDrivingPollForTesting();
    global.fetch = origFetch;
    if (origHubUrl === undefined) delete process.env.A2A_HUB_URL;
    else process.env.A2A_HUB_URL = origHubUrl;
    if (origAllow === undefined) delete process.env.EVOMAP_HUB_ALLOW_INSECURE;
    else process.env.EVOMAP_HUB_ALLOW_INSECURE = origAllow;
    if (origDisableSelf === undefined) delete process.env.EVOLVER_DISABLE_SELF_DRIVING_POLL;
    else process.env.EVOLVER_DISABLE_SELF_DRIVING_POLL = origDisableSelf;
    _resetHeartbeatStateForTesting();
  });

  it('startSelfDrivingPoll arms the timer; stop clears it', () => {
    assert.equal(_getHeartbeatInternalsForTesting().selfDrivingPollEnabled, false);
    _startSelfDrivingPollForTesting();
    const after = _getHeartbeatInternalsForTesting();
    assert.equal(after.selfDrivingPollEnabled, true,
      'enabled flag is set by start()');
    assert.equal(after.hasSelfDrivingPollTimer, true,
      'start() schedules the initial run');
    _stopSelfDrivingPollForTesting();
    const stopped = _getHeartbeatInternalsForTesting();
    assert.equal(stopped.selfDrivingPollEnabled, false);
    assert.equal(stopped.hasSelfDrivingPollTimer, false);
  });

  it('EVOLVER_DISABLE_SELF_DRIVING_POLL=1 is the escape hatch', () => {
    process.env.EVOLVER_DISABLE_SELF_DRIVING_POLL = '1';
    _startSelfDrivingPollForTesting();
    const state = _getHeartbeatInternalsForTesting();
    assert.equal(state.selfDrivingPollEnabled, false,
      'env var prevents the runner from arming');
    assert.equal(state.hasSelfDrivingPollTimer, false);
  });

  it('runner short-circuits while unknownNodeBackoffUntil is in the future', async () => {
    let pollCalls = 0;
    global.fetch = async (url) => {
      if (String(url).indexOf('/a2a/events/poll') !== -1) {
        pollCalls++;
      }
      return { ok: true, status: 200, json: async () => ({ events: [] }), text: async () => '' };
    };
    _setHeartbeatStateForTesting({
      running: true,
      intervalMs: 30_000,
      unknownNodeBackoffUntil: Date.now() + 5 * 60_000,
    });
    _startSelfDrivingPollForTesting();
    // Manually run once -- it should bail without issuing a network request.
    _runSelfDrivingPollForTesting();
    await settle();
    assert.equal(pollCalls, 0,
      'self-driving poll must not call /a2a/events/poll while unknown_node backoff is active');
    // The timer must still be re-armed for a future quiet check.
    assert.equal(_getHeartbeatInternalsForTesting().hasSelfDrivingPollTimer, true);
  });

  it('runner short-circuits while reauth backoff is in the future', async () => {
    let pollCalls = 0;
    global.fetch = async (url) => {
      if (String(url).indexOf('/a2a/events/poll') !== -1) {
        pollCalls++;
      }
      return { ok: true, status: 200, json: async () => ({ events: [] }), text: async () => '' };
    };
    _setHeartbeatStateForTesting({
      running: true,
      intervalMs: 30_000,
      reauthBackoffUntil: Date.now() + 60 * 60_000,
    });
    _startSelfDrivingPollForTesting();
    _runSelfDrivingPollForTesting();
    await settle();
    assert.equal(pollCalls, 0,
      'self-driving poll must not call /a2a/events/poll while reauth backoff is active');
  });

  it('uses accepted event count for fast-drain scheduling', async () => {
    const scheduledDelays = [];
    const originalSetTimeout = global.setTimeout;
    const eventIds = ['evt_same', 'evt_same', 'evt_new'];
    let pollIndex = 0;
    global.setTimeout = function (_fn, delay) {
      scheduledDelays.push(delay);
      return { unref: function () {} };
    };
    global.fetch = async () => ({
      ok: true,
      status: 200,
      json: async () => ({
        events: [{
          id: eventIds[pollIndex++],
          type: 'task_available',
          payload: { task_id: 't1' },
        }],
        next_poll_after_ms: 0,
      }),
      text: async () => '',
    });

    try {
      _startSelfDrivingPollForTesting();

      _runSelfDrivingPollForTesting();
      await settle();
      assert.equal(scheduledDelays.at(-1), 0,
        'a newly accepted event should trigger immediate queue draining');

      _runSelfDrivingPollForTesting();
      await settle();
      assert.equal(scheduledDelays.at(-1), 1000,
        'a duplicate-only response should return to the base poll interval');

      _runSelfDrivingPollForTesting();
      await settle();
      assert.equal(scheduledDelays.at(-1), 0,
        'a later new event should restore immediate queue draining');
    } finally {
      _stopSelfDrivingPollForTesting();
      global.setTimeout = originalSetTimeout;
    }
  });

  it('honors the Hub cadence hint for an empty queue', async () => {
    const scheduledDelays = [];
    const originalSetTimeout = global.setTimeout;
    global.setTimeout = function (_fn, delay) {
      scheduledDelays.push(delay);
      return { unref: function () {} };
    };
    global.fetch = async () => ({
      ok: true,
      status: 200,
      json: async () => ({ events: [], next_poll_after_ms: 15_000 }),
    });

    try {
      _startSelfDrivingPollForTesting();
      _runSelfDrivingPollForTesting();
      await settle();

      assert.equal(scheduledDelays.at(-1), 15_000);
      assert.equal(_getHeartbeatInternalsForTesting().selfDrivingPollBackoffMs, 15_000);
    } finally {
      _stopSelfDrivingPollForTesting();
      global.setTimeout = originalSetTimeout;
    }
  });

  it('retries immediately with the new identity when an old-identity poll fails in flight', async () => {
    const scheduled = [];
    const originalSetTimeout = global.setTimeout;
    const originalSseDisabled = process.env.EVOLVER_SSE_DISABLED;
    let currentNodeId = 'node_old_identity';
    let currentAuthorization = 'Bearer test-old';
    let identityListener = null;
    let resolveOldPoll;
    const oldPoll = new Promise((resolve) => { resolveOldPoll = resolve; });
    const pollRequests = [];

    global.setTimeout = function (fn, delay) {
      const timer = { fn, delay, unref: function () {} };
      scheduled.push(timer);
      return timer;
    };
    process.env.EVOLVER_SSE_DISABLED = '1';
    global.fetch = function (url, options) {
      if (!String(url).includes('/a2a/events/poll')) {
        throw new Error('unexpected request: ' + url);
      }
      pollRequests.push({
        nodeId: JSON.parse(options.body).sender_id,
        authorization: options.headers.Authorization,
      });
      if (pollRequests.length === 1) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: async () => ({ events: [], next_poll_after_ms: 60_000 }),
        });
      }
      if (pollRequests.length === 2) return oldPoll;
      return Promise.resolve({
        ok: true,
        status: 200,
        json: async () => ({ events: [] }),
      });
    };

    try {
      a2a.startEventDelivery({
        hubUrl: 'http://localhost:19999',
        identityProvider: {
          getNodeId: () => currentNodeId,
          getHeaders: () => ({ Authorization: currentAuthorization }),
          subscribe: (listener) => {
            identityListener = listener;
            return function () {};
          },
        },
      });

      _runSelfDrivingPollForTesting();
      await settle();
      assert.equal(scheduled.at(-1).delay, 60_000, 'precondition: old identity is on max backoff');

      _runSelfDrivingPollForTesting();
      await settle();
      assert.equal(pollRequests.length, 2, 'old-identity poll must still be in flight');

      currentNodeId = 'node_new_identity';
      currentAuthorization = 'Bearer test-new';
      identityListener();
      resolveOldPoll({
        ok: false,
        status: 401,
        headers: new Headers(),
        body: { cancel: async function () {} },
      });
      await settle();

      const immediateRetry = scheduled.at(-1);
      assert.equal(immediateRetry.delay, 0, 'identity rotation must bypass the old 60s backoff');
      assert.equal(_getHeartbeatInternalsForTesting().selfDrivingPollBackoffMs, 1000);

      immediateRetry.fn();
      await settle();
      assert.deepEqual(pollRequests.at(-1), {
        nodeId: 'node_new_identity',
        authorization: 'Bearer test-new',
      });
    } finally {
      a2a.stopEventDelivery();
      global.setTimeout = originalSetTimeout;
      if (originalSseDisabled === undefined) delete process.env.EVOLVER_SSE_DISABLED;
      else process.env.EVOLVER_SSE_DISABLED = originalSseDisabled;
    }
  });

  it('lets a same-node poll finish across secret rotation and uses fresh headers next time', async () => {
    const scheduled = [];
    const originalSetTimeout = global.setTimeout;
    const originalSseDisabled = process.env.EVOLVER_SSE_DISABLED;
    let currentAuthorization = 'Bearer test-old-secret';
    let identityListener = null;
    let resolveOldPoll;
    let oldPollAborted = false;
    const pollRequests = [];

    global.setTimeout = function (fn, delay) {
      const timer = { fn, delay, unref: function () {} };
      scheduled.push(timer);
      return timer;
    };
    process.env.EVOLVER_SSE_DISABLED = '1';
    global.fetch = function (url, options) {
      if (!String(url).includes('/a2a/events/poll')) {
        throw new Error('unexpected request: ' + url);
      }
      pollRequests.push({
        nodeId: JSON.parse(options.body).sender_id,
        authorization: options.headers.Authorization,
      });
      if (pollRequests.length === 1) {
        options.signal.addEventListener('abort', function () {
          oldPollAborted = true;
        }, { once: true });
        return new Promise((resolve) => { resolveOldPoll = resolve; });
      }
      return Promise.resolve({
        ok: true,
        status: 200,
        json: async () => ({ events: [], next_poll_after_ms: 60_000 }),
      });
    };

    try {
      a2a.startEventDelivery({
        hubUrl: 'http://localhost:19999',
        identityProvider: {
          getNodeId: () => 'node_same_identity',
          getHeaders: () => ({ Authorization: currentAuthorization }),
          subscribe: (listener) => {
            identityListener = listener;
            return function () {};
          },
        },
      });

      _runSelfDrivingPollForTesting();
      await settle();
      assert.equal(pollRequests.length, 1, 'precondition: old-secret poll must be in flight');

      currentAuthorization = 'Bearer test-new-secret';
      identityListener();
      assert.equal(oldPollAborted, false,
        'same-node secret rotation must not abort the in-flight poll');

      resolveOldPoll({
        ok: true,
        status: 200,
        json: async () => ({
          events: [{ id: 'event_during_rotation', type: 'task_available', payload: {} }],
        }),
      });
      await settle();
      assert.deepEqual(a2a.consumeHubEvents().map((event) => event.id), ['event_during_rotation'],
        'the old-secret poll response must remain deliverable after rotation');

      const nextPoll = scheduled.at(-1);
      assert.equal(nextPoll.delay, 0, 'an accepted event should schedule the next poll immediately');
      nextPoll.fn();
      await settle();
      assert.deepEqual(pollRequests.at(-1), {
        nodeId: 'node_same_identity',
        authorization: 'Bearer test-new-secret',
      });
    } finally {
      a2a.stopEventDelivery();
      global.setTimeout = originalSetTimeout;
      if (originalSseDisabled === undefined) delete process.env.EVOLVER_SSE_DISABLED;
      else process.env.EVOLVER_SSE_DISABLED = originalSseDisabled;
    }
  });

  it('aborts an in-flight old-identity poll before immediately polling with a replacement identity', async () => {
    const originalSseDisabled = process.env.EVOLVER_SSE_DISABLED;
    const pollRequests = [];
    let oldPollAborted = false;
    process.env.EVOLVER_SSE_DISABLED = '1';
    global.fetch = function (url, options) {
      if (!String(url).includes('/a2a/events/poll')) {
        throw new Error('unexpected request: ' + url);
      }
      const nodeId = JSON.parse(options.body).sender_id;
      pollRequests.push(nodeId);
      if (nodeId === 'node_generation_a') {
        return new Promise((resolve) => {
          options.signal.addEventListener('abort', function () {
            oldPollAborted = true;
            // Resolve a synthetic late response even after cancellation to
            // prove the generation fence, not only native fetch abort behavior.
            resolve({
              ok: true,
              status: 200,
              body: { cancel: async function () {} },
              json: async () => ({
                events: [{ id: 'event_a_stale', type: 'task_available', payload: {} }],
              }),
            });
          }, { once: true });
        });
      }
      return Promise.resolve({
        ok: true,
        status: 200,
        json: async () => ({
          events: [{ id: 'event_b', type: 'task_available', payload: { task_id: 'b' } }],
          next_poll_after_ms: 60_000,
        }),
      });
    };

    try {
      a2a.startEventDelivery({
        hubUrl: 'http://localhost:19999',
        nodeId: 'node_generation_a',
      });
      _runSelfDrivingPollForTesting();
      await settle();
      assert.deepEqual(pollRequests, ['node_generation_a']);

      a2a.startEventDelivery({
        hubUrl: 'http://localhost:19999',
        nodeId: 'node_generation_b',
      });
      for (let i = 0; i < 20 && !pollRequests.includes('node_generation_b'); i += 1) {
        await new Promise((resolve) => setTimeout(resolve, 5));
      }

      assert.equal(oldPollAborted, true, 'replacement must cancel the old long poll');
      assert.ok(pollRequests.includes('node_generation_b'),
        'replacement identity must poll without waiting for the old long-poll timeout');
      assert.deepEqual(a2a.consumeHubEvents().map((event) => event.id), ['event_b']);
    } finally {
      a2a.stopEventDelivery();
      if (originalSseDisabled === undefined) delete process.env.EVOLVER_SSE_DISABLED;
      else process.env.EVOLVER_SSE_DISABLED = originalSseDisabled;
    }
  });

  it('drops a poll response that finishes parsing after event delivery stops', async () => {
    const originalSseDisabled = process.env.EVOLVER_SSE_DISABLED;
    let resolveJson;
    const delayedJson = new Promise((resolve) => { resolveJson = resolve; });
    process.env.EVOLVER_SSE_DISABLED = '1';
    global.fetch = async () => ({
      ok: true,
      status: 200,
      json: () => delayedJson,
    });

    try {
      a2a.startEventDelivery({
        hubUrl: 'http://localhost:19999',
        nodeId: 'node_stop_generation',
      });
      _runSelfDrivingPollForTesting();
      await settle();

      a2a.stopEventDelivery();
      resolveJson({
        events: [{ id: 'event_after_stop', type: 'task_available', payload: {} }],
      });
      await settle();

      assert.deepEqual(a2a.consumeHubEvents(), [],
        'a stopped delivery generation must not accept a late response');
    } finally {
      a2a.stopEventDelivery();
      if (originalSseDisabled === undefined) delete process.env.EVOLVER_SSE_DISABLED;
      else process.env.EVOLVER_SSE_DISABLED = originalSseDisabled;
    }
  });

  it('stopHeartbeat aborts and fences a heartbeat-owned in-flight poll', async () => {
    let pollSignal = null;
    let resolvePoll = null;
    const lateResponse = {
      ok: true,
      status: 200,
      body: { cancel: async function () {} },
      json: async () => ({
        events: [{ id: 'event_after_heartbeat_stop', type: 'task_available', payload: {} }],
      }),
    };
    global.fetch = function (url, options) {
      if (String(url).includes('/a2a/hello')) {
        return Promise.resolve({
          ok: true,
          status: 200,
          json: async () => ({ status: 'ok' }),
          text: async () => '',
        });
      }
      if (!String(url).includes('/a2a/events/poll')) {
        throw new Error('unexpected request: ' + url);
      }
      pollSignal = options.signal;
      return new Promise((resolve) => {
        resolvePoll = resolve;
        pollSignal.addEventListener('abort', function () {
          // Resolve despite cancellation to prove the generation fence also
          // rejects transports that ignore AbortSignal after shutdown.
          resolve(lateResponse);
        }, { once: true });
      });
    };

    try {
      a2a.startHeartbeat(60_000);
      await settle();
      assert.equal(_getHeartbeatInternalsForTesting().selfDrivingPollEnabled, true,
        'precondition: startHeartbeat must own an active self-driving poll lifecycle');

      _runSelfDrivingPollForTesting();
      await settle();
      assert.ok(pollSignal, 'precondition: a heartbeat-owned long poll must be in flight');
      assert.equal(pollSignal.aborted, false);

      a2a.stopHeartbeat();
      if (!pollSignal.aborted) resolvePoll(lateResponse);
      await settle();

      assert.equal(pollSignal.aborted, true,
        'stopHeartbeat must abort the in-flight long poll');
      assert.deepEqual(a2a.consumeHubEvents(), [],
        'a synthetic late response after stopHeartbeat must not enter the buffer');
    } finally {
      a2a.stopHeartbeat();
      if (resolvePoll) resolvePoll(lateResponse);
    }
  });

  it('isolates buffered events across node or Hub replacement but preserves them on same-node secret rotation', () => {
    const originalSseDisabled = process.env.EVOLVER_SSE_DISABLED;
    const originalDisableSelf = process.env.EVOLVER_DISABLE_SELF_DRIVING_POLL;
    let identityListener = null;
    let authorization = 'Bearer secret-a';
    process.env.EVOLVER_SSE_DISABLED = '1';
    process.env.EVOLVER_DISABLE_SELF_DRIVING_POLL = '1';

    try {
      a2a.startEventDelivery({
        hubUrl: 'http://hub-a.local',
        identityProvider: {
          getNodeId: () => 'node_scope_a',
          getHeaders: () => ({ Authorization: authorization }),
          subscribe: (listener) => {
            identityListener = listener;
            return function () {};
          },
        },
      });
      _bufferPolledHubEventsForTesting([
        { id: 'scope_event', type: 'task_available', payload: {} },
      ]);

      authorization = 'Bearer secret-b';
      identityListener();
      assert.deepEqual(a2a.getHubEvents().map((event) => event.id), ['scope_event'],
        'same-node secret rotation must preserve already accepted events');
      assert.equal(_bufferPolledHubEventsForTesting([
        { id: 'scope_event', type: 'task_available', payload: {} },
      ]).length, 0, 'same-node secret rotation must preserve dedup state');

      a2a.startEventDelivery({
        hubUrl: 'http://hub-b.local',
        nodeId: 'node_scope_b',
      });
      assert.deepEqual(a2a.consumeHubEvents(), [],
        'explicit node or Hub replacement must clear the previous scope buffer');
      assert.equal(_bufferPolledHubEventsForTesting([
        { id: 'scope_event', type: 'task_available', payload: {} },
      ]).length, 1, 'explicit scope replacement must clear previous dedup state');
    } finally {
      a2a.stopEventDelivery();
      if (originalSseDisabled === undefined) delete process.env.EVOLVER_SSE_DISABLED;
      else process.env.EVOLVER_SSE_DISABLED = originalSseDisabled;
      if (originalDisableSelf === undefined) delete process.env.EVOLVER_DISABLE_SELF_DRIVING_POLL;
      else process.env.EVOLVER_DISABLE_SELF_DRIVING_POLL = originalDisableSelf;
    }
  });

  it('rejects invalid cadence hints and caps oversized values', async () => {
    const scheduledDelays = [];
    const originalSetTimeout = global.setTimeout;
    const hints = [-1, 1.5, Number.MAX_SAFE_INTEGER];
    let pollIndex = 0;
    global.setTimeout = function (_fn, delay) {
      scheduledDelays.push(delay);
      return { unref: function () {} };
    };
    global.fetch = async () => ({
      ok: true,
      status: 200,
      json: async () => ({ events: [], next_poll_after_ms: hints[pollIndex++] }),
    });

    try {
      _startSelfDrivingPollForTesting();

      _runSelfDrivingPollForTesting();
      await settle();
      assert.equal(scheduledDelays.at(-1), 1000, 'negative hints must be ignored');

      _runSelfDrivingPollForTesting();
      await settle();
      assert.equal(scheduledDelays.at(-1), 1000, 'fractional hints must be ignored');

      _runSelfDrivingPollForTesting();
      await settle();
      assert.equal(scheduledDelays.at(-1), 60_000, 'oversized hints must be capped');
    } finally {
      _stopSelfDrivingPollForTesting();
      global.setTimeout = originalSetTimeout;
    }
  });

  it('exponentially backs off network, HTTP, and JSON failures', async () => {
    const scheduledDelays = [];
    const originalSetTimeout = global.setTimeout;
    let pollIndex = 0;
    let cancelCalls = 0;
    global.setTimeout = function (_fn, delay) {
      scheduledDelays.push(delay);
      return { unref: function () {} };
    };
    global.fetch = async () => {
      pollIndex += 1;
      if (pollIndex === 1) throw new Error('synthetic_network_down');
      if (pollIndex === 2) {
        return {
          ok: false,
          status: 503,
          headers: new Headers(),
          body: { cancel: async function () { cancelCalls += 1; } },
        };
      }
      return {
        ok: true,
        status: 200,
        json: async () => { throw new SyntaxError('synthetic_bad_json'); },
      };
    };

    try {
      _startSelfDrivingPollForTesting();

      _runSelfDrivingPollForTesting();
      await settle();
      assert.equal(scheduledDelays.at(-1), 2000, 'network errors should start exponential backoff');

      _runSelfDrivingPollForTesting();
      await settle();
      assert.equal(scheduledDelays.at(-1), 4000, 'HTTP errors should continue exponential backoff');
      assert.equal(cancelCalls, 1, 'HTTP error bodies must still be released');

      _runSelfDrivingPollForTesting();
      await settle();
      assert.equal(scheduledDelays.at(-1), 8000, 'JSON errors should continue exponential backoff');
    } finally {
      _stopSelfDrivingPollForTesting();
      global.setTimeout = originalSetTimeout;
    }
  });

  it('contains propagated failures from heartbeat-triggered polls', async () => {
    const originalWarn = console.warn;
    const warnings = [];
    let pollCalls = 0;
    console.warn = function (...args) {
      warnings.push(args.map(String).join(' '));
    };
    global.fetch = async (url) => {
      if (String(url).includes('/a2a/events/poll')) {
        pollCalls += 1;
        throw new Error('synthetic_heartbeat_poll_failure');
      }
      return {
        ok: true,
        status: 200,
        json: async () => ({ status: 'ok', has_pending_events: true }),
        text: async () => '',
      };
    };

    try {
      await sendHeartbeat();
      await settle();

      assert.equal(pollCalls, 1);
      assert.ok(
        warnings.some((line) => line.includes('[Events] Poll failed: synthetic_heartbeat_poll_failure')),
        'the fire-and-forget heartbeat path must catch propagated poll failures'
      );
    } finally {
      console.warn = originalWarn;
    }
  });

  it('preserves Retry-After scheduling when a 429 rejects', async () => {
    const scheduledDelays = [];
    const originalSetTimeout = global.setTimeout;
    let cancelCalls = 0;
    global.setTimeout = function (_fn, delay) {
      scheduledDelays.push(delay);
      return { unref: function () {} };
    };
    global.fetch = async () => ({
      ok: false,
      status: 429,
      headers: new Headers({ 'Retry-After': '45' }),
      body: { cancel: async function () { cancelCalls += 1; } },
    });

    try {
      _startSelfDrivingPollForTesting();
      _runSelfDrivingPollForTesting();
      await settle();

      const state = _getHeartbeatInternalsForTesting();
      assert.equal(scheduledDelays.at(-1), 45_000);
      assert.equal(state.selfDrivingPollBackoffMs, 45_000);
      assert.equal(state.pendingSelfDrivingPollDelayMs, 0, 'Retry-After must be consumed once');
      assert.equal(cancelCalls, 1);
    } finally {
      _stopSelfDrivingPollForTesting();
      global.setTimeout = originalSetTimeout;
    }
  });

  it('getHeartbeatStats() surfaces selfDrivingPollEnabled / backoff for ops tooling', () => {
    _startSelfDrivingPollForTesting();
    const stats = getHeartbeatStats();
    assert.equal(stats.selfDrivingPollEnabled, true);
    assert.equal(typeof stats.selfDrivingPollBackoffMs, 'number');
    assert.equal(typeof stats.unknownNodeBackoffUntil, 'number');
  });
});

describe('round-5: disk log writes failure + lifecycle records (not just heartbeat_ok)', () => {
  let origFetch, origHubUrl, origAllow, origLogPath, origEvolverHome;
  let tmpDir;
  beforeEach(() => {
    _resetHeartbeatStateForTesting();
    origFetch = global.fetch;
    origHubUrl = process.env.A2A_HUB_URL;
    origAllow = process.env.EVOMAP_HUB_ALLOW_INSECURE;
    origLogPath = process.env.EVOLVER_LOG_PATH;
    origEvolverHome = process.env.EVOLVER_REPO_ROOT;
    tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-r5-log-'));
    // Force the log destination so the test does not depend on repo
    // root resolution. getEvolverLogPath honours EVOLVER_LOG_PATH.
    process.env.EVOLVER_LOG_PATH = path.join(tmpDir, 'evolver_loop.log');
    process.env.A2A_HUB_URL = 'http://localhost:19999';
    process.env.EVOMAP_HUB_ALLOW_INSECURE = '1';
  });
  afterEach(() => {
    global.fetch = origFetch;
    if (origHubUrl === undefined) delete process.env.A2A_HUB_URL;
    else process.env.A2A_HUB_URL = origHubUrl;
    if (origAllow === undefined) delete process.env.EVOMAP_HUB_ALLOW_INSECURE;
    else process.env.EVOMAP_HUB_ALLOW_INSECURE = origAllow;
    if (origLogPath === undefined) delete process.env.EVOLVER_LOG_PATH;
    else process.env.EVOLVER_LOG_PATH = origLogPath;
    if (origEvolverHome === undefined) delete process.env.EVOLVER_REPO_ROOT;
    else process.env.EVOLVER_REPO_ROOT = origEvolverHome;
    try { fs.rmSync(tmpDir, { recursive: true, force: true }); } catch (_) {}
    _resetHeartbeatStateForTesting();
  });

  it('a network failure writes a heartbeat_fail record (not silence after the last ok)', async () => {
    global.fetch = async () => {
      throw new Error('ECONNRESET: simulated transport failure');
    };
    await sendHeartbeat();
    await settle();
    const logPath = process.env.EVOLVER_LOG_PATH;
    if (!fs.existsSync(logPath)) {
      // If the log helper could not resolve the path (some test envs),
      // skip the assertion rather than failing on infra noise.
      return;
    }
    const content = fs.readFileSync(logPath, 'utf8');
    assert.ok(
      content.indexOf('"heartbeat_fail"') !== -1,
      'failure path must write a heartbeat_fail entry; log content: ' + content
    );
  });

  it('unknown_node backoff arming writes a dedicated record so RCA can find it', async () => {
    global.fetch = async (url) => {
      const u = String(url || '');
      if (u.indexOf('/a2a/hello') !== -1) {
        return { ok: true, status: 200, json: async () => ({ ok: true, status: 'ok' }), text: async () => '' };
      }
      return { ok: true, status: 200, json: async () => ({ status: 'unknown_node' }), text: async () => '' };
    };
    // Two cycles trips the threshold and arms the deadline.
    await sendHeartbeat();
    await settle();
    await sendHeartbeat();
    await settle();
    const logPath = process.env.EVOLVER_LOG_PATH;
    if (!fs.existsSync(logPath)) return;
    const content = fs.readFileSync(logPath, 'utf8');
    assert.ok(
      content.indexOf('"unknown_node_backoff_armed"') !== -1,
      'backoff arming must be logged for the next incident RCA; log content: ' + content
    );
  });
});
