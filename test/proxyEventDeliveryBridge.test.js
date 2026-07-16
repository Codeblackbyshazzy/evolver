'use strict';

const test = require('node:test');
const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');

const { EvoMapProxy } = require('../src/proxy');
const { MailboxStore } = require('../src/proxy/mailbox/store');
const { TraceControl } = require('../src/proxy/extensions/traceControl');
const protocol = require('../src/gep/a2aProtocol');

function silentLogger() {
  return { log() {}, warn() {}, error() {}, info() {}, debug() {} };
}

async function waitFor(predicate, timeoutMs = 1_000) {
  const deadline = Date.now() + timeoutMs;
  while (!predicate()) {
    if (Date.now() >= deadline) throw new Error('timed out waiting for condition');
    await new Promise((resolve) => setTimeout(resolve, 5));
  }
}

test('accepted Hub events reach the proxy mailbox and apply trace config once', () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-event-bridge-'));
  const proxy = new EvoMapProxy({ dataDir, hubUrl: 'https://example.invalid', logger: silentLogger() });
  proxy.store = new MailboxStore(dataDir);
  proxy.skillUpdater = { pollAndApply() { return 0; } };
  proxy.traceControl = new TraceControl({ store: proxy.store, logger: silentLogger() });
  let handlerCalls = 0;
  const originalHandle = proxy._handleInboundReceived.bind(proxy);
  proxy._handleInboundReceived = function () {
    handlerCalls++;
    return originalHandle();
  };

  try {
    protocol._testing._resetHubEventBufferForTesting();
    protocol.startEventDelivery({
      hubUrl: '',
      nodeId: '',
      enableSse: false,
      onEventsAccepted: (events) => proxy.acceptHubEvents(events),
    });
    const event = { id: 'evt_trace_stop', type: 'trace_collection_config', payload: { enabled: false } };

    assert.equal(protocol._testing._bufferPolledHubEventsForTesting([event]).length, 1);
    assert.equal(proxy.store.getState('trace_collection_enabled'), 'false');
    assert.equal(proxy.store.getById(event.id).status, 'delivered');
    assert.equal(proxy.store.poll({ type: event.type }).length, 0);
    assert.equal(handlerCalls, 1);

    assert.equal(protocol._testing._bufferPolledHubEventsForTesting([event]).length, 0);
    assert.equal(handlerCalls, 1, 'duplicate Hub ids must not re-run proxy extensions');
    assert.equal(proxy.acceptHubEvents([event]), 0, 'mailbox-level duplicate must not count as inserted');
    assert.equal(handlerCalls, 1, 'a duplicate racing in from mailbox delivery must not re-run extensions');
  } finally {
    protocol.stopEventDelivery();
    proxy.store.close();
    fs.rmSync(dataDir, { recursive: true, force: true });
  }
});

test('failed Hub mailbox writes retry locally and apply extensions exactly once', async () => {
  const dataDir = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-event-bridge-retry-'));
  const proxy = new EvoMapProxy({
    dataDir,
    hubUrl: 'https://example.invalid',
    logger: silentLogger(),
    hubEventRetryBaseMs: 5,
    hubEventRetryMaxMs: 10,
  });
  proxy.store = new MailboxStore(dataDir);
  proxy.skillUpdater = { pollAndApply() { return 0; } };
  proxy.traceControl = new TraceControl({ store: proxy.store, logger: silentLogger() });

  const originalWriteInbound = proxy.store.writeInbound.bind(proxy.store);
  let writeAttempts = 0;
  proxy.store.writeInbound = (message) => {
    writeAttempts++;
    if (writeAttempts === 1) throw new Error('injected transient mailbox failure');
    return originalWriteInbound(message);
  };

  let handlerCalls = 0;
  const originalHandle = proxy._handleInboundReceived.bind(proxy);
  proxy._handleInboundReceived = function () {
    handlerCalls++;
    return originalHandle();
  };

  try {
    protocol._testing._resetHubEventBufferForTesting();
    protocol.startEventDelivery({
      hubUrl: '',
      nodeId: '',
      enableSse: false,
      onEventsAccepted: (events) => proxy.acceptHubEvents(events),
    });
    const event = { id: 'evt_trace_retry', type: 'trace_collection_config', payload: { enabled: false } };

    assert.equal(protocol._testing._bufferPolledHubEventsForTesting([event]).length, 1);
    assert.equal(proxy.store.getById(event.id), null, 'the injected first write must not reach the mailbox');
    assert.equal(proxy._pendingHubEvents.size, 1);
    assert.equal(handlerCalls, 0);

    await waitFor(() => proxy.store.getById(event.id)?.status === 'delivered');

    assert.equal(writeAttempts, 2, 'the proxy should retry without another Hub delivery');
    assert.equal(proxy.store.getState('trace_collection_enabled'), 'false');
    assert.equal(proxy.store.poll({ type: event.type }).length, 0);
    assert.equal(proxy._pendingHubEvents.size, 0);
    assert.equal(handlerCalls, 1);

    assert.equal(protocol._testing._bufferPolledHubEventsForTesting([event]).length, 0);
    await new Promise((resolve) => setTimeout(resolve, 15));
    assert.equal(writeAttempts, 2, 'the protocol duplicate must not create another mailbox write');
    assert.equal(handlerCalls, 1, 'the protocol duplicate must not re-run extensions');
  } finally {
    protocol.stopEventDelivery();
    await proxy.stop();
    proxy.store.close();
    fs.rmSync(dataDir, { recursive: true, force: true });
  }
});

test('pending Hub retries deduplicate input and stop without leaking timers', async () => {
  const proxy = new EvoMapProxy({
    hubUrl: 'https://example.invalid',
    logger: silentLogger(),
    hubEventRetryBaseMs: 25,
  });
  let writeAttempts = 0;
  proxy.store = {
    getById() { return null; },
    writeInbound() {
      writeAttempts++;
      throw new Error('mailbox unavailable');
    },
  };
  const event = { id: 'evt_pending_shutdown', type: 'dm', payload: { content: 'hello' } };

  assert.equal(proxy.acceptHubEvents([event]), 0);
  assert.equal(proxy.acceptHubEvents([event]), 0);
  assert.equal(writeAttempts, 1, 'duplicate pending ids must share one retry entry');
  assert.equal(proxy._pendingHubEvents.size, 1);
  assert.notEqual(proxy._hubEventRetryTimer, null);

  await proxy.stop();
  assert.equal(proxy._pendingHubEvents.size, 0);
  assert.equal(proxy._hubEventRetryTimer, null);

  await new Promise((resolve) => setTimeout(resolve, 40));
  assert.equal(writeAttempts, 1, 'stop must prevent the pending timer from writing');
});

test('proxy wall-clock drift replaces SSE and restores poll fallback', () => {
  const script = String.raw`
    const assert = require('node:assert/strict');
    const Module = require('node:module');
    const instances = [];
    const originalLoad = Module._load;
    class TestEventSource {
      constructor() { this.closed = false; instances.push(this); }
      addEventListener() {}
      removeEventListener() {}
      close() { this.closed = true; }
    }
    Module._load = function (request) {
      if (request === 'eventsource') return { EventSource: TestEventSource };
      return originalLoad.apply(this, arguments);
    };

    const hubFetch = require('./src/gep/hubFetch');
    let drainPoolCalls = 0;
    hubFetch.drainPool = () => { drainPoolCalls++; };
    const protocol = require('./src/gep/a2aProtocol');
    const { LifecycleManager } = require('./src/proxy/lifecycle/manager');
    const store = {
      getState(key) { return key === 'node_id' ? 'node_aaaaaaaaaaaa' : null; },
      setState() {}, countPending() { return 0; }, writeInbound() {}, writeInboundBatch() {},
    };
    let now = 1_000;
    let driftTick = null;
    const realNow = Date.now;
    const realSetInterval = global.setInterval;
    Date.now = () => now;
    global.setInterval = (fn) => { driftTick = fn; return { unref() {} }; };
    try {
      protocol.startEventDelivery({
        hubUrl: 'https://example.invalid',
        nodeId: 'node_aaaaaaaaaaaa',
      });
      assert.equal(instances.length, 1);
      instances[0].onopen();
      const manager = new LifecycleManager({
        hubUrl: 'https://example.invalid',
        store,
        logger: { log() {}, warn() {}, error() {} },
        onWake: () => protocol.recoverEventDeliveryAfterWake(),
      });
      manager._running = true;
      manager.pokeHeartbeatLoop = () => {};
      manager._startDriftDetector();
      now += 100_000;
      driftTick();

      assert.equal(instances[0].closed, true);
      assert.equal(instances.length, 2);
      assert.equal(drainPoolCalls, 1, 'proxy wake recovery must discard stale Hub connections');
      const state = protocol._testing._getHeartbeatInternalsForTesting();
      assert.equal(state.selfDrivingPollEnabled, true);
      assert.equal(state.hasSelfDrivingPollTimer, true);
      protocol.stopEventDelivery();
    } finally {
      Date.now = realNow;
      global.setInterval = realSetInterval;
      Module._load = originalLoad;
    }
  `;
  const result = spawnSync(process.execPath, ['-e', script], {
    cwd: path.resolve(__dirname, '..'),
    encoding: 'utf8',
    env: { ...process.env, NODE_ENV: 'test' },
  });
  assert.equal(result.status, 0, [result.stdout, result.stderr].filter(Boolean).join('\n'));
});
