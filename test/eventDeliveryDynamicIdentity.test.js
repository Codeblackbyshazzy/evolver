'use strict';

const assert = require('node:assert/strict');
const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');
const { test } = require('node:test');

function waitFor(predicate, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  return new Promise((resolve, reject) => {
    function check() {
      if (predicate()) return resolve();
      if (Date.now() >= deadline) return reject(new Error('timed out waiting for dynamic event identity'));
      setTimeout(check, 20);
    }
    check();
  });
}

function readJsonBody(req) {
  return new Promise((resolve, reject) => {
    let body = '';
    req.setEncoding('utf8');
    req.on('data', (chunk) => { body += chunk; });
    req.on('end', () => {
      try { resolve(body ? JSON.parse(body) : {}); } catch (error) { reject(error); }
    });
    req.on('error', reject);
  });
}

test('proxy event delivery follows lifecycle secret rotation and delayed node recovery', {
  timeout: 15_000,
}, async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-dynamic-delivery-'));
  const originalHome = process.env.EVOLVER_HOME;
  const originalAllowInsecure = process.env.EVOMAP_HUB_ALLOW_INSECURE;
  const legacySecret = '1'.repeat(64);
  const firstProxySecret = '2'.repeat(64);
  const rotatedProxySecret = '3'.repeat(64);
  const recoveredProxySecret = '4'.repeat(64);
  const stableNodeId = 'node_aaaaaaaaaaaa';
  const fallbackNodeId = 'node_ffffffffffff';
  const seen = { hello: [], stream: [], poll: [] };
  const openStreams = new Set();
  let phase = 'rotation';
  let rotationHelloCount = 0;
  let recoveryHelloCount = 0;
  let rotationStreamClosed = false;

  process.env.EVOMAP_HUB_ALLOW_INSECURE = '1';

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1');
    if (url.pathname === '/a2a/hello') {
      const body = await readJsonBody(req);
      seen.hello.push({ phase, nodeId: body.sender_id });
      if (phase === 'recovery' && recoveryHelloCount++ === 0) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: 'synthetic_first_hello_failure' }));
        return;
      }
      const secret = phase === 'rotation'
        ? (rotationHelloCount++ === 0 ? firstProxySecret : rotatedProxySecret)
        : recoveredProxySecret;
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({
        ok: true,
        payload: {
          node_secret: secret,
          node_secret_version: phase === 'rotation' ? rotationHelloCount : 1,
        },
      }));
      return;
    }

    if (url.pathname === '/a2a/events/stream') {
      seen.stream.push({
        phase,
        nodeId: url.searchParams.get('node_id'),
        authorization: req.headers.authorization || '',
      });
      openStreams.add(res);
      res.on('close', () => {
        openStreams.delete(res);
        if (phase === 'rotation') rotationStreamClosed = true;
      });
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      });
      res.write(': connected\n\n');
      return;
    }

    if (url.pathname === '/a2a/events/poll') {
      const body = await readJsonBody(req);
      seen.poll.push({
        phase,
        nodeId: body.sender_id,
        authorization: req.headers.authorization || '',
      });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ events: [], next_poll_after_ms: 60_000 }));
      return;
    }

    res.writeHead(404).end();
  });

  let protocol;
  try {
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const hubUrl = 'http://127.0.0.1:' + server.address().port;
    const { MailboxStore } = require('../src/proxy/mailbox/store');
    const { LifecycleManager } = require('../src/proxy/lifecycle/manager');
    protocol = require('../src/gep/a2aProtocol');

    const rotationHome = path.join(root, 'rotation-home');
    fs.mkdirSync(rotationHome, { recursive: true });
    fs.writeFileSync(path.join(rotationHome, 'node_secret'), legacySecret, { mode: 0o600 });
    fs.writeFileSync(path.join(rotationHome, 'node_secret_version'), '99', { mode: 0o600 });
    fs.writeFileSync(path.join(rotationHome, 'node_secret_source'), 'hub_rotate', { mode: 0o600 });
    process.env.EVOLVER_HOME = rotationHome;

    const rotationStore = new MailboxStore(path.join(rotationHome, 'mailbox'));
    rotationStore.setState('node_id', stableNodeId);
    const rotationLifecycle = new LifecycleManager({ hubUrl, store: rotationStore, logger: console });
    assert.equal((await rotationLifecycle.hello()).ok, true);

    protocol.startEventDelivery({
      hubUrl,
      nodeId: fallbackNodeId,
      identityProvider: {
        getNodeId: () => rotationLifecycle.nodeId,
        getHeaders: () => rotationLifecycle._buildHeaders(),
        subscribe: (listener) => rotationLifecycle.onDeliveryIdentityChange(listener),
      },
    });
    await waitFor(() => seen.stream.some((entry) => entry.phase === 'rotation'), 3000);

    assert.equal((await rotationLifecycle.hello({ rotateSecret: true })).ok, true);
    const rotationDeliveries = seen.stream.concat(seen.poll).filter((entry) => entry.phase === 'rotation');
    assert.ok(rotationDeliveries.some((entry) => entry.authorization === 'Bearer ' + firstProxySecret));
    assert.equal(rotationDeliveries.some((entry) => entry.authorization === 'Bearer ' + legacySecret), false);
    assert.equal(rotationDeliveries.every((entry) => entry.nodeId === stableNodeId), true);
    assert.equal(seen.poll.filter((entry) => entry.phase === 'rotation').length, 0,
      'healthy SSE must suppress the persistent long-poll channel');
    assert.equal(seen.stream.filter((entry) => entry.phase === 'rotation').length, 1,
      'same-node secret rotation must keep the connected SSE transport');
    assert.equal(rotationStreamClosed, false,
      'same-node secret rotation must not close the active SSE response');
    protocol.stopEventDelivery();

    phase = 'recovery';
    const recoveryHome = path.join(root, 'recovery-home');
    process.env.EVOLVER_HOME = recoveryHome;
    const recoveryStore = new MailboxStore(path.join(recoveryHome, 'mailbox'));
    const recoveryLifecycle = new LifecycleManager({ hubUrl, store: recoveryStore, logger: console });
    assert.equal((await recoveryLifecycle.hello()).ok, false);
    assert.equal(recoveryLifecycle.nodeId, null);

    protocol.startEventDelivery({
      hubUrl,
      nodeId: fallbackNodeId,
      identityProvider: {
        getNodeId: () => recoveryLifecycle.nodeId,
        getHeaders: () => recoveryLifecycle._buildHeaders(),
        subscribe: (listener) => recoveryLifecycle.onDeliveryIdentityChange(listener),
      },
    });
    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(seen.stream.some((entry) => entry.phase === 'recovery'), false);
    assert.equal(seen.poll.some((entry) => entry.phase === 'recovery'), false);

    assert.equal((await recoveryLifecycle.hello()).ok, true);
    const successfulNodeId = recoveryLifecycle.nodeId;
    await waitFor(() => seen.stream.some((entry) => entry.phase === 'recovery'), 3000);

    const recoveryHellos = seen.hello.filter((entry) => entry.phase === 'recovery');
    assert.equal(recoveryHellos.length, 2);
    assert.notEqual(recoveryHellos[0].nodeId, recoveryHellos[1].nodeId);
    const recoveryDeliveries = seen.stream.concat(seen.poll).filter((entry) => entry.phase === 'recovery');
    assert.equal(recoveryDeliveries.every((entry) => entry.nodeId === successfulNodeId), true);
    assert.equal(recoveryDeliveries.every((entry) => entry.nodeId !== fallbackNodeId), true);
    assert.equal(recoveryDeliveries.every((entry) => entry.authorization === 'Bearer ' + recoveredProxySecret), true);
    assert.equal(
      protocol._testing._getHeartbeatInternalsForTesting().selfDrivingPollEnabled,
      false,
      'recovered healthy SSE must pause persistent long-poll after any startup fallback request',
    );
  } finally {
    if (protocol) protocol.stopEventDelivery();
    for (const stream of openStreams) stream.destroy();
    await new Promise((resolve) => server.close(resolve));
    if (originalHome === undefined) delete process.env.EVOLVER_HOME;
    else process.env.EVOLVER_HOME = originalHome;
    if (originalAllowInsecure === undefined) delete process.env.EVOMAP_HUB_ALLOW_INSECURE;
    else process.env.EVOMAP_HUB_ALLOW_INSECURE = originalAllowInsecure;
    fs.rmSync(root, { recursive: true, force: true });
  }
});

test('repeated event delivery start replaces the connected SSE identity and subscription', {
  timeout: 10_000,
}, async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-delivery-restart-'));
  const originalHome = process.env.EVOLVER_HOME;
  const originalAllowInsecure = process.env.EVOMAP_HUB_ALLOW_INSECURE;
  const nodeA = 'node_111111111111';
  const nodeB = 'node_222222222222';
  const secretA = '5'.repeat(64);
  const secretB = '6'.repeat(64);
  const seen = { stream: [], poll: [] };
  const openStreams = new Set();
  let streamAClosed = false;
  let unsubscribeA = 0;
  let unsubscribeB = 0;

  process.env.EVOLVER_HOME = root;
  process.env.EVOMAP_HUB_ALLOW_INSECURE = '1';

  const server = http.createServer(async (req, res) => {
    const url = new URL(req.url, 'http://127.0.0.1');
    if (url.pathname === '/a2a/events/stream') {
      const entry = {
        nodeId: url.searchParams.get('node_id'),
        authorization: req.headers.authorization || '',
      };
      seen.stream.push(entry);
      openStreams.add(res);
      res.on('close', () => {
        openStreams.delete(res);
        if (entry.nodeId === nodeA) streamAClosed = true;
      });
      res.writeHead(200, {
        'Content-Type': 'text/event-stream',
        'Cache-Control': 'no-cache',
        Connection: 'keep-alive',
      });
      res.write(': connected\n\n');
      return;
    }
    if (url.pathname === '/a2a/events/poll') {
      const body = await readJsonBody(req);
      seen.poll.push({
        nodeId: body.sender_id,
        authorization: req.headers.authorization || '',
      });
      res.writeHead(200, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ events: [], next_poll_after_ms: 60_000 }));
      return;
    }
    res.writeHead(404).end();
  });

  const protocol = require('../src/gep/a2aProtocol');
  try {
    await new Promise((resolve) => server.listen(0, '127.0.0.1', resolve));
    const hubUrl = 'http://127.0.0.1:' + server.address().port;
    const providerA = {
      getNodeId: () => nodeA,
      getHeaders: () => ({ Authorization: 'Bearer ' + secretA }),
      subscribe: () => () => { unsubscribeA += 1; },
    };
    const providerB = {
      getNodeId: () => nodeB,
      getHeaders: () => ({ Authorization: 'Bearer ' + secretB }),
      subscribe: () => () => { unsubscribeB += 1; },
    };

    protocol.startEventDelivery({ hubUrl, identityProvider: providerA });
    await protocol._testing._runSelfDrivingPollForTesting();
    await waitFor(() => seen.stream.some((entry) => entry.nodeId === nodeA) &&
      seen.poll.some((entry) => entry.nodeId === nodeA), 3000);

    protocol.startEventDelivery({ hubUrl, identityProvider: providerB });
    await protocol._testing._runSelfDrivingPollForTesting();
    await waitFor(() => streamAClosed && seen.stream.some((entry) => entry.nodeId === nodeB) &&
      seen.poll.some((entry) => entry.nodeId === nodeB), 3000);

    assert.equal(unsubscribeA, 1, 'replacing provider A must unsubscribe it exactly once');
    const streamB = seen.stream.find((entry) => entry.nodeId === nodeB);
    const pollB = seen.poll.find((entry) => entry.nodeId === nodeB);
    assert.equal(streamB.authorization, 'Bearer ' + secretB);
    assert.equal(pollB.authorization, 'Bearer ' + secretB);
    assert.equal(seen.stream.filter((entry) => entry.nodeId === nodeA).length, 1);

    protocol.stopEventDelivery();
    assert.equal(unsubscribeB, 1, 'stopping provider B must unsubscribe it exactly once');
  } finally {
    protocol.stopEventDelivery();
    for (const stream of openStreams) stream.destroy();
    await new Promise((resolve) => server.close(resolve));
    if (originalHome === undefined) delete process.env.EVOLVER_HOME;
    else process.env.EVOLVER_HOME = originalHome;
    if (originalAllowInsecure === undefined) delete process.env.EVOMAP_HUB_ALLOW_INSECURE;
    else process.env.EVOMAP_HUB_ALLOW_INSECURE = originalAllowInsecure;
    fs.rmSync(root, { recursive: true, force: true });
  }
});
