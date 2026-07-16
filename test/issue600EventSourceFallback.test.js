const assert = require('node:assert/strict');
const fs = require('node:fs');
const os = require('node:os');
const path = require('node:path');
const { spawnSync } = require('node:child_process');
const { test } = require('node:test');

test('uses the packaged EventSource runtime without reading host credentials', () => {
  const evolverHome = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-issue600-'));
  const testSecret = '6'.repeat(64);
  const childScript = String.raw`
    const assert = require('node:assert/strict');
    const Module = require('node:module');

    delete globalThis.EventSource;

    let captured = null;
    let closed = false;
    let fetchPromise = null;
    const originalLoad = Module._load;
    Module._load = function (request, parent, isMain) {
      if (request === 'eventsource') {
        return {
          EventSource: class TestEventSource {
            constructor(url, options) {
              captured = { url: String(url), options: options || {} };
              fetchPromise = options.fetch(url, {
                headers: new Headers({ Accept: 'text/event-stream' }),
              });
            }

            close() {
              closed = true;
            }
          },
        };
      }

      const loaded = originalLoad.apply(this, arguments);
      if (request === './hubFetch' && parent && /src[\\/]gep[\\/]a2aProtocol\.js$/.test(parent.filename)) {
        return Object.assign({}, loaded, {
          hubEventStreamFetch: async function (url, options) {
            return {
              url: String(url),
              headers: Object.fromEntries(new Headers(options.headers).entries()),
            };
          },
        });
      }
      return loaded;
    };

    (async function () {
      const { hubOpenEventStream } = require('./src/gep/a2aProtocol');
      const result = hubOpenEventStream({ nodeId: 'issue600-node', durationMs: 12345 });

      assert.equal(result.ok, true, result.error || 'expected the SSE stream to open');
      assert.ok(captured, 'the packaged EventSource constructor should be called');
      assert.equal(
        captured.url,
        'https://example.invalid/base/a2a/events/stream?node_id=issue600-node&duration_ms=12345',
      );
      assert.equal(typeof captured.options.fetch, 'function');

      const request = await fetchPromise;
      assert.equal(request.url, captured.url);
      assert.deepEqual(request.headers, {
        accept: 'text/event-stream',
        authorization: 'Bearer ' + process.env.A2A_NODE_SECRET,
        'x-evomap-node-secret-version': '8',
      });

      result.close();
      assert.equal(closed, true);
    })().catch(function (error) {
      console.error(error && error.stack ? error.stack : error);
      process.exitCode = 1;
    });
  `;

  try {
    const result = spawnSync(process.execPath, ['-e', childScript], {
      cwd: path.resolve(__dirname, '..'),
      encoding: 'utf8',
      env: {
        PATH: process.env.PATH || '',
        NODE_PATH: process.env.NODE_PATH || '',
        HOME: evolverHome,
        EVOLVER_HOME: evolverHome,
        A2A_HUB_URL: 'https://example.invalid/base',
        A2A_NODE_SECRET: testSecret,
        A2A_NODE_SECRET_VERSION: '8',
        NODE_ENV: 'test',
      },
    });

    assert.equal(
      result.status,
      0,
      ['isolated issue #600 probe failed', result.stdout, result.stderr].filter(Boolean).join('\n'),
    );
    assert.equal(result.stdout.includes(testSecret), false, 'test secret must not be logged');
    assert.equal(result.stderr.includes(testSecret), false, 'test secret must not be logged');
  } finally {
    fs.rmSync(evolverHome, { recursive: true, force: true });
  }
});

test('scopes proxy node identity to managed SSE and poll delivery', () => {
  const evolverHome = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-issue600-node-scope-'));
  const legacyNodeId = 'node_111111111111';
  const storeNodeId = 'node_222222222222';
  fs.writeFileSync(path.join(evolverHome, 'node_id'), legacyNodeId, { mode: 0o600 });

  const childScript = String.raw`
    const assert = require('node:assert/strict');
    const http = require('node:http');

    (async function () {
      const legacyNodeId = 'node_111111111111';
      const storeNodeId = 'node_222222222222';
      const seen = { stream: [], poll: [] };
      const rejected = [];

      const server = http.createServer(function (req, res) {
        const url = new URL(req.url, 'http://127.0.0.1');
        if (url.pathname === '/a2a/events/stream') {
          const nodeId = url.searchParams.get('node_id');
          seen.stream.push(nodeId);
          if (nodeId !== storeNodeId) {
            rejected.push(nodeId);
            res.writeHead(401).end();
            return;
          }
          res.writeHead(200, {
            'Content-Type': 'text/event-stream',
            'Cache-Control': 'no-cache',
            Connection: 'keep-alive',
          });
          res.write(': connected\\n\\n');
          return;
        }

        if (url.pathname === '/a2a/events/poll') {
          let body = '';
          req.setEncoding('utf8');
          req.on('data', function (chunk) { body += chunk; });
          req.on('end', function () {
            const senderId = JSON.parse(body).sender_id;
            seen.poll.push(senderId);
            if (senderId !== storeNodeId) {
              rejected.push(senderId);
              res.writeHead(401).end();
              return;
            }
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ events: [], next_poll_after_ms: 60000 }));
          });
          return;
        }

        res.writeHead(404).end();
      });
      await new Promise(function (resolve) { server.listen(0, '127.0.0.1', resolve); });
      const hubUrl = 'http://127.0.0.1:' + server.address().port;

      const protocol = require('./src/gep/a2aProtocol');
      assert.equal(protocol.getNodeId(), legacyNodeId, 'precondition: legacy ID must be cached');
      protocol.startEventDelivery({ hubUrl, nodeId: storeNodeId });
      protocol._testing._runSelfDrivingPollForTesting();

      const deadline = Date.now() + 3000;
      while (Date.now() < deadline && (seen.stream.length === 0 || seen.poll.length === 0)) {
        await new Promise(function (resolve) { setTimeout(resolve, 20); });
      }

      protocol.stopEventDelivery();
      await new Promise(function (resolve) { server.close(resolve); });
      assert.deepEqual(seen.stream, [storeNodeId], 'SSE query must use the proxy store node ID');
      assert.deepEqual(seen.poll, [storeNodeId], 'poll sender_id must use the proxy store node ID');
      assert.deepEqual(rejected, [], 'the Hub must not observe the cached legacy node ID');
      assert.equal(seen.stream.includes(legacyNodeId), false);
      assert.equal(seen.poll.includes(legacyNodeId), false);
    })().catch(function (error) {
      console.error(error && error.stack ? error.stack : error);
      process.exitCode = 1;
    });
  `;

  try {
    const result = spawnSync(process.execPath, ['-e', childScript], {
      cwd: path.resolve(__dirname, '..'),
      encoding: 'utf8',
      env: {
        PATH: process.env.PATH || '',
        NODE_PATH: process.env.NODE_PATH || '',
        HOME: evolverHome,
        EVOLVER_HOME: evolverHome,
        EVOMAP_HUB_ALLOW_INSECURE: '1',
        NODE_ENV: 'test',
      },
    });

    assert.equal(
      result.status,
      0,
      ['isolated proxy node identity probe failed', result.stdout, result.stderr].filter(Boolean).join('\n'),
    );
  } finally {
    fs.rmSync(evolverHome, { recursive: true, force: true });
  }
});

test('passes the proxy store node identity into event delivery startup', () => {
  const index = fs.readFileSync(path.resolve(__dirname, '..', 'index.js'), 'utf8');
  assert.match(
    index,
    /startEventDelivery\(\{[\s\S]*?hubUrl:[\s\S]*?nodeId: proxyInfo && proxyInfo\.nodeId,[\s\S]*?enableSse: parseBoolEnv\(process\.env\.EVOLVER_PROXY_SSE_ENABLED, true\),[\s\S]*?\}\)/,
  );
});

test('proxy SSE env accepts common boolean values and defaults to enabled', () => {
  const { parseBoolEnv } = require('..');

  assert.equal(parseBoolEnv(undefined, true), true, 'unset must default to enabled');
  for (const value of ['true', 'on', 'yes']) {
    assert.equal(parseBoolEnv(value, true), true, value + ' must enable proxy SSE');
  }
  for (const value of ['false', 'off', 'no', '0']) {
    assert.equal(parseBoolEnv(value, true), false, value + ' must disable proxy SSE');
  }
});

test('proxy delivery defaults to SSE and preserves an explicit opt-out', () => {
  const evolverHome = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-proxy-sse-opt-out-'));
  const childScript = String.raw`
    const assert = require('node:assert/strict');
    const Module = require('node:module');

    const instances = [];
    const originalLoad = Module._load;
    class TestEventSource {
      constructor(url) {
        this.url = String(url);
        this.closed = false;
        this.onmessage = null;
        this.onerror = null;
        instances.push(this);
      }
      addEventListener() {}
      removeEventListener() {}
      close() { this.closed = true; }
    }
    Module._load = function (request, parent, isMain) {
      if (request === 'eventsource') return { EventSource: TestEventSource };
      return originalLoad.apply(this, arguments);
    };

    try {
      let nodeId = 'node_aaaaaaaaaaaa';
      let notifyIdentityChanged = null;
      const identityProvider = {
        getNodeId: function () { return nodeId; },
        getHeaders: function () { return {}; },
        subscribe: function (listener) {
          notifyIdentityChanged = listener;
          return function () { notifyIdentityChanged = null; };
        },
      };
      const protocol = require('./src/gep/a2aProtocol');

      protocol.startEventDelivery({
        hubUrl: 'https://example.invalid',
        identityProvider,
      });
      assert.equal(instances.length, 1, 'default proxy delivery must open SSE');
      assert.equal(
        protocol._testing._getHeartbeatInternalsForTesting().selfDrivingPollEnabled,
        true,
        'default proxy delivery must still start the self-driving poll fallback',
      );

      nodeId = 'node_bbbbbbbbbbbb';
      notifyIdentityChanged();
      assert.equal(instances.length, 2, 'identity changes must restart default SSE delivery');
      assert.equal(instances[0].closed, true, 'identity changes must close the previous stream');

      protocol.startEventDelivery({
        hubUrl: 'https://example.invalid',
        identityProvider,
        enableSse: false,
      });
      assert.equal(instances.length, 2, 'explicit opt-out must not open another SSE stream');
      assert.equal(instances[1].closed, true, 'explicit opt-out must close the active stream');

      nodeId = 'node_cccccccccccc';
      notifyIdentityChanged();
      assert.equal(instances.length, 2, 'identity changes must preserve the explicit opt-out');
      protocol.stopEventDelivery();

      protocol.startEventStream();
      assert.equal(instances.length, 3, 'normal explicit SSE startup must remain unchanged');
      protocol.stopEventStream();
    } finally {
      Module._load = originalLoad;
    }
  `;

  try {
    const result = spawnSync(process.execPath, ['-e', childScript], {
      cwd: path.resolve(__dirname, '..'),
      encoding: 'utf8',
      env: {
        PATH: process.env.PATH || '',
        NODE_PATH: process.env.NODE_PATH || '',
        HOME: evolverHome,
        EVOLVER_HOME: evolverHome,
        A2A_HUB_URL: 'https://example.invalid',
        NODE_ENV: 'test',
      },
    });
    assert.equal(
      result.status,
      0,
      ['isolated proxy SSE default/opt-out probe failed', result.stdout, result.stderr].filter(Boolean).join('\n'),
    );
  } finally {
    fs.rmSync(evolverHome, { recursive: true, force: true });
  }
});

test('buffers named Hub events from the packaged EventSource runtime', () => {
  const evolverHome = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-issue600-named-'));
  const childScript = String.raw`
    const assert = require('node:assert/strict');
    const http = require('node:http');

    (async function () {
      const server = http.createServer(function (req, res) {
        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          Connection: 'keep-alive',
        });
        res.write('event: work_assigned\n');
        res.write('data: {"type":"work_assigned","payload":{"task_id":"task-600"}}\n\n');
      });
      await new Promise(function (resolve) { server.listen(0, '127.0.0.1', resolve); });
      process.env.A2A_HUB_URL = 'http://127.0.0.1:' + server.address().port;

      const protocol = require('./src/gep/a2aProtocol');
      protocol.startEventStream();

      const deadline = Date.now() + 3000;
      let events = [];
      while (Date.now() < deadline && events.length === 0) {
        await new Promise(function (resolve) { setTimeout(resolve, 20); });
        events = protocol.consumeHubEvents();
      }

      protocol.stopEventStream();
      await new Promise(function (resolve) { server.close(resolve); });
      assert.deepEqual(events, [{
        type: 'work_assigned',
        payload: { task_id: 'task-600' },
      }]);
    })().catch(function (error) {
      console.error(error && error.stack ? error.stack : error);
      process.exitCode = 1;
    });
  `;

  try {
    const result = spawnSync(process.execPath, ['-e', childScript], {
      cwd: path.resolve(__dirname, '..'),
      encoding: 'utf8',
      env: {
        PATH: process.env.PATH || '',
        NODE_PATH: process.env.NODE_PATH || '',
        HOME: evolverHome,
        EVOLVER_HOME: evolverHome,
        EVOMAP_HUB_ALLOW_INSECURE: '1',
        NODE_ENV: 'test',
      },
    });

    assert.equal(
      result.status,
      0,
      ['isolated named SSE probe failed', result.stdout, result.stderr].filter(Boolean).join('\n'),
    );
  } finally {
    fs.rmSync(evolverHome, { recursive: true, force: true });
  }
});

test('HTTP 204 stops managed SSE reconnects across wake recovery until explicitly restarted', () => {
  const evolverHome = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sse-204-'));
  const childScript = String.raw`
    const assert = require('node:assert/strict');
    const http = require('node:http');
    const Module = require('node:module');

    const originalLoad = Module._load;
    Module._load = function (request, parent, isMain) {
      if (request === 'eventsource') {
        throw new Error('synthetic_eventsource_unavailable');
      }
      return originalLoad.apply(this, arguments);
    };

    (async function () {
      let streamRequestCount = 0;
      let pollRequestCount = 0;
      const server = http.createServer(function (req, res) {
        const url = new URL(req.url, 'http://127.0.0.1');
        if (url.pathname === '/a2a/events/poll') {
          pollRequestCount += 1;
          req.resume();
          req.on('end', function () {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ events: [], next_poll_after_ms: 60000 }));
          });
          return;
        }
        if (url.pathname !== '/a2a/events/stream') {
          res.writeHead(404).end();
          return;
        }

        streamRequestCount += 1;
        if (streamRequestCount === 1) {
          res.writeHead(204);
          res.end();
          return;
        }

        res.writeHead(200, {
          'Content-Type': 'text/event-stream',
          'Cache-Control': 'no-cache',
          Connection: 'keep-alive',
        });
        res.end('data: {"type":"task_available","payload":{"source":"explicit-restart"}}\n\n');
      });
      await new Promise(function (resolve) { server.listen(0, '127.0.0.1', resolve); });
      process.env.A2A_HUB_URL = 'http://127.0.0.1:' + server.address().port;

      const protocol = require('./src/gep/a2aProtocol');
      protocol._testing._startSelfDrivingPollForTesting();
      protocol.startEventStream();

      const stopDeadline = Date.now() + 3000;
      while (Date.now() < stopDeadline && protocol.isEventStreamActive()) {
        await new Promise(function (resolve) { setTimeout(resolve, 20); });
      }
      assert.equal(protocol.isEventStreamActive(), false, 'HTTP 204 should clear the active stream');

      await new Promise(function (resolve) { setTimeout(resolve, 5250); });
      assert.equal(streamRequestCount, 1, 'HTTP 204 should not schedule the managed 5s reconnect');
      assert.ok(pollRequestCount > 0, 'HTTP 204 should leave long-poll available as the fallback');

      protocol._runWakeRecovery();
      await new Promise(function (resolve) { setTimeout(resolve, 250); });
      assert.equal(streamRequestCount, 1, 'wake recovery must preserve the HTTP 204 server stop');
      assert.equal(protocol.isEventStreamActive(), false, 'wake recovery must not reactivate a server-stopped stream');

      protocol.startEventStream();
      const eventDeadline = Date.now() + 3000;
      let events = [];
      while (Date.now() < eventDeadline && events.length === 0) {
        await new Promise(function (resolve) { setTimeout(resolve, 20); });
        events = protocol.consumeHubEvents();
      }

      protocol.stopEventDelivery();
      await new Promise(function (resolve) { server.close(resolve); });
      assert.equal(streamRequestCount, 2, 'an explicit restart should open a new stream');
      assert.deepEqual(events, [{
        type: 'task_available',
        payload: { source: 'explicit-restart' },
      }]);
    })().catch(function (error) {
      console.error(error && error.stack ? error.stack : error);
      process.exitCode = 1;
    });
  `;

  try {
    const result = spawnSync(process.execPath, ['-e', childScript], {
      cwd: path.resolve(__dirname, '..'),
      encoding: 'utf8',
      env: {
        PATH: process.env.PATH || '',
        NODE_PATH: process.env.NODE_PATH || '',
        HOME: evolverHome,
        EVOLVER_HOME: evolverHome,
        EVOMAP_HUB_ALLOW_INSECURE: '1',
        NODE_ENV: 'test',
      },
    });

    assert.equal(
      result.status,
      0,
      ['isolated HTTP 204 SSE probe failed', result.stdout, result.stderr].filter(Boolean).join('\n'),
    );
  } finally {
    fs.rmSync(evolverHome, { recursive: true, force: true });
  }
});

test('restarts a server-stopped SSE stream only when the identity delivery scope changes', () => {
  const evolverHome = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sse-identity-scope-'));
  const childScript = String.raw`
    const assert = require('node:assert/strict');
    const Module = require('node:module');

    const instances = [];
    const originalLoad = Module._load;

    class TestEventSource {
      constructor(url) {
        this.url = String(url);
        this.closed = false;
        this.onmessage = null;
        this.onerror = null;
        instances.push(this);
      }

      addEventListener() {}
      removeEventListener() {}
      close() { this.closed = true; }
    }

    Module._load = function (request, parent, isMain) {
      if (request === 'eventsource') return { EventSource: TestEventSource };
      return originalLoad.apply(this, arguments);
    };

    try {
      let nodeId = 'node_aaaaaaaaaaaa';
      let secret = 'a'.repeat(64);
      let notifyIdentityChanged = null;
      const identityProvider = {
        getNodeId: function () { return nodeId; },
        getHeaders: function () { return { Authorization: 'Bearer ' + secret }; },
        subscribe: function (listener) {
          notifyIdentityChanged = listener;
          return function () { notifyIdentityChanged = null; };
        },
      };

      const protocol = require('./src/gep/a2aProtocol');
      protocol.startEventDelivery({
        hubUrl: 'https://example.invalid',
        identityProvider,
      });
      assert.equal(instances.length, 1);
      assert.equal(new URL(instances[0].url).searchParams.get('node_id'), nodeId);

      instances[0].onerror({ code: 204 });
      assert.equal(protocol.isEventStreamActive(), false);
      assert.equal(instances.length, 1, 'HTTP 204 must not automatically reconnect node A');

      secret = 'b'.repeat(64);
      notifyIdentityChanged();
      assert.equal(instances.length, 1, 'same-node secret rotation must not restart SSE');

      nodeId = 'node_bbbbbbbbbbbb';
      notifyIdentityChanged();
      assert.equal(instances.length, 2, 'node A to B must create exactly one new SSE stream');
      assert.equal(new URL(instances[1].url).searchParams.get('node_id'), nodeId);
      assert.equal(protocol.isEventStreamActive(), true);

      secret = 'c'.repeat(64);
      notifyIdentityChanged();
      assert.equal(instances.length, 2, 'same-node secret rotation must leave node B SSE connected');
      assert.equal(instances[1].closed, false);

      protocol.stopEventDelivery();
      assert.equal(instances[1].closed, true);
    } finally {
      Module._load = originalLoad;
    }
  `;

  try {
    const result = spawnSync(process.execPath, ['-e', childScript], {
      cwd: path.resolve(__dirname, '..'),
      encoding: 'utf8',
      env: {
        PATH: process.env.PATH || '',
        NODE_PATH: process.env.NODE_PATH || '',
        HOME: evolverHome,
        EVOLVER_HOME: evolverHome,
        EVOLVER_DISABLE_SELF_DRIVING_POLL: '1',
        NODE_ENV: 'test',
      },
    });

    assert.equal(
      result.status,
      0,
      ['isolated identity scope SSE probe failed', result.stdout, result.stderr].filter(Boolean).join('\n'),
    );
  } finally {
    fs.rmSync(evolverHome, { recursive: true, force: true });
  }
});

test('ignores delayed errors from stopped or replaced EventSource instances', () => {
  const evolverHome = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sse-stale-error-'));
  const childScript = String.raw`
    const assert = require('node:assert/strict');
    const Module = require('node:module');

    const instances = [];
    const scheduledDelays = [];
    const originalLoad = Module._load;
    const originalSetTimeout = global.setTimeout;

    class TestEventSource {
      constructor() {
        this.closed = false;
        this.onmessage = null;
        this.onerror = null;
        instances.push(this);
      }

      addEventListener() {}
      removeEventListener() {}
      close() { this.closed = true; }
    }

    Module._load = function (request, parent, isMain) {
      if (request === 'eventsource') return { EventSource: TestEventSource };
      return originalLoad.apply(this, arguments);
    };
    global.setTimeout = function (_fn, delay) {
      scheduledDelays.push(delay);
      return { unref: function () {} };
    };

    try {
      const protocol = require('./src/gep/a2aProtocol');
      const options = {
        hubUrl: 'https://example.invalid',
        nodeId: 'node_aaaaaaaaaaaa',
      };

      protocol.startEventDelivery(options);
      assert.equal(instances.length, 1);
      const firstError = instances[0].onerror;

      protocol.startEventDelivery(options);
      assert.equal(instances.length, 2);
      assert.equal(instances[0].closed, true);
      const secondError = instances[1].onerror;

      firstError(new Error('stale ordinary error'));
      assert.equal(protocol.isEventStreamActive(), true);
      assert.equal(instances[1].closed, false);
      assert.deepEqual(scheduledDelays, []);

      protocol.startEventDelivery(options);
      assert.equal(instances.length, 3);
      assert.equal(instances[1].closed, true);
      const thirdError = instances[2].onerror;

      secondError({ code: 204 });
      assert.equal(protocol.isEventStreamActive(), true);
      assert.equal(instances[2].closed, false);
      assert.deepEqual(scheduledDelays, []);

      protocol.stopEventDelivery();
      assert.equal(protocol.isEventStreamActive(), false);
      thirdError(new Error('stale error after stop'));
      assert.equal(protocol.isEventStreamActive(), false);
      assert.deepEqual(scheduledDelays, []);
    } finally {
      global.setTimeout = originalSetTimeout;
      Module._load = originalLoad;
    }
  `;

  try {
    const result = spawnSync(process.execPath, ['-e', childScript], {
      cwd: path.resolve(__dirname, '..'),
      encoding: 'utf8',
      env: {
        PATH: process.env.PATH || '',
        NODE_PATH: process.env.NODE_PATH || '',
        HOME: evolverHome,
        EVOLVER_HOME: evolverHome,
        EVOLVER_DISABLE_SELF_DRIVING_POLL: '1',
        NODE_ENV: 'test',
      },
    });

    assert.equal(
      result.status,
      0,
      ['isolated stale SSE error probe failed', result.stdout, result.stderr].filter(Boolean).join('\n'),
    );
  } finally {
    fs.rmSync(evolverHome, { recursive: true, force: true });
  }
});

test('uses packaged EventSource as the sole persistent delivery channel while healthy', () => {
  const evolverHome = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sse-poll-primary-'));
  const childScript = String.raw`
    const assert = require('node:assert/strict');
    const Module = require('node:module');

    const instances = [];
    const originalLoad = Module._load;
    class TestEventSource {
      constructor() {
        this.closed = false;
        this.onopen = null;
        this.onmessage = null;
        this.onerror = null;
        instances.push(this);
      }
      addEventListener() {}
      removeEventListener() {}
      close() { this.closed = true; }
    }

    Module._load = function (request, parent, isMain) {
      if (request === 'eventsource') return { EventSource: TestEventSource };
      const loaded = originalLoad.apply(this, arguments);
      if (request === './hubFetch' && parent && /src[\\/]gep[\\/]a2aProtocol\.js$/.test(parent.filename)) {
        return Object.assign({}, loaded, {
          hubFetch: async function (url) {
            if (String(url).includes('/a2a/hello')) {
              return {
                ok: true,
                status: 200,
                json: async function () { return { status: 'ok' }; },
              };
            }
            if (String(url).includes('/a2a/events/poll')) {
              return {
                ok: true,
                status: 200,
                json: async function () { return { events: [], next_poll_after_ms: 60000 }; },
              };
            }
            throw new Error('unexpected request: ' + url);
          },
        });
      }
      return loaded;
    };

    (async function () {
      const protocol = require('./src/gep/a2aProtocol');
      const options = {
        hubUrl: 'https://example.invalid',
        nodeId: 'node_aaaaaaaaaaaa',
      };

      protocol.startEventDelivery(options);
      assert.equal(instances.length, 1);
      assert.equal(
        protocol._testing._getHeartbeatInternalsForTesting().selfDrivingPollEnabled,
        true,
        'poll must cover the connection until EventSource actually opens',
      );

      instances[0].onopen({ type: 'open' });
      let state = protocol._testing._getHeartbeatInternalsForTesting();
      assert.equal(state.selfDrivingPollEnabled, false, 'healthy SSE must stop persistent poll');
      assert.equal(state.hasSelfDrivingPollTimer, false, 'healthy SSE must clear the next poll timer');

      protocol.startHeartbeat(60000);
      await new Promise(function (resolve) { setImmediate(resolve); });
      await new Promise(function (resolve) { setImmediate(resolve); });
      state = protocol._testing._getHeartbeatInternalsForTesting();
      assert.equal(
        state.selfDrivingPollEnabled,
        false,
        'startup hello completion must not restart poll behind healthy SSE',
      );

      protocol._runWakeRecovery();
      assert.equal(instances.length, 2, 'wake recovery must create a replacement EventSource');
      assert.equal(instances[0].closed, true, 'wake recovery must close the stale EventSource');
      state = protocol._testing._getHeartbeatInternalsForTesting();
      assert.equal(
        state.selfDrivingPollEnabled,
        true,
        'poll must cover wake recovery until the replacement EventSource opens',
      );
      assert.equal(state.hasSelfDrivingPollTimer, true, 'wake recovery must schedule poll immediately');

      instances[1].onopen({ type: 'open' });
      state = protocol._testing._getHeartbeatInternalsForTesting();
      assert.equal(state.selfDrivingPollEnabled, false, 'healthy replacement SSE must stop poll again');
      assert.equal(state.hasSelfDrivingPollTimer, false);

      const secondError = instances[1].onerror;
      secondError(new Error('stream disconnected'));
      state = protocol._testing._getHeartbeatInternalsForTesting();
      assert.equal(state.selfDrivingPollEnabled, true, 'disconnect must restore poll fallback');
      assert.equal(state.hasSelfDrivingPollTimer, true, 'disconnect must schedule poll immediately');

      protocol.startEventStream();
      assert.equal(instances.length, 3, 'reconnect must create a new EventSource');
      instances[2].onopen({ type: 'open' });
      state = protocol._testing._getHeartbeatInternalsForTesting();
      assert.equal(state.selfDrivingPollEnabled, false, 'successful reconnect must stop poll again');
      assert.equal(state.hasSelfDrivingPollTimer, false);

      protocol.stopHeartbeat();
      protocol.stopEventDelivery();
    })().catch(function (error) {
      console.error(error && error.stack ? error.stack : error);
      process.exitCode = 1;
    }).finally(function () {
      Module._load = originalLoad;
    });
  `;

  try {
    const result = spawnSync(process.execPath, ['-e', childScript], {
      cwd: path.resolve(__dirname, '..'),
      encoding: 'utf8',
      env: {
        PATH: process.env.PATH || '',
        NODE_PATH: process.env.NODE_PATH || '',
        HOME: evolverHome,
        EVOLVER_HOME: evolverHome,
        A2A_HUB_URL: 'https://example.invalid',
        A2A_NODE_ID: 'node_aaaaaaaaaaaa',
        A2A_NODE_SECRET: '7'.repeat(64),
        NODE_ENV: 'test',
      },
    });
    assert.equal(
      result.status,
      0,
      ['isolated primary SSE/poll lifecycle probe failed', result.stdout, result.stderr].filter(Boolean).join('\n'),
    );
  } finally {
    fs.rmSync(evolverHome, { recursive: true, force: true });
  }
});

test('wake recovery replaces quiet poll timers and leaves the SSE fallback at 0ms', () => {
  const evolverHome = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sse-wake-poll-timer-'));
  const childScript = String.raw`
    const assert = require('node:assert/strict');
    const Module = require('node:module');

    const instances = [];
    const scheduled = [];
    const originalLoad = Module._load;
    const originalSetTimeout = global.setTimeout;
    const originalClearTimeout = global.clearTimeout;

    class TestEventSource {
      constructor() {
        this.closed = false;
        this.onopen = null;
        this.onmessage = null;
        this.onerror = null;
        instances.push(this);
      }
      addEventListener() {}
      removeEventListener() {}
      close() { this.closed = true; }
    }

    Module._load = function (request, parent, isMain) {
      if (request === 'eventsource') return { EventSource: TestEventSource };
      const loaded = originalLoad.apply(this, arguments);
      if (request === './hubFetch' && parent && /src[\\/]gep[\\/]a2aProtocol\.js$/.test(parent.filename)) {
        return Object.assign({}, loaded, {
          drainPool: function () {},
          hubFetch: async function () {
            throw new Error('poll request must stay behind its fake timer');
          },
        });
      }
      return loaded;
    };

    global.setTimeout = function (fn, delay) {
      const timer = {
        fn: fn,
        delay: delay,
        cleared: false,
        unref: function () {},
      };
      scheduled.push(timer);
      return timer;
    };
    global.clearTimeout = function (timer) {
      if (timer && Object.prototype.hasOwnProperty.call(timer, 'cleared')) {
        timer.cleared = true;
        return;
      }
      originalClearTimeout(timer);
    };

    try {
      const protocol = require('./src/gep/a2aProtocol');
      const options = {
        hubUrl: 'https://example.invalid',
        nodeId: 'node_aaaaaaaaaaaa',
      };
      const quietCases = [
        { name: 'unknown_node', state: { unknownNodeBackoffUntil: Date.now() + 60_000 } },
        { name: 'reauth', state: { reauthBackoffUntil: Date.now() + 60_000 } },
      ];

      for (const quietCase of quietCases) {
        protocol.startEventDelivery(options);
        protocol._testing._setHeartbeatStateForTesting(Object.assign({
          unknownNodeBackoffUntil: 0,
          reauthBackoffUntil: 0,
        }, quietCase.state));
        protocol._testing._runSelfDrivingPollForTesting();

        let active = scheduled.filter(function (timer) { return !timer.cleared; });
        assert.deepEqual(
          active.map(function (timer) { return timer.delay; }),
          [300000],
          quietCase.name + ' must first arm the five-minute quiet timer',
        );

        protocol._testing._setHeartbeatStateForTesting({
          unknownNodeBackoffUntil: 0,
          reauthBackoffUntil: 0,
        });
        const wakeScheduleStart = scheduled.length;
        protocol._runWakeRecovery();

        assert.deepEqual(
          scheduled.slice(wakeScheduleStart).map(function (timer) { return timer.delay; }),
          [1000, 0],
          quietCase.name + ' wake recovery must shorten quiet mode before arming immediate SSE fallback',
        );
        active = scheduled.filter(function (timer) { return !timer.cleared; });
        assert.deepEqual(
          active.map(function (timer) { return timer.delay; }),
          [0],
          quietCase.name + ' final poll timer must stay at 0ms, not be overwritten by the 1s re-arm',
        );

        protocol.stopEventDelivery();
        protocol._testing._resetHeartbeatStateForTesting();
      }
    } finally {
      global.setTimeout = originalSetTimeout;
      global.clearTimeout = originalClearTimeout;
      Module._load = originalLoad;
    }
  `;

  try {
    const result = spawnSync(process.execPath, ['-e', childScript], {
      cwd: path.resolve(__dirname, '..'),
      encoding: 'utf8',
      env: {
        PATH: process.env.PATH || '',
        NODE_PATH: process.env.NODE_PATH || '',
        HOME: evolverHome,
        EVOLVER_HOME: evolverHome,
        A2A_HUB_URL: 'https://example.invalid',
        A2A_NODE_ID: 'node_aaaaaaaaaaaa',
        A2A_NODE_SECRET: '7'.repeat(64),
        NODE_ENV: 'test',
      },
    });
    assert.equal(
      result.status,
      0,
      ['isolated wake poll timer probe failed', result.stdout, result.stderr].filter(Boolean).join('\n'),
    );
  } finally {
    fs.rmSync(evolverHome, { recursive: true, force: true });
  }
});

test('uses fetch fallback SSE as the sole persistent channel and resumes poll on EOF', () => {
  const evolverHome = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sse-poll-fetch-'));
  const childScript = String.raw`
    const assert = require('node:assert/strict');
    const http = require('node:http');
    const Module = require('node:module');

    const originalLoad = Module._load;
    Module._load = function (request, parent, isMain) {
      if (request === 'eventsource') throw new Error('force fetch fallback');
      return originalLoad.apply(this, arguments);
    };

    (async function () {
      let streamCount = 0;
      let pollCount = 0;
      let activeStreamResponse = null;
      const streamResponses = [];
      const server = http.createServer(function (req, res) {
        const url = new URL(req.url, 'http://127.0.0.1');
        if (url.pathname === '/a2a/events/stream') {
          streamCount++;
          activeStreamResponse = res;
          streamResponses.push(res);
          res.writeHead(200, {
            'Content-Type': 'text/event-stream; charset=utf-8',
            'Cache-Control': 'no-cache',
            Connection: 'keep-alive',
          });
          res.write(': connected\\n\\n');
          return;
        }
        if (url.pathname === '/a2a/events/poll') {
          pollCount++;
          req.resume();
          req.on('end', function () {
            res.writeHead(200, { 'Content-Type': 'application/json' });
            res.end(JSON.stringify({ events: [], next_poll_after_ms: 60000 }));
          });
          return;
        }
        res.writeHead(404).end();
      });
      await new Promise(function (resolve) { server.listen(0, '127.0.0.1', resolve); });
      const hubUrl = 'http://127.0.0.1:' + server.address().port;
      const protocol = require('./src/gep/a2aProtocol');

      protocol.startEventDelivery({ hubUrl, nodeId: 'node_aaaaaaaaaaaa' });
      let deadline = Date.now() + 3000;
      while (Date.now() < deadline) {
        const state = protocol._testing._getHeartbeatInternalsForTesting();
        if (streamCount === 1 && state.selfDrivingPollEnabled === false) break;
        await new Promise(function (resolve) { setTimeout(resolve, 10); });
      }
      let state = protocol._testing._getHeartbeatInternalsForTesting();
      assert.equal(streamCount, 1, 'fetch fallback must open one SSE request');
      assert.equal(state.selfDrivingPollEnabled, false, 'validated fetch SSE must stop persistent poll');
      assert.equal(pollCount, 0, 'healthy fetch SSE must not leave a concurrent long poll');

      activeStreamResponse.end();
      deadline = Date.now() + 3000;
      while (Date.now() < deadline && pollCount === 0) {
        await new Promise(function (resolve) { setTimeout(resolve, 10); });
      }
      state = protocol._testing._getHeartbeatInternalsForTesting();
      assert.equal(state.selfDrivingPollEnabled, true, 'fetch SSE EOF must restore poll fallback');
      assert.ok(pollCount > 0, 'fetch SSE EOF must schedule a poll without startup delay');

      protocol.startEventStream();
      deadline = Date.now() + 3000;
      while (Date.now() < deadline) {
        state = protocol._testing._getHeartbeatInternalsForTesting();
        if (streamCount === 2 && state.selfDrivingPollEnabled === false) break;
        await new Promise(function (resolve) { setTimeout(resolve, 10); });
      }
      assert.equal(streamCount, 2, 'fetch fallback must reconnect');
      assert.equal(state.selfDrivingPollEnabled, false, 'healthy fetch SSE reconnect must stop poll again');

      protocol.stopEventDelivery();
      streamResponses.forEach(function (res) {
        try { res.end(); } catch (_) {}
      });
      await new Promise(function (resolve) { server.close(resolve); });
    })().catch(function (error) {
      console.error(error && error.stack ? error.stack : error);
      process.exitCode = 1;
    }).finally(function () {
      Module._load = originalLoad;
    });
  `;

  try {
    const result = spawnSync(process.execPath, ['-e', childScript], {
      cwd: path.resolve(__dirname, '..'),
      encoding: 'utf8',
      timeout: 10000,
      env: {
        PATH: process.env.PATH || '',
        NODE_PATH: process.env.NODE_PATH || '',
        HOME: evolverHome,
        EVOLVER_HOME: evolverHome,
        EVOMAP_HUB_ALLOW_INSECURE: '1',
        NODE_ENV: 'test',
      },
    });
    assert.equal(
      result.status,
      0,
      ['isolated fetch SSE/poll lifecycle probe failed', result.stdout, result.stderr].filter(Boolean).join('\n'),
    );
  } finally {
    fs.rmSync(evolverHome, { recursive: true, force: true });
  }
});

test('does not open fetch fallback SSE for a non-event-stream response', () => {
  const evolverHome = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sse-fetch-content-type-'));
  const childScript = String.raw`
    const assert = require('node:assert/strict');
    const Module = require('node:module');

    let canceled = false;
    const originalLoad = Module._load;
    Module._load = function (request, parent, isMain) {
      if (request === 'eventsource') throw new Error('force fetch fallback');
      const loaded = originalLoad.apply(this, arguments);
      if (request === './hubFetch' && parent && /src[\\/]gep[\\/]a2aProtocol\.js$/.test(parent.filename)) {
        return Object.assign({}, loaded, {
          hubFetch: async function () {
            return {
              ok: true,
              status: 200,
              headers: new Headers({ 'Content-Type': 'application/json' }),
              body: { cancel: async function () { canceled = true; } },
            };
          },
        });
      }
      return loaded;
    };

    (async function () {
      const protocol = require('./src/gep/a2aProtocol');
      const result = protocol.hubOpenEventStream({
        hubUrl: 'https://example.invalid',
        nodeId: 'node_aaaaaaaaaaaa',
        forceFetchFallback: true,
      });
      let opened = false;
      result.eventSource.onopen = function () { opened = true; };
      const error = await new Promise(function (resolve) {
        result.eventSource.onerror = resolve;
      });
      assert.equal(opened, false, 'invalid content type must not emit onopen');
      assert.equal(canceled, true, 'invalid response body must be released');
      assert.match(String(error && error.message || error), /content_type_invalid/);
      result.close();
    })().catch(function (error) {
      console.error(error && error.stack ? error.stack : error);
      process.exitCode = 1;
    }).finally(function () {
      Module._load = originalLoad;
    });
  `;

  try {
    const result = spawnSync(process.execPath, ['-e', childScript], {
      cwd: path.resolve(__dirname, '..'),
      encoding: 'utf8',
      env: {
        PATH: process.env.PATH || '',
        NODE_PATH: process.env.NODE_PATH || '',
        HOME: evolverHome,
        EVOLVER_HOME: evolverHome,
        NODE_ENV: 'test',
      },
    });
    assert.equal(
      result.status,
      0,
      ['isolated fetch SSE content-type probe failed', result.stdout, result.stderr].filter(Boolean).join('\n'),
    );
  } finally {
    fs.rmSync(evolverHome, { recursive: true, force: true });
  }
});
