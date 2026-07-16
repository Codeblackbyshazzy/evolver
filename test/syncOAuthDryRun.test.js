'use strict';

const { after, before, test } = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');

const REPO_ROOT = path.resolve(__dirname, '..');
const OAUTH_TOKEN = 'oauth-access-token';
const NODE_ID = 'node_a0b1c2d3e4f5';

let mock;

function startMockHub() {
  const requests = [];
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://localhost');
    requests.push({
      authorization: req.headers.authorization,
      method: req.method,
      pathname: url.pathname,
      nodeId: url.searchParams.get('node_id'),
    });
    res.writeHead(200, { 'Content-Type': 'application/json' });
    res.end(JSON.stringify({ assets: [], count: 0, has_more: false, next_cursor: null }));
  });

  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const address = server.address();
      resolve({
        requests,
        server,
        url: 'http://127.0.0.1:' + address.port,
      });
    });
  });
}

function runSync(evolverHome) {
  const args = [
    'index.js',
    'sync',
    '--scope=published',
    '--no-unpublished-list',
    '--dry-run',
  ];
  const env = {
    ...process.env,
    A2A_HUB_URL: mock.url,
    A2A_HUB_TOKEN: '',
    A2A_NODE_ID: '',
    A2A_NODE_SECRET: '',
    A2A_NODE_SECRET_VERSION: '',
    EVOMAP_HUB_ALLOW_INSECURE: '1',
    EVOMAP_NODE_SECRET: '',
    EVOMAP_NODE_SECRET_VERSION: '',
    EVOLVER_HOME: evolverHome,
    GEP_ASSETS_DIR: path.join(evolverHome, 'assets', 'gep'),
  };

  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, args, { cwd: REPO_ROOT, env });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => { stdout += chunk.toString('utf8'); });
    child.stderr.on('data', (chunk) => { stderr += chunk.toString('utf8'); });
    const timeout = setTimeout(() => {
      child.kill('SIGTERM');
      reject(new Error('sync --dry-run timed out\nstdout=' + stdout + '\nstderr=' + stderr));
    }, 15000);
    child.on('error', (error) => {
      clearTimeout(timeout);
      reject(error);
    });
    child.on('exit', (status, signal) => {
      clearTimeout(timeout);
      resolve({ signal, status, stderr, stdout });
    });
  });
}

function createHome({ expiresAt, withOAuth = true } = {}) {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sync-oauth-dry-run-'));
  const evolverHome = path.join(root, 'home');
  fs.mkdirSync(evolverHome, { recursive: true });
  fs.writeFileSync(path.join(evolverHome, 'node_id'), NODE_ID);
  if (withOAuth) {
    fs.writeFileSync(path.join(evolverHome, 'oauth_token.json'), JSON.stringify({
      access_token: OAUTH_TOKEN,
      expires_at: expiresAt,
      scope: 'intentionally-not-trusted-by-cli',
    }, null, 2));
  }
  return { evolverHome, root };
}

function snapshotTree(root) {
  const snapshot = {};
  function visit(directory) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const fullPath = path.join(directory, entry.name);
      const relativePath = path.relative(root, fullPath);
      const stat = fs.statSync(fullPath, { bigint: true });
      if (entry.isDirectory()) {
        snapshot[relativePath + path.sep] = {
          mtimeNs: stat.mtimeNs.toString(),
          type: 'directory',
        };
        visit(fullPath);
        continue;
      }
      snapshot[relativePath] = {
        content: fs.readFileSync(fullPath).toString('base64'),
        mtimeNs: stat.mtimeNs.toString(),
        type: 'file',
      };
    }
  }
  visit(root);
  return snapshot;
}

before(async () => {
  mock = await startMockHub();
});

after(() => {
  mock.server.close();
});

test('sync --dry-run accepts OAuth-only auth without writing files', async () => {
  const { evolverHome, root } = createHome({ expiresAt: Date.now() + 60 * 60 * 1000 });
  const beforeFiles = snapshotTree(root);
  const requestOffset = mock.requests.length;

  const result = await runSync(evolverHome);

  assert.equal(result.status, 0, 'stdout=' + result.stdout + '\nstderr=' + result.stderr);
  const requests = mock.requests.slice(requestOffset);
  assert.ok(requests.length > 0, 'OAuth-only dry-run must reach the Hub list endpoint');
  assert.ok(requests.every((request) => request.method === 'GET'));
  assert.ok(requests.every((request) => request.pathname === '/a2a/assets/published-by-me'));
  assert.ok(requests.every((request) => request.authorization === 'Bearer ' + OAUTH_TOKEN));
  assert.ok(requests.every((request) => request.nodeId === NODE_ID));
  assert.ok(requests.every((request) => request.pathname !== '/a2a/hello'));
  assert.deepEqual(snapshotTree(root), beforeFiles, 'dry-run must not write identity or asset-store files');
});

test('sync --dry-run rejects an expired OAuth-only token before any request', async () => {
  const { evolverHome, root } = createHome({ expiresAt: Date.now() - 60 * 1000 });
  const beforeFiles = snapshotTree(root);
  const requestOffset = mock.requests.length;

  const result = await runSync(evolverHome);

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /existing node_secret or a valid OAuth access token/);
  assert.deepEqual(mock.requests.slice(requestOffset), [], 'expired OAuth must not produce an unauthenticated request');
  assert.deepEqual(snapshotTree(root), beforeFiles, 'rejection must not refresh or rewrite local state');
});

test('sync --dry-run rejects missing OAuth and node secret before any request', async () => {
  const { evolverHome, root } = createHome({ withOAuth: false });
  const beforeFiles = snapshotTree(root);
  const requestOffset = mock.requests.length;

  const result = await runSync(evolverHome);

  assert.notEqual(result.status, 0);
  assert.match(result.stderr, /existing node_secret or a valid OAuth access token/);
  assert.deepEqual(mock.requests.slice(requestOffset), [], 'missing auth must not produce an unauthenticated request');
  assert.deepEqual(snapshotTree(root), beforeFiles, 'rejection must not create identity or asset-store files');
});
