'use strict';

const { after, before, test } = require('node:test');
const assert = require('node:assert/strict');
const { spawn } = require('node:child_process');
const fs = require('node:fs');
const http = require('node:http');
const os = require('node:os');
const path = require('node:path');

const REPO_ROOT = path.resolve(__dirname, '..');
const NODE_A = 'node_aaaaaaaaaaaa';
const NODE_B = 'node_bbbbbbbbbbbb';
const SECRET_A = 'a'.repeat(64);
const SECRET_B = 'b'.repeat(64);

let mockHub;

function startMockHub() {
  const requests = [];
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://localhost');
    requests.push({
      authorization: req.headers.authorization,
      nodeId: url.searchParams.get('node_id'),
      pathname: url.pathname,
      secretVersion: req.headers['x-evomap-node-secret-version'],
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

function writeMixedIdentity(root, mailboxNodeId = NODE_B) {
  const evolverHome = path.join(root, 'home');
  const mailboxDir = path.join(evolverHome, 'mailbox');
  fs.mkdirSync(mailboxDir, { recursive: true });
  fs.writeFileSync(path.join(evolverHome, 'node_id'), NODE_A);
  fs.writeFileSync(path.join(evolverHome, 'node_secret'), SECRET_A);
  fs.writeFileSync(path.join(evolverHome, 'node_secret_version'), '3');
  fs.writeFileSync(path.join(evolverHome, 'node_secret_source'), 'hub_rotate');
  fs.writeFileSync(path.join(mailboxDir, 'state.json'), JSON.stringify({
    marker: 'preserve-node-b-mailbox',
    node_id: mailboxNodeId,
    node_secret: SECRET_B,
    node_secret_source: 'hub_rotate',
    node_secret_version: '99',
  }, null, 2) + '\n');
  return evolverHome;
}

function snapshotFiles(root) {
  const result = {};
  function visit(directory) {
    for (const entry of fs.readdirSync(directory, { withFileTypes: true })) {
      const fullPath = path.join(directory, entry.name);
      if (entry.isDirectory()) {
        visit(fullPath);
        continue;
      }
      result[path.relative(root, fullPath)] = fs.readFileSync(fullPath).toString('base64');
    }
  }
  visit(root);
  return result;
}

function runSync({ dryRun, evolverHome, root, useEnvNodeId }) {
  const args = ['index.js', 'sync', '--scope=published', '--no-unpublished-list'];
  if (dryRun) args.push('--dry-run');
  const env = {
    ...process.env,
    A2A_HUB_TOKEN: '',
    A2A_HUB_URL: mockHub.url,
    A2A_NODE_ID: useEnvNodeId ? NODE_A : '',
    A2A_NODE_SECRET: '',
    A2A_NODE_SECRET_VERSION: '',
    EVOMAP_HUB_ALLOW_INSECURE: '1',
    EVOMAP_NODE_SECRET: '',
    EVOMAP_NODE_SECRET_VERSION: '',
    EVOLVER_HOME: evolverHome,
    GEP_ASSETS_DIR: path.join(root, 'assets', 'gep'),
  };

  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, args, { cwd: REPO_ROOT, env });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (chunk) => { stdout += chunk.toString('utf8'); });
    child.stderr.on('data', (chunk) => { stderr += chunk.toString('utf8'); });
    const timeout = setTimeout(() => {
      child.kill('SIGTERM');
      reject(new Error('sync timed out'));
    }, 15_000);
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

function assertUsesNodeA(requests) {
  assert.ok(requests.length > 0, 'sync must reach the Hub');
  assert.ok(requests.every((request) => request.pathname === '/a2a/assets/published-by-me'));
  assert.ok(requests.every((request) => request.nodeId === NODE_A));
  assert.ok(requests.every((request) => request.authorization === 'Bearer ' + SECRET_A));
  assert.ok(requests.every((request) => request.secretVersion === '3'));
}

function assertUsesSameNodeMailboxTuple(requests) {
  assert.ok(requests.length > 0, 'sync must reach the Hub');
  assert.ok(requests.every((request) => request.nodeId === NODE_A));
  assert.ok(requests.every((request) => request.authorization === 'Bearer ' + SECRET_B));
  assert.ok(requests.every((request) => request.secretVersion === '99'));
}

before(async () => {
  mockHub = await startMockHub();
});

after(() => {
  mockHub.server.close();
});

test('sync --dry-run keeps persisted node A isolated from explicit mailbox node B', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-identity-tuple-dry-'));
  const evolverHome = writeMixedIdentity(root);
  const beforeFiles = snapshotFiles(root);
  const requestOffset = mockHub.requests.length;

  const result = await runSync({ dryRun: true, evolverHome, root, useEnvNodeId: false });

  assert.equal(result.status, 0, 'stdout=' + result.stdout + '\nstderr=' + result.stderr);
  assertUsesNodeA(mockHub.requests.slice(requestOffset));
  assert.deepEqual(snapshotFiles(root), beforeFiles, 'dry-run must remain byte-for-byte read-only');
  assert.equal((result.stdout + result.stderr).includes(SECRET_A), false);
  assert.equal((result.stdout + result.stderr).includes(SECRET_B), false);
});

test('real sync does not copy explicit mailbox node B credentials into env/persisted node A', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-identity-tuple-real-'));
  const evolverHome = writeMixedIdentity(root);
  const identityBefore = snapshotFiles(evolverHome);
  const requestOffset = mockHub.requests.length;

  const result = await runSync({ dryRun: false, evolverHome, root, useEnvNodeId: true });

  assert.equal(result.status, 0, 'stdout=' + result.stdout + '\nstderr=' + result.stderr);
  assertUsesNodeA(mockHub.requests.slice(requestOffset));
  assert.deepEqual(snapshotFiles(evolverHome), identityBefore, 'real sync must not cross-write node B credentials into node A state');
  assert.equal(fs.readFileSync(path.join(evolverHome, 'node_secret'), 'utf8'), SECRET_A);
  assert.equal((result.stdout + result.stderr).includes(SECRET_A), false);
  assert.equal((result.stdout + result.stderr).includes(SECRET_B), false);
});

test('same-node mailbox and persisted tuples still select the newer rotated version', async () => {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-identity-tuple-same-node-'));
  const evolverHome = writeMixedIdentity(root, NODE_A);
  const beforeFiles = snapshotFiles(root);
  const requestOffset = mockHub.requests.length;

  const result = await runSync({ dryRun: true, evolverHome, root, useEnvNodeId: false });

  assert.equal(result.status, 0, 'stdout=' + result.stdout + '\nstderr=' + result.stderr);
  assertUsesSameNodeMailboxTuple(mockHub.requests.slice(requestOffset));
  assert.deepEqual(snapshotFiles(root), beforeFiles);
  assert.equal((result.stdout + result.stderr).includes(SECRET_A), false);
  assert.equal((result.stdout + result.stderr).includes(SECRET_B), false);
});
