// End-to-end test for `node index.js sync` dedup logic.
//
// We spin up an in-process HTTP mock for /a2a/assets/published-by-me (and
// purchased) so we can verify what the CLI does given a controlled set of
// Hub responses. The two failure modes we want to lock down:
//
// 1. A bundled default-seed gene (id e.g. `gene_gep_repair_from_errors`,
//    no hub_asset_id) must NOT cause Hub copies of the same id to silently
//    skip on first sync. We allow the default to win unless the user
//    passes --force, but the run must clearly report "id_collision" so
//    the user understands why nothing changed.
// 2. With --force, the local entry is overwritten with the Hub copy and a
//    hub_asset_id is recorded; subsequent runs without --force become
//    no-ops via the hub_asset_id dedup check.

const { describe, it, before, after } = require('node:test');
const assert = require('node:assert/strict');
const fs = require('fs');
const os = require('os');
const path = require('path');
const http = require('http');

const { spawn } = require('child_process');

function startMock(handlers) {
  const requests = [];
  const server = http.createServer((req, res) => {
    const url = new URL(req.url, 'http://x');
    requests.push({ method: req.method, pathname: url.pathname, headers: req.headers, url });
    const route = handlers[url.pathname];
    if (!route) {
      res.writeHead(404, { 'Content-Type': 'application/json' });
      res.end(JSON.stringify({ error: 'not_found' }));
      return;
    }
    let body = '';
    req.on('data', (c) => { body += c; });
    req.on('end', () => {
      try {
        const result = route({ url, body, headers: req.headers });
        res.writeHead(result.status || 200, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify(result.body || {}));
      } catch (e) {
        res.writeHead(500, { 'Content-Type': 'application/json' });
        res.end(JSON.stringify({ error: e.message }));
      }
    });
  });
  return new Promise((resolve) => {
    server.listen(0, '127.0.0.1', () => {
      const addr = server.address();
      resolve({ server, url: 'http://127.0.0.1:' + addr.port, requests });
    });
  });
}

// Async version of spawn so the in-process mock HTTP server keeps servicing
// requests while the child runs. spawnSync would block this event loop and
// deadlock against our own mock.
function runSync(env, extraArgs) {
  const cwd = path.resolve(__dirname, '..');
  const argv = ['index.js', 'sync', '--scope=published'].concat(extraArgs || []);
  return new Promise((resolve, reject) => {
    const child = spawn(process.execPath, argv, {
      cwd,
      env: { ...process.env, EVOMAP_HUB_ALLOW_INSECURE: '1', ...env },
    });
    let stdout = '';
    let stderr = '';
    child.stdout.on('data', (d) => { stdout += d.toString('utf8'); });
    child.stderr.on('data', (d) => { stderr += d.toString('utf8'); });
    const t = setTimeout(() => {
      child.kill('SIGTERM');
      reject(new Error('runSync timed out\nstdout=' + stdout + '\nstderr=' + stderr));
    }, 15000);
    child.on('exit', (status, signal) => {
      clearTimeout(t);
      resolve({ stdout, stderr, status, signal });
    });
    child.on('error', (e) => { clearTimeout(t); reject(e); });
  });
}

function mkSandbox() {
  const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sync-'));
  const assetsDir = path.join(root, 'assets', 'gep');
  fs.mkdirSync(assetsDir, { recursive: true });
  return { root, assetsDir };
}

function listFilesRecursive(root) {
  if (!fs.existsSync(root)) return [];
  const files = [];
  for (const entry of fs.readdirSync(root, { withFileTypes: true })) {
    const fullPath = path.join(root, entry.name);
    if (entry.isDirectory()) {
      files.push(...listFilesRecursive(fullPath).map((relativePath) => path.join(entry.name, relativePath)));
    }
    else files.push(path.relative(root, fullPath));
  }
  return files.sort();
}

function snapshotFiles(root) {
  return Object.fromEntries(listFilesRecursive(root).map((relativePath) => [
    relativePath,
    fs.readFileSync(path.join(root, relativePath)).toString('base64'),
  ]));
}

function unauthenticatedEnv(evolverHome) {
  return {
    A2A_NODE_ID: '',
    A2A_NODE_SECRET: '',
    EVOMAP_NODE_SECRET: '',
    A2A_NODE_SECRET_VERSION: '',
    EVOMAP_NODE_SECRET_VERSION: '',
    A2A_HUB_TOKEN: '',
    EVOLVER_HOME: evolverHome,
    GEP_ASSETS_DIR: path.join(evolverHome, 'assets', 'gep'),
  };
}

function writePersistedIdentity(evolverHome, { nodeId, secret, version, source, suppression }) {
  fs.mkdirSync(evolverHome, { recursive: true });
  if (nodeId) fs.writeFileSync(path.join(evolverHome, 'node_id'), nodeId);
  if (secret) fs.writeFileSync(path.join(evolverHome, 'node_secret'), secret);
  if (version) fs.writeFileSync(path.join(evolverHome, 'node_secret_version'), String(version));
  if (source) fs.writeFileSync(path.join(evolverHome, 'node_secret_source'), source);
  if (suppression) fs.writeFileSync(path.join(evolverHome, 'node_secret_env_suppressed'), suppression);
}

function writeMailboxIdentity(evolverHome, state) {
  const mailboxDir = path.join(evolverHome, 'mailbox');
  fs.mkdirSync(mailboxDir, { recursive: true });
  fs.writeFileSync(path.join(mailboxDir, 'state.json'), JSON.stringify(state, null, 2) + '\n');
}

describe('sync dedup (id collision vs hub_asset_id)', () => {
  let mock;
  before(async () => {
    const hubAsset = {
      asset_id: 'hub-asset-aaaa1111',
      asset_type: 'Gene',
      local_id: 'gene_gep_repair_from_errors',
      payload: {
        id: 'gene_gep_repair_from_errors',
        category: 'repair',
        signals_match: ['error'],
        strategy: ['hub-strategy-step'],
        validation: [],
        summary: 'hub copy',
      },
    };
    mock = await startMock({
      '/a2a/assets/published-by-me': () => ({
        body: { assets: [hubAsset], count: 1, has_more: false, next_cursor: null, node_ids: ['node-test'] },
      }),
      '/a2a/assets/purchased': () => ({
        body: { assets: [], count: 0, has_more: false, next_cursor: null, node_ids: ['node-test'] },
      }),
      // Detail endpoint not needed because we ship payload inline.
    });
  });
  after(() => {
    if (mock) mock.server.close();
  });

  it('skips a hub asset whose local_id matches a default-seed gene (no --force)', async () => {
    const { assetsDir } = mkSandbox();
    fs.writeFileSync(
      path.join(assetsDir, 'genes.json'),
      JSON.stringify({ version: 1, genes: [{ id: 'gene_gep_repair_from_errors', strategy: ['local-default'] }] }, null, 2)
    );

    const r = await runSync({
      A2A_HUB_URL: mock.url,
      A2A_NODE_ID: 'node_aaaaaaaaaaaa',
      A2A_NODE_SECRET: 'a'.repeat(64),
      GEP_ASSETS_DIR: assetsDir,
    });

    assert.equal(r.status, 0, 'sync should exit 0; stdout=' + r.stdout + ' stderr=' + r.stderr);
    assert.match(r.stdout, /id_collision=1/);
    assert.match(r.stdout, /--force/);
    const genes = JSON.parse(fs.readFileSync(path.join(assetsDir, 'genes.json'), 'utf8')).genes;
    assert.equal(genes.length, 1);
    assert.equal(genes[0].strategy[0], 'local-default', 'local default must be preserved');
    assert.equal(genes[0].hub_asset_id, undefined, 'no hub_asset_id should be written without --force');
  });

  it('overwrites the default-seed gene with the Hub copy when --force is set', async () => {
    const { assetsDir } = mkSandbox();
    fs.writeFileSync(
      path.join(assetsDir, 'genes.json'),
      JSON.stringify({ version: 1, genes: [{ id: 'gene_gep_repair_from_errors', strategy: ['local-default'] }] }, null, 2)
    );

    const r = await runSync({
      A2A_HUB_URL: mock.url,
      A2A_NODE_ID: 'node_aaaaaaaaaaaa',
      A2A_NODE_SECRET: 'a'.repeat(64),
      GEP_ASSETS_DIR: assetsDir,
    }, ['--force']);

    assert.equal(r.status, 0, 'sync should exit 0; stdout=' + r.stdout + ' stderr=' + r.stderr);
    assert.match(r.stdout, /synced=1/);
    const genes = JSON.parse(fs.readFileSync(path.join(assetsDir, 'genes.json'), 'utf8')).genes;
    assert.equal(genes.length, 1);
    assert.equal(genes[0].strategy[0], 'hub-strategy-step', 'Hub strategy must overwrite local default');
    assert.equal(genes[0].hub_asset_id, 'hub-asset-aaaa1111');
  });

  it('is idempotent on a second run: hub_asset_id match -> already_synced (not re-fetched)', async () => {
    const { assetsDir } = mkSandbox();
    fs.writeFileSync(
      path.join(assetsDir, 'genes.json'),
      JSON.stringify({
        version: 1,
        genes: [{
          id: 'gene_gep_repair_from_errors',
          strategy: ['hub-strategy-step'],
          hub_asset_id: 'hub-asset-aaaa1111',
          synced_at: '2026-05-04T00:00:00.000Z',
        }],
      }, null, 2)
    );

    const r = await runSync({
      A2A_HUB_URL: mock.url,
      A2A_NODE_ID: 'node_aaaaaaaaaaaa',
      A2A_NODE_SECRET: 'a'.repeat(64),
      GEP_ASSETS_DIR: assetsDir,
    });

    assert.equal(r.status, 0);
    assert.match(r.stdout, /already_synced=1/);
    assert.match(r.stdout, /id_collision=0/);
    assert.doesNotMatch(r.stdout, /--force/, 'no --force suggestion when there is no real id collision');
  });

  it('does not seed or create asset-store files in dry-run mode', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sync-dry-run-'));
    const assetsDir = path.join(root, 'missing', 'gep');
    const evolverHome = path.join(root, 'home');
    const exportPath = path.join(root, 'dry-run.gepx');
    const requestCountBefore = mock.requests.length;

    const r = await runSync({
      A2A_HUB_URL: mock.url,
      A2A_NODE_ID: 'node_aaaaaaaaaaaa',
      A2A_NODE_SECRET: 'a'.repeat(64),
      EVOLVER_HOME: evolverHome,
      GEP_ASSETS_DIR: assetsDir,
    }, ['--dry-run', '--force', '--export=' + exportPath]);

    assert.equal(r.status, 0, 'dry-run should exit 0; stdout=' + r.stdout + ' stderr=' + r.stderr);
    assert.match(r.stdout, /\[dry-run\] Would sync: Gene hub-asset-aaaa1111/);
    assert.match(r.stdout, /dry-run mode: no asset-store or export files were modified/);
    assert.match(r.stdout, /\[dry-run\] Would export to /);
    const requests = mock.requests.slice(requestCountBefore);
    assert.equal(requests.filter((request) => request.pathname === '/a2a/assets/hub-asset-aaaa1111').length, 0,
      'inline list payload must avoid an unnecessary detail request');
    assert.equal(fs.existsSync(assetsDir), false, 'dry-run must not create the asset-store directory');
    assert.equal(fs.existsSync(evolverHome), false, 'authenticated dry-run must not create identity state');
    assert.equal(fs.existsSync(exportPath), false, 'dry-run must not create an export bundle');
    assert.deepEqual(listFilesRecursive(root), [], 'dry-run must not create store, lock, temp, or export files');
  });

  it('counts a dry-run detail HTTP failure as a fetch error without reporting success', async () => {
    const detailAssetId = 'hub-asset-detail-500';
    const detailMock = await startMock({
      '/a2a/assets/published-by-me': () => ({
        body: {
          assets: [{ asset_id: detailAssetId, asset_type: 'Gene', local_id: 'gene_detail_500' }],
          count: 1,
          has_more: false,
          next_cursor: null,
        },
      }),
      ['/a2a/assets/' + detailAssetId]: () => ({ status: 500, body: { error: 'detail unavailable' } }),
    });
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sync-dry-run-detail-500-'));
    const assetsDir = path.join(root, 'missing', 'gep');
    const evolverHome = path.join(root, 'home');

    try {
      const r = await runSync({
        A2A_HUB_URL: detailMock.url,
        A2A_NODE_ID: 'node_aaaaaaaaaaaa',
        A2A_NODE_SECRET: 'a'.repeat(64),
        EVOLVER_HOME: evolverHome,
        GEP_ASSETS_DIR: assetsDir,
      }, ['--dry-run']);

      assert.equal(r.status, 1, 'dry-run should fail when an asset cannot be validated; stdout=' + r.stdout + ' stderr=' + r.stderr);
      assert.match(r.stdout, /synced=0/);
      assert.match(r.stdout, /errors=1/);
      assert.doesNotMatch(r.stdout, /Would sync:/);
      assert.ok(detailMock.requests.some((request) =>
        request.method === 'GET' && request.pathname === '/a2a/assets/' + detailAssetId));
      assert.deepEqual(listFilesRecursive(root), [], 'failed dry-run detail fetch must not write any files');
    } finally {
      detailMock.server.close();
    }
  });

  it('falls back to an id-collision skip when detail fetch fails for a listed local id', async () => {
    const assetId = 'hub-asset-collision-detail-500';
    const localId = 'gene_collision_detail_500';
    const detailMock = await startMock({
      '/a2a/assets/published-by-me': () => ({
        body: {
          assets: [{ asset_id: assetId, asset_type: 'Gene', local_id: localId }],
          count: 1,
          has_more: false,
          next_cursor: null,
        },
      }),
      ['/a2a/assets/' + assetId]: () => ({ status: 500, body: { error: 'detail unavailable' } }),
    });
    const { assetsDir } = mkSandbox();
    fs.writeFileSync(
      path.join(assetsDir, 'genes.json'),
      JSON.stringify({ version: 1, genes: [{ id: localId, strategy: ['keep-local'] }] }, null, 2)
    );

    try {
      const result = await runSync({
        A2A_HUB_URL: detailMock.url,
        A2A_NODE_ID: 'node_aaaaaaaaaaaa',
        A2A_NODE_SECRET: 'a'.repeat(64),
        GEP_ASSETS_DIR: assetsDir,
      });

      assert.equal(result.status, 0, 'known local collision must fail closed; stdout=' + result.stdout + ' stderr=' + result.stderr);
      assert.match(result.stdout, /synced=0/);
      assert.match(result.stdout, /id_collision=1/);
      assert.match(result.stdout, /errors=0/);
      assert.match(result.stdout, /--force/);
      const genes = JSON.parse(fs.readFileSync(path.join(assetsDir, 'genes.json'), 'utf8')).genes;
      assert.deepEqual(genes, [{ id: localId, strategy: ['keep-local'] }]);
    } finally {
      detailMock.server.close();
    }
  });

  it('fetches and validates missing list payload during dry-run before reporting success', async () => {
    const detailAssetId = 'hub-asset-detail-success';
    const detailMock = await startMock({
      '/a2a/assets/published-by-me': () => ({
        body: {
          assets: [{ asset_id: detailAssetId, asset_type: 'Gene', local_id: 'gene_detail_success' }],
          count: 1,
          has_more: false,
          next_cursor: null,
        },
      }),
      ['/a2a/assets/' + detailAssetId]: () => ({
        body: {
          payload: {
            id: 'gene_detail_success',
            category: 'repair',
            signals_match: ['error'],
            strategy: ['validated-detail-step'],
            validation: [],
          },
        },
      }),
    });
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sync-dry-run-detail-success-'));
    const assetsDir = path.join(root, 'missing', 'gep');
    const evolverHome = path.join(root, 'home');

    try {
      const r = await runSync({
        A2A_HUB_URL: detailMock.url,
        A2A_NODE_ID: 'node_aaaaaaaaaaaa',
        A2A_NODE_SECRET: 'a'.repeat(64),
        EVOLVER_HOME: evolverHome,
        GEP_ASSETS_DIR: assetsDir,
      }, ['--dry-run']);

      assert.equal(r.status, 0, 'dry-run detail fetch should succeed; stdout=' + r.stdout + ' stderr=' + r.stderr);
      assert.match(r.stdout, new RegExp('Would sync: Gene ' + detailAssetId));
      assert.match(r.stdout, /synced=1/);
      assert.match(r.stdout, /errors=0/);
      assert.ok(detailMock.requests.some((request) =>
        request.method === 'GET' && request.pathname === '/a2a/assets/' + detailAssetId));
      assert.deepEqual(listFilesRecursive(root), [], 'successful dry-run detail fetch must remain read-only');
    } finally {
      detailMock.server.close();
    }
  });

  it('preserves standard Gene fields in dry-run validation and real sync', async () => {
    const assetId = 'hub-asset-standard-gene';
    const genePayload = {
      id: 'gene_standard_contract',
      category: 'repair',
      signals_match: ['error'],
      strategy: ['inspect logs'],
      validation: ['node --test'],
      preconditions: ['tests exist'],
      constraints: { max_files: 3, forbidden_paths: ['.git'] },
      anti_patterns: ['ignore failures'],
      routing_hint: { tier: 'mid', reasoning_level: 'high' },
      summary: 'standard contract gene',
    };
    const standardMock = await startMock({
      '/a2a/assets/published-by-me': () => ({
        body: {
          assets: [{ asset_id: assetId, asset_type: 'Gene', local_id: genePayload.id, payload: genePayload }],
          count: 1,
          has_more: false,
          next_cursor: null,
        },
      }),
    });
    const { assetsDir } = mkSandbox();

    try {
      const env = {
        A2A_HUB_URL: standardMock.url,
        A2A_NODE_ID: 'node_aaaaaaaaaaaa',
        A2A_NODE_SECRET: 'a'.repeat(64),
        GEP_ASSETS_DIR: assetsDir,
      };
      const dryRunResult = await runSync(env, ['--dry-run', '--force']);
      const realResult = await runSync(env, ['--force']);

      assert.equal(dryRunResult.status, 0, 'dry-run should validate the standard Gene; stdout=' + dryRunResult.stdout + ' stderr=' + dryRunResult.stderr);
      assert.match(dryRunResult.stdout, new RegExp('Would sync: Gene ' + assetId));
      assert.equal(realResult.status, 0, 'real sync should persist the same validated Gene; stdout=' + realResult.stdout + ' stderr=' + realResult.stderr);
      const genes = JSON.parse(fs.readFileSync(path.join(assetsDir, 'genes.json'), 'utf8')).genes;
      const syncedGene = genes.find((gene) => gene.hub_asset_id === assetId);
      assert.ok(syncedGene);
      assert.deepEqual(syncedGene.signals_match, genePayload.signals_match);
      assert.deepEqual(syncedGene.preconditions, genePayload.preconditions);
      assert.deepEqual(syncedGene.constraints, genePayload.constraints);
      assert.deepEqual(syncedGene.anti_patterns, genePayload.anti_patterns);
      assert.deepEqual(syncedGene.routing_hint, genePayload.routing_hint);
    } finally {
      standardMock.server.close();
    }
  });

  it('rejects invalid Gene payloads consistently without writing them', async () => {
    const assetId = 'hub-asset-invalid-gene';
    const invalidMock = await startMock({
      '/a2a/assets/published-by-me': () => ({
        body: {
          assets: [{
            asset_id: assetId,
            asset_type: 'Gene',
            local_id: 'gene_invalid_contract',
            payload: { id: 'gene_invalid_contract', category: 'repair', signals: ['wrong-field'], strategy: ['step'] },
          }],
          count: 1,
          has_more: false,
          next_cursor: null,
        },
      }),
    });
    const { assetsDir } = mkSandbox();

    try {
      const env = {
        A2A_HUB_URL: invalidMock.url,
        A2A_NODE_ID: 'node_aaaaaaaaaaaa',
        A2A_NODE_SECRET: 'a'.repeat(64),
        GEP_ASSETS_DIR: assetsDir,
      };
      const dryRunResult = await runSync(env, ['--dry-run', '--force']);
      const realResult = await runSync(env, ['--force']);

      assert.equal(dryRunResult.status, 1);
      assert.equal(realResult.status, 1);
      assert.match(dryRunResult.stdout, /errors=1/);
      assert.doesNotMatch(dryRunResult.stdout, /Would sync:/);
      const genes = JSON.parse(fs.readFileSync(path.join(assetsDir, 'genes.json'), 'utf8')).genes;
      assert.equal(genes.some((gene) => gene.hub_asset_id === assetId), false);
    } finally {
      invalidMock.server.close();
    }
  });

  it('falls back to an id-collision skip when a listed local id has a malformed payload', async () => {
    const assetId = 'hub-asset-collision-malformed';
    const localId = 'gene_collision_malformed';
    const malformedMock = await startMock({
      '/a2a/assets/published-by-me': () => ({
        body: {
          assets: [{
            asset_id: assetId,
            asset_type: 'Gene',
            local_id: localId,
            payload: { id: localId, category: 'repair', strategy: ['invalid-without-signals-match'] },
          }],
          count: 1,
          has_more: false,
          next_cursor: null,
        },
      }),
    });
    const { assetsDir } = mkSandbox();
    fs.writeFileSync(
      path.join(assetsDir, 'genes.json'),
      JSON.stringify({ version: 1, genes: [{ id: localId, strategy: ['keep-local'] }] }, null, 2)
    );

    try {
      const result = await runSync({
        A2A_HUB_URL: malformedMock.url,
        A2A_NODE_ID: 'node_aaaaaaaaaaaa',
        A2A_NODE_SECRET: 'a'.repeat(64),
        GEP_ASSETS_DIR: assetsDir,
      });

      assert.equal(result.status, 0, 'known local collision must fail closed; stdout=' + result.stdout + ' stderr=' + result.stderr);
      assert.match(result.stdout, /id_collision=1/);
      assert.match(result.stdout, /errors=0/);
      assert.match(result.stdout, /--force/);
      const genes = JSON.parse(fs.readFileSync(path.join(assetsDir, 'genes.json'), 'utf8')).genes;
      assert.deepEqual(genes, [{ id: localId, strategy: ['keep-local'] }]);
    } finally {
      malformedMock.server.close();
    }
  });

  it('uses the prepared payload id for collision safety when the list local id is an alias', async () => {
    const assetId = 'hub-asset-list-id-alias';
    const listLocalId = 'gene_list_alias';
    const payloadId = 'gene_effective_collision';
    const aliasMock = await startMock({
      '/a2a/assets/published-by-me': () => ({
        body: {
          assets: [{
            asset_id: assetId,
            asset_type: 'Gene',
            local_id: listLocalId,
            payload: {
              id: payloadId,
              category: 'repair',
              signals_match: ['error'],
              strategy: ['hub-copy'],
              validation: [],
            },
          }],
          count: 1,
          has_more: false,
          next_cursor: null,
        },
      }),
    });
    const { assetsDir } = mkSandbox();
    fs.writeFileSync(
      path.join(assetsDir, 'genes.json'),
      JSON.stringify({ version: 1, genes: [{ id: payloadId, strategy: ['keep-local'] }] }, null, 2)
    );

    try {
      const result = await runSync({
        A2A_HUB_URL: aliasMock.url,
        A2A_NODE_ID: 'node_aaaaaaaaaaaa',
        A2A_NODE_SECRET: 'a'.repeat(64),
        GEP_ASSETS_DIR: assetsDir,
      });

      assert.equal(result.status, 0, 'effective payload id collision must be skipped; stdout=' + result.stdout + ' stderr=' + result.stderr);
      assert.match(result.stdout, /id_collision=1/);
      assert.match(result.stdout, /errors=0/);
      assert.match(result.stdout, /--force/);
      const genes = JSON.parse(fs.readFileSync(path.join(assetsDir, 'genes.json'), 'utf8')).genes;
      assert.deepEqual(genes, [{ id: payloadId, strategy: ['keep-local'] }]);
    } finally {
      aliasMock.server.close();
    }
  });

  it('rejects duplicate effective local ids in one Hub batch instead of overwriting', async () => {
    const duplicateMock = await startMock({
      '/a2a/assets/published-by-me': () => ({
        body: {
          assets: ['first', 'second'].map((suffix) => ({
            asset_id: 'hub-asset-' + suffix,
            asset_type: 'Gene',
            local_id: 'gene_duplicate',
            payload: {
              id: 'gene_duplicate',
              category: 'repair',
              signals_match: ['error'],
              strategy: [suffix],
              validation: [],
            },
          })),
          count: 2,
          has_more: false,
          next_cursor: null,
        },
      }),
    });
    const { assetsDir } = mkSandbox();

    try {
      const result = await runSync({
        A2A_HUB_URL: duplicateMock.url,
        A2A_NODE_ID: 'node_aaaaaaaaaaaa',
        A2A_NODE_SECRET: 'a'.repeat(64),
        GEP_ASSETS_DIR: assetsDir,
      }, ['--force']);

      assert.equal(result.status, 1);
      assert.match(result.stdout, /synced=1/);
      assert.match(result.stdout, /errors=1/);
      const genes = JSON.parse(fs.readFileSync(path.join(assetsDir, 'genes.json'), 'utf8')).genes;
      const duplicates = genes.filter((gene) => gene.id === 'gene_duplicate');
      assert.equal(duplicates.length, 1);
      assert.equal(duplicates[0].hub_asset_id, 'hub-asset-first');
    } finally {
      duplicateMock.server.close();
    }
  });

  it('still reports a duplicate batch error after the first prepared asset hits a local collision', async () => {
    const localId = 'gene_duplicate_collision';
    const duplicateMock = await startMock({
      '/a2a/assets/published-by-me': () => ({
        body: {
          assets: ['first', 'second'].map((suffix) => ({
            asset_id: 'hub-collision-' + suffix,
            asset_type: 'Gene',
            local_id: localId,
            payload: {
              id: localId,
              category: 'repair',
              signals_match: ['error'],
              strategy: [suffix],
              validation: [],
            },
          })),
          count: 2,
          has_more: false,
          next_cursor: null,
        },
      }),
    });
    const { assetsDir } = mkSandbox();
    fs.writeFileSync(
      path.join(assetsDir, 'genes.json'),
      JSON.stringify({ version: 1, genes: [{ id: localId, strategy: ['keep-local'] }] }, null, 2)
    );

    try {
      const result = await runSync({
        A2A_HUB_URL: duplicateMock.url,
        A2A_NODE_ID: 'node_aaaaaaaaaaaa',
        A2A_NODE_SECRET: 'a'.repeat(64),
        GEP_ASSETS_DIR: assetsDir,
      });

      assert.equal(result.status, 1, 'duplicate prepared ids must remain an error; stdout=' + result.stdout + ' stderr=' + result.stderr);
      assert.match(result.stdout, /synced=0/);
      assert.match(result.stdout, /id_collision=1/);
      assert.match(result.stdout, /errors=1/);
      const genes = JSON.parse(fs.readFileSync(path.join(assetsDir, 'genes.json'), 'utf8')).genes;
      assert.deepEqual(genes, [{ id: localId, strategy: ['keep-local'] }]);
    } finally {
      duplicateMock.server.close();
    }
  });

  it('keeps an upsert I/O failure as an error instead of reclassifying it as a collision', async () => {
    const assetId = 'hub-asset-upsert-io-error';
    const localId = 'gene_upsert_io_error';
    const { assetsDir } = mkSandbox();
    fs.writeFileSync(
      path.join(assetsDir, 'genes.json'),
      JSON.stringify({ version: 1, genes: [{ id: localId, strategy: ['existing-local'] }] }, null, 2)
    );
    const movedAssetsDir = assetsDir + '-before-io-error';
    const ioErrorMock = await startMock({
      '/a2a/assets/published-by-me': () => ({
        body: {
          assets: [{ asset_id: assetId, asset_type: 'Gene', local_id: localId }],
          count: 1,
          has_more: false,
          next_cursor: null,
        },
      }),
      ['/a2a/assets/' + assetId]: () => {
        // The CLI loads the local store before requesting detail. Replace the
        // directory now so the validated asset reaches upsert and fails there.
        fs.renameSync(assetsDir, movedAssetsDir);
        fs.writeFileSync(assetsDir, 'not a directory');
        return {
          body: {
            payload: {
              id: localId,
              category: 'repair',
              signals_match: ['error'],
              strategy: ['hub-copy'],
              validation: [],
            },
          },
        };
      },
    });

    try {
      const result = await runSync({
        A2A_HUB_URL: ioErrorMock.url,
        A2A_NODE_ID: 'node_aaaaaaaaaaaa',
        A2A_NODE_SECRET: 'a'.repeat(64),
        GEP_ASSETS_DIR: assetsDir,
      }, ['--force']);

      assert.equal(result.status, 1, 'upsert I/O failure must remain an error; stdout=' + result.stdout + ' stderr=' + result.stderr);
      assert.match(result.stdout, /synced=0/);
      assert.match(result.stdout, /id_collision=0/);
      assert.match(result.stdout, /errors=1/);
      const genes = JSON.parse(fs.readFileSync(path.join(movedAssetsDir, 'genes.json'), 'utf8')).genes;
      assert.deepEqual(genes, [{ id: localId, strategy: ['existing-local'] }]);
    } finally {
      ioErrorMock.server.close();
    }
  });

  it('refuses unauthenticated dry-run without registering or writing identity state', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sync-dry-run-auth-'));
    const evolverHome = path.join(root, 'home');
    const requestCountBefore = mock.requests.length;

    const r = await runSync({
      A2A_HUB_URL: mock.url,
      ...unauthenticatedEnv(evolverHome),
    }, ['--dry-run']);

    assert.notEqual(r.status, 0, 'unauthenticated dry-run must fail; stdout=' + r.stdout + ' stderr=' + r.stderr);
    assert.match(r.stderr, /Dry-run requires an existing node_secret/);
    assert.deepEqual(mock.requests.slice(requestCountBefore), [], 'dry-run must not call any Hub endpoint');
    assert.equal(fs.existsSync(evolverHome), false, 'dry-run must not create EVOLVER_HOME');
    assert.deepEqual(listFilesRecursive(root), [], 'dry-run must not create identity or asset-store files');
  });

  it('refuses env-secret dry-run without generating a node id', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sync-dry-run-node-id-'));
    const evolverHome = path.join(root, 'home');
    const requestCountBefore = mock.requests.length;

    const r = await runSync({
      A2A_HUB_URL: mock.url,
      ...unauthenticatedEnv(evolverHome),
      A2A_NODE_SECRET: 'b'.repeat(64),
    }, ['--dry-run']);

    assert.notEqual(r.status, 0, 'dry-run without node id must fail; stdout=' + r.stdout + ' stderr=' + r.stderr);
    assert.match(r.stderr, /Dry-run requires an existing node_id/);
    assert.deepEqual(mock.requests.slice(requestCountBefore), [], 'dry-run must fail before any Hub request');
    assert.equal(fs.existsSync(evolverHome), false, 'dry-run must not generate node_id or EVOLVER_HOME');
    assert.deepEqual(listFilesRecursive(root), []);
  });

  it('keeps persisted rotated-only identity byte-for-byte unchanged during dry-run', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sync-dry-run-persisted-'));
    const evolverHome = path.join(root, 'home');
    writePersistedIdentity(evolverHome, {
      nodeId: 'node_bbbbbbbbbbbb',
      secret: 'c'.repeat(64),
      version: 7,
      source: 'hub_rotate',
    });
    const before = snapshotFiles(evolverHome);
    const requestCountBefore = mock.requests.length;

    const r = await runSync({
      A2A_HUB_URL: mock.url,
      ...unauthenticatedEnv(evolverHome),
    }, ['--dry-run']);

    assert.equal(r.status, 0, 'persisted rotated dry-run should succeed; stdout=' + r.stdout + ' stderr=' + r.stderr);
    const requests = mock.requests.slice(requestCountBefore);
    assert.ok(requests.length > 0);
    assert.ok(requests.every((request) => request.headers.authorization === 'Bearer ' + 'c'.repeat(64)));
    assert.ok(requests.every((request) => request.headers['x-evomap-node-secret-version'] === '7'));
    assert.deepEqual(snapshotFiles(evolverHome), before, 'dry-run must not backfill mailbox state');
  });

  it('keeps mailbox rotated-only identity byte-for-byte unchanged during dry-run', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sync-dry-run-mailbox-'));
    const evolverHome = path.join(root, 'home');
    writePersistedIdentity(evolverHome, { nodeId: 'node_cccccccccccc' });
    writeMailboxIdentity(evolverHome, {
      node_secret: 'd'.repeat(64),
      node_secret_version: '8',
      node_secret_source: 'hub_rotate',
      marker: 'preserve-me',
    });
    const before = snapshotFiles(evolverHome);
    const requestCountBefore = mock.requests.length;

    const r = await runSync({
      A2A_HUB_URL: mock.url,
      ...unauthenticatedEnv(evolverHome),
    }, ['--dry-run']);

    assert.equal(r.status, 0, 'mailbox rotated dry-run should succeed; stdout=' + r.stdout + ' stderr=' + r.stderr);
    const requests = mock.requests.slice(requestCountBefore);
    assert.ok(requests.length > 0);
    assert.ok(requests.every((request) => request.headers.authorization === 'Bearer ' + 'd'.repeat(64)));
    assert.ok(requests.every((request) => request.headers['x-evomap-node-secret-version'] === '8'));
    assert.deepEqual(snapshotFiles(evolverHome), before, 'dry-run must not backfill persisted secret files');
  });

  it('uses mailbox-only identity during dry-run without changing any bytes', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sync-dry-run-mailbox-only-'));
    const evolverHome = path.join(root, 'home');
    const nodeSecret = '1'.repeat(64);
    writeMailboxIdentity(evolverHome, {
      node_id: 'node_eeeeeeeeeeee',
      node_secret: nodeSecret,
      marker: 'preserve-mailbox-only',
    });
    const before = snapshotFiles(evolverHome);
    const requestCountBefore = mock.requests.length;

    const r = await runSync({
      A2A_HUB_URL: mock.url,
      ...unauthenticatedEnv(evolverHome),
    }, ['--dry-run']);

    assert.equal(r.status, 0, 'mailbox-only dry-run should succeed; stdout=' + r.stdout + ' stderr=' + r.stderr);
    const requests = mock.requests.slice(requestCountBefore);
    assert.ok(requests.length > 0, 'mailbox-only dry-run should perform Hub GET requests');
    assert.ok(requests.every((request) => request.method === 'GET'));
    assert.ok(requests.every((request) => request.headers.authorization === 'Bearer ' + nodeSecret));
    assert.ok(requests.every((request) => request.pathname === '/a2a/assets/published-by-me'));
    assert.deepEqual(snapshotFiles(evolverHome), before, 'dry-run must not backfill or rewrite mailbox-only identity');
  });

  it('uses the same mailbox-only node identity for dry-run and real sync', async () => {
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sync-mailbox-identity-parity-'));
    const evolverHome = path.join(root, 'home');
    const assetsDir = path.join(root, 'assets', 'gep');
    const nodeId = 'node_eeeeeeeeeeee';
    writeMailboxIdentity(evolverHome, {
      node_id: nodeId,
      node_secret: '1'.repeat(64),
    });
    const requestCountBefore = mock.requests.length;

    const dryRunResult = await runSync({
      A2A_HUB_URL: mock.url,
      ...unauthenticatedEnv(evolverHome),
      GEP_ASSETS_DIR: assetsDir,
    }, ['--dry-run', '--force']);
    const realResult = await runSync({
      A2A_HUB_URL: mock.url,
      ...unauthenticatedEnv(evolverHome),
      GEP_ASSETS_DIR: assetsDir,
    }, ['--force']);

    assert.equal(dryRunResult.status, 0, 'dry-run should succeed; stdout=' + dryRunResult.stdout + ' stderr=' + dryRunResult.stderr);
    assert.equal(realResult.status, 0, 'real sync should succeed; stdout=' + realResult.stdout + ' stderr=' + realResult.stderr);
    const requests = mock.requests.slice(requestCountBefore);
    assert.ok(requests.length >= 2);
    assert.ok(requests.every((request) => request.url.searchParams.get('node_id') === nodeId));
    assert.equal(fs.readFileSync(path.join(evolverHome, 'node_id'), 'utf8'), nodeId);
  });

  it('keeps stale suppression state unchanged while using the current env secret', async () => {
    const crypto = require('crypto');
    const root = fs.mkdtempSync(path.join(os.tmpdir(), 'evolver-sync-dry-run-suppression-'));
    const evolverHome = path.join(root, 'home');
    const staleSecret = 'e'.repeat(64);
    const currentSecret = 'f'.repeat(64);
    const staleMarker = 'sha256:' + crypto.createHash('sha256').update(staleSecret).digest('hex');
    writePersistedIdentity(evolverHome, {
      nodeId: 'node_dddddddddddd',
      secret: 'a'.repeat(64),
      version: 4,
      source: 'env_seed',
      suppression: staleMarker,
    });
    const before = snapshotFiles(evolverHome);
    const requestCountBefore = mock.requests.length;

    const r = await runSync({
      A2A_HUB_URL: mock.url,
      ...unauthenticatedEnv(evolverHome),
      A2A_NODE_SECRET: currentSecret,
      A2A_NODE_SECRET_VERSION: '9',
    }, ['--dry-run']);

    assert.equal(r.status, 0, 'current env secret dry-run should succeed; stdout=' + r.stdout + ' stderr=' + r.stderr);
    const requests = mock.requests.slice(requestCountBefore);
    assert.ok(requests.length > 0, 'authenticated dry-run should perform GET requests');
    assert.ok(requests.every((request) => request.method === 'GET'));
    assert.ok(requests.every((request) => request.headers.authorization === 'Bearer ' + currentSecret));
    assert.deepEqual(snapshotFiles(evolverHome), before, 'dry-run must not clear suppression or rewrite persisted credentials');
  });

  it('reads an existing store during dry-run without changing its files', async () => {
    const { assetsDir } = mkSandbox();
    const genesFile = path.join(assetsDir, 'genes.json');
    const capsulesFile = path.join(assetsDir, 'capsules.json');
    fs.writeFileSync(genesFile, JSON.stringify({
      version: 1,
      genes: [{
        id: 'gene_gep_repair_from_errors',
        strategy: ['hub-strategy-step'],
        hub_asset_id: 'hub-asset-aaaa1111',
      }],
    }, null, 2));
    fs.writeFileSync(capsulesFile, JSON.stringify({
      version: 1,
      capsules: [{ id: 'local-only-capsule', summary: 'keep me' }],
    }, null, 2));
    const beforeGenes = fs.readFileSync(genesFile);
    const beforeCapsules = fs.readFileSync(capsulesFile);
    const genesMtime = fs.statSync(genesFile).mtimeMs;
    const capsulesMtime = fs.statSync(capsulesFile).mtimeMs;

    const r = await runSync({
      A2A_HUB_URL: mock.url,
      A2A_NODE_ID: 'node_aaaaaaaaaaaa',
      A2A_NODE_SECRET: 'a'.repeat(64),
      GEP_ASSETS_DIR: assetsDir,
    }, ['--dry-run']);

    assert.equal(r.status, 0, 'dry-run should exit 0; stdout=' + r.stdout + ' stderr=' + r.stderr);
    assert.match(r.stdout, /already_synced=1/);
    assert.match(r.stdout, /Local-only \(not on Hub\): genes=0 capsules=1/);
    assert.deepEqual(fs.readFileSync(genesFile), beforeGenes);
    assert.deepEqual(fs.readFileSync(capsulesFile), beforeCapsules);
    assert.equal(fs.statSync(genesFile).mtimeMs, genesMtime);
    assert.equal(fs.statSync(capsulesFile).mtimeMs, capsulesMtime);
    assert.deepEqual(listFilesRecursive(assetsDir), ['capsules.json', 'genes.json']);
  });
});

describe('sync persisted asset integrity', () => {
  it('preserves first-party Capsule source types in dry-run and real sync', async () => {
    const sourceTypes = ['skill2gep_hook', 'conversation_distillation'];
    const sourceTypeMock = await startMock({
      '/a2a/assets/published-by-me': () => ({
        body: {
          assets: sourceTypes.map((sourceType) => ({
            asset_id: 'hub-source-type-' + sourceType,
            asset_type: 'Capsule',
            local_id: 'capsule_' + sourceType,
            payload: {
              id: 'capsule_' + sourceType,
              outcome: { status: 'success', score: 1 },
              source_type: sourceType,
            },
          })),
          count: sourceTypes.length,
          has_more: false,
          next_cursor: null,
        },
      }),
    });
    const { assetsDir } = mkSandbox();

    try {
      const env = {
        A2A_HUB_URL: sourceTypeMock.url,
        A2A_NODE_ID: 'node_aaaaaaaaaaaa',
        A2A_NODE_SECRET: 'a'.repeat(64),
        GEP_ASSETS_DIR: assetsDir,
      };
      const dryRunResult = await runSync(env, ['--dry-run']);
      const realResult = await runSync(env);

      assert.equal(dryRunResult.status, 0, 'dry-run should accept first-party source types; stdout=' + dryRunResult.stdout + ' stderr=' + dryRunResult.stderr);
      assert.match(dryRunResult.stdout, /synced=2/);
      assert.match(dryRunResult.stdout, /errors=0/);
      assert.equal(realResult.status, 0, 'real sync should persist first-party source types; stdout=' + realResult.stdout + ' stderr=' + realResult.stderr);
      assert.match(realResult.stdout, /synced=2/);
      assert.match(realResult.stdout, /errors=0/);

      const capsules = JSON.parse(fs.readFileSync(path.join(assetsDir, 'capsules.json'), 'utf8')).capsules;
      for (const sourceType of sourceTypes) {
        const capsule = capsules.find((item) => item.hub_asset_id === 'hub-source-type-' + sourceType);
        assert.ok(capsule, 'expected persisted capsule for ' + sourceType);
        assert.equal(capsule.source_type, sourceType);
      }
    } finally {
      sourceTypeMock.server.close();
    }
  });

  it('recomputes Gene and Capsule asset ids after sync metadata is added', async () => {
    const { verifyAssetId } = require('../src/gep/contentHash');
    const staleAssetId = 'sha256:' + '0'.repeat(64);
    const integrityMock = await startMock({
      '/a2a/assets/published-by-me': () => ({
        body: {
          assets: [
            {
              asset_id: 'hub-integrity-gene',
              asset_type: 'Gene',
              local_id: 'gene_integrity',
              payload: {
                type: 'Gene',
                id: 'gene_integrity',
                asset_id: staleAssetId,
                category: 'regulatory',
                signals_match: ['policy_violation'],
                trigger: 'manual_review',
                strategy: ['enforce policy'],
                validation: ['node --test'],
                metadata: { author: 'hub' },
              },
            },
            {
              asset_id: 'hub-integrity-capsule',
              asset_type: 'Capsule',
              local_id: 'capsule_integrity',
              payload: {
                type: 'Capsule',
                id: 'capsule_integrity',
                asset_id: staleAssetId,
                trigger: ['policy_violation'],
                gene: 'gene_integrity',
                summary: 'policy enforced',
                confidence: 0.9,
                blast_radius: { files: 1, lines: 2 },
                outcome: { status: 'success', score: 0.9 },
                execution_trace: [],
                content: 'safe markdown content',
                diff: 'diff --git a/src/example.js b/src/example.js',
                success_reason: 'validation passed',
                trigger_context: { prompt: 'review policy' },
              },
            },
          ],
          count: 2,
          has_more: false,
          next_cursor: null,
        },
      }),
      '/a2a/assets/purchased': () => ({
        body: { assets: [], count: 0, has_more: false, next_cursor: null },
      }),
    });
    const { assetsDir } = mkSandbox();

    try {
      const result = await runSync({
        A2A_HUB_URL: integrityMock.url,
        A2A_NODE_ID: 'node_aaaaaaaaaaaa',
        A2A_NODE_SECRET: 'a'.repeat(64),
        GEP_ASSETS_DIR: assetsDir,
      }, ['--force']);

      assert.equal(result.status, 0, 'sync should persist both assets; stdout=' + result.stdout + ' stderr=' + result.stderr);
      assert.doesNotMatch(result.stderr, /Gene schema validation warning/);
      const gene = JSON.parse(fs.readFileSync(path.join(assetsDir, 'genes.json'), 'utf8')).genes
        .find((item) => item.id === 'gene_integrity');
      const capsule = JSON.parse(fs.readFileSync(path.join(assetsDir, 'capsules.json'), 'utf8')).capsules
        .find((item) => item.id === 'capsule_integrity');

      assert.ok(gene);
      assert.ok(capsule);
      assert.notEqual(gene.asset_id, staleAssetId);
      assert.notEqual(capsule.asset_id, staleAssetId);
      assert.equal(verifyAssetId(gene), true);
      assert.equal(verifyAssetId(capsule), true);
      assert.equal(gene.category, 'regulatory');
      assert.equal(gene.trigger, 'manual_review');
      assert.deepEqual(gene.metadata, { author: 'hub' });
      assert.equal(capsule.content, 'safe markdown content');
      assert.equal(capsule.diff, 'diff --git a/src/example.js b/src/example.js');
      assert.equal(capsule.success_reason, 'validation passed');
      assert.deepEqual(capsule.trigger_context, { prompt: 'review policy' });
    } finally {
      integrityMock.server.close();
    }
  });
});
