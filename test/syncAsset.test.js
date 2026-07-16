'use strict';

const assert = require('node:assert/strict');
const { describe, it } = require('node:test');

const { prepareSyncAsset } = require('../src/gep/syncAsset');
const { verifyAssetId } = require('../src/gep/contentHash');
const { createGene, validateGene, VALID_CATEGORIES } = require('../src/gep/schemas/gene');

const context = {
  assetId: 'hub_asset_1',
  localId: 'local_fallback',
  summary: 'list summary',
  syncedAt: '2026-07-13T00:00:00.000Z',
};

describe('prepareSyncAsset', () => {
  it('preserves standard Gene fields and adds sync metadata', () => {
    const payload = {
      type: 'Gene',
      id: 'gene_standard',
      category: 'repair',
      signals_match: ['timeout', 'http_500'],
      strategy: ['Inspect logs', 'Apply fix'],
      validation: ['node --test'],
      preconditions: ['Reproduction exists'],
      constraints: { max_files: 3, forbidden_paths: ['.git', 'secrets'] },
      anti_patterns: ['blind retry'],
      routing_hint: { tier: 'mid', reasoning_level: 'high' },
      tool_policy: { deny: ['shell'], severity: 'block' },
      schema_version: '1.8.0',
      summary: 'standard gene',
      epigenetic_marks: [{ mark: 'verified' }],
      learning_history: [{ outcome: 'success' }],
      trigger: 'manual_review',
      parent: 'sha256:parent',
      postconditions: ['policy applied'],
      metadata: { author: 'hub' },
      performance_metrics: { success_rate: 0.99 },
      anti_pattern: true,
      failure_reason: 'unsafe default',
      model_name: 'evox-test',
      domain: 'governance',
      asset_id: 'sha256:' + '0'.repeat(64),
    };

    const result = prepareSyncAsset({ ...context, assetType: 'Gene', payload });

    assert.equal(result.id, payload.id);
    assert.equal(result.schema_version, payload.schema_version);
    assert.deepEqual(result.signals_match, payload.signals_match);
    assert.deepEqual(result.preconditions, payload.preconditions);
    assert.deepEqual(result.constraints, payload.constraints);
    assert.deepEqual(result.anti_patterns, payload.anti_patterns);
    assert.deepEqual(result.routing_hint, payload.routing_hint);
    assert.deepEqual(result.tool_policy, payload.tool_policy);
    assert.deepEqual(result.validation, payload.validation);
    for (const field of ['trigger', 'parent', 'postconditions', 'metadata', 'performance_metrics', 'anti_pattern', 'failure_reason', 'model_name', 'domain']) {
      assert.deepEqual(result[field], payload[field], field);
    }
    assert.equal(result.hub_asset_id, context.assetId);
    assert.equal(result.synced_at, context.syncedAt);
    assert.notEqual(result.asset_id, payload.asset_id);
    assert.equal(verifyAssetId(result), true);
    assert.equal(Object.hasOwn(result, 'signals'), false);
  });

  it('accepts Hub regulatory Genes with string triggers', () => {
    const result = prepareSyncAsset({
      ...context,
      assetType: 'Gene',
      payload: {
        id: 'gene_regulatory',
        category: 'regulatory',
        signals_match: ['policy_violation'],
        trigger: 'manual_review',
        strategy: ['enforce policy'],
      },
    });

    assert.equal(result.category, 'regulatory');
    assert.equal(result.trigger, 'manual_review');
    assert.equal(verifyAssetId(result), true);
  });

  it('keeps the Hub regulatory exception out of standard Gene APIs', () => {
    assert.equal(VALID_CATEGORIES.includes('regulatory'), false);
    assert.equal(createGene({ category: 'regulatory' }).category, 'innovate');
    assert.throws(() => validateGene({
      type: 'Gene',
      id: 'gene_regulatory',
      category: 'regulatory',
      signals_match: [],
      strategy: [],
    }), /Gene\.category must be one of/);
  });

  it('defaults optional Hub array fields when omitted', () => {
    const gene = prepareSyncAsset({
      ...context,
      assetType: 'Gene',
      payload: {
        id: 'gene_without_strategy',
        category: 'repair',
        signals_match: ['timeout'],
      },
    });
    const capsule = prepareSyncAsset({
      ...context,
      assetType: 'Capsule',
      payload: {
        id: 'capsule_without_trace',
        outcome: { status: 'success' },
      },
    });

    assert.deepEqual(gene.strategy, []);
    assert.deepEqual(capsule.trigger, []);
    assert.deepEqual(capsule.execution_trace, []);
  });

  it('rejects a Gene missing the standard signals_match field', () => {
    assert.throws(() => prepareSyncAsset({
      ...context,
      assetType: 'Gene',
      payload: { id: 'gene_legacy', category: 'repair', signals: ['error'], strategy: ['fix'] },
    }), /Gene\.signals_match must be an array/);
  });

  it('rejects malformed Gene fields instead of defaulting them away', () => {
    assert.throws(() => prepareSyncAsset({
      ...context,
      assetType: 'Gene',
      payload: { id: 'gene_bad', category: 'repair', signals_match: 'error', strategy: [] },
    }), /Gene\.signals_match must be an array/);
    assert.throws(() => prepareSyncAsset({
      ...context,
      assetType: 'Gene',
      payload: { id: 'gene_bad', category: 'repair', signals_match: [], strategy: [], constraints: [] },
    }), /Gene\.constraints must be an object/);
    assert.throws(() => prepareSyncAsset({
      ...context,
      assetType: 'Gene',
      payload: {
        id: 'gene_bad',
        category: 'repair',
        signals_match: [],
        strategy: [],
        routing_hint: { tier: 'unlimited' },
      },
    }), /Gene\.routing_hint\.tier must be one of/);
    assert.throws(() => prepareSyncAsset({
      ...context,
      assetType: 'Gene',
      payload: {
        id: 'gene_bad',
        category: 'repair',
        signals_match: [],
        strategy: [],
        trigger: ['manual_review'],
      },
    }), /Gene\.trigger must be a string/);
  });

  it('preserves standard Capsule fields and compatibility genes_used', () => {
    const payload = {
      type: 'Capsule',
      id: 'capsule_standard',
      schema_version: '1.8.0',
      trigger: ['timeout'],
      gene: 'gene_standard',
      genes_used: ['gene_standard'],
      summary: 'standard capsule',
      confidence: 0.9,
      blast_radius: { files: 2, lines: 12 },
      outcome: { status: 'success', score: 0.95 },
      env_fingerprint: { platform: 'darwin', arch: 'arm64' },
      success_streak: 2,
      success_reason: 'tests passed',
      source_type: 'reused',
      reused_asset_id: 'sha256:source',
      a2a: { eligible_to_broadcast: true },
      strategy: ['apply fix'],
      execution_trace: [{ command: 'node --test', exit_code: 0 }],
      visibility: 'private',
      scope: ['repo'],
      cost_tier: 'standard',
      pack_of: ['pack_1'],
      author: { handle: 'tester', evox_install_id: 'install_1' },
      parent: 'sha256:parent',
      validation: ['node --test'],
      code_snippet: 'return safeValue;',
      content: 'safe markdown content',
      diff: 'diff --git a/src/example.js b/src/example.js',
      preconditions: ['clean worktree'],
      postconditions: ['tests pass'],
      metadata: { tags: ['sync'] },
      performance_metrics: { latency_ms: 12 },
      capsule_id: 'capsule_alias',
      failure_reason: 'previous timeout',
      diff_snapshot: 'diff --git a/a b/a',
      lesson_learned: 'validate before write',
      model_name: 'evox-test',
      trigger_context: { prompt: 'repair timeout', context_signals: ['timeout'] },
      skills_used: [{ type: 'internal', skill_id: 'debugging', name: 'Debugging' }],
      domain: 'backend',
      asset_id: 'sha256:' + '1'.repeat(64),
    };

    const result = prepareSyncAsset({ ...context, assetType: 'Capsule', payload });

    for (const field of ['schema_version', 'trigger', 'gene', 'genes_used', 'confidence', 'blast_radius', 'outcome', 'env_fingerprint', 'success_streak', 'success_reason', 'source_type', 'reused_asset_id', 'a2a', 'strategy', 'execution_trace', 'visibility', 'scope', 'cost_tier', 'pack_of', 'author', 'parent', 'validation', 'code_snippet', 'content', 'diff', 'preconditions', 'postconditions', 'metadata', 'performance_metrics', 'capsule_id', 'failure_reason', 'diff_snapshot', 'lesson_learned', 'model_name', 'trigger_context', 'skills_used', 'domain']) {
      assert.deepEqual(result[field], payload[field], field);
    }
    assert.equal(result.hub_asset_id, context.assetId);
    assert.equal(result.synced_at, context.syncedAt);
    assert.notEqual(result.asset_id, payload.asset_id);
    assert.equal(verifyAssetId(result), true);
  });

  for (const sourceType of ['skill2gep_hook', 'conversation_distillation']) {
    it('preserves Hub Capsule source_type ' + sourceType, () => {
      const result = prepareSyncAsset({
        ...context,
        assetType: 'Capsule',
        payload: {
          id: 'capsule_' + sourceType,
          outcome: { status: 'success' },
          source_type: sourceType,
        },
      });

      assert.equal(result.source_type, sourceType);
      assert.equal(verifyAssetId(result), true);
    });
  }

  it('copies only explicit contract fields from plain payloads', () => {
    const payload = JSON.parse('{"id":"gene_safe","category":"repair","signals_match":["error"],"strategy":["fix"],"constructor":{"polluted":true},"prototype":{"polluted":true},"unknown_field":"drop"}');
    const result = prepareSyncAsset({ ...context, assetType: 'Gene', payload });

    assert.equal(Object.getPrototypeOf(result), Object.prototype);
    assert.equal(Object.hasOwn(result, 'constructor'), false);
    assert.equal(Object.hasOwn(result, 'prototype'), false);
    assert.equal(Object.hasOwn(result, 'unknown_field'), false);
    assert.equal({}.polluted, undefined);
  });

  it('rejects malformed Capsule payloads before normalization', () => {
    assert.throws(() => prepareSyncAsset({
      ...context,
      assetType: 'Capsule',
      payload: { id: 'capsule_bad', outcome: {}, trigger: [], execution_trace: [] },
    }), /Capsule\.outcome\.status must be one of/);
    assert.throws(() => prepareSyncAsset({
      ...context,
      assetType: 'Capsule',
      payload: { id: 'capsule_bad', outcome: { status: 'success' }, trigger: 'timeout', execution_trace: [] },
    }), /Capsule\.trigger must be an array/);
    assert.throws(() => prepareSyncAsset({
      ...context,
      assetType: 'Capsule',
      payload: {
        id: 'capsule_bad',
        outcome: { status: 'success' },
        trigger: [],
        execution_trace: [],
        visibility: 'everyone',
      },
    }), /Capsule\.visibility must be one of/);
    for (const sourceType of [42, '', ' padded ', 'x'.repeat(129), 'line\nbreak']) {
      assert.throws(() => prepareSyncAsset({
        ...context,
        assetType: 'Capsule',
        payload: {
          id: 'capsule_bad_source_type',
          outcome: { status: 'success' },
          source_type: sourceType,
        },
      }), /Capsule\.source_type must be null or a non-empty string of at most 128 characters/);
    }
  });

  it('uses local id and list summary only when payload omits them', () => {
    const result = prepareSyncAsset({
      ...context,
      assetType: 'Gene',
      payload: { category: 'optimize', signals_match: [], strategy: [] },
    });
    assert.equal(result.id, context.localId);
    assert.equal(result.summary, context.summary);
  });

  it('requires explicit sync metadata so conversion stays deterministic', () => {
    assert.throws(() => prepareSyncAsset({
      assetType: 'Gene',
      assetId: context.assetId,
      localId: context.localId,
      payload: { category: 'repair', signals_match: [], strategy: [] },
    }), /syncedAt is required/);
  });
});
