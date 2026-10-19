import {
  chMigrationClient,
  createTable,
  getTableKeys,
  runClickhouseMigrationCommands,
} from '../clickhouse/migration';
import { type CodeMigrationEnv, getIsCluster, writeSqlDump } from './helpers';

/**
 * Repartitions `profiles` from toYYYYMM(created_at) to a hash of the id.
 *
 * ReplacingMergeTree only removes duplicates inside one partition. An
 * anonymous profile that comes back after a while can get a new created_at,
 * so its rows landed in different monthly partitions and never merged. With
 * the partition derived from the id, every row of a profile shares a
 * partition. Sixteen buckets keep each partition small enough to OPTIMIZE
 * FINAL on its own.
 *
 * Unlike 27-sessions-sort-key-id.ts this copies BEFORE swapping. The profile
 * buffer reads a profile back from ClickHouse when it is not in its Redis
 * cache and merges the incoming change into it; a profile missing from the
 * live table would come back empty, and the merged row, being newest, would
 * replace the profile's history. So:
 *   1. Create the new table and copy the old one into it while the old one
 *      stays live.
 *   2. Swap the new table in with EXCHANGE TABLES, which is atomic.
 *   3. Copy again from the old table, picking up profiles written to it
 *      during step 1. Those were just written, so the buffer still has them
 *      cached for the moment between the swap and this pass.
 *
 * Every batch only copies a profile whose latest last_seen_at in the old
 * table is newer than anything the new table has for it. ReplacingMergeTree
 * keeps the newest row, so an older copy would change nothing; skipping it
 * makes each pass, and a rerun after a failure, write only what is missing.
 *
 * The copy keeps one row per profile, built the way the profile buffer reads
 * a profile (argMax by last_seen_at, earliest created_at), so readers see the
 * same values as before and the cross-partition duplicates are gone.
 *
 * Batches follow the sort key: contiguous project_id ranges of at most
 * MAX_ROWS_PER_BATCH rows, and projects larger than that split by id hash.
 *
 * The old table stays as `profiles_<SUFFIX>`. Drop it once the copy is
 * verified:
 *   Non-clustered: DROP TABLE profiles_<SUFFIX>
 *   Clustered:     DROP TABLE profiles_<SUFFIX> ON CLUSTER '{cluster}';
 *                  DROP TABLE profiles_<SUFFIX>_replicated ON CLUSTER '{cluster}'
 *
 * Safe to rerun after a failure at any step: the swap steps are skipped once
 * the live table has the new partition key, and the copy rule above makes the
 * passes idempotent.
 */

const SUFFIX = '20261019';
const NEW_TABLE = `profiles_new_${SUFFIX}`;
const OLD_TABLE = `profiles_${SUFFIX}`;
const DISTRIBUTION_HASH = 'cityHash64(project_id)';
const PARTITION_BUCKETS = 16;
const MAX_ROWS_PER_BATCH = 2_000_000;

const COLUMNS = [
  '`id` String CODEC(ZSTD(3))',
  '`is_external` Bool',
  '`first_name` String CODEC(ZSTD(3))',
  '`last_name` String CODEC(ZSTD(3))',
  '`email` String CODEC(ZSTD(3))',
  '`avatar` String CODEC(ZSTD(3))',
  '`properties` Map(String, String) CODEC(ZSTD(3))',
  '`project_id` String CODEC(ZSTD(3))',
  '`groups` Array(String) DEFAULT [] CODEC(ZSTD(3))',
  '`created_at` DateTime64(3) CODEC(Delta(4), LZ4)',
  '`last_seen_at` DateTime64(3) CODEC(Delta(4), LZ4)',
];

const COLUMN_NAMES = COLUMNS.map((column) => column.split('`')[1] ?? '');
const GROUP_KEY = new Set(['project_id', 'id']);

/**
 * Aliases are prefixed so they do not shadow the source columns inside
 * argMax(); INSERT … (columns) SELECT maps by position, not by name.
 */
function latestProfileSelect(column: string) {
  if (GROUP_KEY.has(column)) {
    return column;
  }
  if (column === 'created_at') {
    return 'min(created_at) AS v_created_at';
  }
  if (column === 'last_seen_at') {
    return 'max(last_seen_at) AS v_last_seen_at';
  }
  return `argMax(${column}, last_seen_at) AS v_${column}`;
}

function quote(value: string) {
  return `'${value.replaceAll('\\', '\\\\').replaceAll("'", "\\'")}'`;
}

/**
 * parallel_distributed_insert_select would run this whole statement on each
 * shard, where the GLOBAL subquery's unqualified table does not exist; it has
 * to run once, on the initiator, against the Distributed tables.
 */
function copyBatchSql(source: string, target: string, where: string) {
  const outputColumns = COLUMN_NAMES.map((column) =>
    GROUP_KEY.has(column) ? column : `v_${column}`
  );
  return `INSERT INTO ${target} (${COLUMN_NAMES.join(', ')})
SELECT ${outputColumns.join(', ')}
FROM (
  SELECT ${COLUMN_NAMES.map(latestProfileSelect).join(', ')}
  FROM ${source}
  WHERE ${where}
  GROUP BY project_id, id
) AS latest
GLOBAL LEFT JOIN (
  SELECT project_id, id, max(last_seen_at) AS known_last_seen_at
  FROM ${target}
  WHERE ${where}
  GROUP BY project_id, id
) AS known USING (project_id, id)
WHERE latest.v_last_seen_at > known.known_last_seen_at
SETTINGS insert_distributed_sync = 1, parallel_distributed_insert_select = 0, join_use_nulls = 0`;
}

const local = (table: string, isClustered: boolean) =>
  isClustered ? `${table}_replicated` : table;

const onCluster = (isClustered: boolean) =>
  isClustered ? " ON CLUSTER '{cluster}'" : '';

function createNewTableSqls(isClustered: boolean) {
  return createTable({
    name: NEW_TABLE,
    columns: COLUMNS,
    indices: [
      'INDEX idx_first_name first_name TYPE bloom_filter GRANULARITY 1',
      'INDEX idx_last_name last_name TYPE bloom_filter GRANULARITY 1',
      'INDEX idx_email email TYPE bloom_filter GRANULARITY 1',
    ],
    engine: 'ReplacingMergeTree(last_seen_at)',
    orderBy: ['project_id', 'id'],
    partitionBy: `cityHash64(id) % ${PARTITION_BUCKETS}`,
    settings: { index_granularity: 8192 },
    distributionHash: DISTRIBUTION_HASH,
    replicatedVersion: '2',
    isClustered,
  });
}

/**
 * Clustered, the `profiles` Distributed table routes to whatever is named
 * profiles_replicated, so only the `_replicated` tables are exchanged. The new
 * table's own Distributed wrapper, used for the first pass, would point at the
 * renamed-away table afterwards, so it is replaced by one for the old table.
 */
function swapSqls(isClustered: boolean, { exchange }: { exchange: boolean }) {
  const cluster = onCluster(isClustered);
  const sqls: string[] = [];
  if (exchange) {
    sqls.push(
      `EXCHANGE TABLES ${local(NEW_TABLE, isClustered)} AND ${local('profiles', isClustered)}${cluster}`
    );
  }
  if (isClustered) {
    sqls.push(`DROP TABLE IF EXISTS ${NEW_TABLE}${cluster}`);
  }
  sqls.push(
    `RENAME TABLE ${local(NEW_TABLE, isClustered)} TO ${local(OLD_TABLE, isClustered)}${cluster}`
  );
  if (isClustered) {
    sqls.push(`CREATE TABLE IF NOT EXISTS ${OLD_TABLE}${cluster} AS ${OLD_TABLE}_replicated
ENGINE = Distributed('{cluster}', currentDatabase(), ${OLD_TABLE}_replicated, ${DISTRIBUTION_HASH})`);
  }
  return sqls;
}

interface ProjectRows {
  project_id: string;
  rows: string;
}

function splitLargeProject(projectId: string, rows: number) {
  const parts = Math.ceil(rows / MAX_ROWS_PER_BATCH);
  return Array.from(
    { length: parts },
    (_, part) =>
      `project_id = ${quote(projectId)} AND cityHash64(id) % ${parts} = ${part}`
  );
}

function projectRange(first: string, last: string) {
  return `project_id >= ${quote(first)} AND project_id <= ${quote(last)}`;
}

/** WHERE clauses that together cover every row of the old table once. */
async function getCopyBatches(source: string) {
  const response = await chMigrationClient.query({
    query: `SELECT project_id, count() AS rows FROM ${source} GROUP BY project_id ORDER BY project_id`,
    format: 'JSONEachRow',
  });
  const projects = await response.json<ProjectRows>();

  const batches: string[] = [];
  let range: { first: string; last: string; rows: number } | undefined;

  for (const project of projects) {
    const rows = Number(project.rows);
    const isLarge = rows > MAX_ROWS_PER_BATCH;
    const rangeIsFull = range && range.rows + rows > MAX_ROWS_PER_BATCH;

    if (range && (isLarge || rangeIsFull)) {
      batches.push(projectRange(range.first, range.last));
      range = undefined;
    }
    if (isLarge) {
      batches.push(...splitLargeProject(project.project_id, rows));
      continue;
    }
    range = range
      ? { ...range, last: project.project_id, rows: range.rows + rows }
      : { first: project.project_id, last: project.project_id, rows };
  }
  if (range) {
    batches.push(projectRange(range.first, range.last));
  }
  return batches;
}

/** Runs in the background once the api is up; see the rerun notes above. */
export const online = true;

export async function up(env: CodeMigrationEnv) {
  const isClustered = getIsCluster(env);
  const isDry = process.argv.includes('--dry');
  const tables = await getTableKeys();
  const live = tables.get(local('profiles', isClustered));
  const isSwapped = live?.partitionKey.includes('cityHash64(id)') ?? false;
  const isRenamePending =
    isSwapped && tables.has(local(NEW_TABLE, isClustered));

  const executed: string[] = [];
  const run = async (sqls: string[]) => {
    executed.push(...sqls);
    if (!isDry) {
      await runClickhouseMigrationCommands(sqls);
    }
  };

  if (!isSwapped) {
    await run(createNewTableSqls(isClustered));
    const batches = await getCopyBatches('profiles');
    await run(
      batches.map((where) => copyBatchSql('profiles', NEW_TABLE, where))
    );
    await run(swapSqls(isClustered, { exchange: true }));
  } else if (isRenamePending) {
    await run(swapSqls(isClustered, { exchange: false }));
  }

  // A dry run has not swapped anything, so the old rows are still in
  // `profiles` unless an earlier run already moved them.
  const source = isDry && !isSwapped ? 'profiles' : OLD_TABLE;
  const catchUpBatches = await getCopyBatches(source);
  await run(
    catchUpBatches.map((where) => copyBatchSql(OLD_TABLE, 'profiles', where))
  );

  writeSqlDump(import.meta.url, executed);
}
