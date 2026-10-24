import * as NodeServices from "@effect/platform-node/NodeServices";
import { assert, it } from "@effect/vitest";
import * as Deferred from "effect/Deferred";
import * as Effect from "effect/Effect";
import * as FileSystem from "effect/FileSystem";
import * as Layer from "effect/Layer";
import * as Path from "effect/Path";
import * as SqlClient from "effect/sql/SqlClient";
import * as TestClock from "effect/testing/TestClock";

import * as ServerConfig from "../config.ts";
import * as EventStore from "../orchestration-v2/EventStore.ts";
import * as ProjectionMaintenance from "../orchestration-v2/ProjectionMaintenance.ts";
import * as ProjectionStore from "../orchestration-v2/ProjectionStore.ts";
import * as DatabaseStorage from "./DatabaseStorage.ts";
import * as Sqlite from "./Sqlite.ts";

const freePages = Effect.map(
  Effect.flatMap(
    SqlClient.SqlClient,
    (sql) => sql<{ readonly freelist_count: number }>`PRAGMA freelist_count`,
  ),
  (rows) => rows[0]?.freelist_count ?? 0,
);

const storageLayer = <E>(
  config: Layer.Layer<ServerConfig.ServerConfig, E, FileSystem.FileSystem | Path.Path>,
) =>
  DatabaseStorage.layer.pipe(
    Layer.provide(ProjectionMaintenance.layer),
    Layer.provide(Layer.merge(EventStore.layer, ProjectionStore.layer)),
    Layer.provideMerge(Sqlite.layerMemory),
    Layer.provideMerge(config),
    Layer.provide(NodeServices.layer),
  );

const testConfig = ServerConfig.layerTest(process.cwd(), { prefix: "database-storage-" });

it.effect("a new database returns freed pages online without losing data", () =>
  Effect.gen(function* () {
    const sql = yield* SqlClient.SqlClient;
    const storage = yield* DatabaseStorage.DatabaseStorage;
    assert.deepEqual(yield* sql`PRAGMA auto_vacuum`, [{ auto_vacuum: 2 }]);
    yield* sql`CREATE TABLE retained (value TEXT)`;
    yield* sql`INSERT INTO retained VALUES ('keep')`;
    yield* sql`CREATE TABLE discarded (value BLOB)`;
    yield* sql`INSERT INTO discarded VALUES (zeroblob(1048576))`;
    yield* sql`DROP TABLE discarded`;
    assert.isAbove(yield* freePages, 0);
    yield* storage.reclaim;
    assert.equal(yield* freePages, 0);
    assert.deepEqual(yield* sql`SELECT value FROM retained`, [{ value: "keep" }]);
  }).pipe(Effect.provide(storageLayer(testConfig))),
);

// The first retention pass on a large database stalls foreground writes behind
// WAL checkpoints unless it rests between pages.
it.effect("the background pass rests between pages and records completion after it", () =>
  Effect.gen(function* () {
    const startupTime = Date.UTC(2026, 9, 19);
    yield* TestClock.setTime(startupTime);
    const fs = yield* FileSystem.FileSystem;
    const path = yield* Path.Path;
    const config = yield* ServerConfig.ServerConfig;
    const markerPath = path.join(path.dirname(config.dbPath), "history-retained-at");
    const pageDone = yield* Deferred.make<void>();
    const completed = yield* Deferred.make<void>();
    const markerWritten = yield* Deferred.make<void>();
    // One page of 30 ms work, then the pacer, then the rest of the pass.
    const compactEventStore = Effect.gen(function* () {
      yield* Effect.sleep(30);
      yield* Deferred.succeed(pageDone, undefined);
      yield* yield* ProjectionMaintenance.CompactionPacer;
      yield* Deferred.succeed(completed, undefined);
      return { deletedEventCount: 0, deletedReceiptCount: 0, reclaimableBytes: 0 };
    });
    yield* Effect.gen(function* () {
      yield* TestClock.adjust(60_000);
      yield* TestClock.adjust(30);
      yield* Deferred.await(pageDone);
      // 30 ms of work earns a 270 ms rest at 10% duty.
      yield* TestClock.adjust(269);
      assert.isFalse(yield* Deferred.isDone(completed));
      assert.isFalse(yield* fs.exists(markerPath));
      yield* TestClock.adjust(1);
      yield* Deferred.await(markerWritten);
      assert.isTrue(yield* Deferred.isDone(completed));
    }).pipe(
      Effect.provide(
        DatabaseStorage.layer.pipe(
          Layer.provide(
            Layer.succeed(ProjectionMaintenance.ProjectionMaintenanceV2, {
              compactEventStore,
              verify: Effect.die("unexpected verification"),
              rebuild: Effect.die("unexpected rebuild"),
            }),
          ),
          Layer.provide(Sqlite.layerMemory),
        ),
      ),
      Effect.provideService(FileSystem.FileSystem, {
        ...fs,
        writeFileString: (file, content, options) =>
          fs
            .writeFileString(file, content, options)
            .pipe(Effect.ensuring(Deferred.succeed(markerWritten, undefined))),
      }),
    );
  }).pipe(Effect.provide(testConfig.pipe(Layer.provideMerge(NodeServices.layer)))),
);
