// 持久通用 KV（Node，RELAY_STORE=sqlite），目前只存角色头像 av:*。
// 依赖 optionalDependencies 的 better-sqlite3；装不上时 kvStore 工厂回退到内存。
// 接口对齐 Workers KV 用到的子集（get/put/delete），见 kvStore.js 顶部说明。

import { createRequire } from 'node:module';

// 计算式 require：阻止 esbuild/wrangler 把 better-sqlite3(Node-only)静态打进 Workers bundle。
function loadSqlite() {
    const require = createRequire(import.meta.url);
    return require(['better', 'sqlite3'].join('-'));
}

export class SqliteKvStore {
    constructor(path = './outbox.db') {
        this.kind = 'sqlite';
        const Database = loadSqlite();
        this.db = new Database(path);
        this.db.pragma('journal_mode = WAL');
        this.db.exec(`
            CREATE TABLE IF NOT EXISTS kv_store (
                key       TEXT PRIMARY KEY,
                value     TEXT NOT NULL,
                expiresAt INTEGER NOT NULL DEFAULT 0
            );
        `);
        // 每小时清一次过期键（头像 TTL 60 天，频率无需更高）
        this._timer = setInterval(() => this.sweep(), 60 * 60 * 1000);
        if (this._timer.unref) this._timer.unref();
    }

    async get(key, opts) {
        const row = this.db.prepare('SELECT value, expiresAt FROM kv_store WHERE key = ?').get(key);
        if (!row) return null;
        if (row.expiresAt && Date.now() > row.expiresAt) {
            this.db.prepare('DELETE FROM kv_store WHERE key = ?').run(key);
            return null;
        }
        if (opts?.type === 'json') {
            try { return JSON.parse(row.value); } catch { return null; }
        }
        return row.value;
    }

    async put(key, value, opts) {
        const ttlSec = opts?.expirationTtl;
        const expiresAt = Number.isFinite(ttlSec) && ttlSec > 0 ? Date.now() + ttlSec * 1000 : 0;
        this.db.prepare('INSERT OR REPLACE INTO kv_store (key, value, expiresAt) VALUES (?, ?, ?)')
            .run(key, String(value), expiresAt);
    }

    async delete(key) {
        this.db.prepare('DELETE FROM kv_store WHERE key = ?').run(key);
    }

    sweep() {
        this.db.prepare('DELETE FROM kv_store WHERE expiresAt > 0 AND expiresAt < ?').run(Date.now());
    }
}
