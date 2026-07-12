// 通用小型 KV 存储（目前只用于角色头像 av:* 前缀）。
//
// 头像路由过去直接摸 c.env.OUTBOX（Workers KV 绑定），VPS/Node 部署下没有该绑定 → 503。
// 本工厂对齐 outboxStore/proactiveStore 的三实现模式：
//   - Workers：直接返回 env.OUTBOX KV 绑定（接口本就吻合）
//   - Node 默认：内存 Map + TTL（进程重启头像丢失，客户端「检查推送」会提示补传）
//   - Node 持久（RELAY_STORE=sqlite）：kv_store 表，与 outbox 共用同一 db 文件
//
// 接口对齐 Workers KV 用到的子集：
//   get(key, { type: 'json' }?) → string | object | null
//   put(key, value, { expirationTtl: 秒 }?)

// Node 进程级单例（Workers 每次 fetch 新 env，KV 绑定本就共享，不缓存）。
let _nodeSingleton = null;

export async function createKvStore(env) {
    if (env && env.OUTBOX && typeof env.OUTBOX.put === 'function') {
        return env.OUTBOX;
    }
    if (_nodeSingleton) return _nodeSingleton;
    const storeKind = (typeof process !== 'undefined' && process.env?.RELAY_STORE) || 'memory';
    if (storeKind === 'sqlite') {
        try {
            // 计算式路径：阻止 esbuild/wrangler 把 sqlite store(及 better-sqlite3)静态打进 Workers bundle。
            const mod = await import(/* @vite-ignore */ './sqliteKvStore' + '.js');
            _nodeSingleton = new mod.SqliteKvStore(process.env.RELAY_SQLITE_PATH || './outbox.db');
            return _nodeSingleton;
        } catch (e) {
            console.warn('[kv] sqlite 不可用，回退到内存:', e?.message);
        }
    }
    _nodeSingleton = new MemoryKvStore();
    return _nodeSingleton;
}

// ===== 内存实现（Node 默认）=====
export class MemoryKvStore {
    constructor() {
        this.kind = 'memory';
        this.map = new Map(); // key → { value, expiresAt }
    }

    async get(key, opts) {
        const rec = this.map.get(key);
        if (!rec) return null;
        if (rec.expiresAt && Date.now() > rec.expiresAt) {
            this.map.delete(key);
            return null;
        }
        if (opts?.type === 'json') {
            try { return JSON.parse(rec.value); } catch { return null; }
        }
        return rec.value;
    }

    async put(key, value, opts) {
        const ttlSec = opts?.expirationTtl;
        this.map.set(key, {
            value: String(value),
            expiresAt: Number.isFinite(ttlSec) && ttlSec > 0 ? Date.now() + ttlSec * 1000 : 0,
        });
    }

    async delete(key) {
        this.map.delete(key);
    }
}
