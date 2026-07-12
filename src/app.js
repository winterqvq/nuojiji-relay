// Hono app —— 一份代码，Workers 和 Node 共用。
//
// 路由：
//   GET  /health                 健康检查（设置页测连接用）
//   POST /generate               提交生成（fire-and-forget，202）
//   GET  /outbox?inboxId=&since=  拉取已生成结果
//   POST /ack                    确认并删除
//   GET  /api/push/vapid-key     取 VAPID 公钥（复用 APP 现有订阅流程）
//   POST /api/push/subscribe     注册推送订阅
//   DELETE /api/push/unsubscribe 退订

import { Hono } from 'hono';
import { cors } from 'hono/cors';
import { requireSecret } from './util/auth.js';
import { createOutboxStore } from './store/outboxStore.js';
import { createSubStore, subKey } from './store/subStore.js';
import { createProactiveStore, PROACTIVE_WINDOW_CAP } from './store/proactiveStore.js';
import { createKvStore } from './store/kvStore.js';
import { runGeneration } from './ai/aiCaller.js';
import { dispatchPush } from './push/pushSender.js';
import { getVapidPublicKey } from './push/webPush.js';
import { makeMessageId, nowMs, extractPushBodies } from './util/ids.js';

const VERSION = '1.0.0';

export function createApp() {
    const app = new Hono();

    // 中继是用户自己的后端，APP 从套壳 (https://localhost / capacitor://localhost) 或
    // 网页 (https://*.pages.dev) 跨域请求 → 放开 CORS（鉴权靠 Bearer secret，不靠 origin）。
    app.use('*', cors({
        origin: (o) => o || '*',
        allowMethods: ['GET', 'POST', 'DELETE', 'OPTIONS'],
        allowHeaders: ['Authorization', 'Content-Type'],
    }));

    // 每个请求懒初始化 store（Workers 每次 fetch 都新 env；Node 进程级缓存见下）
    const stores = { outbox: null, sub: null, proactive: null, kv: null };
    async function getStores(env) {
        if (env && env.OUTBOX) {
            // Workers：KV 绑定每次都现取，store 实例无状态可重建
            return {
                outbox: await createOutboxStore(env),
                sub: await createSubStore(env),
                proactive: await createProactiveStore(env),
                kv: await createKvStore(env),
            };
        }
        // Node：进程级单例
        if (!stores.outbox) stores.outbox = await createOutboxStore(env);
        if (!stores.sub) stores.sub = await createSubStore(env);
        if (!stores.proactive) stores.proactive = await createProactiveStore(env);
        if (!stores.kv) stores.kv = await createKvStore(env);
        return stores;
    }

    app.get('/health', async (c) => {
        const { outbox } = await getStores(c.env);
        return c.json({ ok: true, store: outbox.kind || 'unknown', version: VERSION });
    });

    // 🖼️ 角色头像公开读取（无鉴权）——iOS 通知扩展(独立进程,App 没运行)要能直接 GET 下载，
    //    附到 Communication Notification 显示在通知左侧。头像只是公开可见的角色头像，无敏感信息。
    //    存在 KV（OUTBOX namespace 的 av: 前缀），由 POST /avatar 写入。
    app.get('/avatar/:key', async (c) => {
        const key = c.req.param('key');
        if (!key || !/^[\w.-]{1,128}$/.test(key)) return c.json({ error: 'bad key' }, 400);
        const { kv } = await getStores(c.env);
        if (!kv) return c.json({ error: 'no store' }, 503);
        const rec = await kv.get(`av:${key}`, { type: 'json' }).catch(() => null);
        if (!rec || !rec.b64) return c.json({ error: 'not found' }, 404);
        try {
            const bin = Uint8Array.from(atob(rec.b64), (ch) => ch.charCodeAt(0));
            return new Response(bin, {
                status: 200,
                headers: {
                    'content-type': rec.mime || 'image/png',
                    'cache-control': 'public, max-age=86400',
                    'access-control-allow-origin': '*',
                },
            });
        } catch {
            return c.json({ error: 'decode failed' }, 500);
        }
    });

    // 以下全部要鉴权
    app.use('/avatar', requireSecret); // POST /avatar 写入要鉴权（GET /avatar/:key 上面已公开放行）
    app.use('/generate', requireSecret);
    app.use('/outbox', requireSecret);
    app.use('/ack', requireSecret);
    app.use('/api/push/subscribe', requireSecret);
    app.use('/api/push/unsubscribe', requireSecret);
    app.use('/api/push/diag', requireSecret);
    app.use('/proactive/*', requireSecret);

    app.post('/generate', async (c) => {
        let body;
        try { body = await c.req.json(); } catch { return c.json({ error: 'invalid json' }, 400); }
        const { requestId, inboxId, messages, settings, maxTokens, meta } = body || {};
        if (!requestId || !inboxId || !Array.isArray(messages) || !settings) {
            return c.json({ error: 'requestId / inboxId / messages / settings required' }, 400);
        }

        const { outbox, sub } = await getStores(c.env);

        // 幂等：同 requestId 在 TTL 内只处理一次。
        //   H4 修：命中幂等时，若 outbox 里那条结果还在（未被 ack 删）→ 直接【返回它】，让手机端拿到内容；
        //   只有结果已被取走删除时才回 409。否则旧码一律 409 无内容 → 手机重提交（网络抖/重启）就永久
        //   拿不到这条回复（6h 内）= 数据丢失。
        if (await outbox.seenRequest(requestId)) {
            try {
                const existing = (await outbox.list(inboxId, 0)).find(it => it && String(it.requestId) === String(requestId));
                if (existing && !existing.error && existing.content) {
                    return c.json({ accepted: true, requestId, generated: true, replayed: true }, 202);
                }
            } catch { /* 查不到就照旧 409 */ }
            return c.json({ duplicate: true, requestId }, 409);
        }
        await outbox.markRequest(requestId);

        // ⚠️ 在请求生命周期内「同步」跑完生成 + 写 outbox，再返回。
        //    早期用 c.executionCtx.waitUntil 在响应后跑后台任务，但 Cloudflare 免费版 Workers 对
        //    waitUntil 的 CPU/时长有严格配额，AI 调用(数秒~十几秒)常被掐断 → outbox 永远空。
        //    手机端是 fire-and-forget + 轮询，不在乎 /generate 响应快慢，故改同步等待最可靠。
        const id = makeMessageId(requestId);
        let item;
        try {
            const content = await runGeneration(settings, messages, maxTokens);
            item = {
                id, requestId,
                charId: meta?.charId ?? null, roundId: meta?.roundId ?? null, userId: meta?.userId ?? null,
                content, error: null, createdAt: nowMs(),
            };
        } catch (e) {
            item = {
                id, requestId,
                charId: meta?.charId ?? null, roundId: meta?.roundId ?? null, userId: meta?.userId ?? null,
                content: null, error: String(e?.message || e), createdAt: nowMs(),
            };
        }
        await outbox.put(inboxId, item);

        // 发推送（best-effort，丢了靠手机轮询补）。逐条发：把生成内容拆成各条可见消息，每条发一个带内容的通知，
        // 模拟真人逐条发消息的体验。拆分是通用 JSON-Lines 文本提取（取 {"t":"text","c":"..."} 的可见文本），
        // 不含任何提示词逻辑。标题用角色名（手机随 meta 传来）。
        const pushWork = (async () => {
            try {
                // ⚠️ 生成失败（502 等）不发推送：手机端排水对 error item 一律丢弃不写气泡，
                //    若这里仍弹「你有一条新消息」→ 用户点进去聊天里却什么都没有 = 假通知。
                //    失败靠手机端轮询 / 控制台 WARN 暴露即可，不打扰用户。
                if (item.error) return;
                const subs = await sub.list(inboxId);
                if (!subs.length) return;
                const title = meta?.charName || '糯叽机';
                // 🔒 通知隐私模式（手机端 meta 带来）：正文换「你有一条新消息」，标题/头像保留。
                const bodies = meta?.notifPrivacy
                    ? extractPushBodies(item.content).map(() => '你有一条新消息')
                    : extractPushBodies(item.content);
                const sleep = (ms) => new Promise((r) => setTimeout(r, ms));
                let i = 0;
                for (const body of bodies) {
                    // 逐条之间加真人节奏延迟（按字数估打字时长），第一条立即发。
                    // ⚠️ Cloudflare Workers waitUntil 有时长上限，单条延迟封顶 + 总条数防超时。
                    if (i > 0) {
                        const delay = Math.min(4000, 600 + (body?.length || 0) * 120);
                        await sleep(delay);
                    }
                    const payload = {
                        title, body, charId: item.charId, userId: item.userId, kind: 'relay-outbox',
                        // 🖼️ iOS 通知扩展：头像 URL + 发信人 + 会话 id（meta 随手机端 submitGeneration 传来）
                        avatarUrl: meta?.avatarUrl || null,
                        senderName: title,
                        conversationId: `${item.userId}_${item.charId}`,
                        mutableContent: true,
                    };
                    for (const s of subs) {
                        const res = await dispatchPush(c.env, s, payload);
                        if (res?.gone) await sub.remove(inboxId, s);
                    }
                    i++;
                }
            } catch (e) {
                console.warn('[generate] push failed:', e?.message);
            }
        })();
        try {
            if (typeof c.executionCtx?.waitUntil === 'function') c.executionCtx.waitUntil(pushWork);
            else pushWork.catch(() => {});
        } catch { pushWork.catch(() => {}); }

        // outbox 已写入，返回（手机轮询会拉到）。202 语义保留。
        return c.json({ accepted: true, requestId, generated: !item.error }, 202);
    });

    app.get('/outbox', async (c) => {
        const inboxId = c.req.query('inboxId');
        const since = Number(c.req.query('since') || 0);
        if (!inboxId) return c.json({ error: 'inboxId required' }, 400);
        const { outbox } = await getStores(c.env);
        const items = await outbox.list(inboxId, since);
        return c.json({ items, now: nowMs() });
    });

    app.post('/ack', async (c) => {
        let body;
        try { body = await c.req.json(); } catch { return c.json({ error: 'invalid json' }, 400); }
        const { inboxId, ids } = body || {};
        if (!inboxId || !Array.isArray(ids)) return c.json({ error: 'inboxId / ids required' }, 400);
        const { outbox } = await getStores(c.env);
        const acked = await outbox.ack(inboxId, ids);
        return c.json({ acked });
    });

    app.get('/api/push/vapid-key', async (c) => {
        const publicKey = await getVapidPublicKey(c.env);
        if (!publicKey) return c.json({ error: 'VAPID not configured' }, 503);
        return c.json({ publicKey });
    });

    app.post('/api/push/subscribe', async (c) => {
        let body;
        try { body = await c.req.json(); } catch { return c.json({ error: 'invalid json' }, 400); }
        const { inboxId, subscription, channel } = body || {};
        if (!inboxId || !subscription) return c.json({ error: 'inboxId / subscription required' }, 400);
        // 默认 web 通道（PWA）；apns/fcm 由套壳显式带 channel
        const entry = subscription.channel ? subscription : { channel: channel || 'web', sub: subscription };
        try {
            const { sub } = await getStores(c.env);
            await sub.add(inboxId, entry);
            // apns/fcm 每设备单 token：token 轮换会留下旧行，每条推送/自检都会发两遍。
            // 注册成功后清掉同 inbox 同 channel 的旧订阅，只保留这条最新的。web 多端共存不清。
            const ch = entry.channel || 'web';
            if ((ch === 'apns' || ch === 'fcm') && typeof sub.pruneChannel === 'function') {
                await sub.pruneChannel(inboxId, ch, subKey(entry));
            }
        } catch (e) {
            // 把真实异常返回（而非裸 500），便于手机端「检查推送」直接显示后端报错（如 KV 未绑定 / put 失败）。
            return c.json({ error: 'subscribe failed', detail: String(e?.message || e), hasKV: !!(c.env && c.env.OUTBOX) }, 500);
        }
        return c.json({ ok: true });
    });

    // 🩺 推送链路自检（设置页「检查推送」按钮调）：
    //   - 列出本 inbox 当前注册了哪些推送订阅通道（web/apns/fcm），不回 token 全文（脱敏）。
    //   - test:true 时对每条订阅真发一条测试推送，回每条的投递结果（含中转/APNs 的 reason）。
    //   一眼能看出：没有 apns 订阅 = 客户端没注册成功；有订阅但 dispatch 失败 = 中转/凭据问题。
    app.post('/api/push/diag', async (c) => {
        let body;
        try { body = await c.req.json(); } catch { body = {}; }
        const { inboxId, test } = body || {};
        if (!inboxId) return c.json({ error: 'inboxId required' }, 400);
        const { sub } = await getStores(c.env);
        const subs = await sub.list(inboxId);
        const mask = (s) => {
            const t = s?.token || s?.sub?.token || s?.sub?.endpoint || '';
            const tail = String(t).slice(-6);
            return { channel: s?.channel || 'web', idTail: tail ? `…${tail}` : null };
        };
        const channels = subs.map(mask);
        const result = { inboxId, count: subs.length, channels };

        // 🔬 头像链路诊断：查这个 inbox 注册过的 pair 里有没有存 avatarUrl，
        //    以及该 URL 指向的头像在 KV 里是否真的存在（避免「上传失败/过期」却不自知）。
        try {
            const { proactive, kv } = await getStores(c.env);
            const recs = (proactive?.listByInbox ? await proactive.listByInbox(inboxId) : []) || [];
            result.avatars = [];
            for (const r of recs) {
                const url = r?.avatarUrl || null;
                let stored = null;
                if (url && kv) {
                    const m = String(url).match(/\/avatar\/([\w.-]+)$/);
                    if (m) {
                        const raw = await kv.get(`av:${m[1]}`).catch(() => null);
                        stored = raw ? `present(${raw.length}b)` : 'MISSING-in-KV';
                    } else stored = 'unparseable-url';
                }
                result.avatars.push({
                    charId: r?.charId ?? null, charName: r?.timeSpec?.charName ?? null,
                    avatarUrl: url, kvStatus: url ? stored : 'NO-avatarUrl-registered',
                });
            }
        } catch (e) {
            result.avatarsError = String(e?.message || e);
        }

        // ⚠️ test:true「真发测试推送」已停用。
        //    现象：有客户端(疑似旧版本/后台保活循环)每 5~10 分钟反复调本端点 test:true，
        //    每次对每条订阅各发一条「推送链路自检(带头像)」→ 用户被自检通知轰炸。
        //    诊断本身只需「查订阅是否存在 + 头像 KV 是否在」，不必真发通知。
        //    故无条件不再 dispatch，只回订阅清单 + 头像状态；UI 拿不到 dispatch 时显示「已停用真发」。
        if (test && subs.length) {
            result.dispatch = subs.map((s) => ({
                channel: s?.channel || 'web', ok: null, gone: false,
                reason: '测试推送已停用(防自检通知轰炸)，本项仅列出订阅是否存在',
            }));
        }
        return c.json(result);
    });

    // 🖼️ 写入角色头像（鉴权）：客户端注册 proactive 时上传角色头像 base64，存 KV 供推送/扩展使用。
    //    body: { key, dataUrl }（dataUrl = "data:image/png;base64,xxx"）。返回公开读取路径 { url }。
    app.post('/avatar', async (c) => {
        let body;
        try { body = await c.req.json(); } catch { return c.json({ error: 'invalid json' }, 400); }
        const { key, dataUrl } = body || {};
        if (!key || !/^[\w.-]{1,128}$/.test(key)) return c.json({ error: 'bad key' }, 400);
        if (typeof dataUrl !== 'string' || !dataUrl.startsWith('data:')) return c.json({ error: 'dataUrl required' }, 400);
        const m = dataUrl.match(/^data:([^;]+);base64,(.+)$/);
        if (!m) return c.json({ error: 'bad dataUrl' }, 400);
        const mime = m[1], b64 = m[2];
        // 限大小：通知头像几十 KB 足够，封顶 ~512KB base64 防滥用 KV。
        if (b64.length > 512 * 1024) return c.json({ error: 'avatar too large' }, 413);
        const { kv } = await getStores(c.env);
        if (!kv) return c.json({ error: 'no store' }, 503);
        try {
            await kv.put(`av:${key}`, JSON.stringify({ mime, b64 }), { expirationTtl: 60 * 60 * 24 * 60 }); // 60 天
        } catch (e) {
            return c.json({ error: 'put failed', detail: String(e?.message || e) }, 500);
        }
        return c.json({ ok: true, url: `/avatar/${key}` });
    });

    app.delete('/api/push/unsubscribe', async (c) => {
        let body;
        try { body = await c.req.json(); } catch { return c.json({ error: 'invalid json' }, 400); }
        const { inboxId, subscription, endpoint } = body || {};
        if (!inboxId) return c.json({ error: 'inboxId required' }, 400);
        const { sub } = await getStores(c.env);
        await sub.remove(inboxId, subscription || { endpoint });
        return c.json({ ok: true });
    });

    // ===== Phase 2：后端代理主动消息 =====

    // 注册/更新一对的全量配置（含手机端拼好的 promptTemplate）
    app.post('/proactive/register', async (c) => {
        let body;
        try { body = await c.req.json(); } catch { return c.json({ error: 'invalid json' }, 400); }
        const {
            inboxId, userId, charId, promptTemplate, proactiveProfile, lifeState,
            intensity, proactiveBias, recentMessages, aiSettings, quietHours,
            charUtcOffsetSeconds, proactiveEnabledAt, lastInteractionAt, enabled,
            mode, interval, intervalUnit, probability, timeSpec, mcpContextServers, avatarUrl, notifPrivacy,
            mcpToolServers, mcpProactiveToolUse,
        } = body || {};
        if (!inboxId || userId == null || charId == null || !promptTemplate || !aiSettings) {
            return c.json({ error: 'inboxId / userId / charId / promptTemplate / aiSettings required' }, 400);
        }
        // M6：输入大小封顶，防 KV 值过大(25MB 限制)写失败/存储 bloat。promptTemplate 含人设+世界书，
        //   正常几 KB~几十 KB；给 256KB 上限足够，超了拒绝（多半是异常/恶意输入）。
        if (typeof promptTemplate === 'string' && promptTemplate.length > 256 * 1024) {
            return c.json({ error: 'promptTemplate too large (>256KB)' }, 413);
        }
        // D：mcpToolServers/mcpContextServers 也封顶（含 cachedTools 的 inputSchema 可能很大），防 KV bloat。
        for (const [field, val] of [['mcpToolServers', mcpToolServers], ['mcpContextServers', mcpContextServers]]) {
            if (val != null) {
                if (Array.isArray(val) && val.length > 32) return c.json({ error: `${field} too many (>32)` }, 413);
                let sz = 0; try { sz = JSON.stringify(val).length; } catch { /* ignore */ }
                if (sz > 128 * 1024) return c.json({ error: `${field} too large (>128KB)` }, 413);
            }
        }
        const { proactive } = await getStores(c.env);
        await proactive.upsert({
            inboxId, userId: String(userId), charId: String(charId),
            mode: mode === 'interval' ? 'interval' : 'impulse',
            interval: interval ?? 60, intervalUnit: intervalUnit || 'minutes', probability: probability || 'medium',
            promptTemplate, proactiveProfile: proactiveProfile || null, lifeState: lifeState || {},
            intensity: intensity || 'normal', proactiveBias: proactiveBias || 0,
            recentMessages: Array.isArray(recentMessages) ? recentMessages.slice(-PROACTIVE_WINDOW_CAP) : [],
            aiSettings, quietHours: quietHours || null,
            charUtcOffsetSeconds: charUtcOffsetSeconds ?? null,
            proactiveEnabledAt: proactiveEnabledAt || Date.now(),
            lastInteractionAt: lastInteractionAt || 0,
            enabled: enabled !== false,
            timeSpec: timeSpec || null, // 🕒 时间穿透：tick 时用它把 §NOW_*§ 哨兵填成即时真时间
            mcpContextServers: Array.isArray(mcpContextServers) ? mcpContextServers : [], // 🧠 第三方记忆 MCP 直连配置
            // 🛠️ 主动用工具：action-mode MCP server 规格（含 cachedTools）+ 全局开关，tick 时跑 tool-loop。
            mcpToolServers: Array.isArray(mcpToolServers) ? mcpToolServers : [],
            mcpProactiveToolUse: !!mcpProactiveToolUse,
            avatarUrl: typeof avatarUrl === 'string' ? avatarUrl : null, // 🖼️ 角色头像公开 URL，推送时带给 iOS 通知扩展显示在左侧
            notifPrivacy: !!notifPrivacy, // 🔒 通知隐私模式：推送时正文换「你有一条新消息」，标题/头像保留
        });
        return c.json({ ok: true });
    });

    // 🔒 即时刷新本 inbox 所有 pair 的通知隐私标志（用户切开关时调，无需重跑整个注册）。
    app.post('/proactive/privacy', async (c) => {
        let body;
        try { body = await c.req.json(); } catch { return c.json({ error: 'invalid json' }, 400); }
        const { inboxId, notifPrivacy } = body || {};
        if (!inboxId) return c.json({ error: 'inboxId required' }, 400);
        const { proactive } = await getStores(c.env);
        const recs = (proactive?.listByInbox ? await proactive.listByInbox(inboxId) : []) || [];
        let updated = 0;
        for (const r of recs) {
            if (await proactive.patch(inboxId, String(r.userId), String(r.charId), { notifPrivacy: !!notifPrivacy })) updated++;
        }
        return c.json({ ok: true, updated });
    });

    // 增量同步滑窗消息 + lifeState + lastInteractionAt（整窗替换，无 delta）
    app.post('/proactive/sync-messages', async (c) => {
        let body;
        try { body = await c.req.json(); } catch { return c.json({ error: 'invalid json' }, 400); }
        const { inboxId, userId, charId, recentMessages, lifeState, lastInteractionAt, promptTemplate, timeSpec } = body || {};
        if (!inboxId || userId == null || charId == null) {
            return c.json({ error: 'inboxId / userId / charId required' }, 400);
        }
        const { proactive } = await getStores(c.env);
        const patch = {};
        if (Array.isArray(recentMessages)) patch.recentMessages = recentMessages.slice(-PROACTIVE_WINDOW_CAP);
        if (lifeState) patch.lifeState = lifeState;
        if (typeof lastInteractionAt === 'number') patch.lastInteractionAt = lastInteractionAt;
        // 🧠 手机端每次往来重建的「与前台同质量」prompt（含最新记忆/总结/世界书/日历）→ patch 覆盖旧模板，
        //    根治后端代理主动消息上下文不即时。timeSpec 同步刷新（角色名/时段表/异地 offset 可能变）。
        if (typeof promptTemplate === 'string' && promptTemplate) patch.promptTemplate = promptTemplate;
        if (timeSpec) patch.timeSpec = timeSpec;
        const ok = await proactive.patch(inboxId, String(userId), String(charId), patch);
        if (!ok) return c.json({ error: 'pair not registered' }, 404);
        return c.json({ ok: true });
    });

    // 🖼️ 单独回写一对的角色头像 URL（不重跑整个注册）。
    //   手机端「检查推送」发现 NO-avatarUrl-registered 时调，补传头像后回写，无需关开主动消息开关。
    app.post('/proactive/set-avatar', async (c) => {
        let body;
        try { body = await c.req.json(); } catch { return c.json({ error: 'invalid json' }, 400); }
        const { inboxId, userId, charId, avatarUrl } = body || {};
        if (!inboxId || userId == null || charId == null) return c.json({ error: 'inboxId / userId / charId required' }, 400);
        if (typeof avatarUrl !== 'string' || !avatarUrl) return c.json({ error: 'avatarUrl required' }, 400);
        const { proactive } = await getStores(c.env);
        const ok = await proactive.patch(inboxId, String(userId), String(charId), { avatarUrl });
        if (!ok) return c.json({ error: 'pair not registered' }, 404);
        return c.json({ ok: true });
    });

    app.post('/proactive/unregister', async (c) => {
        let body;
        try { body = await c.req.json(); } catch { return c.json({ error: 'invalid json' }, 400); }
        const { inboxId, userId, charId } = body || {};
        if (!inboxId || userId == null || charId == null) return c.json({ error: 'inboxId / userId / charId required' }, 400);
        const { proactive } = await getStores(c.env);
        await proactive.remove(inboxId, String(userId), String(charId));
        return c.json({ ok: true });
    });

    // 走线下剧情：暂停/恢复该 inbox 的所有主动生成。
    // 手机端走线下时心跳式 pause（带 durationMs 自动过期，防没发 resume 永久哑火），退出时 resume。
    app.post('/proactive/pause', async (c) => {
        let body;
        try { body = await c.req.json(); } catch { return c.json({ error: 'invalid json' }, 400); }
        const { inboxId, paused, durationMs } = body || {};
        if (!inboxId) return c.json({ error: 'inboxId required' }, 400);
        const { proactive } = await getStores(c.env);
        if (paused === false) {
            await proactive.setPause(inboxId, 0);
            return c.json({ ok: true, paused: false });
        }
        // 默认 10 分钟，手机端每隔几分钟续期；上限 1 小时防异常长暂停。
        const dur = Math.min(60 * 60 * 1000, Math.max(60 * 1000, Number(durationMs) || 10 * 60 * 1000));
        const until = nowMs() + dur;
        await proactive.setPause(inboxId, until);
        return c.json({ ok: true, paused: true, pausedUntil: until });
    });

    app.get('/proactive/status', async (c) => {
        const inboxId = c.req.query('inboxId');
        if (!inboxId) return c.json({ error: 'inboxId required' }, 400);
        const { proactive } = await getStores(c.env);
        const rows = await proactive.listByInbox(inboxId);
        // 不回 promptTemplate/key 等敏感内容，只回状态
        return c.json({
            pairs: rows.map(r => ({
                userId: r.userId, charId: r.charId, enabled: r.enabled,
                windowSize: (r.recentMessages || []).length,
                lastFiredAt: r.lastFiredAt || 0, updatedAt: r.updatedAt,
            })),
        });
    });

    return app;
}
