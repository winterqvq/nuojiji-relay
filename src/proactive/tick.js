// Cron tick：遍历已启用的 pair，重算 impulse，命中则实时调 AI 生成主动消息 → outbox + 推送。
// worker.js 的 scheduled 和 server.js 的 node-cron 都调 runProactiveTick(env)。

import { createProactiveStore, BACKEND_FIRE_COOLDOWN_MS } from '../store/proactiveStore.js';
import { createOutboxStore } from '../store/outboxStore.js';
import { createSubStore } from '../store/subStore.js';
import { shouldFire, shouldFireInterval } from './impulseEngine.js';
import { runGeneration } from '../ai/aiCaller.js';
import { dispatchPush } from '../push/pushSender.js';
import { nowMs } from '../util/ids.js';
import { renderTimeTokens } from '../util/timeTokens.js';
import { buildMemoryContext } from './mcpContext.js';

// 把滑窗消息渲染成转录文本（喂进 promptTemplate 的 {{RECENT_MESSAGES}}）
function renderTranscript(recentMessages) {
    if (!Array.isArray(recentMessages) || recentMessages.length === 0) return '(no recent messages)';
    return recentMessages.map((m) => {
        const who = (m.sender === 'me' || m.role === 'user') ? 'User' : 'Char';
        const text = m.text || m.content || '';
        return `${who}: ${text}`;
    }).join('\n');
}

// 占位替换：后端唯一接触 prompt 的地方，只做字符串替换，无任何话术。
function fillTemplate(template, { transcript, reason, memory }) {
    return String(template || '')
        .replaceAll('{{RECENT_MESSAGES}}', transcript)
        .replaceAll('{{IMPULSE_REASON}}', reason || '')
        .replaceAll('{{MEMORY_CONTEXT}}', memory || '');
}

export async function runProactiveTick(env) {
    const proactive = await createProactiveStore(env);
    const outbox = await createOutboxStore(env);
    const sub = await createSubStore(env);
    const now = nowMs();

    const pairs = await proactive.listEnabled();
    let fired = 0;

    // inbox 级暂停缓存：用户走线下剧情时手机端调 /proactive/pause，该 inbox 整个跳过本轮生成。
    // 同一 inbox 多对只查一次。
    const pauseCache = new Map();
    async function isInboxPaused(inboxId) {
        if (pauseCache.has(inboxId)) return pauseCache.get(inboxId);
        let paused = false;
        try { paused = (await proactive.getPausedUntil(inboxId)) > now; } catch { paused = false; }
        pauseCache.set(inboxId, paused);
        return paused;
    }

    for (const rec of pairs) {
        try {
            // 走线下剧情中：跳过该 inbox 的所有主动生成（用户在前台沉浸剧情，不该被线上消息打断）
            if (await isInboxPaused(rec.inboxId)) continue;

            // 后端冷却：上次触发太近就跳过（防 1 分钟 cron 连发）
            if (rec.lastFiredAt && (now - rec.lastFiredAt) < BACKEND_FIRE_COOLDOWN_MS) continue;

            // 两种触发档：'impulse'(真人模式) / 'interval'(普通后台主动，计时+概率高中低)
            let verdict;
            if (rec.mode === 'interval') {
                verdict = shouldFireInterval({
                    now, lastFiredAt: rec.lastFiredAt || 0,
                    interval: rec.interval, intervalUnit: rec.intervalUnit, probability: rec.probability,
                });
            } else {
                verdict = shouldFire({
                    profile: rec.proactiveProfile,
                    lifeState: rec.lifeState,
                    now,
                    lastInteractionAt: rec.lastInteractionAt || 0,
                    scheduleCtx: null, // 设备专属，后端无
                    intensity: rec.intensity || 'normal',
                    unansweredStreak: (rec.lifeState && rec.lifeState.unansweredStreak) || 0,
                    proactiveEnabledAt: rec.proactiveEnabledAt || 0,
                    proactiveBias: rec.proactiveBias || 0,
                    userActiveAt: 0, // 设备专属信号，后端默认 0
                    charUtcOffsetSeconds: rec.charUtcOffsetSeconds ?? null,
                });
            }

            if (!verdict.fire) continue;

            // 命中 → 实时生成。messages 只有一条 system（手机端拼好的完整 prompt + 填充滑窗）
            const transcript = renderTranscript(rec.recentMessages);
            // 🧠 直连第三方记忆 MCP 检索（关软件也能用最新记忆）；失败/无配置 → 空串不阻断生成。
            let memory = '';
            try {
                memory = await buildMemoryContext(
                    rec.mcpContextServers,
                    rec.recentMessages,
                    { userId: rec.userId, characterId: rec.charId }
                );
            } catch (e) {
                console.warn('[proactive] memory context failed:', e?.message);
            }
            // 先填即时真时间哨兵（§NOW_*§），再填滑窗/理由/记忆占位符。
            const timedTemplate = renderTimeTokens(rec.promptTemplate, rec.timeSpec, now);
            const systemContent = fillTemplate(timedTemplate, { transcript, reason: verdict.reason, memory });
            const messages = [{ role: 'system', content: systemContent }];

            let content = null, error = null;
            try {
                content = await runGeneration(rec.aiSettings, messages, rec.aiSettings?.maxTokens || null);
            } catch (e) {
                error = String(e?.message || e);
            }

            const requestId = `proactive_${rec.userId}_${rec.charId}_${now}`;
            const item = {
                id: `relay_${requestId}`, requestId,
                charId: rec.charId, userId: rec.userId,
                roundId: requestId, content, error, createdAt: nowMs(),
                proactive: true,
            };
            await outbox.put(rec.inboxId, item);

            // 简单更新后端 lifeState（完整 evolve 仍在手机端，下次 sync 覆盖）
            const ls = rec.lifeState || {};
            await proactive.patch(rec.inboxId, rec.userId, rec.charId, {
                lastFiredAt: now,
                lifeState: { ...ls, lastImpulseAt: now, lastProactiveSentAt: now },
            });

            // 发推送叫醒
            try {
                const subs = await sub.list(rec.inboxId);
                const payload = { title: '糯叽机', body: '有新消息', charId: rec.charId, userId: rec.userId, kind: 'relay-outbox' };
                for (const s of subs) {
                    const res = await dispatchPush(env, s, payload);
                    if (res?.gone) await sub.remove(rec.inboxId, s);
                }
            } catch (e) { console.warn('[proactive] push failed:', e?.message); }

            fired++;
        } catch (e) {
            console.warn('[proactive] pair tick failed:', e?.message);
        }
    }

    return { pairs: pairs.length, fired };
}