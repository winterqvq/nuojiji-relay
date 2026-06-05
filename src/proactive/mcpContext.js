// 后端代理主动消息的「记忆检索注入」—— tick 时直连第三方 context 模式记忆 MCP 检索，
// 把结果格式化成可填进 promptTemplate {{MEMORY_CONTEXT}} 占位符的文本。
//
// 镜像手机端 src/utils/aiApiWithMcp.js 的 gatherMcpContext：同样的 query 构造、关键字 gating、
// 多 key 检索参数、输出文本格式 —— 让后端主动消息看到的记忆与手机端一致。
//
// query 来源：注册时手机传来的滑窗 recentMessages（最近几条 user 消息拼接）。
// 主动消息没有「user 刚说的话」，用滑窗近况是最贴近的近似（决策点 ①A）。

import { callTool, mcpContentToText } from '../mcp/mcpClient.js';

const CONTEXT_QUERY_RECENT_USER_MSGS = 3;
const TOOL_CALL_TIMEOUT_MS = 45000;

// 取最近 N 条 user 消息拼成检索 query（遇到 char 消息就停，只收最近一轮近况）
function buildQuery(recentMessages) {
    const parts = [];
    for (let i = (recentMessages?.length || 0) - 1; i >= 0; i--) {
        const m = recentMessages[i];
        if (!m) continue;
        const isUser = m.sender === 'me' || m.role === 'user';
        if (isUser) {
            const t = m.text || m.content || '';
            if (t) parts.unshift(t);
            if (parts.length >= CONTEXT_QUERY_RECENT_USER_MSGS) break;
        } else {
            break; // 遇到 char/assistant 停
        }
    }
    // user 近况太少时退而用全部滑窗末尾拼（保证有 query 触发检索）
    if (parts.length === 0) {
        const tail = (recentMessages || []).slice(-CONTEXT_QUERY_RECENT_USER_MSGS)
            .map((m) => m.text || m.content || '').filter(Boolean);
        return tail.join('\n').slice(0, 2000);
    }
    return parts.join('\n').slice(0, 2000);
}

// 关键字 gating：server 设了 triggerKeywords 则 query 必须含其一才触发（与手机端一致）
function passesKeywords(server, query) {
    const raw = server?.triggerKeywords;
    if (!raw || typeof raw !== 'string') return true;
    const kws = [...new Set(raw.split(/[,，\n]+/).map((k) => k.trim().toLowerCase()).filter(Boolean))];
    if (kws.length === 0) return true;
    const text = (query || '').toLowerCase();
    return kws.some((kw) => text.includes(kw));
}

/**
 * 检索所有 context server，返回要填进 {{MEMORY_CONTEXT}} 的文本（无命中 → 空串）。
 * @param {Array} servers - 注册时存的 mcpContextServers
 * @param {Array} recentMessages - 滑窗消息
 * @param {object} actor - { userId, characterId }，passActor 的 server 注入
 */
export async function buildMemoryContext(servers, recentMessages, actor) {
    if (!Array.isArray(servers) || servers.length === 0) return '';
    const query = buildQuery(recentMessages);
    if (!query) return '';

    const active = servers.filter((s) => s?.url && passesKeywords(s, query));
    if (active.length === 0) return '';

    const sections = [];
    await Promise.all(active.map(async (server) => {
        const toolName = server.contextToolName || 'search_memory';
        const limit = Math.max(1, parseInt(server.contextLimit, 10) || 5);
        // 多种命名都带上：不同记忆 server 接 query/question、limit/top_k/n_results 各异
        let args = { query, question: query, limit, top_k: limit, n_results: limit };
        if (server.passActor && actor && (actor.userId || actor.characterId)) {
            args = { ...args, _actor: actor };
        }
        try {
            const { content, isError } = await callTool(server, toolName, args, { timeoutMs: TOOL_CALL_TIMEOUT_MS });
            const text = mcpContentToText(content);
            if (isError || !text) return;
            const clipped = text.length > 4000 ? text.slice(0, 4000) + '\n…[truncated]' : text;
            sections.push(`— from "${server.name || 'memory'}":\n${clipped}`);
        } catch (e) {
            console.warn(`[mcpContext] ${server.name || server.url} retrieval failed:`, e?.message || e);
        }
    }));

    if (sections.length === 0) return '';
    // 输出格式与手机端 gatherMcpContext 一致
    return `\n[Relevant Memory / RAG — retrieved from your connected external memory store, use as background knowledge]\n${sections.join('\n\n')}\n`;
}
