// 精简 MCP 客户端（Streamable HTTP transport, JSON-RPC 2.0）
//
// 镜像手机端 src/utils/mcpClient.js 的协议，但只实现后端代理主动消息需要的最小集：
//   - initialize → notifications/initialized → tools/call
//   - 响应支持 application/json（单一）或 text/event-stream（SSE 整段解析）
//   - 不缓存 session（每次 callTool 完整握手；后端 tick 频率低，简单可靠优先）
//   - 后端是服务器，无 CORS，直连 server.url（不走 Worker proxy）
//
// 只导出 callTool（context 检索用）。不需要 listTools。

const PROTOCOL_VERSION = '2025-03-26';
const CLIENT_INFO = { name: 'nuojiji-relay', version: '1.0.0' };
const DEFAULT_TIMEOUT_MS = 45000;

// 从 server 配置构建 auth header（与手机端 buildAuthHeaders 同语义，但不带内建 token）
function buildAuthHeaders(server) {
    const headers = {};
    const auth = server?.auth || { type: 'none' };
    if (auth.type === 'bearer' && auth.value) {
        headers.Authorization = `Bearer ${auth.value}`;
    } else if (auth.type === 'header' && auth.headerName && auth.value) {
        headers[auth.headerName] = auth.value;
    }
    return headers;
}

// 解析「一整段已收完的 SSE 文本」，回第一个 id 匹配的 JSON-RPC response。
function parseSseText(text, expectedId) {
    const normalized = String(text || '').replace(/\r\n/g, '\n').replace(/\r/g, '\n');
    for (const rawEvent of normalized.split('\n\n')) {
        const dataLines = [];
        for (const line of rawEvent.split('\n')) {
            if (line.startsWith('data:')) dataLines.push(line.slice(5).trim());
        }
        if (dataLines.length === 0) continue;
        let parsed;
        try { parsed = JSON.parse(dataLines.join('\n')); } catch { continue; }
        if (parsed && parsed.id === expectedId) return parsed;
    }
    throw new Error('SSE parse: no matching response in body');
}

// 发一个 JSON-RPC 消息，回 { result, sessionId }
async function sendRequest(server, message, { sessionId, timeoutMs }) {
    const headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json, text/event-stream',
        ...buildAuthHeaders(server),
    };
    if (sessionId) headers['Mcp-Session-Id'] = sessionId;

    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    let res;
    try {
        res = await fetch(server.url, {
            method: 'POST',
            headers,
            body: JSON.stringify(message),
            signal: controller.signal,
        });
    } catch (e) {
        throw new Error(`MCP network error: ${e?.message || e}`);
    } finally {
        clearTimeout(timer);
    }

    if (!res.ok) {
        let errBody = '';
        try { errBody = await res.text(); } catch { /* ignore */ }
        throw new Error(`MCP HTTP ${res.status}${errBody ? ` — ${errBody.slice(0, 200)}` : ''}`);
    }

    const newSessionId = res.headers.get('Mcp-Session-Id') || sessionId || null;
    // 通知类（无 id）服务器可回 202 No Content
    if (res.status === 202 || message.id === undefined) {
        return { result: null, sessionId: newSessionId };
    }

    const ct = (res.headers.get('Content-Type') || '').toLowerCase();
    const text = await res.text();
    let payload;
    if (ct.includes('text/event-stream')) {
        payload = parseSseText(text, message.id);
    } else {
        try { payload = JSON.parse(text); }
        catch { throw new Error(`MCP invalid JSON: ${text.slice(0, 200)}`); }
    }
    if (payload?.error) {
        throw new Error(`MCP error ${payload.error.code ?? '?'}: ${payload.error.message || 'unknown'}`);
    }
    return { result: payload?.result ?? null, sessionId: newSessionId };
}

// 完整握手：initialize → notifications/initialized
async function handshake(server, timeoutMs) {
    const { result, sessionId } = await sendRequest(server, {
        jsonrpc: '2.0', id: 1, method: 'initialize',
        params: { protocolVersion: PROTOCOL_VERSION, capabilities: {}, clientInfo: CLIENT_INFO },
    }, { timeoutMs });
    try {
        await sendRequest(server, { jsonrpc: '2.0', method: 'notifications/initialized' }, { sessionId, timeoutMs });
    } catch { /* 部分实现允许省略 */ }
    return { sessionId };
}

// 把 MCP content 数组转纯文本（镜像手机端 mcpContentToText）
export function mcpContentToText(content) {
    if (!Array.isArray(content)) return '';
    return content
        .map((c) => {
            if (typeof c === 'string') return c;
            if (c?.type === 'text' && typeof c.text === 'string') return c.text;
            if (typeof c?.text === 'string') return c.text;
            return '';
        })
        .filter(Boolean)
        .join('\n');
}

/**
 * 调用一个 tool，回 { content, isError }。每次完整握手（不缓存 session）。
 */
export async function callTool(server, name, args = {}, { timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
    if (!server?.url) throw new Error('MCP server url missing');
    const { sessionId } = await handshake(server, timeoutMs);
    const { result } = await sendRequest(server, {
        jsonrpc: '2.0', id: 3, method: 'tools/call',
        params: { name, arguments: args },
    }, { sessionId, timeoutMs });
    return { content: result?.content || [], isError: !!result?.isError };
}
