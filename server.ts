import { Application, Router } from "https://deno.land/x/oak@v12.6.1/mod.ts";
import { decode as jwtDecode } from "https://deno.land/x/djwt@v3.0.1/mod.ts";

const BASE_URL = "https://api.enginelabs.ai";
const CLERK_BASE = "https://clerk.cto.new";
const ORIGIN = "https://cto.new";

interface ChatMessage {
  role: string;
  content: string;
}

interface ChatRequest {
  model?: string;
  messages: ChatMessage[];
  stream?: boolean;
}

interface SSEChunk {
  id: string;
  object: string;
  created: number;
  model: string;
  choices: Array<{
    index: number;
    delta: { content?: string };
    finish_reason: string | null;
    logprobs: null;
  }>;
}

interface CompletionResponse {
  id: string;
  object: string;
  created: number;
  model: string;
  choices: Array<{
    index: number;
    message: { role: string; content: string };
    finish_reason: string;
    logprobs: null;
  }>;
  usage: {
    prompt_tokens: number;
    completion_tokens: number;
    total_tokens: number;
  };
}

// 创建 SSE 格式的响应块
function createSSEChunk(
  requestId: string,
  model: string,
  content: string,
  finishReason: string | null = null,
): string {
  const chunk: SSEChunk = {
    id: requestId,
    object: "chat.completion.chunk",
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [
      {
        index: 0,
        delta: content ? { content } : {},
        finish_reason: finishReason,
        logprobs: null,
      },
    ],
  };
  return `data: ${JSON.stringify(chunk)}\n\n`;
}

// 创建非流式响应
function createCompletionResponse(
  requestId: string,
  model: string,
  content: string,
): CompletionResponse {
  return {
    id: requestId,
    object: "chat.completion",
    created: Math.floor(Date.now() / 1000),
    model,
    choices: [
      {
        index: 0,
        message: { role: "assistant", content },
        finish_reason: "stop",
        logprobs: null,
      },
    ],
    usage: { prompt_tokens: 0, completion_tokens: 0, total_tokens: 0 },
  };
}

// 从 cookie 中提取 session ID
async function extractSessionFromCookie(
  cookie: string,
): Promise<string | null> {
  // 尝试从 __client JWT 中解码
  const match = cookie.match(/__client=([^;]+)/);
  if (match) {
    try {
      const clientJwt = match[1];
      const [, payload] = jwtDecode(clientJwt);
      if (
        payload && typeof payload === "object" && "rotating_token" in payload
      ) {
        console.log("从 __client 中提取到 rotating_token");
      }
    } catch (e) {
      console.warn(`解析 __client JWT 失败: ${e}`);
    }
  }

  // 尝试获取 sessions
  try {
    const resp = await fetch(`${CLERK_BASE}/v1/client`, {
      headers: {
        Cookie: cookie,
        Origin: ORIGIN,
      },
    });

    if (resp.ok) {
      const data = await resp.json();
      const sessions = data?.response?.sessions || [];
      if (sessions.length > 0) {
        const sessionId = sessions[0].id;
        console.log(`获取到 session_id: ${sessionId}`);
        return sessionId;
      }
    }
  } catch (e) {
    console.error(`获取 session 失败: ${e}`);
  }

  return null;
}

// 使用 cookie 获取新的 JWT token
async function getJwtFromCookie(cookie: string): Promise<string> {
  const sessionId = await extractSessionFromCookie(cookie);
  if (!sessionId) {
    throw new Error("无法从 Cookie 中提取 session_id");
  }

  const tokenUrl =
    `${CLERK_BASE}/v1/client/sessions/${sessionId}/tokens?__clerk_api_version=2025-04-10`;

  try {
    const resp = await fetch(tokenUrl, {
      method: "POST",
      headers: {
        Cookie: cookie,
        "Content-Type": "application/x-www-form-urlencoded",
        Origin: ORIGIN,
        Referer: `${ORIGIN}/`,
      },
      body: "",
    });

    if (!resp.ok) {
      throw new Error(`HTTP ${resp.status}`);
    }

    const data = await resp.json();
    const jwtToken = data.jwt;
    if (!jwtToken) {
      throw new Error("响应中缺少 jwt 字段");
    }
    console.log("成功获取 JWT token");
    return jwtToken;
  } catch (e) {
    console.error(`获取 JWT 失败: ${e}`);
    throw new Error(`无法获取 JWT token: ${e}`);
  }
}

// 流式聊天生成器
async function* streamChatGenerator(
  requestId: string,
  model: string,
  chatHistoryId: string,
  userId: string,
  jwtToken: string,
  fullPrompt: string,
): AsyncGenerator<Uint8Array> {
  const encoder = new TextEncoder();
  const wsUrl =
    `wss://api.enginelabs.ai/engine-agent/chat-histories/${chatHistoryId}/buffer/stream?token=${userId}`;

  // 立即发送一个空增量
  yield encoder.encode(createSSEChunk(requestId, model, ""));

  let receivedUpdate = false;
  let lastBufferType: string | null = null;
  let inThinkingBlock = false;
  const modeByType: Record<string, "snapshot" | "delta"> = {};
  const prevContentByType: Record<string, string> = {};

  try {
    const ws = new WebSocket(wsUrl);
    ws.binaryType = "arraybuffer";

    // 等待连接打开
    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => {
        console.log(`WebSocket 已连接: ${chatHistoryId}`);
        resolve();
      };
      ws.onerror = (e) => reject(e);
    });

    // 触发聊天
    const triggerChat = async () => {
      const payload = {
        prompt: fullPrompt,
        chatHistoryId,
        adapterName: model,
      };
      const headers = {
        Authorization: `Bearer ${jwtToken}`,
        "Content-Type": "application/json",
        Origin: ORIGIN,
        Referer: `${ORIGIN}/${chatHistoryId}`,
      };

      try {
        const resp = await fetch(`${BASE_URL}/engine-agent/chat`, {
          method: "POST",
          headers,
          body: JSON.stringify(payload),
        });
        if (!resp.ok) {
          const text = await resp.text();
          console.warn(`触发消息失败: ${resp.status} ${text.slice(0, 200)}`);
        }
      } catch (e) {
        console.error(`触发消息异常: ${e}`);
      }
    };

    // 启动触发任务
    triggerChat();

    // 处理 WebSocket 消息
    for await (const event of wsMessageIterator(ws)) {
      if (event.type === "close") break;
      if (event.type === "error") {
        console.error("WebSocket 错误:", event.error);
        break;
      }

      try {
        const data = JSON.parse(event.data);
        const msgType = data.type;

        if (msgType === "update") {
          receivedUpdate = true;
          const bufferStr = data.buffer || "{}";
          try {
            const bufferData = JSON.parse(bufferStr);
            const bufferType = bufferData.type;

            if (bufferType === "chat" || bufferType === "thinking") {
              const content = bufferData.chat?.content || "";
              if (content) {
                // 检测类型切换
                if (bufferType !== lastBufferType) {
                  // 如果之前在 thinking 块中，先关闭标签
                  if (inThinkingBlock) {
                    yield encoder.encode(
                      createSSEChunk(requestId, model, "</think>"),
                    );
                    inThinkingBlock = false;
                  }

                  // 如果切换到 thinking，打开标签
                  if (bufferType === "thinking") {
                    yield encoder.encode(
                      createSSEChunk(requestId, model, "<think>"),
                    );
                    inThinkingBlock = true;
                  }

                  lastBufferType = bufferType;
                }

                // 仅发送增量
                // 自适应增量/快照模式
                const prev = prevContentByType[bufferType] ?? "";
                let mode = modeByType[bufferType];
                let delta = "";
                if (!mode && prev) {
                  if (content.startsWith(prev)) {
                    mode = "snapshot";
                    modeByType[bufferType] = mode;
                  } else {
                    mode = "delta";
                    modeByType[bufferType] = mode;
                  }
                }
                if (mode === "snapshot") {
                  delta = content.slice(prev.length);
                  prevContentByType[bufferType] = content;
                } else if (mode === "delta") {
                  delta = content;
                  prevContentByType[bufferType] = prev + content;
                } else {
                  // 首次收到该类型：按增量输出并记录
                  delta = content;
                  prevContentByType[bufferType] = content;
                }
                if (delta) {
                  yield encoder.encode(createSSEChunk(requestId, model, delta));
                }
              }
            }
          } catch (e) {
            // JSON 解析失败，忽略
          }
        } else if (msgType === "state") {
          const state = data.state || {};
          if (!state.inProgress && receivedUpdate) {
            // 结束前，如果还在 thinking 块中，关闭标签
            if (inThinkingBlock) {
              yield encoder.encode(
                createSSEChunk(requestId, model, "</think>"),
              );
              inThinkingBlock = false;
            }
            break;
          }
        }
      } catch (e) {
        // 忽略解析错误
      }
    }

    ws.close();

    // 发送结束标记
    yield encoder.encode(createSSEChunk(requestId, model, "", "stop"));
    yield encoder.encode("data: [DONE]\n\n");
  } catch (e) {
    console.error(`流式处理异常: ${e}`);
    yield encoder.encode(
      createSSEChunk(requestId, model, `错误: ${e}`, "stop"),
    );
    yield encoder.encode("data: [DONE]\n\n");
  }
}

// WebSocket 消息迭代器
async function* wsMessageIterator(
  ws: WebSocket,
): AsyncGenerator<
  { type: "message"; data: string } | { type: "close" } | {
    type: "error";
    error: Event;
  }
> {
  const queue: Array<{ type: string; data?: string; error?: Event }> = [];
  let resolver: (() => void) | null = null;

  ws.onmessage = (event) => {
    queue.push({ type: "message", data: event.data });
    resolver?.();
  };

  ws.onclose = () => {
    queue.push({ type: "close" });
    resolver?.();
  };

  ws.onerror = (error) => {
    queue.push({ type: "error", error });
    resolver?.();
  };

  while (true) {
    if (queue.length === 0) {
      await new Promise<void>((resolve) => {
        resolver = resolve;
      });
    }

    const item = queue.shift();
    if (!item) continue;

    if (item.type === "close") {
      yield { type: "close" };
      break;
    }

    if (item.type === "error") {
      yield { type: "error", error: item.error! };
      break;
    }

    if (item.type === "message") {
      yield { type: "message", data: item.data! };
    }
  }
}

// 非流式聊天
async function nonStreamChat(
  requestId: string,
  model: string,
  chatHistoryId: string,
  userId: string,
  jwtToken: string,
  fullPrompt: string,
): Promise<string> {
  const wsUrl =
    `wss://api.enginelabs.ai/engine-agent/chat-histories/${chatHistoryId}/buffer/stream?token=${userId}`;
  let fullContent = "";

  try {
    const ws = new WebSocket(wsUrl);
    ws.binaryType = "arraybuffer";

    // 等待连接打开
    await new Promise<void>((resolve, reject) => {
      ws.onopen = () => {
        console.log(`WebSocket 已连接 (非流式): ${chatHistoryId}`);
        resolve();
      };
      ws.onerror = (e) => reject(e);
    });

    // 发送 prompt
    const payload = {
      prompt: fullPrompt,
      chatHistoryId,
      adapterName: model,
    };
    const headers = {
      Authorization: `Bearer ${jwtToken}`,
      "Content-Type": "application/json",
      Origin: ORIGIN,
      Referer: `${ORIGIN}/${chatHistoryId}`,
    };

    const resp = await fetch(`${BASE_URL}/engine-agent/chat`, {
      method: "POST",
      headers,
      body: JSON.stringify(payload),
    });
    console.log(`POST /engine-agent/chat 状态: ${resp.status}`);

    // 接收所有消息
    let receivedUpdate = false;
    let lastBufferType: string | null = null;
    let inThinkingBlock = false;
    const modeByType: Record<string, "snapshot" | "delta"> = {};
    const prevContentByType: Record<string, string> = {};

    for await (const event of wsMessageIterator(ws)) {
      if (event.type === "close") break;
      if (event.type === "error") {
        console.error("WebSocket 错误:", event.error);
        break;
      }

      try {
        const data = JSON.parse(event.data);
        const msgType = data.type;

        if (msgType === "update") {
          receivedUpdate = true;
          const bufferStr = data.buffer || "{}";
          try {
            const bufferData = JSON.parse(bufferStr);
            const bufferType = bufferData.type;

            if (bufferType === "chat" || bufferType === "thinking") {
              const content = bufferData.chat?.content;
              if (content) {
                console.log(
                  `提取到内容 (非流式, ${bufferType})，长度: ${content.length}`,
                );

                // 检测类型切换
                if (bufferType !== lastBufferType) {
                  // 如果之前在 thinking 块中，先关闭标签
                  if (inThinkingBlock) {
                    fullContent += "</think>";
                    inThinkingBlock = false;
                  }

                  // 如果切换到 thinking，打开标签
                  if (bufferType === "thinking") {
                    fullContent += "<think>";
                    inThinkingBlock = true;
                  }

                  lastBufferType = bufferType;
                }

                // 非流式模式直接使用完整内容（服务端已经返回完整的）
                const prev = prevContentByType[bufferType] ?? "";
                let mode = modeByType[bufferType];
                let delta = "";
                if (!mode && prev) {
                  if (content.startsWith(prev)) {
                    mode = "snapshot";
                    modeByType[bufferType] = mode;
                  } else {
                    mode = "delta";
                    modeByType[bufferType] = mode;
                  }
                }
                if (mode === "snapshot") {
                  delta = content.slice(prev.length);
                  prevContentByType[bufferType] = content;
                } else if (mode === "delta") {
                  delta = content;
                  prevContentByType[bufferType] = prev + content;
                } else {
                  delta = content;
                  prevContentByType[bufferType] = content;
                }

                if (delta) {
                  fullContent += delta;
                }
              }
            }
          } catch (e) {
            console.warn(`
解析 buffer 失败 (非流式): ${e}`);
          }
        } else if (msgType === "state") {
          const state = data.state || {};
          console.log(
            `收到 state 消息 (非流式): inProgress=${state.inProgress}`,
          );
          if (!state.inProgress) {
            if (receivedUpdate) {
              // 结束前，如果还在 thinking 块中，关闭标签
              if (inThinkingBlock) {
                fullContent += "</think>";
                inThinkingBlock = false;
              }
              console.log("已收到 update 消息，任务完成 (非流式)");
              break;
            } else {
              console.log("尚未收到 update 消息 (非流式)，继续等待...");
            }
          }
        }
      } catch (e) {
        // 忽略解析错误
      }
    }

    ws.close();
    return fullContent;
  } catch (e) {
    console.error(`非流式处理错误: ${e}`);
    throw new Error(`处理请求失败: ${e}`);
  }
}

// 路由设置
const router = new Router();

// 聊天接口
router.post("/v1/chat/completions", async (ctx) => {
  // 获取 Authorization header
  const authorization = ctx.request.headers.get("authorization");
  if (!authorization || !authorization.startsWith("Bearer ")) {
    ctx.response.status = 401;
    ctx.response.body = { error: "需要 Bearer token (CLERK_COOKIE)" };
    return;
  }

  // 移除 "Bearer " 前缀，并将 ..... 替换为 '; '
  let clerkCookie = authorization.slice(7);
  clerkCookie = clerkCookie.replace(/\.\.\.\.\./g, "; ");

  // 解析请求
  let requestData: ChatRequest;
  try {
    requestData = await ctx.request.body({ type: "json" }).value;
  } catch (e) {
    ctx.response.status = 400;
    ctx.response.body = { error: `无效的 JSON: ${e}` };
    return;
  }

  const model = requestData.model || "ClaudeSonnet4_5";
  const messages = requestData.messages || [];
  const stream = requestData.stream || false;

  if (!messages || messages.length === 0) {
    ctx.response.status = 400;
    ctx.response.body = { error: "messages 不能为空" };
    return;
  }

  // 将多轮对话转换为单轮对话
  const conversationParts: string[] = [];
  for (const msg of messages) {
    const role = msg.role || "unknown";
    const content = msg.content || "";
    if (content) {
      conversationParts.push(`${role}:\n${content}\n\n`);
    }
  }

  const fullPrompt = conversationParts.join("\n\n");
  console.log(`转换后的单轮 prompt 长度: ${fullPrompt.length}`);

  if (!fullPrompt.trim()) {
    ctx.response.status = 400;
    ctx.response.body = { error: "整合后的消息内容为空" };
    return;
  }

  // 获取 JWT token
  let jwtToken: string;
  try {
    jwtToken = await getJwtFromCookie(clerkCookie);
  } catch (e) {
    ctx.response.status = 401;
    ctx.response.body = { error: `${e}` };
    return;
  }

  // 解析 JWT 获取 user_id
  let userId: string;
  try {
    const [, payload] = jwtDecode(jwtToken);
    if (!payload || typeof payload !== "object" || !("sub" in payload)) {
      throw new Error("JWT 中没有 sub 字段");
    }
    userId = payload.sub as string;
  } catch (e) {
    ctx.response.status = 401;
    ctx.response.body = { error: `无效的 JWT: ${e}` };
    return;
  }

  // 生成新的聊天历史 ID
  const chatHistoryId = crypto.randomUUID();
  console.log(`生成新的聊天历史 ID: ${chatHistoryId}`);

  const requestId = `chatcmpl-${crypto.randomUUID()}`;

  if (stream) {
    // 流式响应
    ctx.response.headers.set(
      "Content-Type",
      "text/event-stream; charset=utf-8",
    );
    ctx.response.headers.set("Cache-Control", "no-cache");
    ctx.response.headers.set("Connection", "keep-alive");
    ctx.response.headers.set("X-Accel-Buffering", "no");

    const body = streamChatGenerator(
      requestId,
      model,
      chatHistoryId,
      userId,
      jwtToken,
      fullPrompt,
    );

    ctx.response.body = body;
  } else {
    // 非流式响应
    try {
      const fullContent = await nonStreamChat(
        requestId,
        model,
        chatHistoryId,
        userId,
        jwtToken,
        fullPrompt,
      );
      ctx.response.body = createCompletionResponse(
        requestId,
        model,
        fullContent,
      );
    } catch (e) {
      ctx.response.status = 500;
      ctx.response.body = { error: `处理请求失败: ${e}` };
    }
  }
});

// 列出模型
router.get("/v1/models", (ctx) => {
  const models = [
    {
      id: "ClaudeSonnet4_5",
      object: "model",
      created: 1234567890,
      owned_by: "enginelabs",
    },
    {
      id: "GPT5",
      object: "model",
      created: 1234567890,
      owned_by: "enginelabs",
    }
  ];
  ctx.response.body = { object: "list", data: models };
});

// 健康检查
router.get("/", (ctx) => {
  ctx.response.body = {
    status: "ok",
    service: "enginelabs-2api-v3",
    version: "3.0.0",
  };
});

// 应用设置
const app = new Application();

// 日志中间件
app.use(async (ctx, next) => {
  const start = Date.now();
  await next();
  const ms = Date.now() - start;
  console.log(`${ctx.request.method} ${ctx.request.url} - ${ms}ms`);
});

// 错误处理
app.use(async (ctx, next) => {
  try {
    await next();
  } catch (err) {
    console.error("错误:", err);
    ctx.response.status = 500;
    ctx.response.body = { error: "Internal Server Error" };
  }
});

app.use(router.routes());
app.use(router.allowedMethods());

const port = Number(Deno.env.get('PORT') || 10000);
console.log(`🚀 服务器运行在 http://0.0.0.0:${port}`);
console.log(`📚 Enginelabs-2API-V3 Deno 版本 v3.0.0`);
await app.listen({ port });
