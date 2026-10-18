import express from "express";
import type { Express, Request, Response } from "express";
import { selectRoute } from "../providers/route-selector.js";
import { forwardOpenAICodexResponse } from "../providers/openai/codex-transport.js";
import type { OpenAIResponsesRequest } from "../protocol/openai-responses-types.js";
import type { OpenAISubscriptionAccount } from "../providers/openai/token-refresher.js";
import type { ModelRoutingConfig } from "../protocol/model-ref.js";
import { parseSseLines } from "../protocol/sse.js";

type ForwardOpenAI = typeof forwardOpenAICodexResponse;

export interface ResponsesRoutesOptions {
  getOpenAIAccount: () => OpenAISubscriptionAccount | null;
  prepareOpenAIAccount?: (account: OpenAISubscriptionAccount) => Promise<boolean>;
  forwardOpenAI?: ForwardOpenAI;
  modelRouting?: ModelRoutingConfig;
}

function isResponsesRequest(value: unknown): value is OpenAIResponsesRequest {
  return (
    typeof value === "object" &&
    value !== null &&
    typeof (value as { model?: unknown }).model === "string" &&
    Array.isArray((value as { input?: unknown }).input)
  );
}

async function sendUpstreamResponse(upstream: globalThis.Response, res: Response): Promise<void> {
  const contentType = upstream.headers.get("content-type");
  if (contentType) res.setHeader("content-type", contentType);

  res.status(upstream.status);
  if (!upstream.body) {
    res.end();
    return;
  }

  if (contentType?.includes("text/event-stream")) {
    res.setHeader("cache-control", "no-cache");
    res.flushHeaders?.();
  }

  const reader = upstream.body.getReader();
  try {
    while (true) {
      const { value, done } = await reader.read();
      if (done) break;
      if (value) res.write(Buffer.from(value));
    }
  } finally {
    res.end();
  }
}

/**
 * The Codex backend is always called with stream:true (see toCodexBackendRequest),
 * so a client that sent stream:false would otherwise receive raw SSE. Fold the
 * stream into the final Responses object, which is what such clients expect.
 * Falls back to the items seen in response.output_item.done when the final
 * event carries an empty `output`.
 */
async function collectResponseJson(upstream: globalThis.Response): Promise<Record<string, unknown> | undefined> {
  const reader = upstream.body?.getReader();
  const decoder = new TextDecoder();
  let remainder = "";
  let final: Record<string, unknown> | undefined;
  const items: unknown[] = [];

  const apply = (event: unknown): void => {
    if (typeof event !== "object" || event === null) return;
    const ev = event as { type?: string; item?: unknown; response?: Record<string, unknown> };
    if (ev.type === "response.output_item.done" && ev.item) items.push(ev.item);
    if (
      ev.type === "response.completed" ||
      ev.type === "response.incomplete" ||
      ev.type === "response.failed"
    ) {
      final = ev.response;
    }
  };

  while (reader) {
    const { value, done } = await reader.read();
    if (done) break;
    const parsed = parseSseLines(remainder + decoder.decode(value, { stream: true }));
    remainder = parsed.remainder;
    parsed.events.forEach(apply);
  }
  const tail = remainder + decoder.decode();
  if (tail.trim()) parseSseLines(tail + "\n").events.forEach(apply);

  if (!final) return undefined;
  const output = final.output;
  if (!Array.isArray(output) || output.length === 0) return { ...final, output: items };
  return final;
}

export function mountResponsesRoutes(app: Express, opts: ResponsesRoutesOptions): void {
  const forwardOpenAI = opts.forwardOpenAI ?? forwardOpenAICodexResponse;
  const prepareOpenAIAccount = opts.prepareOpenAIAccount ?? (async () => true);

  app.post("/v1/responses", express.json({ limit: "10mb" }), async (req: Request, res: Response) => {
    if (!isResponsesRequest(req.body)) {
      res.status(400).json({
        error: {
          type: "invalid_request_error",
          message: "Expected Responses request with string model and input array",
        },
      });
      return;
    }

    const route = selectRoute(req.body.model, opts.modelRouting);
    if (route.provider !== "openai_subscription") {
      res.status(501).json({
        error: {
          type: "unsupported_provider",
          message: `Responses ingress for ${route.provider} is not implemented yet`,
        },
      });
      return;
    }

    const account = opts.getOpenAIAccount();
    if (!account) {
      res.status(503).json({
        error: {
          type: "no_accounts",
          message: "No OpenAI subscription accounts are configured",
        },
      });
      return;
    }

    const ready = await prepareOpenAIAccount(account);
    if (!ready) {
      res.status(401).json({
        error: {
          type: "authentication_error",
          message: "OpenAI subscription token refresh failed",
        },
      });
      return;
    }

    const body: OpenAIResponsesRequest = {
      ...req.body,
      model: route.upstreamModel,
    };
    const upstream = await forwardOpenAI({
      account,
      body,
      stream: body.stream === true,
    });
    if (
      body.stream !== true &&
      upstream.ok &&
      (upstream.headers.get("content-type") ?? "").includes("text/event-stream")
    ) {
      const json = await collectResponseJson(upstream);
      if (!json) {
        res.status(502).json({
          error: { type: "proxy_error", message: "Codex stream ended without a final response" },
        });
        return;
      }
      res.status(200).json(json);
      return;
    }
    await sendUpstreamResponse(upstream, res);
  });
}
