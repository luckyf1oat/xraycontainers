import { Container } from "@cloudflare/containers";

export class XrayContainer4 extends Container {
  defaultPort = 8080;
  sleepAfter = "2h";
}

export default {
  async fetch(request, env) {
    const url = new URL(request.url);

    if (url.pathname === "/health") {
      return Response.json(
        {
          ok: true,
          service: "xray-container-worker",
          ts: new Date().toISOString(),
        },
        { status: 200 },
      );
    }

    const container = env.XRAY4.getByName("default");
    return container.fetch(request);
  },

  async scheduled(_event, env, _ctx) {
    const container = env.XRAY4.getByName("default");
    const runAt = new Date().toISOString();

    try {
      const wakeResp = await container.fetch("http://container.local/");
      console.log(`[keepalive] ${runAt} / -> ${wakeResp.status}`);
    } catch (err) {
      console.log(`[keepalive] ${runAt} wake failed: ${err?.message || String(err)}`);
    }
  },
};