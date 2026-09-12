import type { AuthProvider } from "../src/auth.ts";

/** Example only. Set VALIDA_DEMO_TOKEN before enabling this in valida.json. */
export const auth: AuthProvider = {
  authenticate(request) {
    const expected = process.env.VALIDA_DEMO_TOKEN;
    if (!expected) throw new Error("VALIDA_DEMO_TOKEN is not configured");
    const token = request.headers.get("authorization")?.replace(/^Bearer\s+/i, "");
    if (token !== expected) throw new Error("Invalid token");
    return { identity: "demo-user", is_authenticated: true, permissions: ["read", "write"] };
  },
  authorize(context) {
    if (context.action === "delete" && !context.permissions.includes("admin")) return false;
    return true;
  },
};
