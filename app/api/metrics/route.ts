import { NextRequest, NextResponse } from "next/server";
import { METRIC_REGISTRY, calculateMetric } from "@/lib/metrics";
import { verifySessionToken, COOKIE_NAME } from "@/lib/auth";

export async function GET(request: NextRequest) {
  try {
    const authHeader = request.headers.get("authorization");
    const bearerToken = authHeader?.startsWith("Bearer ") ? authHeader.slice(7) : null;
    const cookieToken = request.cookies.get(COOKIE_NAME)?.value;
    const token = bearerToken || cookieToken;

    let tenantId: number | undefined;
    if (token) {
      const session = await verifySessionToken(token);
      if (session?.tenantId) {
        tenantId = session.tenantId;
      }
    }

    const { searchParams } = new URL(request.url);
    const metricId = searchParams.get("id");

    if (metricId) {
      const result = await calculateMetric(metricId, tenantId);
      const definition = METRIC_REGISTRY[metricId];
      return NextResponse.json({
        metric: definition,
        result,
      });
    }

    // Return all metrics calculated
    const metricIds = Object.keys(METRIC_REGISTRY);
    const calculatedPromises = metricIds.map(async (id) => {
      const result = await calculateMetric(id, tenantId);
      return {
        ...METRIC_REGISTRY[id],
        result,
      };
    });

    const metrics = await Promise.all(calculatedPromises);

    return NextResponse.json({
      metrics,
    });
  } catch (error: any) {
    console.error("[Semantic Metrics API] Error:", error);
    return NextResponse.json({ error: error.message || "Failed to calculate metrics" }, { status: 500 });
  }
}
