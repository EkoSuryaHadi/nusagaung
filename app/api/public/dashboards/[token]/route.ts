import { NextRequest, NextResponse } from "next/server";
import { PrismaClient } from "@prisma/client";
import { queryDuckDB } from "@/lib/duckdb";
import { sanitizeIdentifier, sanitizeLayer } from "@/lib/queryGuard";

const prisma = new PrismaClient();

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ token: string }> }
) {
  try {
    const { token } = await params;

    if (!token) {
      return NextResponse.json({ error: "Share token required" }, { status: 400 });
    }

    const dashboard = await prisma.dashboard.findFirst({
      where: {
        shareToken: token,
        isPublic: true,
      },
      include: {
        widgets: true,
      },
    });

    if (!dashboard) {
      return NextResponse.json({ error: "Public dashboard not found or link has expired" }, { status: 404 });
    }

    // Execute queries for widgets
    const widgetDataPromises = dashboard.widgets.map(async (widget) => {
      try {
        const config = JSON.parse(widget.config || "{}");
        const tableName = config.tableName || config.dataSource || dashboard.sourceTable;
        const layerName = config.layer || dashboard.sourceLayer || "GOLD";

        if (!tableName) {
          return { widgetId: widget.id, data: [] };
        }

        const safeTable = sanitizeIdentifier(tableName).toLowerCase();
        const safeLayer = sanitizeLayer(layerName);
        const limit = parseInt(config.limit || "500", 10);

        const sql = `SELECT * FROM pg.${safeLayer}."${safeTable}" LIMIT ${limit}`;
        const data = await queryDuckDB(sql);

        return { widgetId: widget.id, data };
      } catch (err: any) {
        console.error(`[Public Dashboard API] Widget ${widget.id} query error:`, err);
        return { widgetId: widget.id, data: [], error: err.message };
      }
    });

    const widgetResults = await Promise.all(widgetDataPromises);
    const dataMap: Record<number, any[]> = {};
    widgetResults.forEach((res) => {
      dataMap[res.widgetId] = res.data;
    });

    return NextResponse.json({
      dashboard: {
        id: dashboard.id,
        name: dashboard.name,
        description: dashboard.description,
        layout: dashboard.layout,
        widgets: dashboard.widgets,
        sourceTable: dashboard.sourceTable,
        sourceLayer: dashboard.sourceLayer,
      },
      widgetData: dataMap,
    });
  } catch (error: any) {
    console.error("[Public Dashboard API] Error fetching public dashboard:", error);
    return NextResponse.json({ error: error.message || "Failed to load public dashboard" }, { status: 500 });
  }
}
