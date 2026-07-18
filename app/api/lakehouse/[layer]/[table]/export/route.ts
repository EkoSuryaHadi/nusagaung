import { NextRequest, NextResponse } from "next/server";
import { queryDuckDB } from "@/lib/duckdb";
import { sanitizeIdentifier, sanitizeLayer, checkQueryGuard } from "@/lib/queryGuard";
import { logAudit } from "@/lib/audit";

export async function GET(
  request: NextRequest,
  { params }: { params: Promise<{ layer: string; table: string }> }
) {
  try {
    const { layer: rawLayer, table: rawTable } = await params;
    const layer = sanitizeLayer(rawLayer);
    const table = sanitizeIdentifier(rawTable).toLowerCase();

    const { searchParams } = new URL(request.url);
    const format = (searchParams.get("format") || "csv").toLowerCase();
    const limit = parseInt(searchParams.get("limit") || "100000", 10);

    // Record Audit Log asynchronously
    logAudit({
      action: "EXPORT",
      resource: "LakehouseTable",
      details: { layer, table, format, limit },
      ipAddress: request.headers.get("x-forwarded-for") || undefined,
      userAgent: request.headers.get("user-agent") || undefined,
    });

    const sql = `SELECT * FROM pg.${layer}."${table}" LIMIT ${limit}`;
    const guard = checkQueryGuard(sql);
    if (!guard.valid) {
      return NextResponse.json({ error: guard.reason }, { status: 400 });
    }

    const rows = await queryDuckDB(sql);

    if (format === "json") {
      return new NextResponse(JSON.stringify(rows, null, 2), {
        status: 200,
        headers: {
          "Content-Type": "application/json",
          "Content-Disposition": `attachment; filename="${layer}_${table}.json"`,
        },
      });
    }

    if (rows.length === 0) {
      return new NextResponse("", {
        status: 200,
        headers: {
          "Content-Type": "text/csv; charset=utf-8",
          "Content-Disposition": `attachment; filename="${layer}_${table}.csv"`,
        },
      });
    }

    // Convert rows to CSV/TSV string
    const delimiter = format === "tsv" ? "\t" : ",";
    const headers = Object.keys(rows[0]);
    
    const csvHeader = headers.map(h => `"${String(h).replace(/"/g, '""')}"`).join(delimiter);
    const csvRows = rows.map(row => {
      return headers
        .map(h => {
          const val = row[h];
          if (val === null || val === undefined) return '""';
          const strVal = String(val).replace(/"/g, '""');
          return `"${strVal}"`;
        })
        .join(delimiter);
    });

    const csvContent = [csvHeader, ...csvRows].join("\n");
    const mimeType = format === "tsv" ? "text/tab-separated-values" : "text/csv";
    const ext = format === "tsv" ? "tsv" : "csv";

    return new NextResponse(csvContent, {
      status: 200,
      headers: {
        "Content-Type": `${mimeType}; charset=utf-8`,
        "Content-Disposition": `attachment; filename="${layer}_${table}.${ext}"`,
      },
    });
  } catch (error: any) {
    console.error("[Export API] Error exporting table:", error);
    return NextResponse.json({ error: error.message || "Export failed" }, { status: 500 });
  }
}
