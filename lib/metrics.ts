import { queryDuckDB } from "@/lib/duckdb";
import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

export interface MetricDefinition {
  id: string;
  name: string;
  category: "FINANCE" | "OPERATIONS" | "DATA_GOVERNANCE" | "PIPELINE";
  description: string;
  unit?: string;
  queryType: "PRISMA" | "DUCKDB";
  sqlTemplate?: string;
}

export const METRIC_REGISTRY: Record<string, MetricDefinition> = {
  TOTAL_DATA_SOURCES: {
    id: "TOTAL_DATA_SOURCES",
    name: "Total Data Sources",
    category: "DATA_GOVERNANCE",
    description: "Jumlah total sumber data terdaftar pada tenant",
    unit: "sources",
    queryType: "PRISMA",
  },
  ACTIVE_PIPELINES: {
    id: "ACTIVE_PIPELINES",
    name: "Active Pipelines",
    category: "PIPELINE",
    description: "Jumlah ETL Pipeline berstatus ACTIVE",
    unit: "pipelines",
    queryType: "PRISMA",
  },
  PIPELINE_SUCCESS_RATE: {
    id: "PIPELINE_SUCCESS_RATE",
    name: "Pipeline Success Rate",
    category: "PIPELINE",
    description: "Persentase eksekusi pipeline yang berhasil",
    unit: "%",
    queryType: "PRISMA",
  },
  TOTAL_ROWS_PROCESSED: {
    id: "TOTAL_ROWS_PROCESSED",
    name: "Total Rows Processed",
    category: "OPERATIONS",
    description: "Jumlah total baris data yang diproses oleh seluruh pipeline",
    unit: "rows",
    queryType: "PRISMA",
  },
  AVG_PIPELINE_DURATION: {
    id: "AVG_PIPELINE_DURATION",
    name: "Avg Pipeline Duration",
    category: "PIPELINE",
    description: "Rata-rata durasi eksekusi pipeline dalam detik",
    unit: "seconds",
    queryType: "PRISMA",
  },
  FAILED_PIPELINE_RUNS: {
    id: "FAILED_PIPELINE_RUNS",
    name: "Failed Pipeline Runs",
    category: "PIPELINE",
    description: "Jumlah total eksekusi pipeline yang mengalami kesalahan",
    unit: "runs",
    queryType: "PRISMA",
  },
  TOTAL_AUDIT_EVENTS: {
    id: "TOTAL_AUDIT_EVENTS",
    name: "Total Audit Events",
    category: "DATA_GOVERNANCE",
    description: "Jumlah jejak audit keamanan data yang tercatat",
    unit: "events",
    queryType: "PRISMA",
  },
};

export async function calculateMetric(metricId: string, tenantId?: number): Promise<{ value: number; formatted: string; details?: any }> {
  const def = METRIC_REGISTRY[metricId];
  if (!def) {
    throw new Error(`Metric '${metricId}' is not defined in Metric Registry.`);
  }

  if (metricId === "TOTAL_DATA_SOURCES") {
    const count = await prisma.dataSource.count({
      where: tenantId ? { tenantId } : {},
    });
    return { value: count, formatted: `${count.toLocaleString("id-ID")} sources` };
  }

  if (metricId === "ACTIVE_PIPELINES") {
    const count = await prisma.pipeline.count({
      where: {
        status: "ACTIVE",
        ...(tenantId ? { tenantId } : {}),
      },
    });
    return { value: count, formatted: `${count.toLocaleString("id-ID")} active` };
  }

  if (metricId === "PIPELINE_SUCCESS_RATE") {
    const totalRuns = await prisma.pipelineRun.count();
    if (totalRuns === 0) return { value: 100, formatted: "100%" };
    const successRuns = await prisma.pipelineRun.count({
      where: { status: "SUCCESS" },
    });
    const rate = Math.round((successRuns / totalRuns) * 100);
    return { value: rate, formatted: `${rate}%` };
  }

  if (metricId === "TOTAL_ROWS_PROCESSED") {
    const result = await prisma.pipelineRun.aggregate({
      _sum: { rowsOutput: true },
    });
    const totalRows = result._sum.rowsOutput || 0;
    return { value: totalRows, formatted: `${totalRows.toLocaleString("id-ID")} rows` };
  }

  if (metricId === "AVG_PIPELINE_DURATION") {
    const result = await prisma.pipelineRun.aggregate({
      _avg: { duration: true },
      where: { status: "SUCCESS" },
    });
    const avgSec = Math.round(result._avg.duration || 0);
    return { value: avgSec, formatted: `${avgSec} detik` };
  }

  if (metricId === "FAILED_PIPELINE_RUNS") {
    const count = await prisma.pipelineRun.count({
      where: { status: "FAILED" },
    });
    return { value: count, formatted: `${count.toLocaleString("id-ID")} failed` };
  }

  if (metricId === "TOTAL_AUDIT_EVENTS") {
    const count = await prisma.auditLog.count({
      where: tenantId ? { tenantId } : {},
    });
    return { value: count, formatted: `${count.toLocaleString("id-ID")} events` };
  }

  return { value: 0, formatted: "0" };
}
