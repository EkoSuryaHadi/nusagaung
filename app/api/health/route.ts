import { NextRequest, NextResponse } from "next/server";
import { PrismaClient } from "@prisma/client";
import { getDuckDB } from "@/lib/duckdb";
import os from "os";

const prisma = new PrismaClient();

export async function GET(request: NextRequest) {
  const startTime = Date.now();
  const checks: Record<string, { status: "UP" | "DOWN"; latencyMs?: number; details?: any }> = {};

  // 1. PostgreSQL Health Check
  try {
    const pgStart = Date.now();
    await prisma.$queryRaw`SELECT 1`;
    checks.postgres = {
      status: "UP",
      latencyMs: Date.now() - pgStart,
    };
  } catch (err: any) {
    checks.postgres = {
      status: "DOWN",
      details: err.message,
    };
  }

  // 2. DuckDB Engine Health Check
  try {
    const duckStart = Date.now();
    const db = await getDuckDB();
    checks.duckdb = {
      status: "UP",
      latencyMs: Date.now() - duckStart,
    };
  } catch (err: any) {
    checks.duckdb = {
      status: "DOWN",
      details: err.message,
    };
  }

  // 3. System Memory & Resource Usage
  const totalMem = os.totalmem();
  const freeMem = os.freemem();
  const usedMem = totalMem - freeMem;
  const memUsagePct = Math.round((usedMem / totalMem) * 100);

  checks.system = {
    status: memUsagePct < 95 ? "UP" : "DOWN",
    details: {
      totalMemoryMb: Math.round(totalMem / (1024 * 1024)),
      freeMemoryMb: Math.round(freeMem / (1024 * 1024)),
      usedMemoryPct: `${memUsagePct}%`,
      cpus: os.cpus().length,
      platform: os.platform(),
      uptimeSeconds: Math.round(process.uptime()),
    },
  };

  const isHealthy = Object.values(checks).every((c) => c.status === "UP");
  const totalLatencyMs = Date.now() - startTime;

  return NextResponse.json(
    {
      status: isHealthy ? "HEALTHY" : "UNHEALTHY",
      timestamp: new Date().toISOString(),
      totalLatencyMs,
      checks,
    },
    { status: isHealthy ? 200 : 503 }
  );
}
