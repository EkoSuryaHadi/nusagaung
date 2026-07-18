import { PrismaClient } from "@prisma/client";

const prisma = new PrismaClient();

export interface AuditParams {
  userId?: number | null;
  tenantId?: number | null;
  action: string;      // LOGIN | UPLOAD | PIPELINE_RUN | LAKEHOUSE_DROP | SHARE_LINK | QUERY | EXPORT
  resource: string;    // Source, Pipeline, LakehouseTable, Dashboard, Auth
  details?: Record<string, any> | string | null;
  ipAddress?: string | null;
  userAgent?: string | null;
}

/**
  * Log system activity for audit trail and data governance.
  * Async non-blocking execution to ensure zero impact on API response time.
  */
export async function logAudit(params: AuditParams): Promise<void> {
  try {
    const detailsStr = typeof params.details === "object" && params.details !== null
      ? JSON.stringify(params.details)
      : (params.details as string | null) ?? null;

    await prisma.auditLog.create({
      data: {
        userId: params.userId ?? null,
        tenantId: params.tenantId ?? null,
        action: params.action,
        resource: params.resource,
        details: detailsStr,
        ipAddress: params.ipAddress ?? null,
        userAgent: params.userAgent ?? null,
      },
    });
  } catch (error) {
    console.error("[AuditLog] Failed to record audit log:", error);
  }
}
