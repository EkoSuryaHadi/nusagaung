import { NextRequest, NextResponse } from "next/server";
import { PrismaClient } from "@prisma/client";
import { cryptoNative } from "@/lib/auth"; // or crypto.randomUUID()

const prisma = new PrismaClient();

export async function POST(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const dashboardId = parseInt(id, 10);

    if (isNaN(dashboardId)) {
      return NextResponse.json({ error: "Invalid dashboard ID" }, { status: 400 });
    }

    const dashboard = await prisma.dashboard.findUnique({
      where: { id: dashboardId },
    });

    if (!dashboard) {
      return NextResponse.json({ error: "Dashboard not found" }, { status: 404 });
    }

    // Generate share token if not exists, or toggle
    const shareToken = dashboard.shareToken || crypto.randomUUID();
    const updated = await prisma.dashboard.update({
      where: { id: dashboardId },
      data: {
        isPublic: true,
        shareToken,
      },
      select: {
        id: true,
        name: true,
        isPublic: true,
        shareToken: true,
      },
    });

    return NextResponse.json({
      success: true,
      shareToken: updated.shareToken,
      shareUrl: `/public/dashboards/${updated.shareToken}`,
      dashboard: updated,
    });
  } catch (error: any) {
    console.error("[Dashboard Share API] Error generating share token:", error);
    return NextResponse.json({ error: error.message || "Failed to share dashboard" }, { status: 500 });
  }
}

export async function DELETE(
  request: NextRequest,
  { params }: { params: Promise<{ id: string }> }
) {
  try {
    const { id } = await params;
    const dashboardId = parseInt(id, 10);

    if (isNaN(dashboardId)) {
      return NextResponse.json({ error: "Invalid dashboard ID" }, { status: 400 });
    }

    const updated = await prisma.dashboard.update({
      where: { id: dashboardId },
      data: {
        isPublic: false,
        shareToken: null,
      },
      select: {
        id: true,
        isPublic: true,
        shareToken: true,
      },
    });

    return NextResponse.json({
      success: true,
      message: "Public share revoked successfully",
      dashboard: updated,
    });
  } catch (error: any) {
    console.error("[Dashboard Share API] Error revoking share token:", error);
    return NextResponse.json({ error: error.message || "Failed to revoke share token" }, { status: 500 });
  }
}
