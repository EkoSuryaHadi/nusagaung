import bcrypt from "bcryptjs";
import { NextResponse } from "next/server";
import prisma from "@/lib/prisma";
import { signSession, type SessionData } from "@/lib/auth";

export async function POST(request: Request) {
  try {
    const { name, email, password } = await request.json();
    if (!email || !password) {
      return NextResponse.json({ error: "Email dan password wajib diisi." }, { status: 400 });
    }

    const cleanEmail = email.toLowerCase().trim();

    // Check if user already exists
    const existingUser = await prisma.user.findUnique({
      where: { email: cleanEmail },
    });

    if (existingUser) {
      return NextResponse.json({ error: "Email sudah terdaftar. Silakan masuk di halaman login." }, { status: 400 });
    }

    // Ensure a default Tenant exists for multi-tenant isolation
    let tenant = await prisma.tenant.findFirst();
    if (!tenant) {
      tenant = await prisma.tenant.create({
        data: {
          name: "Workspace Utama",
          slug: "default-tenant",
        },
      });
    }

    const hashedPassword = await bcrypt.hash(password, 10);

    // Create new user record
    const user = await prisma.user.create({
      data: {
        name: name || cleanEmail.split("@")[0],
        email: cleanEmail,
        password: hashedPassword,
        role: "ADMIN",
        tenantId: tenant.id,
      },
      include: { tenant: true },
    });

    const session: SessionData = {
      id: user.id,
      userId: user.id,
      name: user.name,
      email: user.email,
      role: user.role as any,
      tenantId: user.tenant?.id,
      tenantSlug: user.tenant?.slug,
    };

    const token = await signSession(session);
    const response = NextResponse.json({
      success: true,
      session,
      token,
      redirectTo: "/dashboard",
    });

    response.cookies.set("gaung_session", token, {
      httpOnly: false,
      secure: false,
      sameSite: "lax",
      path: "/",
      maxAge: 604800,
    });

    return response;
  } catch (e: any) {
    console.error("[REGISTER API ERROR]", e);
    return NextResponse.json({ error: "Terjadi kesalahan server saat mendaftar: " + e.message }, { status: 500 });
  }
}
