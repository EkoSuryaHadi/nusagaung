"use client";

import { useEffect, useState } from "react";
import { authFetch } from "@/lib/auth-client";

interface TenantInfo {
  tenantId?: number;
  tenantName?: string;
  tenantSlug?: string;
}

export default function TenantSelector() {
  const [tenant, setTenant] = useState<TenantInfo>({});

  useEffect(() => {
    authFetch("/api/auth/session")
      .then((r) => r.json())
      .then((data) => {
        if (data.session) {
          const rawSlug = data.session.tenantSlug;
          const displaySlug = (!rawSlug || rawSlug === "default-tenant") ? "nusa2" : rawSlug;
          const displayName = data.session.tenantName || displaySlug;
          setTenant({
            tenantId: data.session.tenantId,
            tenantName: displayName,
            tenantSlug: displaySlug,
          });
        }
      })
      .catch(() => {});
  }, []);

  const nameToDisplay = tenant.tenantName || tenant.tenantSlug || "nusa2";
  const initial = nameToDisplay.charAt(0).toUpperCase();

  return (
    <div className="px-5 py-3 border-b border-amber-950/40">
      <div className="flex items-center gap-2.5">
        <div className="w-7 h-7 rounded-lg bg-amber-500/10 border border-amber-500/30 flex items-center justify-center text-xs font-bold text-amber-400 shrink-0">
          {initial}
        </div>
        <div className="min-w-0">
          <p className="text-[10px] text-neutral-500 uppercase tracking-wider font-semibold">Tenant</p>
          <p className="text-xs text-amber-200/90 truncate font-medium">{nameToDisplay}</p>
        </div>
      </div>
    </div>
  );
}
