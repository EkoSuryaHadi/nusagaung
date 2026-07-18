"use client";
import React, { createContext, useContext, useState, useEffect } from "react";

type SidebarContextType = {
  collapsed: boolean;
  setCollapsed: (v: boolean) => void;
};

const SidebarContext = createContext<SidebarContextType | undefined>(undefined);

if (typeof window !== "undefined") {
  (window as any).__sidebar_logs = (window as any).__sidebar_logs || [];
}

function logMsg(msg: string) {
  if (typeof window !== "undefined") {
    (window as any).__sidebar_logs.push(`${new Date().toISOString()}: ${msg}`);
  }
}

export function SidebarProvider({ children }: { children: React.ReactNode }) {
  const [collapsed, setCollapsed] = useState(() => {
    logMsg("SidebarProvider init state: false");
    return false;
  });

  const customSetCollapsed = (v: boolean) => {
    logMsg(`setCollapsed called with: ${v}`);
    setCollapsed(v);
  };

  useEffect(() => {
    logMsg(`SidebarProvider mounted/updated. collapsed=${collapsed}`);
  }, [collapsed]);

  return (
    <SidebarContext.Provider value={{ collapsed, setCollapsed: customSetCollapsed }}>
      <div className="flex min-h-screen">
        {children}
      </div>
    </SidebarContext.Provider>
  );
}

export function useSidebar() {
  const context = useContext(SidebarContext);
  if (!context) throw new Error("useSidebar must be used within SidebarProvider");
  return context;
}

export function MainContent({ children }: { children: React.ReactNode }) {
  const { collapsed } = useSidebar();
  return (
    <main className={`flex-1 transition-all duration-300 ${collapsed ? "lg:ml-16" : "lg:ml-56"}`}>
      {children}
    </main>
  );
}
