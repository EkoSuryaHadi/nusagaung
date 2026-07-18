"use client";

import React from "react";
import { Sparkles, TrendingUp, AlertTriangle, Link2, Trophy, BarChart2 } from "lucide-react";

interface AutoInsightCardProps {
  insights?: string[];
  title?: string;
}

export default function AutoInsightCard({ insights = [], title = "💡 Auto-Insights (Bahasa Indonesia)" }: AutoInsightCardProps) {
  if (!insights || insights.length === 0) {
    return (
      <div className="p-4 rounded-xl border border-amber-500/20 bg-amber-950/10 text-amber-200/60 text-xs text-center">
        Belum ada narasi auto-insight untuk dataset ini. Tambahkan step <code>INSIGHT</code> pada pipeline Anda.
      </div>
    );
  }

  return (
    <div className="p-4 rounded-2xl border border-amber-500/30 bg-linear-to-br from-neutral-900 via-neutral-900/90 to-amber-950/20 shadow-xl backdrop-blur-md">
      <div className="flex items-center justify-between pb-3 border-b border-amber-500/20 mb-3">
        <div className="flex items-center gap-2">
          <div className="p-1.5 rounded-lg bg-amber-500/10 text-amber-400 border border-amber-500/20">
            <Sparkles className="w-4 h-4 animate-spin-slow" />
          </div>
          <h4 className="text-sm font-semibold text-amber-200">{title}</h4>
        </div>
        <span className="text-[10px] font-mono px-2 py-0.5 rounded bg-amber-500/10 text-amber-300 border border-amber-500/20">
          AI Generated
        </span>
      </div>

      <div className="space-y-2 text-xs text-neutral-300">
        {insights.map((item, idx) => {
          let Icon = BarChart2;
          if (item.includes("Rata-rata")) Icon = TrendingUp;
          if (item.includes("anomali") || item.includes("outlier")) Icon = AlertTriangle;
          if (item.includes("Hubungan") || item.includes("Korelasi")) Icon = Link2;
          if (item.includes("Kategori terbanyak")) Icon = Trophy;

          return (
            <div key={idx} className="flex items-start gap-2.5 p-2 rounded-lg bg-neutral-800/40 hover:bg-neutral-800/80 transition-all border border-neutral-800">
              <Icon className="w-4 h-4 text-amber-400 mt-0.5 shrink-0" />
              <p className="leading-relaxed">
                {item.split("**").map((part, i) =>
                  i % 2 === 1 ? <strong key={i} className="text-amber-300 font-semibold">{part}</strong> : part
                )}
              </p>
            </div>
          );
        })}
      </div>
    </div>
  );
}
