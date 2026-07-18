"use client";

import React, { useState } from "react";
import { ShieldCheck, ShieldAlert, Sparkles, CheckCircle2, Info } from "lucide-react";

interface DataQualityBadgeProps {
  score?: number | null;
  details?: {
    completeness?: number;
    uniqueness?: number;
    consistency?: number;
    freshness?: number;
    accuracy?: number;
  } | string | null;
}

export default function DataQualityBadge({ score = 100, details }: DataQualityBadgeProps) {
  const [showTooltip, setShowTooltip] = useState(false);

  const numScore = score ?? 100;
  
  let parsedDetails: any = {
    completeness: 100,
    uniqueness: 100,
    consistency: 100,
    freshness: 100,
    accuracy: 100,
  };

  if (typeof details === "string") {
    try {
      parsedDetails = JSON.parse(details);
    } catch (e) {
      // Fallback
    }
  } else if (details && typeof details === "object") {
    parsedDetails = details;
  }

  let badgeColor = "bg-emerald-950/80 text-emerald-300 border-emerald-500/40";
  let dotColor = "bg-emerald-400";
  let label = "Excellent";

  if (numScore < 50) {
    badgeColor = "bg-rose-950/80 text-rose-300 border-rose-500/40";
    dotColor = "bg-rose-400";
    label = "Poor";
  } else if (numScore < 70) {
    badgeColor = "bg-amber-950/80 text-amber-300 border-amber-500/40";
    dotColor = "bg-amber-400";
    label = "Fair";
  } else if (numScore < 90) {
    badgeColor = "bg-amber-900/60 text-amber-200 border-amber-400/30";
    dotColor = "bg-amber-300";
    label = "Good";
  }

  return (
    <div className="relative inline-block">
      <div
        onMouseEnter={() => setShowTooltip(true)}
        onMouseLeave={() => setShowTooltip(false)}
        className={`flex items-center gap-2 px-2.5 py-1 rounded-full border text-xs font-medium cursor-pointer transition-all ${badgeColor}`}
      >
        <span className={`w-2 h-2 rounded-full animate-pulse ${dotColor}`} />
        <span className="font-mono font-bold">{numScore.toFixed(1)}</span>
        <span>/ 100</span>
        <span className="opacity-75">({label})</span>
      </div>

      {showTooltip && (
        <div className="absolute z-50 bottom-full mb-2 left-0 w-64 p-3 rounded-xl bg-neutral-900 border border-neutral-700 shadow-2xl text-xs text-neutral-200 backdrop-blur-md">
          <div className="flex items-center justify-between font-semibold border-b border-neutral-800 pb-2 mb-2">
            <span className="flex items-center gap-1.5 text-amber-400">
              <ShieldCheck className="w-4 h-4" /> Data Quality Score
            </span>
            <span className="font-mono text-sm">{numScore.toFixed(1)}%</span>
          </div>

          <div className="space-y-1.5 font-mono">
            <div className="flex justify-between">
              <span className="text-neutral-400">Completeness:</span>
              <span>{parsedDetails.completeness ?? 100}%</span>
            </div>
            <div className="flex justify-between">
              <span className="text-neutral-400">Uniqueness:</span>
              <span>{parsedDetails.uniqueness ?? 100}%</span>
            </div>
            <div className="flex justify-between">
              <span className="text-neutral-400">Consistency:</span>
              <span>{parsedDetails.consistency ?? 100}%</span>
            </div>
            <div className="flex justify-between">
              <span className="text-neutral-400">Freshness:</span>
              <span>{parsedDetails.freshness ?? 100}%</span>
            </div>
            <div className="flex justify-between">
              <span className="text-neutral-400">Accuracy:</span>
              <span>{parsedDetails.accuracy ?? 100}%</span>
            </div>
          </div>
        </div>
      )}
    </div>
  );
}
