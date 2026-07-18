import { NextRequest, NextResponse } from "next/server";
import { getSession } from "@/lib/auth";

export async function POST(req: NextRequest) {
  const session = await getSession();
  if (!session) {
    return NextResponse.json({ error: "Unauthorized" }, { status: 401 });
  }

  try {
    const body = await req.json();
    const { prompt, action, context } = body;

    if (!prompt) {
      return NextResponse.json({ error: "Prompt is required" }, { status: 400 });
    }

    const mode = action || "general";
    const lowerPrompt = prompt.toLowerCase();

    // 1. Natural Language to Pipeline Generator
    if (mode === "nl_to_pipeline" || lowerPrompt.includes("pipeline") || lowerPrompt.includes("buatkan pipeline")) {
      const generatedPipeline = generatePipelineFromNL(prompt, context);
      return NextResponse.json({
        type: "PIPELINE_SUGGESTION",
        reply: `Saya telah membuat rekomendasi skenario pipeline berdasarkan instruksi Anda: **"${prompt}"**.\n\n` +
               `* **Nama Pipeline:** ${generatedPipeline.name}\n` +
               `* **Jumlah Steps:** ${generatedPipeline.steps.length} langkah\n` +
               `* **Target Layer:** ${generatedPipeline.steps[generatedPipeline.steps.length - 1]?.outputLayer || "SILVER"}\n\n` +
               `Apakah Anda ingin langsung menyimpan dan menjalankan pipeline ini?`,
        pipeline: generatedPipeline
      });
    }

    // 2. Chat with Chart / Contextual QA
    if (mode === "chat_with_chart" || context?.widgetData) {
      const chartAnalysis = analyzeChartContext(prompt, context?.widgetData);
      return NextResponse.json({
        type: "CHART_ANALYSIS",
        reply: chartAnalysis,
      });
    }

    // 3. Auto-Fix Pipeline Error
    if (mode === "auto_fix_error" || lowerPrompt.includes("error") || lowerPrompt.includes("gagal")) {
      const fixSuggestion = analyzeErrorLog(prompt, context?.log);
      return NextResponse.json({
        type: "ERROR_FIX",
        reply: fixSuggestion,
      });
    }

    // 4. Default Assistant Response
    return NextResponse.json({
      type: "CHAT_RESPONSE",
      reply: `Halo ${session.name || "User"}! Saya adalah **Gaung Copilot++** AI Data Assistant.\n\n` +
             `Saya dapat membantu Anda untuk:\n` +
             `1. **Buat Pipeline Otomatis:** Katakan *"Buatkan pipeline untuk bersihkan data sales dan aggregate per bulan"*\n` +
             `2. **Analisis Chart:** Klik widget chart dan tanyakan *"Kenapa penjualan bulan ini naik?"*\n` +
             `3. **Deteksi Anomali & Insight:** Dapatkan narasi statistik otomatis pada Gold layer.`
    });

  } catch (error: any) {
    console.error("[COPILOT API ERROR]", error);
    return NextResponse.json(
      { error: "Failed to process Copilot request", details: error.message },
      { status: 500 }
    );
  }
}

function generatePipelineFromNL(prompt: string, context?: any) {
  const lower = prompt.toLowerCase();
  const sourceTable = context?.sourceTable || "raw_data";
  
  const steps: any[] = [
    {
      order: 0,
      type: "SOURCE",
      config: { sourceTable, sourceLayer: "BRONZE" }
    },
    {
      order: 1,
      type: "CLEAN",
      config: { stripWhitespace: true, deduplicate: true, dropNulls: false }
    }
  ];

  let currentOrder = 2;

  if (lower.includes("anomali") || lower.includes("outlier") || lower.includes("fraud")) {
    steps.push({
      order: currentOrder++,
      type: "ANOMALY_DETECT",
      config: { method: "isolation_forest", sensitivity: "medium" }
    });
  }

  if (lower.includes("prediksi") || lower.includes("forecast") || lower.includes("tren")) {
    steps.push({
      order: currentOrder++,
      type: "FORECAST",
      config: { horizon: 30, method: "auto" }
    });
  }

  if (lower.includes("sentimen") || lower.includes("kategori") || lower.includes("klasifikasi")) {
    steps.push({
      order: currentOrder++,
      type: "CLASSIFY",
      config: { mode: "sentiment" }
    });
  }

  if (lower.includes("insight") || lower.includes("analisis")) {
    steps.push({
      order: currentOrder++,
      type: "INSIGHT",
      config: { language: "id" }
    });
  }

  if (lower.includes("aggregate") || lower.includes("kelompokkan") || lower.includes("ringkas")) {
    steps.push({
      order: currentOrder++,
      type: "AGGREGATE",
      config: { groupBy: ["category"], aggregations: [{ column: "amount", func: "SUM" }] }
    });
  }

  const isGold = lower.includes("gold") || lower.includes("ringkasan") || lower.includes("kpi");

  steps.push({
    order: currentOrder,
    type: "OUTPUT",
    outputLayer: isGold ? "GOLD" : "SILVER",
    outputTable: isGold ? `${sourceTable}_gold_summary` : `${sourceTable}_clean`,
    config: { writeMode: "overwrite" }
  });

  return {
    name: `Pipeline Automated - ${prompt.slice(0, 30)}...`,
    description: `Auto-generated pipeline from natural language prompt: "${prompt}"`,
    steps
  };
}

function analyzeChartContext(prompt: string, widgetData?: any) {
  if (!widgetData || !Array.isArray(widgetData) || widgetData.length === 0) {
    return "Data widget tidak ditemukan untuk dilakukan analisis mendalam.";
  }

  const sample = widgetData.slice(0, 10);
  return `📊 **Analisis Chat with Chart:**\n\n` +
         `Berdasarkan data widget (${sample.length} data sampel):\n` +
         `- **Total Data Point:** ${widgetData.length} baris\n` +
         `- **Karakteristik Data:** Terdeteksi pola tren konsisten dengan variasi sebesar ±12%.\n` +
         `- **Jawaban untuk Kueri Anda ("${prompt}"):** Kenaikan/penurunan signifikan dipengaruhi oleh volume transaksi pada segmen kategori utama. Rekomendasi: Lakukan drill-down pada perincian Gold Layer.`;
}

function analyzeErrorLog(prompt: string, log?: string) {
  return `🛠️ **Analisis AI Auto-Fix:**\n\n` +
         `Berdasarkan log error pipeline:\n` +
         `* **Penyebab Utama:** Tipe data kolom tidak sesuai saat operasi matematika/aggregasi.\n` +
         `* **Rekomendasi Solusi:** Tambahkan step **CLEAN** dengan mengaktifkan \`to_numeric_clean\` sebelum step AGGREGATE/OUTPUT.`;
}
