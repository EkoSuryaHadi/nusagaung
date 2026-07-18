import { queryDuckDB } from "@/lib/duckdb";
import { sanitizeIdentifier, sanitizeLayer } from "@/lib/queryGuard";
import path from "path";
import fs from "fs";

/**
 * Parquet Storage Engine Helper for long-term compressed columnar data archiving.
 */

export async function archiveTableToParquet(layer: string, table: string, outputDir?: string): Promise<{ success: boolean; filePath: string }> {
  const safeLayer = sanitizeLayer(layer);
  const safeTable = sanitizeIdentifier(table).toLowerCase();

  const targetDir = outputDir || path.join(process.cwd(), "uploads", "archives");
  if (!fs.existsSync(targetDir)) {
    fs.mkdirSync(targetDir, { recursive: true });
  }

  const fileName = `${safeLayer}_${safeTable}_${Date.now()}.parquet`;
  const fullPath = path.join(targetDir, fileName).replace(/\\/g, "/");

  const sql = `COPY (SELECT * FROM pg.${safeLayer}."${safeTable}") TO '${fullPath}' (FORMAT PARQUET, COMPRESSION 'SNAPPY')`;
  
  try {
    await queryDuckDB(sql);
    console.log(`[Parquet Storage] Successfully archived ${safeLayer}.${safeTable} to ${fullPath}`);
    return { success: true, filePath: fullPath };
  } catch (error: any) {
    console.error(`[Parquet Storage] Failed to archive ${safeLayer}.${safeTable}:`, error);
    throw new Error(`Failed to convert table to Parquet: ${error.message}`);
  }
}
