/**
 * PII Data Masking System for Data Governance & Privacy Compliance.
 * Automatically masks sensitive columns (NIK, Phone, Email, Credit Cards, Bank Accounts).
 */

export function maskEmail(email: string): string {
  if (!email || typeof email !== "string" || !email.includes("@")) return email;
  const [user, domain] = email.split("@");
  if (user.length <= 2) return `${user[0]}*@${domain}`;
  return `${user[0]}${"*".repeat(user.length - 2)}${user[user.length - 1]}@${domain}`;
}

export function maskPhone(phone: string): string {
  if (!phone || typeof phone !== "string") return phone;
  const clean = phone.trim();
  if (clean.length < 7) return clean;
  const start = clean.slice(0, 4);
  const end = clean.slice(-4);
  return `${start}${"*".repeat(Math.max(4, clean.length - 8))}${end}`;
}

export function maskNIK(nik: string): string {
  if (!nik || typeof nik !== "string") return nik;
  const clean = nik.trim();
  if (clean.length < 10) return clean;
  return `${clean.slice(0, 4)}${"*".repeat(clean.length - 4)}`;
}

export function maskCardNumber(card: string): string {
  if (!card || typeof card !== "string") return card;
  const clean = card.replace(/\s|-/g, "");
  if (clean.length < 12) return card;
  return `${clean.slice(0, 4)}-****-****-${clean.slice(-4)}`;
}

export function isPIIColumn(colName: string): "EMAIL" | "PHONE" | "NIK" | "CARD" | null {
  const col = colName.toLowerCase().replace(/[^a-z0-9]/g, "");
  if (col.includes("email") || col.includes("surel")) return "EMAIL";
  if (col.includes("phone") || col.includes("telepon") || col.includes("hp") || col.includes("wa")) return "PHONE";
  if (col.includes("nik") || col.includes("ktp") || col.includes("identity")) return "NIK";
  if (col.includes("card") || col.includes("rekening") || col.includes("creditcard") || col.includes("accnum")) return "CARD";
  return null;
}

export function maskRowData(row: Record<string, any>): Record<string, any> {
  const masked: Record<string, any> = {};
  for (const [key, value] of Object.entries(row)) {
    if (value === null || value === undefined) {
      masked[key] = value;
      continue;
    }
    const piiType = isPIIColumn(key);
    const strVal = String(value);
    
    switch (piiType) {
      case "EMAIL":
        masked[key] = maskEmail(strVal);
        break;
      case "PHONE":
        masked[key] = maskPhone(strVal);
        break;
      case "NIK":
        masked[key] = maskNIK(strVal);
        break;
      case "CARD":
        masked[key] = maskCardNumber(strVal);
        break;
      default:
        masked[key] = value;
    }
  }
  return masked;
}

export function maskDataSet(rows: Record<string, any>[]): Record<string, any>[] {
  return rows.map(maskRowData);
}
