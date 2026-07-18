"use client";

import { useState } from "react";
import { useRouter } from "next/navigation";
import Link from "next/link";
import { UserPlus } from "lucide-react";
import { storeAuth } from "@/lib/auth-client";

export default function RegisterPage() {
  const router = useRouter();
  const [name, setName] = useState("");
  const [email, setEmail] = useState("");
  const [password, setPassword] = useState("");
  const [error, setError] = useState("");
  const [loading, setLoading] = useState(false);

  async function handleRegister(e: React.FormEvent) {
    e.preventDefault();
    setLoading(true);
    setError("");
    try {
      const res = await fetch("/api/auth/register", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify({ email, password, name }),
      });
      const data = await res.json();
      if (!res.ok) throw new Error(data.error);

      storeAuth(data.token, data.session);
      router.push("/dashboard");
    } catch (err: any) {
      setError(err.message || "Registration failed");
    } finally {
      setLoading(false);
    }
  }

  return (
    <div style={{ display: "flex", minHeight: "100vh" }}>
      <div
        style={{
          width: "45%",
          background: "var(--bg-root)",
          position: "relative",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          overflow: "hidden",
        }}
      >
        <div style={{ padding: "3rem", textAlign: "center", maxWidth: 440, zIndex: 1 }}>
          <div style={{ display: "inline-block", width: 48, height: 48, borderRadius: 14, background: "var(--bg-card)", border: "1px solid var(--border-default)", marginBottom: 24, padding: 12 }}>
            <span style={{ fontSize: 24 }}>🌊</span>
          </div>
          <h1 style={{ fontFamily: "var(--font-display)", fontSize: 32, fontWeight: 300, color: "var(--text-primary)", marginBottom: 12 }}>
            Bergabung dengan <span style={{ color: "var(--gold-400)", fontStyle: "italic" }}>Gaung</span>
          </h1>
          <p style={{ fontSize: 14, color: "var(--text-secondary)", lineHeight: 1.6 }}>
            Platform Data Lakehouse + ML Intelligence Indonesia.
          </p>
        </div>
      </div>

      <div
        style={{
          flex: 1,
          background: "var(--bg-surface)",
          display: "flex",
          alignItems: "center",
          justifyContent: "center",
          padding: 32,
        }}
      >
        <div style={{ width: "100%", maxWidth: 380 }}>
          <h2 style={{ fontSize: 22, fontWeight: 500, color: "var(--text-primary)", marginBottom: 6 }}>
            Buat Akun Baru
          </h2>
          <p style={{ fontSize: 13, color: "var(--text-secondary)", marginBottom: 28 }}>
            Daftar untuk mulai mengelola data & visualisasi dashboard.
          </p>

          {error && (
            <div style={{ background: "var(--clay-dim)", border: "1px solid var(--clay-400)", borderRadius: 10, padding: 12, marginBottom: 20, fontSize: 13, color: "var(--clay-400)" }}>
              {error}
            </div>
          )}

          <form onSubmit={handleRegister} style={{ display: "flex", flexDirection: "column", gap: 18 }}>
            <div>
              <label style={{ display: "block", fontSize: 12, fontWeight: 500, color: "var(--text-secondary)", marginBottom: 6 }}>
                Nama Lengkap
              </label>
              <input
                type="text"
                required
                value={name}
                onChange={(e) => setName(e.target.value)}
                placeholder="Eko Surya Hadi"
                style={{ width: "100%", padding: "10px 14px", borderRadius: 8, border: "1px solid var(--border-default)", background: "var(--bg-card)", color: "var(--text-primary)", fontSize: 14, outline: "none" }}
              />
            </div>

            <div>
              <label style={{ display: "block", fontSize: 12, fontWeight: 500, color: "var(--text-secondary)", marginBottom: 6 }}>
                Email Address
              </label>
              <input
                type="email"
                required
                value={email}
                onChange={(e) => setEmail(e.target.value)}
                placeholder="nama@perusahaan.com"
                style={{ width: "100%", padding: "10px 14px", borderRadius: 8, border: "1px solid var(--border-default)", background: "var(--bg-card)", color: "var(--text-primary)", fontSize: 14, outline: "none" }}
              />
            </div>

            <div>
              <label style={{ display: "block", fontSize: 12, fontWeight: 500, color: "var(--text-secondary)", marginBottom: 6 }}>
                Password
              </label>
              <input
                type="password"
                required
                value={password}
                onChange={(e) => setPassword(e.target.value)}
                placeholder="••••••••"
                style={{ width: "100%", padding: "10px 14px", borderRadius: 8, border: "1px solid var(--border-default)", background: "var(--bg-card)", color: "var(--text-primary)", fontSize: 14, outline: "none" }}
              />
            </div>

            <button
              type="submit"
              disabled={loading}
              style={{ width: "100%", padding: "12px", borderRadius: 8, background: "var(--gold-500)", color: "#000", fontWeight: 600, border: "none", cursor: "pointer", display: "flex", alignItems: "center", justifyContent: "center", gap: 8, marginTop: 8 }}
            >
              <UserPlus style={{ width: 16, height: 16 }} />
              {loading ? "Mendaftar..." : "Daftar Akun"}
            </button>
          </form>

          <p style={{ marginTop: 24, textAlign: "center", fontSize: 13, color: "var(--text-secondary)" }}>
            Sudah punya akun?{" "}
            <Link href="/login" style={{ color: "var(--gold-400)", textDecoration: "none", fontWeight: 500 }}>
              Masuk di sini
            </Link>
          </p>
        </div>
      </div>
    </div>
  );
}
