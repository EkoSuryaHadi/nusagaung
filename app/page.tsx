import Link from 'next/link';

export default function LandingPage() {
  return (
    <main className="min-h-screen flex flex-col items-center justify-center p-6 page-enter">
      <div className="max-w-5xl mx-auto w-full text-center space-y-12 stagger pt-12">
        
        {/* Badge */}
        <div className="flex justify-center">
          <div className="badge badge-active echo-ring px-4 py-1.5" style={{ boxShadow: 'var(--shadow-glow)' }}>
            Gaung Data Lakehouse
          </div>
        </div>
        
        {/* Hero Text */}
        <h1 className="text-5xl sm:text-7xl font-light tracking-tight text-[var(--text-primary)] leading-tight" style={{ fontFamily: 'var(--font-display)' }}>
          Data Masuk,<br />
          <span className="text-[var(--gold-400)] italic">
            Insight Bergema
          </span>
        </h1>
        
        <p className="text-lg text-[var(--text-secondary)] max-w-2xl mx-auto leading-relaxed">
          Upload data dari mana saja. Transformasi otomatis lewat pipeline ETL visual. 
          Simpan dalam lakehouse 3-tier. Bangun dashboard interaktif. 
          <strong className="text-[var(--text-primary)] font-medium"> Gaung</strong> — resonansi dari data Anda.
        </p>

        {/* CTA Buttons */}
        <div className="flex flex-wrap gap-4 justify-center pt-6">
          <Link href="/register" className="btn btn-primary px-8 py-3.5 text-base echo-ring">
            Mulai Sekarang
          </Link>
          <a href="#features" className="btn btn-secondary px-8 py-3.5 text-base">
            Pelajari Lebih Lanjut ▾
          </a>
        </div>

        {/* Features Grid */}
        <div id="features" className="grid gap-6 md:grid-cols-3 pt-24 text-left">
          {[
            { icon: "📥", title: "Multi-Source Ingest", desc: "CSV, Excel, JSON, API, Database — upload dari mana saja." },
            { icon: "⚙️", title: "Visual ETL Pipeline", desc: "Drag & drop steps: Clean → Transform → Join → Aggregate." },
            { icon: "📊", title: "Interactive Dashboards", desc: "Bangun dashboard interaktif tanpa coding dengan drag & drop." },
          ].map((f) => (
            <div key={f.title} className="card p-8">
              <div className="text-3xl mb-5 opacity-90">{f.icon}</div>
              <h3 className="text-lg font-medium text-[var(--text-primary)] mb-3">{f.title}</h3>
              <p className="text-sm text-[var(--text-secondary)] leading-relaxed">{f.desc}</p>
            </div>
          ))}
        </div>

        {/* Lakehouse Tiers */}
        <div className="pt-24 pb-12 space-y-8">
          <div className="text-center">
            <h2 className="text-3xl font-light text-[var(--text-primary)] mb-4" style={{ fontFamily: 'var(--font-display)' }}>3-Tier Lakehouse Architecture</h2>
            <p className="text-[var(--text-secondary)] text-base max-w-xl mx-auto">Data Anda terstruktur rapi dan teroptimasi secara otomatis dalam tiga lapisan.</p>
          </div>
          
          <div className="grid gap-6 md:grid-cols-3 pt-6 text-left">
            {[
              { tier: "BRONZE", color: "var(--clay-400)", desc: "Raw Data Ingestion & Storage" },
              { tier: "SILVER", color: "var(--text-muted)", desc: "Cleaned, Deduplicated & Validated" },
              { tier: "GOLD", color: "var(--gold-400)", desc: "Aggregated Metrics & Business KPIs" },
            ].map((t) => (
              <div key={t.tier} className="card-raised p-6 relative overflow-hidden group">
                <div className="absolute top-0 right-0 w-32 h-32 bg-[var(--gold-dim)] rounded-full blur-3xl -mr-10 -mt-10 opacity-0 group-hover:opacity-100 transition-opacity duration-500"></div>
                <div className="text-xs font-bold px-2.5 py-1 rounded-full border mb-4 inline-block tracking-wider" 
                     style={{ color: t.color, borderColor: 'var(--border-default)', backgroundColor: 'var(--bg-surface)' }}>
                  {t.tier}
                </div>
                <p className="text-sm text-[var(--text-secondary)]">{t.desc}</p>
              </div>
            ))}
          </div>
        </div>

      </div>
    </main>
  );
}
