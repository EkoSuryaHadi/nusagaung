This is a [Next.js](https://nextjs.org) project bootstrapped with [`create-next-app`](https://nextjs.org/docs/app/api-reference/cli/create-next-app).

## 🚀 Running with Docker (Recommended)

GaungNusa provides full containerization including Next.js App, Python 3.11 ETL/ML Worker, and PostgreSQL 16 Lakehouse Database.

### 1. Start Services
- **Docker Compose:**
  ```bash
  docker compose up -d --build
  ```
- **Podman Compose (Windows / Linux):**
  ```bash
  podman machine start   # jika VM belum berjalan
  podman-compose up -d --build
  ```

### 2. Access Platform
* **Web App:** [http://localhost:3000](http://localhost:3000)
* **PostgreSQL Database:** `localhost:5433` (User: `gaung`, DB: `gaungnusa`)

### 3. Check Logs & Status
```bash
docker compose logs -f app
```

### 4. Stop Services
```bash
docker compose down
```

---

## Local Development (Without Docker)

First, run the development server:

```bash
npm run dev
```

Open [http://localhost:3000](http://localhost:3000) with your browser to see the result.

