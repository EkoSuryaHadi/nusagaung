# Panduan Deployment VPS (Ubuntu) — Gaung

Dokumen ini memandu Anda melakukan deployment aplikasi **Gaung** di VPS Linux (direkomendasikan menggunakan Ubuntu 22.04 LTS atau 24.04 LTS).

---

## Arsitektur Deployment

Aplikasi ini menggunakan komponen-komponen berikut:
1. **Next.js Web App** (Frontend & Backend API) running langsung di VPS (Host) port `3005` dikelola oleh `systemd`.
2. **PostgreSQL** running langsung di VPS (Host) port `5432` sebagai database utama.
3. **WebSocket Bridge** (ws-bridge) running di Docker container port `3008` (WebSocket) dan `3100` (HTTP).
4. **Apache Airflow** (Scheduler & Webserver) running di Docker container.
5. **Nginx** sebagai Reverse Proxy untuk mengarahkan domain/IP ke Next.js (port 3005) dan WebSocket (port 3008), sekaligus untuk HTTPS (SSL).

---

## Langkah 1: Persiapan VPS & Instalasi Dependency

Login ke VPS Anda menggunakan SSH, lalu jalankan perintah berikut untuk mengupdate system dan menginstall dependency dasar:

```bash
# 1. Update system package
sudo apt update && sudo apt upgrade -y

# 2. Install Git, Curl, dan build-essential
sudo apt install -y git curl build-essential

# 3. Install Node.js (Versi 20.x)
curl -fsSL https://deb.nodesource.com/setup_20.x | sudo -E bash -
sudo apt install -y nodejs

# 4. Install Python 3, Pip, dan Virtual Environment
sudo apt install -y python3 python3-pip python3-venv

# 5. Install Docker & Docker Compose
# Hapus package docker bawaan ubuntu lama jika ada
for pkg in docker.io docker-doc docker-compose podman-docker containerd runc; do sudo apt-get remove $pkg; done
# Install docker resmi
sudo apt-get install -y ca-certificates curl gnupg
sudo install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | sudo gpg --dearmor -o /etc/apt/keyrings/docker.gpg
sudo chmod a+r /etc/apt/keyrings/docker.gpg
echo \
  "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu \
  $(. /etc/os-release && echo "$VERSION_CODENAME") stable" | \
  sudo tee /etc/apt/sources.list.d/docker.list > /dev/null
sudo apt update
sudo apt install -y docker-ce docker-ce-cli containerd.io docker-buildx-plugin docker-compose-plugin

# Masukkan user ubuntu ke group docker agar bisa menjalankan docker tanpa sudo
sudo usermod -aG docker $USER
# (Penting: log out dan log in kembali ke SSH agar group docker ini aktif)
```

---

## Langkah 2: Instalasi & Konfigurasi PostgreSQL (Host)

Aplikasi Gaung membutuhkan database PostgreSQL. Kita akan menginstall PostgreSQL langsung di OS Host (VPS).

```bash
# 1. Install PostgreSQL
sudo apt install -y postgresql postgresql-contrib

# 2. Masuk ke PostgreSQL prompt sebagai user postgres
sudo -i -u postgres psql
```

Di dalam PostgreSQL prompt (`postgres=#`), jalankan perintah SQL berikut untuk membuat database dan user:

```sql
-- Buat database gaung
CREATE DATABASE gaung;

-- Buat user gaung dengan password gaung123 (Silakan ganti password ini untuk keamanan!)
CREATE USER gaung WITH PASSWORD 'gaung123';

-- Berikan hak akses penuh ke user gaung untuk database gaung
GRANT ALL PRIVILEGES ON DATABASE gaung TO gaung;

-- Keluar dari psql
\q
exit
```

### [PENTING] Izinkan Docker mengakses PostgreSQL Host
Secara default, PostgreSQL di host hanya mendengarkan koneksi lokal (`localhost`). Karena Apache Airflow berjalan di dalam Docker, dia perlu mengakses PostgreSQL host menggunakan alamat IP gateway Docker (`host.docker.internal`).

1. Edit konfigurasi `postgresql.conf`:
   ```bash
   # Sesuaikan versi postgresql Anda (contoh: 14, 15, atau 16)
   sudo nano /etc/postgresql/*/main/postgresql.conf
   ```
   Cari baris `#listen_addresses = 'localhost'` lalu ubah menjadi:
   ```ini
   listen_addresses = '*'
   ```
   Simpan dengan menekan `Ctrl+O`, lalu `Enter`, dan keluar dengan `Ctrl+X`.

2. Edit konfigurasi `pg_hba.conf`:
   ```bash
   sudo nano /etc/postgresql/*/main/pg_hba.conf
   ```
   Tambahkan baris berikut di bagian paling bawah untuk mengizinkan koneksi dari jaringan internal Docker:
   ```text
   # Mengizinkan koneksi dari Docker containers ke PostgreSQL
   host    all             all             172.17.0.0/16           md5
   host    all             all             172.18.0.0/16           md5
   ```
   Simpan dan keluar.

3. Restart PostgreSQL untuk menerapkan konfigurasi baru:
   ```bash
   sudo systemctl restart postgresql
   ```

---

## Langkah 3: Setup Project di VPS

1. Clone repository proyek Anda ke direktori `/home/ubuntu/gaung` (atau sesuaikan dengan folder home user Anda):
   ```bash
   cd /home/ubuntu
   git clone <URL_REPOSITORY_ANDA> gaung
   cd gaung
   ```

2. Buat file `.env` di root folder:
   ```bash
   cp .env.example .env
   nano .env
   ```
   Sesuaikan isinya dengan konfigurasi produksi. Contoh isi `.env`:
   ```ini
   DATABASE_URL="postgresql://gaung:gaung123@localhost:5432/gaung"
   SESSION_SECRET="gunakan_random_secret_minimal_32_karakter"
   DEEPSEEK_API_KEY=sk-key-anda
   METABASE_EMBED_SECRET=secret-key-metabase-anda
   METABASE_URL=http://localhost:3001
   METABASE_DB_ID=3

   # Python executable path (mengarah ke virtual env yang akan kita buat)
   PYTHON_PATH="/home/ubuntu/gaung/venv/bin/python3"

   # Apache Airflow Configuration
   USE_AIRFLOW=true
   AIRFLOW_API_URL=http://localhost:8080/api/v1
   AIRFLOW_USERNAME=airflow
   AIRFLOW_PASSWORD=airflow

   # WebSocket Configuration
   # Gantilah 'domainanda.com' dengan IP VPS atau Domain asli Anda
   NEXT_PUBLIC_WS_URL=wss://domainanda.com/ws
   GAUNG_WS_URL=http://localhost:3100
   ```
   *(Catatan: Jika Anda belum menggunakan SSL/HTTPS, gunakan `ws://domainanda.com/ws`)*

3. Buat Python Virtual Environment untuk Python Worker (jika dipanggil langsung):
   ```bash
   python3 -m venv venv
   ./venv/bin/pip install --upgrade pip
   ./venv/bin/pip install pandas sqlalchemy cryptography psycopg2-binary requests
   ```

---

## Langkah 4: Setup Next.js Web App & Systemd

1. Install Node dependencies:
   ```bash
   npm install
   ```

2. Jalankan migrasi Prisma untuk mengisi skema database:
   ```bash
   npx prisma migrate deploy
   ```

3. Build aplikasi Next.js untuk production:
   ```bash
   npm run build
   ```

4. Pasang `gaung.service` ke systemd agar aplikasi Next.js otomatis berjalan di background dan hidup kembali jika crash/server reboot:
   ```bash
   # Copy file service ke direktori systemd
   sudo cp gaung.service /etc/systemd/system/gaung.service
   ```

   *Periksa isi file `/etc/systemd/system/gaung.service` untuk memastikan path dan username sesuai:*
   ```ini
   [Unit]
   Description=Gaung - Data Lakehouse Platform
   After=network.target postgresql.service

   [Service]
   Type=simple
   User=ubuntu
   WorkingDirectory=/home/ubuntu/gaung
   Environment=NODE_ENV=production
   Environment=PORT=3005
   ExecStart=/usr/bin/npm run start
   Restart=always
   RestartSec=5

   [Install]
   WantedBy=multi-user.target
   ```

5. Aktifkan dan jalankan service Gaung:
   ```bash
   sudo systemctl daemon-reload
   sudo systemctl enable gaung
   sudo systemctl start gaung
   ```

6. Cek status service untuk memastikan aplikasi berjalan tanpa error:
   ```bash
   sudo systemctl status gaung
   ```

---

## Langkah 5: Jalankan Apache Airflow & WebSocket Bridge (Docker)

Docker Compose akan menjalankan Airflow Webserver, Scheduler, database internal Airflow (PostgreSQL port `5435`), dan WebSocket Bridge server.

1. Jalankan perintah inisialisasi Airflow database (hanya perlu dijalankan sekali):
   ```bash
   docker compose -f docker-compose.airflow.yml up airflow-init
   ```
   *Tunggulah sampai proses selesai dengan status exit 0.*

2. Jalankan semua service Airflow dan WebSocket Bridge di background:
   ```bash
   docker compose -f docker-compose.airflow.yml up -d
   ```

3. Periksa apakah kontainer berjalan lancar:
   ```bash
   docker compose -f docker-compose.airflow.yml ps
   ```

---

## Langkah 6: Konfigurasi Nginx & SSL (HTTPS)

Agar aplikasi dapat diakses publik melalui domain menggunakan port standar (80 untuk HTTP / 443 untuk HTTPS), kita gunakan Nginx sebagai reverse proxy.

1. Install Nginx:
   ```bash
   sudo apt install -y nginx
   ```

2. Buat file konfigurasi Nginx baru untuk Gaung:
   ```bash
   sudo nano /etc/nginx/sites-available/gaung
   ```
   Masukkan konfigurasi berikut (ganti `domainanda.com` dengan domain Anda, atau gunakan IP jika tidak ada domain):
   ```nginx
   server {
       listen 80;
       server_name domainanda.com www.domainanda.com;

       # Frontend & Next.js API
       location / {
           proxy_pass http://127.0.0.1:3005;
           proxy_http_version 1.1;
           proxy_set_header Upgrade $http_upgrade;
           proxy_set_header Connection 'upgrade';
           proxy_set_header Host $host;
           proxy_cache_bypass $http_upgrade;
           proxy_set_header X-Real-IP $remote_addr;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
           proxy_set_header X-Forwarded-Proto $scheme;
       }

       # WebSocket Bridge Proxy
       location /ws {
           proxy_pass http://127.0.0.1:3008;
           proxy_http_version 1.1;
           proxy_set_header Upgrade $http_upgrade;
           proxy_set_header Connection "Upgrade";
           proxy_set_header Host $host;
           proxy_set_header X-Real-IP $remote_addr;
           proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
           proxy_read_timeout 86400s;
           proxy_send_timeout 86400s;
       }
   }
   ```

3. Aktifkan konfigurasi Nginx dan restart Nginx:
   ```bash
   sudo ln -s /etc/nginx/sites-available/gaung /etc/nginx/sites-enabled/
   # Hapus default config Nginx jika ada untuk menghindari konflik
   sudo rm /etc/nginx/sites-enabled/default
   
   # Tes konfigurasi Nginx
   sudo nginx -t
   # Restart Nginx
   sudo systemctl restart nginx
   ```

4. *(Opsional tetapi sangat direkomendasikan)* Memasang SSL gratis menggunakan Certbot (Let's Encrypt):
   ```bash
   sudo apt install -y certbot python3-certbot-nginx
   sudo certbot --nginx -d domainanda.com -d www.domainanda.com
   ```
   Ikuti petunjuk di layar. Certbot akan otomatis mengubah file konfigurasi Nginx Anda untuk mendukung HTTPS dan mengarahkan HTTP ke HTTPS secara otomatis.

---

## Troubleshooting & Pemeliharaan

### Melihat Log Next.js Web App
```bash
sudo journalctl -u gaung.service -f
```

### Melihat Log Docker Containers (Airflow / WS Bridge)
```bash
# Menampilkan log untuk semua container
docker compose -f docker-compose.airflow.yml logs -f

# Menampilkan log ws-bridge saja
docker compose -f docker-compose.airflow.yml logs -f ws-bridge
```

### Merestart Aplikasi
```bash
# Restart Next.js Web App
sudo systemctl restart gaung

# Restart Airflow & WS-Bridge
docker compose -f docker-compose.airflow.yml restart
```
