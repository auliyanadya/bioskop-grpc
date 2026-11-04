# CineBook RPC — Sistem Reservasi Tiket Bioskop

Proyek ini merupakan implementasi sistem reservasi tiket bioskop sederhana menggunakan **Python 3** dan **gRPC**. Sistem menggunakan arsitektur client–server: client mengirim permintaan reservasi kepada server, sedangkan server mengelola status kursi dan data tiket.

Proyek ini dibuat untuk memenuhi tugas proyek mini mata kuliah **Komputasi Paralel dan Terdistribusi**, dengan studi kasus reservasi tiket bioskop oleh beberapa client yang dapat mencoba memesan kursi yang sama secara bersamaan.

## Fitur Utama

- Menampilkan status 40 kursi bioskop (A1–D10) melalui server streaming.
- Memesan kursi dan menghasilkan kode booking.
- Membatalkan pesanan kursi.
- Memeriksa detail tiket menggunakan kode booking.
- Menangani beberapa permintaan client secara bersamaan menggunakan `threading.Lock`.
- Menerapkan deduplikasi request berdasarkan `request_id` untuk menghindari pemrosesan ulang permintaan yang sama selama server masih berjalan.
- Menerapkan timeout dan retry pada pemanggilan RPC dari client.
- Menguji concurrency dengan 20 client.
- Mengukur latensi rata-rata dan throughput untuk 1, 5, dan 20 client.

## Teknologi yang Digunakan

- Python 3.10 atau lebih baru
- gRPC (`grpcio`)
- Protocol Buffers (`grpcio-tools`)
- Matplotlib untuk membuat grafik benchmark
- Git dan GitHub untuk pengelolaan kode sumber

## Arsitektur Singkat

1. Client membuat request dan mengirimkannya melalui gRPC.
2. Server menerima request melalui service `LayananBioskop`.
3. Server memproses data kursi dan tiket.
4. Server mengirimkan response kembali kepada client.
5. Untuk daftar status kursi, server mengirimkan data secara streaming secara berkala.

Server dijalankan pada port TCP **50051**. Untuk demonstrasi dua komputer, server dan client harus berada pada jaringan yang saling dapat berkomunikasi.

## RPC yang Disediakan

| RPC | Jenis | Fungsi |
|---|---|---|
| `GetDaftarKursi` | Server streaming | Mengirim status kursi kepada client secara berkala. |
| `PesanKursi` | Unary | Memproses permintaan pemesanan satu kursi. |
| `BatalPesan` | Unary | Membatalkan pesanan kursi. |
| `CekTiket` | Unary | Memeriksa detail dan status tiket berdasarkan kode booking. |

Definisi layanan dan message berada di `bioskop.proto`.

## Struktur Folder

```text
bioskop-grpc/
├── bioskop.proto
├── bioskop_pb2.py
├── bioskop_pb2_grpc.py
├── server.py
├── client.py
├── benchmark.py
├── requirements.txt
├── testing/
│   └── test_concurrency.py
├── hasil_pengujian/
│   ├── hasil_benchmark.csv
│   └── grafik_benchmark.png
└── README.md
```

Folder `hasil_pengujian/` berisi keluaran pengujian apabila hasil CSV dan grafik telah disimpan ke folder tersebut. Sesuaikan struktur ini jika file hasil masih berada di folder utama proyek.

## Persiapan Lingkungan

### 1. Clone repository

```powershell
git clone https://github.com/auliyanadya/bioskop-grpc.git
cd bioskop-grpc
```

Jika repository sudah pernah di-clone, masuk ke folder proyek dan ambil perubahan terbaru dari branch yang digunakan kelompok:

```powershell
git switch main
git pull origin main
```

### 2. Buat dan aktifkan virtual environment (Windows PowerShell)

Buat virtual environment satu kali pada setiap komputer:

```powershell
python -m venv .venv
```

Aktifkan:

```powershell
.\.venv\Scripts\Activate.ps1
```

Jika PowerShell memblokir aktivasi script, jalankan pada sesi PowerShell tersebut:

```powershell
Set-ExecutionPolicy -Scope Process -ExecutionPolicy RemoteSigned
```

Kemudian aktifkan kembali virtual environment.

### 3. Instal dependensi

```powershell
python -m pip install -r requirements.txt
```

Isi `requirements.txt`:

```text
grpcio
grpcio-tools
matplotlib
```

### 4. Generate kode gRPC dari Protocol Buffers

Jalankan dari folder utama proyek, yaitu folder yang berisi `bioskop.proto`:

```powershell
python -m grpc_tools.protoc -I. --python_out=. --grpc_python_out=. bioskop.proto
```

Perintah tersebut menghasilkan atau memperbarui:

- `bioskop_pb2.py`
- `bioskop_pb2_grpc.py`

Generate ulang file tersebut setiap kali definisi pada `bioskop.proto` diubah.

## Cara Menjalankan pada Satu Komputer

Buka dua terminal di folder proyek. Pastikan virtual environment aktif pada kedua terminal.

### Terminal 1 — Server

```powershell
python server.py
```

Server akan berjalan pada port `50051`.

### Terminal 2 — Client

Untuk koneksi lokal, gunakan alamat default:

```powershell
python client.py
```

Client akan menampilkan menu reservasi tiket bioskop.

## Cara Menjalankan pada Dua Komputer

Contoh pembagian:

- **Komputer 1:** menjalankan server.
- **Komputer 2:** menjalankan client.
- Kedua komputer terhubung ke jaringan Wi-Fi/hotspot yang sama dan jaringan tersebut mengizinkan komunikasi antarkomputer.

### 1. Jalankan server pada Komputer 1

Cari alamat IPv4 Komputer 1:

```powershell
ipconfig
```

Catat IPv4 pada adapter jaringan yang sedang digunakan. Pastikan `server.py` menggunakan binding berikut:

```python
server.add_insecure_port("[::]:50051")
```

Jalankan server:

```powershell
python server.py
```

### 2. Atur firewall Komputer 1 jika diperlukan

Pada PowerShell yang dijalankan sebagai Administrator, izinkan TCP port 50051 dari alamat IPv4 Komputer 2. Ganti `<IP_CLIENT>` dengan alamat IPv4 client yang sebenarnya:

```powershell
New-NetFirewallRule -DisplayName "CineBook gRPC 50051" -Direction Inbound -Protocol TCP -LocalPort 50051 -RemoteAddress <IP_CLIENT> -Action Allow -Profile Public
```

Sesuaikan `-Profile Public` dengan profil jaringan aktif. Aturan firewall harus dibatasi pada jaringan dan alamat client yang diperlukan; jangan menonaktifkan firewall seluruhnya.

### 3. Uji port dari Komputer 2

Ganti `<IP_SERVER>` dengan IPv4 Komputer 1:

```powershell
Test-NetConnection <IP_SERVER> -Port 50051
```

Lanjutkan jika hasilnya menunjukkan:

```text
TcpTestSucceeded : True
```

Jika hasilnya `False`, periksa kembali IP server, status server, firewall, jaringan yang digunakan, dan kemungkinan fitur client/AP isolation pada router atau hotspot.

### 4. Atur alamat server pada Komputer 2

Di PowerShell Komputer 2, ganti `<IP_SERVER>` dengan alamat IPv4 server:

```powershell
$env:BIOSKOP_SERVER="<IP_SERVER>:50051"
```

Variabel tersebut berlaku pada sesi PowerShell yang sedang digunakan. Jalankan client dari terminal yang sama:

```powershell
python client.py
```

File `client.py` membaca alamat dari `BIOSKOP_SERVER`, dengan nilai default `localhost:50051` jika variabel tersebut tidak diatur.

## Pengujian Concurrency

Pengujian ini menjalankan 20 client secara bersamaan yang mencoba memesan kursi A1.

1. Pastikan server berjalan.
2. Pastikan kursi A1 tersedia. Karena data server disimpan di memori, restart server akan mengembalikan data simulasi ke kondisi awal.
3. Pada komputer penguji, atur alamat server jika server berada di komputer lain:

```powershell
$env:BIOSKOP_SERVER="<IP_SERVER>:50051"
```

4. Jalankan:

```powershell
python testing/test_concurrency.py
```

Hasil yang diharapkan adalah satu client berhasil memesan kursi A1 dan 19 client lainnya ditolak karena kursi sudah dipesan. Hasil aktual harus dicatat dari eksekusi program.

## Pengujian Kinerja (Benchmark)

Benchmark menguji RPC `CekTiket` dengan jumlah client bersamaan sebanyak 1, 5, dan 20. Setiap client melakukan 10 request. Program menghitung latensi rata-rata dan throughput berdasarkan response RPC yang diterima.

Jika server berada di komputer lain, pastikan `benchmark.py` menggunakan konfigurasi alamat server melalui environment variable berikut:

```python
import os

SERVER_ADDRESS = os.getenv(
    "BIOSKOP_SERVER",
    "localhost:50051"
)
```

Atur IP server di PowerShell:

```powershell
$env:BIOSKOP_SERVER="<IP_SERVER>:50051"
```

Jalankan benchmark:

```powershell
python benchmark.py
```

Keluaran yang dihasilkan:

- `hasil_benchmark.csv` — data pengujian dalam format CSV.
- `grafik_benchmark.png` — grafik latensi rata-rata dan throughput.

Gunakan angka hasil eksekusi aktual untuk tabel dan analisis laporan. Jangan mengisi hasil pengujian dengan angka perkiraan.

## Mekanisme Concurrency dan Toleransi Kegagalan

- `threading.Lock` digunakan untuk melindungi akses dan perubahan data kursi/tiket agar proses penting tidak berjalan bersamaan tanpa pengamanan.
- `request_id` digunakan sebagai kunci deduplikasi pada server. Client menggunakan UUID untuk identitas request pemesanan dan pembatalan.
- Client menggunakan timeout 2 detik dan retry maksimal 3 kali untuk status `DEADLINE_EXCEEDED` atau `UNAVAILABLE`, dengan exponential backoff.
- Server menggunakan `ThreadPoolExecutor(max_workers=10)` untuk menangani pemanggilan RPC.

## Keterbatasan Implementasi

- Data kursi, tiket, dan riwayat deduplikasi disimpan di memori (`dictionary`), bukan database permanen. Data akan hilang ketika proses server dihentikan atau direstart.
- Karena riwayat deduplikasi disimpan di memori, jaminan pencegahan pemrosesan request duplikat tidak bertahan setelah server restart.
- Koneksi menggunakan `grpc.insecure_channel` tanpa TLS dan ditujukan untuk demonstrasi pada jaringan lokal yang dipercaya. Jangan mengekspos layanan ini ke jaringan publik tanpa pengamanan tambahan.

## Catatan Git untuk Kerja Kelompok

Setiap anggota sebaiknya melakukan commit menggunakan akun Git masing-masing agar kontribusi dapat ditelusuri. Perubahan dikembangkan pada branch masing-masing, kemudian digabungkan ke `main` melalui Pull Request setelah diperiksa dan diuji bersama.

## Referensi Penggunaan

Proyek ini menggunakan Python, gRPC, dan Protocol Buffers. Instruksi instalasi serta menjalankan program di atas ditujukan untuk lingkungan Windows PowerShell.
