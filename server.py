import time
import uuid
import threading
from concurrent import futures
import grpc

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc

# ---------------------------------------------------------
# DATABASE SIMULASI DI MEMORI SERVER
# ---------------------------------------------------------
# KODE BARU (40 Kursi: A1-D10):
DATA_KURSI = {
    f"{baris}{nomor}": {
        "terisi": False,
        "pemesan": "",
        "kode_booking": ""
    }
    for baris in ["A", "B", "C", "D"]
    for nomor in range(1, 11)
}

# Riwayat transaksi yang diproses (request_id -> Status Response)
# Berfungsi untuk deduplikasi (At-Most-Once)
PROSES_REQUEST = {}

# Daftar tiket terdaftar (kode_booking -> rincian tiket)
DATA_TIKET = {}

# Lock untuk keamanan akses data dari banyak thread/client (Concurrency Control)
LOCK = threading.Lock()


# ---------------------------------------------------------
# IMPLEMENTASI SERVICE RPC
# ---------------------------------------------------------
class LayananBioskopServicer(pb_grpc.LayananBioskopServicer):

    # 1. Server Streaming: Mengirim status kursi secara berjangka
    def GetDaftarKursi(self, request, context):
        print("[SERVER] Client terhubung untuk streaming status kursi...")
        while context.is_active():
            with LOCK:
                for no_kursi, info in DATA_KURSI.items():
                    yield pb.KursiStatus(
                        nomor_kursi=no_kursi,
                        terisi=info["terisi"],
                        pemesan=info["pemesan"]
                    )
            time.sleep(3) # Update status setiap 3 detik

    # 2. Unary RPC: Memesan kursi dengan Lock dan Deduplikasi (At-Most-Once)
    def PesanKursi(self, request, context):
        with LOCK:
            # Check Deduplikasi: Jika request_id sudah pernah diproses, kembalikan hasil lama
            if request.request_id in PROSES_REQUEST:
                print(f"[DEDUPLIKASI] Request ID {request.request_id} sudah diproses sebelumnya.")
                return PROSES_REQUEST[request.request_id]

            # Cek validitas nomor kursi
            if request.nomor_kursi not in DATA_KURSI:
                response = pb.PesanResponse(
                    sukses=False,
                    pesan=f"Nomor kursi {request.nomor_kursi} tidak valid!",
                    kode_booking=""
                )
                PROSES_REQUEST[request.request_id] = response
                return response

            # Cek ketersediaan kursi
            kursi = DATA_KURSI[request.nomor_kursi]
            if kursi["terisi"]:
                response = pb.PesanResponse(
                    sukses=False,
                    pesan=f"Gagal! Kursi {request.nomor_kursi} sudah dipesan oleh {kursi['pemesan']}.",
                    kode_booking=""
                )
            else:
                # Proses pemesanan
                kursi["terisi"] = True
                kursi["pemesan"] = request.nama_pemesan
                kode_booking = f"TKT-{uuid.uuid4().hex[:6].upper()}"

                # Simpan kode booking pada data kursi
                kursi["kode_booking"] = kode_booking
                
                DATA_TIKET[kode_booking] = {
                    "nama": request.nama_pemesan,
                    "kursi": request.nomor_kursi,
                    "status": "AKTIF"
                }

                response = pb.PesanResponse(
                    sukses=True,
                    pesan=f"Berhasil memesan kursi {request.nomor_kursi} atas nama {request.nama_pemesan}.",
                    kode_booking=kode_booking
                )
                print(f"[SUCCESS] Kursi {request.nomor_kursi} dipesan oleh {request.nama_pemesan} (Booking: {kode_booking})")

            # Simpan respons ke riwayat deduplikasi
            PROSES_REQUEST[request.request_id] = response
            return response

    # 3. Unary RPC: Pembatalan pesanan
    def BatalPesan(self, request, context):

        with LOCK:

            # Cek apakah request sudah pernah diproses
            if request.request_id in PROSES_REQUEST:
                return PROSES_REQUEST[request.request_id]

            # Validasi nomor kursi
            if request.nomor_kursi not in DATA_KURSI:

                response = pb.PesanResponse(
                    sukses=False,
                    pesan="Kursi tidak ditemukan.",
                    kode_booking=""
                )

                PROSES_REQUEST[request.request_id] = response
                return response

            kursi = DATA_KURSI[request.nomor_kursi]

            # Validasi kepemilikan kursi
            if not kursi["terisi"] or kursi["pemesan"] != request.nama_pemesan:

                response = pb.PesanResponse(
                    sukses=False,
                    pesan=(
                        f"Gagal membatalkan! Kursi "
                        f"{request.nomor_kursi} bukan milik "
                        f"{request.nama_pemesan}."
                    ),
                    kode_booking=""
                )

            else:

                # Ambil kode booking dari kursi
                kode_booking = kursi.get("kode_booking", "")

                # Pastikan data tiket benar-benar tersedia
                if not kode_booking or kode_booking not in DATA_TIKET:

                    response = pb.PesanResponse(
                        sukses=False,
                        pesan="Data tiket tidak ditemukan. Pembatalan dibatalkan.",
                        kode_booking=""
                    )

                    PROSES_REQUEST[request.request_id] = response
                    return response

                # Ubah status tiket lama
                DATA_TIKET[kode_booking]["status"] = "DIBATALKAN"

                # Kosongkan status kursi
                kursi["terisi"] = False
                kursi["pemesan"] = ""
                kursi["kode_booking"] = ""

                response = pb.PesanResponse(
                    sukses=True,
                    pesan=(
                        f"Pemesanan kursi {request.nomor_kursi} "
                        f"berhasil dibatalkan. "
                        f"Kode booking: {kode_booking}"
                    ),
                    kode_booking=kode_booking
                )

                print(
                    f"[BATAL] Kursi {request.nomor_kursi} "
                    f"dibatalkan oleh {request.nama_pemesan} "
                    f"(Booking: {kode_booking})"
                )

            # Simpan hasil untuk mekanisme deduplikasi
            PROSES_REQUEST[request.request_id] = response

            return response


    # 4. Unary RPC: Cek Tiket
    def CekTiket(self, request, context):
        with LOCK:
            tiket = DATA_TIKET.get(request.kode_booking)
            if not tiket:
                return pb.TiketDetail(ditemukan=False, nama_pemesan="", nomor_kursi="", status="TIDAK DITEMUKAN")
            
            return pb.TiketDetail(
                ditemukan=True,
                nama_pemesan=tiket["nama"],
                nomor_kursi=tiket["kursi"],
                status=tiket["status"]
            )

# ---------------------------------------------------------
# MENJALANKAN SERVER
# ---------------------------------------------------------
def serve():
    # Thread pool untuk menangani banyak client bersamaan
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    pb_grpc.add_LayananBioskopServicer_to_server(LayananBioskopServicer(), server)
    server.add_insecure_port("[::]:50051")
    server.start()
    print("==================================================")
    print("Server Bioskop gRPC berjalan di port 50051...")
    print("==================================================")
    server.wait_for_termination()

if __name__ == "__main__":
    serve()