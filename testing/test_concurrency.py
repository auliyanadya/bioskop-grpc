
import uuid
import grpc
import os
import threading
import sys

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

# Mengambil lokasi folder utama proyek
PROJECT_ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(PROJECT_ROOT))

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc


# ==========================================
# KONFIGURASI PENGUJIAN
# ==========================================

SERVER_ADDRESS = os.getenv(
    "BIOSKOP_SERVER",
    "localhost:50051"
)

JUMLAH_CLIENT = 20
KURSI_TARGET = "A1"


# ==========================================
# FUNGSI SIMULASI CLIENT
# ==========================================

def kirim_pemesanan(stub, barrier, nomor_client):

    nama = f"Penguji-{nomor_client:02d}"

    # Setiap client menggunakan request ID berbeda
    request = pb.PesanRequest(
        request_id=str(uuid.uuid4()),
        nama_pemesan=nama,
        nomor_kursi=KURSI_TARGET
    )

    try:
        # Menunggu seluruh client siap mengirim permintaan
        barrier.wait(timeout=10)

        response = stub.PesanKursi(
            request,
            timeout=5.0
        )

        return {
            "client": nama,
            "sukses": response.sukses,
            "pesan": response.pesan,
            "error": False
        }

    except grpc.RpcError as error:

        return {
            "client": nama,
            "sukses": False,
            "pesan": f"RPC Error: {error.code().name}",
            "error": True
        }

    except threading.BrokenBarrierError:

        return {
            "client": nama,
            "sukses": False,
            "pesan": "Barrier gagal",
            "error": True
        }


# ==========================================
# MENJALANKAN PENGUJIAN CONCURRENCY
# ==========================================

def main():

    print("=" * 55)
    print(" PENGUJIAN CONCURRENCY SISTEM RESERVASI BIOSKOP")
    print("=" * 55)

    print(f"Jumlah Client : {JUMLAH_CLIENT}")
    print(f"Kursi Target  : {KURSI_TARGET}")
    print("Metode        : ThreadPoolExecutor")
    print()

    barrier = threading.Barrier(JUMLAH_CLIENT)

    try:

        with grpc.insecure_channel(SERVER_ADDRESS) as channel:

            # Memastikan server sudah siap menerima permintaan
            grpc.channel_ready_future(channel).result(timeout=10)

            stub = pb_grpc.LayananBioskopStub(channel)

            # Menjalankan 20 client secara bersamaan
            with ThreadPoolExecutor(
                max_workers=JUMLAH_CLIENT
            ) as executor:

                futures = [
                    executor.submit(
                        kirim_pemesanan,
                        stub,
                        barrier,
                        nomor
                    )
                    for nomor in range(1, JUMLAH_CLIENT + 1)
                ]

                hasil = [
                    future.result()
                    for future in as_completed(futures)
                ]

    except grpc.FutureTimeoutError:

        print("[ERROR] Server tidak merespons dalam 10 detik.")
        return

    # ======================================
    # ANALISIS HASIL
    # ======================================

    hasil.sort(key=lambda x: x["client"])

    berhasil = sum(
        1 for data in hasil if data["sukses"]
    )

    gagal = sum(
        1 for data in hasil
        if not data["sukses"] and not data["error"]
    )

    error = sum(
        1 for data in hasil if data["error"]
    )

    print("HASIL MASING-MASING CLIENT")
    print("-" * 55)

    for data in hasil:

        if data["error"]:
            status = "ERROR"

        elif data["sukses"]:
            status = "BERHASIL"

        else:
            status = "DITOLAK"

        print(
            f'{data["client"]:<15} | {status:<10} | '
            f'{data["pesan"]}'
        )

    print("\nRINGKASAN PENGUJIAN")
    print("-" * 55)

    print(f"Total Client       : {JUMLAH_CLIENT}")
    print(f"Pemesanan Berhasil : {berhasil}")
    print(f"Pemesanan Ditolak  : {gagal}")
    print(f"RPC Error          : {error}")

    print("-" * 55)

    # Hasil yang diharapkan:
    if berhasil == 1 and gagal == JUMLAH_CLIENT - 1 and error == 0:

        print("[LULUS] Concurrency berjalan dengan baik.")
        print("Hanya satu client berhasil mendapatkan kursi.")

    else:

        print("[PERLU DIPERIKSA] Hasil belum sesuai harapan.")

    print("=" * 55)


if __name__ == "__main__":
    main()
