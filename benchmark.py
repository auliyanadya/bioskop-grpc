
import csv
import time
import uuid
import threading
import grpc

from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc


# ==================================================
# KONFIGURASI BENCHMARK
# ==================================================

SERVER_ADDRESS = "localhost:50051"

CLIENT_SCENARIOS = [1, 5, 20]
REQUEST_PER_CLIENT = 10

CSV_FILE = "hasil_benchmark.csv"
GRAPH_FILE = "grafik_benchmark.png"


# ==================================================
# MENYIAPKAN DATA TIKET UNTUK PENGUJIAN
# ==================================================

def buat_tiket_benchmark(stub):

    # Cari kursi kosong untuk dijadikan data pengujian.
    daftar_kursi = [
        f"{baris}{nomor}"
        for baris in ["A", "B", "C", "D"]
        for nomor in range(1, 11)
    ]

    for nomor_kursi in daftar_kursi:

        request = pb.PesanRequest(
            request_id=str(uuid.uuid4()),
            nama_pemesan="BENCHMARK",
            nomor_kursi=nomor_kursi
        )

        response = stub.PesanKursi(
            request,
            timeout=5.0
        )

        if response.sukses and response.kode_booking:

            print("\nTiket pengujian berhasil dibuat.")
            print(f"Kursi       : {nomor_kursi}")
            print(f"Kode Booking: {response.kode_booking}")

            return response.kode_booking

    raise RuntimeError(
        "Tidak ada kursi tersedia untuk membuat tiket benchmark. "
        "Periksa kondisi data server."
    )


# ==================================================
# SIMULASI CLIENT
# ==================================================

def simulasi_client(client_id, stub, request, barrier):

    daftar_latensi = []
    jumlah_respons = 0
    jumlah_tiket_ditemukan = 0
    jumlah_error = 0

    # Menunggu seluruh client siap sebelum pengujian dimulai.
    barrier.wait(timeout=20)

    for nomor in range(REQUEST_PER_CLIENT):

        waktu_mulai = time.perf_counter()

        try:

            response = stub.CekTiket(
                request,
                timeout=5.0
            )

            waktu_selesai = time.perf_counter()

            latensi = (
                waktu_selesai - waktu_mulai
            ) * 1000

            daftar_latensi.append(latensi)
            jumlah_respons += 1

            if response.ditemukan:
                jumlah_tiket_ditemukan += 1

        except grpc.RpcError as error:

            jumlah_error += 1

            print(
                f"[ERROR] Client {client_id}: "
                f"{error.code().name}"
            )

    return (
        daftar_latensi,
        jumlah_respons,
        jumlah_tiket_ditemukan,
        jumlah_error
    )


# ==================================================
# MENJALANKAN SATU SKENARIO BENCHMARK
# ==================================================

def jalankan_benchmark(stub, kode_booking, jumlah_client):

    print("\n" + "=" * 55)
    print(f" PENGUJIAN DENGAN {jumlah_client} CLIENT BERSAMAAN")
    print("=" * 55)

    request = pb.CekTiketRequest(
        kode_booking=kode_booking
    )

    barrier = threading.Barrier(jumlah_client + 1)

    semua_latensi = []
    total_respons = 0
    total_ditemukan = 0
    total_error = 0

    futures = []

    with ThreadPoolExecutor(
        max_workers=jumlah_client
    ) as executor:

        for nomor in range(1, jumlah_client + 1):

            future = executor.submit(
                simulasi_client,
                nomor,
                stub,
                request,
                barrier
            )

            futures.append(future)

        # Waktu dimulai ketika seluruh worker sudah siap.
        waktu_mulai = time.perf_counter()

        barrier.wait(timeout=20)

        for future in as_completed(futures):

            latensi, respons, ditemukan, error = future.result()

            semua_latensi.extend(latensi)

            total_respons += respons
            total_ditemukan += ditemukan
            total_error += error

    waktu_selesai = time.perf_counter()

    total_durasi = waktu_selesai - waktu_mulai

    total_request = jumlah_client * REQUEST_PER_CLIENT

    # Rata-rata latensi dari respons RPC yang diterima.
    rata_latensi = (
        sum(semua_latensi) / len(semua_latensi)
        if semua_latensi else 0
    )

    # Throughput berdasarkan jumlah respons yang diterima.
    throughput = (
        total_respons / total_durasi
        if total_durasi > 0 else 0
    )

    print(f"Total Client          : {jumlah_client}")
    print(f"Total Request         : {total_request}")
    print(f"Respons Diterima      : {total_respons}")
    print(f"Tiket Ditemukan       : {total_ditemukan}")
    print(f"RPC Error             : {total_error}")
    print(f"Waktu Total           : {total_durasi:.4f} detik")
    print(f"Latensi Rata-rata     : {rata_latensi:.2f} ms")
    print(f"Throughput            : {throughput:.2f} req/detik")

    return {
        "jumlah_client": jumlah_client,
        "total_request": total_request,
        "respons_diterima": total_respons,
        "tiket_ditemukan": total_ditemukan,
        "rpc_error": total_error,
        "waktu_total": total_durasi,
        "latensi_rata_rata": rata_latensi,
        "throughput": throughput
    }


# ==================================================
# MENYIMPAN HASIL KE CSV
# ==================================================

def simpan_csv(hasil):

    kolom = [
        "jumlah_client",
        "total_request",
        "respons_diterima",
        "tiket_ditemukan",
        "rpc_error",
        "waktu_total",
        "latensi_rata_rata",
        "throughput"
    ]

    with open(
        CSV_FILE,
        "w",
        newline="",
        encoding="utf-8"
    ) as file:

        writer = csv.DictWriter(
            file,
            fieldnames=kolom
        )

        writer.writeheader()
        writer.writerows(hasil)

    print(f"\nHasil CSV tersimpan: {CSV_FILE}")


# ==================================================
# MEMBUAT GRAFIK BENCHMARK
# ==================================================

def buat_grafik(hasil):

    try:
        import matplotlib.pyplot as plt

    except ImportError:

        print(
            "Matplotlib belum tersedia. "
            "Grafik tidak dapat dibuat."
        )

        return

    client = [
        data["jumlah_client"]
        for data in hasil
    ]

    latensi = [
        data["latensi_rata_rata"]
        for data in hasil
    ]

    throughput = [
        data["throughput"]
        for data in hasil
    ]

    fig, axes = plt.subplots(
        1, 2,
        figsize=(12, 5)
    )

    # Grafik latensi
    axes[0].plot(
        client,
        latensi,
        marker="o"
    )

    axes[0].set_title("Average Latency")
    axes[0].set_xlabel("Jumlah Client")
    axes[0].set_ylabel("Latensi (ms)")
    axes[0].set_xticks(client)
    axes[0].grid(True)

    # Grafik throughput
    axes[1].plot(
        client,
        throughput,
        marker="o"
    )

    axes[1].set_title("Throughput")
    axes[1].set_xlabel("Jumlah Client")
    axes[1].set_ylabel("Request per Second")
    axes[1].set_xticks(client)
    axes[1].grid(True)

    fig.suptitle(
        "Performance Benchmark Sistem Bioskop gRPC"
    )

    fig.tight_layout()

    plt.savefig(
        GRAPH_FILE,
        dpi=300,
        bbox_inches="tight"
    )

    plt.close()

    print(f"Grafik tersimpan: {GRAPH_FILE}")


# ==================================================
# PROGRAM UTAMA
# ==================================================

def main():

    print("=" * 55)
    print(" BENCHMARK SISTEM RESERVASI BIOSKOP gRPC")
    print("=" * 55)

    try:

        with grpc.insecure_channel(
            SERVER_ADDRESS
        ) as channel:

            print("\nMenghubungkan ke server...")

            grpc.channel_ready_future(
                channel
            ).result(timeout=10)

            print("Server berhasil terhubung.")

            stub = pb_grpc.LayananBioskopStub(channel)

            # Membuat satu tiket untuk seluruh skenario.
            kode_booking = buat_tiket_benchmark(stub)

            hasil = []

            for jumlah_client in CLIENT_SCENARIOS:

                data = jalankan_benchmark(
                    stub,
                    kode_booking,
                    jumlah_client
                )

                hasil.append(data)

                time.sleep(1)

            # Menyimpan dan membuat visualisasi.
            simpan_csv(hasil)
            buat_grafik(hasil)

            # Ringkasan akhir.
            print("\n" + "=" * 65)
            print(" RINGKASAN HASIL BENCHMARK")
            print("=" * 65)

            print(
                f"{'Client':<12}"
                f"{'Latensi (ms)':<20}"
                f"{'Throughput (req/s)':<20}"
            )

            print("-" * 65)

            for data in hasil:

                print(
                    f"{data['jumlah_client']:<12}"
                    f"{data['latensi_rata_rata']:<20.2f}"
                    f"{data['throughput']:<20.2f}"
                )

            print("=" * 65)
            print("Pengujian benchmark selesai.")

    except grpc.FutureTimeoutError:

        print(
            "[ERROR] Server tidak berhasil dihubungi "
            "dalam waktu 10 detik."
        )

    except grpc.RpcError as error:

        print(
            f"[RPC ERROR] {error.code().name}: "
            f"{error.details()}"
        )

    except (RuntimeError, threading.BrokenBarrierError) as error:

        print(f"[ERROR] {error}")


if __name__ == "__main__":
    main()
