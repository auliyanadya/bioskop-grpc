import time
import uuid
import grpc

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc

SERVER_ADDRESS = "localhost:50051"

# ---------------------------------------------------------
# FUNGSI HELPER: RETRY & TIMEOUT (AT-MOST-ONCE)
# ---------------------------------------------------------
def panggil_dengan_retry(metode_rpc, request_obj, maks_percobaan=3):
    """
    Fungsi perantara untuk memanggil RPC dengan mekanisme timeout dan retry.
    Menggunakan ID unik (UUID) agar server tidak menduplikasi transaksi.
    """
    for percobaan in range(1, maks_percobaan + 1):
        try:
            # Memanggil RPC dengan timeout 2.0 detik
            return metode_rpc(request_obj, timeout=2.0)
        except grpc.RpcError as e:
            bisa_ulang = e.code() in (
                grpc.StatusCode.DEADLINE_EXCEEDED,
                grpc.StatusCode.UNAVAILABLE
            )
            if not bisa_ulang or percobaan == maks_percobaan:
                print(f"\n[ERROR] Gagal terhubung ke server ({e.code().name}): {e.details()}")
                return None
            
            jeda = 0.5 * (2 ** (percobaan - 1)) # Exponential backoff
            print(f"\n[RETRY] Koneksi terganggu ({e.code().name}). Percobaan {percobaan} gagal, mencoba lagi dalam {jeda}s...")
            time.sleep(jeda)


# ---------------------------------------------------------
# MENU INTERAKTIF CLIENT
# ---------------------------------------------------------
def tampilkan_menu():
    print("\n==================================================")
    print("      SISTEM RESERVASI TIKET BIOSKOP (gRPC)      ")
    print("==================================================")
    print("1. Lihat Status Kursi (Streaming Real-Time)")
    print("2. Pesan Kursi")
    print("3. Batalkan Pesanan Kursi")
    print("4. Cek Detail Tiket (Kode Booking)")
    print("0. Keluar")
    print("--------------------------------------------------")


def main():
    with grpc.insecure_channel(SERVER_ADDRESS) as channel:
        stub = pb_grpc.LayananBioskopStub(channel)
        
        while True:
            tampilkan_menu()
            pilihan = input("Pilih menu (0-4): ").strip()
            
            if pilihan == "1":
                print("\n--- STATUS KURSI (SERVER STREAMING) ---")
                print("Mengambil status kursi real-time dari server (Tekan Ctrl+C untuk kembali ke menu)...\n")
                try:
                    # Memanggil Server Streaming RPC
                    stream_response = stub.GetDaftarKursi(pb.KosongRequest())
                    jumlah_tampil = 0
                    for kursi in stream_response:
                        status_str = f"TERISI ({kursi.pemesan})" if kursi.terisi else "KOSONG"
                        print(f"  [Kursi {kursi.nomor_kursi}] Status: {status_str}")
                        jumlah_tampil += 1
                        # Pembatas setiap batch 10 kursi
                        if jumlah_tampil % 10 == 0:
                            print("-" * 35)
                except KeyboardInterrupt:
                    print("\n[INFO] Berhenti melihat stream kursi.")
                except grpc.RpcError as e:
                    print(f"\n[ERROR] Gagal streaming status kursi: {e.details()}")

            elif pilihan == "2":
                print("\n--- PESAN KURSI ---")
                nama = input("Masukkan Nama Pemesan  : ").strip()
                kursi = input("Masukkan Nomor Kursi (contoh A1): ").strip().upper()
                
                if not nama or not kursi:
                    print("[WARN] Nama dan nomor kursi tidak boleh kosong!")
                    continue
                
                # Buat UUID unik untuk setiap aksi transaksi
                rid = str(uuid.uuid4())
                req = pb.PesanRequest(request_id=rid, nama_pemesan=nama, nomor_kursi=kursi)
                
                # Panggil RPC dengan proteksi timeout & retry
                res = panggil_dengan_retry(stub.PesanKursi, req)
                if res:
                    print(f"\n>> {res.pesan}")
                    if res.sukses:
                        print(f">> SIMPAN KODE BOOKING ANDA: {res.kode_booking}")

            elif pilihan == "3":
                print("\n--- BATALKAN PESANAN KURSI ---")
                nama = input("Masukkan Nama Pemesan  : ").strip()
                kursi = input("Masukkan Nomor Kursi (contoh A1): ").strip().upper()
                
                rid = str(uuid.uuid4())
                req = pb.BatalRequest(request_id=rid, nama_pemesan=nama, nomor_kursi=kursi)
                
                res = panggil_dengan_retry(stub.BatalPesan, req)
                if res:
                    print(f"\n>> {res.pesan}")

            elif pilihan == "4":
                print("\n--- CEK DETAIL TIKET ---")
                kode = input("Masukkan Kode Booking (contoh TKT-XXXXXX): ").strip().upper()
                
                req = pb.CekTiketRequest(kode_booking=kode)
                res = panggil_dengan_retry(stub.CekTiket, req)
                if res:
                    if res.ditemukan:
                        print(f"\n>> Tiket Ditemukan!")
                        print(f"   Nama Pemesan : {res.nama_pemesan}")
                        print(f"   Nomor Kursi  : {res.nomor_kursi}")
                        print(f"   Status Tiket : {res.status}")
                    else:
                        print("\n>> Kode booking tidak ditemukan!")

            elif pilihan == "0":
                print("\nTerima kasih telah menggunakan sistem reservasi bioskop!")
                break
            else:
                print("\nPilihan tidak valid, silakan coba lagi.")


if __name__ == "__main__":
    main()