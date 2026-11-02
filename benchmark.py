import time
import uuid
import concurrent.futures
import grpc

import bioskop_pb2 as pb
import bioskop_pb2_grpc as pb_grpc

SERVER_ADDRESS = "localhost:50051"
TOTAL_REQUEST_PER_CLIENT = 10

def simulasi_client(client_id, kursi_target):
    """Simulasi 1 client yang melakukan pemesanan berkali-kali."""
    latensi_list = []
    sukses_count = 0
    gagal_count = 0

    with grpc.insecure_channel(SERVER_ADDRESS) as channel:
        stub = pb_grpc.LayananBioskopStub(channel)
        
        for i in range(TOTAL_REQUEST_PER_CLIENT):
            start_time = time.time()
            rid = str(uuid.uuid4())
            nama = f"Client-{client_id}"
            
            req = pb.PesanRequest(request_id=rid, nama_pemesan=nama, nomor_kursi=kursi_target)
            
            try:
                res = stub.PesanKursi(req, timeout=2.0)
                end_time = time.time()
                latensi_list.append((end_time - start_time) * 1000) # ke milidetik
                if res.sukses:
                    sukses_count += 1
                else:
                    gagal_count += 1
            except grpc.RpcError:
                gagal_count += 1

    return latensi_list, sukses_count, gagal_count

def jalankan_benchmark(jumlah_client):
    print(f"\n==================================================")
    print(f" JALANKAN BENCHMARK: {jumlah_client} CLIENT CONCURRENT ")
    print(f"==================================================")
    
    total_request = jumlah_client * TOTAL_REQUEST_PER_CLIENT
    start_total = time.time()
    
    semua_latensi = []
    total_sukses = 0
    total_gagal = 0
    
    # Jalankan client secara bersamaan menggunakan ThreadPoolExecutor
    with concurrent.futures.ThreadPoolExecutor(max_workers=jumlah_client) as executor:
        futures_map = {
            executor.submit(simulasi_client, i + 1, f"A{(i % 5) + 1}"): i 
            for i in range(jumlah_client)
        }
        
        for future in concurrent.futures.as_completed(futures_map):
            latensi, sukses, gagal = future.result()
            semua_latensi.extend(latensi)
            total_sukses += sukses
            total_gagal += gagal
            
    end_total = time.time()
    total_durasi = end_total - start_total
    
    avg_latensi = sum(semua_latensi) / len(semua_latensi) if semua_latensi else 0
    throughput = total_request / total_durasi if total_durasi > 0 else 0
    
    print(f"Total Request Sent : {total_request}")
    print(f"Total Durasi       : {total_durasi:.2f} detik")
    print(f"Rata-rata Latensi  : {avg_latensi:.2f} ms")
    print(f"Throughput         : {throughput:.2f} req/detik")
    print(f"Berhasil/Gagal     : {total_sukses} sukses / {total_gagal} gagal (karena lock/kursi terisi)")
    
    return avg_latensi, throughput

if __name__ == "__main__":
    print("Memulai Pengujian Kinerja Sistem Bioskop gRPC...")
    
    # Uji coba untuk 1, 5, dan 20 client
    skenario = [1, 5, 20]
    hasil = {}
    
    for n in skenario:
        lat, tp = jalankan_benchmark(n)
        hasil[n] = (lat, tp)
        time.sleep(1)
        
    print("\n==================================================")
    print("             RINGKASAN HASIL BENCHMARK            ")
    print("==================================================")
    print("Jumlah Client | Avg Latensi (ms) | Throughput (req/s)")
    print("--------------------------------------------------")
    for n in skenario:
        print(f"   {n:2d} Client   |    {hasil[n][0]:8.2f} ms   |   {hasil[n][1]:8.2f} req/s")
    print("==================================================")