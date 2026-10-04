"""Membuat dataset teks sintetis berbahasa Indonesia (distribusi mirip Zipf)."""
import random
random.seed(42)
vocab = """dan yang di ke dari untuk dengan pada adalah dalam ini itu tidak akan juga oleh karena sebagai
data spark proses paralel cluster node partition stage task transformation action memori disk
mahasiswa kampus belajar teknologi informasi sistem jaringan komputer program algoritma analisis
hasil waktu cepat lambat besar kecil banyak sedikit baru lama kata baris teks file baca tulis
distribusi performa skalabilitas biaya mesin tunggal eksekusi kegagalan toleransi aplikasi layanan
pengguna server cloud basis model pembelajaran kecerdasan buatan internet aman terbuka mudah sulit""".split()
weights = [1/(i+1) for i in range(len(vocab))]
N = 50000
with open("data/bigdata.txt", "w", encoding="utf-8") as f:
    for _ in range(N):
        n = random.randint(8, 20)
        words = random.choices(vocab, weights=weights, k=n)
        line = " ".join(words).capitalize() + random.choice([".", "!", ",", ""])
        f.write(line + "\n")
print("Dataset dibuat:", N, "baris")
