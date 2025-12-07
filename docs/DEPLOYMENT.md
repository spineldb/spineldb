# Deployment Guide untuk GitHub Pages

Panduan ini menjelaskan cara deploy dokumentasi Docusaurus ke GitHub Pages.

## Opsi 1: Deploy Otomatis dengan GitHub Actions (Recommended)

Workflow GitHub Actions sudah dibuat di `.github/workflows/deploy-docs.yml`. 

### Setup Awal:

1. **Enable GitHub Pages di repository:**
   - Buka repository di GitHub
   - Pergi ke **Settings** → **Pages**
   - Di bagian **Source**, pilih **GitHub Actions**
   - Simpan perubahan

2. **Verifikasi baseUrl di `docusaurus.config.js`:**
   
   **Jika repository Anda adalah `spineldb/spineldb`** (bukan `spineldb.github.io`):
   ```js
   baseUrl: '/spineldb/',  // Ganti dengan nama repository Anda
   ```
   
   **Jika repository Anda adalah `spineldb/spineldb.github.io`**:
   ```js
   baseUrl: '/',  // Sudah benar
   ```

3. **Push ke branch main:**
   ```bash
   git add .
   git commit -m "Setup Docusaurus for GitHub Pages"
   git push origin main
   ```

4. **Deployment akan otomatis berjalan:**
   - Buka tab **Actions** di GitHub
   - Workflow akan build dan deploy otomatis
   - Setelah selesai, dokumentasi akan tersedia di:
     - `https://spineldb.github.io/spineldb/` (jika baseUrl: '/spineldb/')
     - `https://spineldb.github.io/` (jika baseUrl: '/' dan repo adalah spineldb.github.io)

## Opsi 2: Deploy Manual

Jika Anda ingin deploy manual:

1. **Build dokumentasi:**
   ```bash
   npm run build
   ```

2. **Deploy menggunakan script Docusaurus:**
   ```bash
   GIT_USER=your-github-username npm run deploy
   ```

   Atau jika menggunakan SSH:
   ```bash
   USE_SSH=true npm run deploy
   ```

## Troubleshooting

### Halaman tidak muncul setelah deployment
- Pastikan GitHub Pages sudah di-enable di Settings → Pages
- Pastikan source di-set ke "GitHub Actions"
- Cek Actions tab untuk melihat error

### Assets tidak loading
- Pastikan `baseUrl` di `docusaurus.config.js` sesuai dengan struktur URL GitHub Pages
- Clear cache browser atau test di incognito mode

### Build gagal
- Pastikan semua dependencies terinstall: `npm ci`
- Cek log di GitHub Actions untuk detail error

