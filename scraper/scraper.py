class WilayahSekolahScraper:
    def __init__(self, db_manager):
        self.db = db_manager

    async def scrape_wilayah(self, page):
        try:
            await page.goto("https://dapo.kemdikbud.go.id/sp")
            await page.wait_for_selector("table#DataTables_Table_0")

            provinsi_rows = await page.query_selector_all("#DataTables_Table_0 tbody tr")

            for row in provinsi_rows:
                link = await row.query_selector("a")
                if link:
                    try:
                        provinsi_nama = await link.inner_text()
                        provinsi_url = await link.get_attribute("href")

                        kolom_data = await row.query_selector_all("td")
                        if len(kolom_data) > 1:
                            data_columns = [
                                await col.inner_text() for col in kolom_data[1:]
                            ]

                            self.db.cursor.execute(
                                """
                                INSERT OR IGNORE INTO nasional (
                                    nama, url, Wilayah, Total_Jml, Total_N, Total_S,
                                    TK_Jml, TK_N, TK_S, KB_Jml, KB_N, KB_S, TPA_Jml, TPA_N, TPA_S,
                                    SPS_Jml, SPS_N, SPS_S, PKBM_Jml, PKBM_N, PKBM_S,
                                    SKB_Jml, SKB_N, SKB_S, SD_Jml, SD_N, SD_S, SMP_Jml, SMP_N, SMP_S,
                                    SMA_Jml, SMA_N, SMA_S, SMK_Jml, SMK_N, SMK_S, SLB_Jml, SLB_N, SLB_S
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                                """,
                                [provinsi_nama, provinsi_url] + data_columns,
                            )
                            self.db.conn.commit()

                            nasional_id = self.db.cursor.lastrowid

                            self.db.cursor.execute(
                                """
                                INSERT OR IGNORE INTO provinsi (nama, url, nasional_id, progress) 
                                VALUES (?, ?, ?, 0)
                                """,
                                (provinsi_nama, provinsi_url, nasional_id),
                            )
                            self.db.conn.commit()

                    except Exception as e:
                        print(f"Error processing provinsi {provinsi_nama}: {e}")
                        self.db.conn.rollback()

        except Exception as e:
            print(f"Error in scrape_wilayah: {e}")
            self.db.conn.rollback()

    async def scrape_kabupaten(self, page):
        self.db.cursor.execute("SELECT id, nama, url, nasional_id FROM provinsi WHERE progress = 0")
        provinsi_list = self.db.cursor.fetchall()

        for provinsi in provinsi_list:
            try:
                await page.goto(f"https://dapo.kemdikbud.go.id{provinsi[2]}")
                await page.wait_for_selector("table#DataTables_Table_0")

                self.db.cursor.execute(
                    "UPDATE provinsi SET progress = 1 WHERE id = ?", (provinsi[0],)
                )
                self.db.conn.commit()

                kabupaten_rows = await page.query_selector_all(
                    "#DataTables_Table_0 tbody tr"
                )
                for row in kabupaten_rows:
                    link = await row.query_selector("a")
                    if link:
                        kabupaten_nama = await link.inner_text()
                        kabupaten_url = await link.get_attribute("href")

                        kolom_data = await row.query_selector_all("td")
                        if len(kolom_data) > 1:
                            data_columns = [
                                await col.inner_text() for col in kolom_data[1:]
                            ]

                            self.db.cursor.execute(
                                """
                                INSERT OR IGNORE INTO kabupaten (
                                    nama, url, provinsi_id, Wilayah, Total_Jml, Total_N, Total_S,
                                    TK_Jml, TK_N, TK_S, KB_Jml, KB_N, KB_S, TPA_Jml, TPA_N, TPA_S,
                                    SPS_Jml, SPS_N, SPS_S, PKBM_Jml, PKBM_N, PKBM_S,
                                    SKB_Jml, SKB_N, SKB_S, SD_Jml, SD_N, SD_S, SMP_Jml, SMP_N, SMP_S,
                                    SMA_Jml, SMA_N, SMA_S, SMK_Jml, SMK_N, SMK_S, SLB_Jml, SLB_N, SLB_S, 
                                    progress
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                                """,
                                [kabupaten_nama, kabupaten_url, provinsi[0]] + data_columns,
                            )
                            self.db.conn.commit()

            except Exception as e:
                print(f"Error scraping kabupaten in {provinsi[1]}: {e}")
                self.db.conn.rollback()

    async def scrape_kecamatan(self, page):
        self.db.cursor.execute("SELECT id, nama, url, provinsi_id FROM kabupaten WHERE progress = 0")
        kabupaten_list = self.db.cursor.fetchall()

        for kabupaten in kabupaten_list:
            try:
                await page.goto(f"https://dapo.kemdikbud.go.id{kabupaten[2]}")
                await page.wait_for_selector("table#DataTables_Table_0")

                self.db.cursor.execute(
                    "UPDATE kabupaten SET progress = 1 WHERE id = ?", (kabupaten[0],)
                )
                self.db.conn.commit()

                kecamatan_rows = await page.query_selector_all(
                    "#DataTables_Table_0 tbody tr"
                )
                for row in kecamatan_rows:
                    link = await row.query_selector("a")
                    if link:
                        kecamatan_nama = await link.inner_text()
                        kecamatan_url = await link.get_attribute("href")

                        kolom_data = await row.query_selector_all("td")
                        if len(kolom_data) > 1:
                            data_columns = [
                                await col.inner_text() for col in kolom_data[1:]
                            ]

                            self.db.cursor.execute(
                                """
                                INSERT OR IGNORE INTO kecamatan (
                                    nama, url, kabupaten_id, Wilayah, Total_Jml, Total_N, Total_S,
                                    TK_Jml, TK_N, TK_S, KB_Jml, KB_N, KB_S, TPA_Jml, TPA_N, TPA_S,
                                    SPS_Jml, SPS_N, SPS_S, PKBM_Jml, PKBM_N, PKBM_S,
                                    SKB_Jml, SKB_N, SKB_S, SD_Jml, SD_N, SD_S, SMP_Jml, SMP_N, SMP_S,
                                    SMA_Jml, SMA_N, SMA_S, SMK_Jml, SMK_N, SMK_S, SLB_Jml, SLB_N, SLB_S, 
                                    progress
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                                """,
                                [kecamatan_nama, kecamatan_url, kabupaten[0]] + data_columns,
                            )
                            self.db.conn.commit()

            except Exception as e:
                print(f"Error scraping kecamatan in {kabupaten[1]}: {e}")
                self.db.conn.rollback()

    async def scrape_sekolah(self, page):
        self.db.cursor.execute("SELECT id, nama, url FROM kecamatan WHERE progress = 0")
        kecamatan_list = self.db.cursor.fetchall()

        for kecamatan in kecamatan_list:
            try:
                await page.goto(f"https://dapo.kemdikbud.go.id{kecamatan[2]}")
                await page.wait_for_selector("table#DataTables_Table_0")

                self.db.cursor.execute(
                    "UPDATE kecamatan SET progress = 1 WHERE id = ?", (kecamatan[0],)
                )
                self.db.conn.commit()

                sekolah_rows = await page.query_selector_all(
                    "#DataTables_Table_0 tbody tr"
                )
                for row in sekolah_rows:
                    link = await row.query_selector("a")
                    if link:
                        sekolah_nama = await link.inner_text()
                        sekolah_url = await link.get_attribute("href")

                        kolom_data = await row.query_selector_all("td")
                        if len(kolom_data) > 1:
                            data_columns = [
                                await col.inner_text() for col in kolom_data[1:]
                            ]

                            while len(data_columns) < 13:
                                data_columns.append('')

                            self.db.cursor.execute(
                                """
                                INSERT OR IGNORE INTO sekolah (
                                    nama, url, NamaSekolah, NPSN, BP, Status, 
                                    Last_Sync, Jml_Sync, PD, Rombel, Guru, Pegawai, 
                                    Kelas, Lab, Perpus, kecamatan_id, progress
                                ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0)
                                """,
                                [
                                    sekolah_nama, 
                                    sekolah_url, 
                                    sekolah_nama,
                                ] + data_columns[:14] + [kecamatan[0]],
                            )
                            self.db.conn.commit()

            except Exception as e:
                print(f"Error scraping sekolah in {kecamatan[1]}: {e}")
                self.db.conn.rollback()