import asyncio
import logging
from typing import Dict, List, Any

class WilayahSekolahScraper:
    def __init__(self, db_manager, logger=None):
        """
        Initialize the scraper with a database manager and optional logger.
        
        :param db_manager: Database management object
        :param logger: Optional logger instance
        """
        self.db = db_manager
        self.logger = logger or logging.getLogger(__name__)
        
        if not self.logger.handlers:
            logging.basicConfig(
                level=logging.INFO,
                format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
            )

    async def _safe_extract_text(self, element, default=""):
        """
        Safely extract text from a page element.
        
        :param element: Playwright page element
        :param default: Default value if extraction fails
        :return: Extracted text or default
        """
        try:
            return await element.inner_text()
        except Exception:
            return default

    async def _safe_get_attribute(self, element, attribute, default=""):
        """
        Safely get attribute from a page element.
        
        :param element: Playwright page element
        :param attribute: Attribute to extract
        :param default: Default value if extraction fails
        :return: Attribute value or default
        """
        try:
            return await element.get_attribute(attribute) or default
        except Exception:
            return default

    async def _extract_data_columns(self, kolom_data: List[Any], max_columns: int = 50) -> List[str]:
        """
        Extract data columns with a default padding mechanism.

        :param kolom_data: List of column elements
        :param max_columns: Maximum number of columns to extract
        :return: List of column texts with padding
        """
        try:
            data_columns = [await col.inner_text() for col in kolom_data[1:]] if len(kolom_data) > 1 else []
            if len(data_columns) < max_columns:
                return data_columns + [''] * (max_columns - len(data_columns))
            return data_columns
        except Exception as e:
            self.logger.error(f"Error in _extract_data_columns: {e}")
            return [''] * max_columns

    async def _process_page_rows(
        self, 
        page, 
        table: str,
        selector: str, 
        insert_func, 
        parent_id_key: str = None, 
        parent_id: int = None, 
        data_mapping_func = None,
        progress_update_key: str = None,
        progress_update_id: int = None
    ):
        """
        Generic method to process rows on a page and insert data.
        """
        try:
            await page.wait_for_selector(table or "table#DataTables_Table_0")
            rows = await page.query_selector_all(selector)
    
            if progress_update_key and progress_update_id:
                self.db.update_progress(progress_update_key, progress_update_id)
    
            rows_processed = 0
            rows_inserted = 0
    
            for row in rows:
                try:
                    kolom_data = await row.query_selector_all("td")
                    
                    if len(kolom_data) <= 1:
                        continue
                    
                    link = await row.query_selector("a")
                    nama = await self._safe_extract_text(link) if link else ""
                    url = await self._safe_get_attribute(link, "href") if link else ""
    
                    data_columns = await self._extract_data_columns(kolom_data)
                    
                    data = {
                        "nama": nama,
                        "url": url,
                        **(({parent_id_key: parent_id} if parent_id_key and parent_id else {}))
                    }
    
                    if data_mapping_func:
                        mapped_data = data_mapping_func(data_columns)
                        data.update(mapped_data)
                    else:
                        for i, col in enumerate(data_columns):
                            data[f'column_{i}'] = col
    
                    data.setdefault('progress', 0)
                    
                    insert_result = insert_func(data)
                    rows_processed += 1
                    
                    if insert_result is not None:
                        rows_inserted += 1
                    else:
                        self.logger.warning(f"Failed to insert data: {data}")
    
                except Exception as row_error:
                    self.logger.error(f"Error processing individual row: {row_error}")
                    continue
            
            self.logger.info(f"Processed {rows_processed} rows, successfully inserted {rows_inserted} rows")
                
        except Exception as e:
            self.logger.error(f"Error processing rows: {e}")
            if progress_update_key and progress_update_id:
                try:
                    self.db.mark_as_processed(progress_update_key, progress_update_id)
                except Exception as mark_error:
                    self.logger.error(f"Could not mark {progress_update_key} as processed: {mark_error}")
    
    async def scrape_wilayah(self, page):
        """
        Scrape national (wilayah) level data.
        
        :param page: Playwright page object
        """
        try:
            await page.goto("https://dapo.kemdikbud.go.id/sp")
            
            await self._process_page_rows(
                page, 
                "table#DataTables_Table_0",  # Corrected table selector
                "table#DataTables_Table_0 tbody tr", 
                self.db.insert_provinsi,
                data_mapping_func=lambda cols: {
                    "Wilayah": cols[0],
                    "Total_Jml": cols[1],
                    "Total_N": cols[2],
                    "Total_S": cols[4],
                    "TK_Jml": cols[5],
                    "TK_N": cols[6],
                    "TK_S": cols[7],
                    "KB_Jml": cols[8],
                    "KB_N": cols[9],
                    "KB_S": cols[10],
                    "TPA_Jml": cols[11],
                    "TPA_N": cols[12],
                    "TPA_S": cols[13],
                    "SPS_Jml": cols[14],
                    "SPS_N": cols[15],
                    "SPS_S": cols[16],
                    "PKBM_Jml": cols[17],
                    "PKBM_N": cols[18],
                    "PKBM_S": cols[19],
                    "SKB_Jml": cols[20],
                    "SKB_N": cols[21],
                    "SKB_S": cols[22],
                    "SD_Jml": cols[23],
                    "SD_N": cols[24],
                    "SD_S": cols[25],
                    "SMP_Jml": cols[26],
                    "SMP_N": cols[27],
                    "SMP_S": cols[28],
                    "SMA_Jml": cols[29],
                    "SMA_N": cols[30],
                    "SMA_S": cols[31],
                    "SMK_Jml": cols[32],
                    "SMK_N": cols[33],
                    "SMK_S": cols[34],
                    "SLB_Jml": cols[35],
                    "SLB_N": cols[36],
                    "SLB_S": cols[37],
                }
            )
        except Exception as e:
            self.logger.error(f"Error in scrape_wilayah: {e}")

    async def scrape_kabupaten(self, page):
        """
        Scrape kabupaten level data for all unprocessed provinces.
        
        :param page: Playwright page object
        """
        provinsi_list = self.db.get_unprocessed_provinsi()

        for provinsi in provinsi_list:
            try:
                await page.goto(f"https://dapo.kemdikbud.go.id{provinsi['url']}")

                await self._process_page_rows(
                    page, 
                    "table#DataTables_Table_0",  # Corrected table selector
                    "table#DataTables_Table_0 tbody tr", 
                    self.db.insert_kabupaten,
                    parent_id_key="provinsi_id", 
                    parent_id=provinsi["id"],
                    progress_update_key="provinsi", 
                    progress_update_id=provinsi["id"],
                    data_mapping_func=lambda cols: {
                        "Wilayah": cols[0],
                        "Total_Jml": cols[1],
                        "Total_N": cols[2],
                        "Total_S": cols[4],
                        "TK_Jml": cols[5],
                        "TK_N": cols[6],
                        "TK_S": cols[7],
                        "KB_Jml": cols[8],
                        "KB_N": cols[9],
                        "KB_S": cols[10],
                        "TPA_Jml": cols[11],
                        "TPA_N": cols[12],
                        "TPA_S": cols[13],
                        "SPS_Jml": cols[14],
                        "SPS_N": cols[15],
                        "SPS_S": cols[16],
                        "PKBM_Jml": cols[17],
                        "PKBM_N": cols[18],
                        "PKBM_S": cols[19],
                        "SKB_Jml": cols[20],
                        "SKB_N": cols[21],
                        "SKB_S": cols[22],
                        "SD_Jml": cols[23],
                        "SD_N": cols[24],
                        "SD_S": cols[25],
                        "SMP_Jml": cols[26],
                        "SMP_N": cols[27],
                        "SMP_S": cols[28],
                        "SMA_Jml": cols[29],
                        "SMA_N": cols[30],
                        "SMA_S": cols[31],
                        "SMK_Jml": cols[32],
                        "SMK_N": cols[33],
                        "SMK_S": cols[34],
                        "SLB_Jml": cols[35],
                        "SLB_N": cols[36],
                        "SLB_S": cols[37],
                    }
                )

            except Exception as e:
                self.logger.error(f"Error scraping kabupaten in {provinsi['nama']}: {e}")

    async def scrape_kecamatan(self, page):
        """
        Scrape kecamatan level data for all unprocessed kabupaten.
        
        :param page: Playwright page object
        """
        kabupaten_list = self.db.get_unprocessed_kabupaten()

        for kabupaten in kabupaten_list:
            try:
                await page.goto(f"https://dapo.kemdikbud.go.id{kabupaten['url']}")

                await self._process_page_rows(
                    page, 
                    "table#DataTables_Table_0",  # Consistent table selector
                    "table#DataTables_Table_0 tbody tr", 
                    self.db.insert_kecamatan,
                    parent_id_key="kecamatan_id", 
                    parent_id=kabupaten["id"],
                    progress_update_key="kabupaten", 
                    progress_update_id=kabupaten["id"],
                    data_mapping_func=lambda cols: {
                        "Wilayah": cols[0],
                    "Total_Jml": cols[1],
                    "Total_N": cols[2],
                    "Total_S": cols[4],
                    "TK_Jml": cols[5],
                    "TK_N": cols[6],
                    "TK_S": cols[7],
                    "KB_Jml": cols[8],
                    "KB_N": cols[9],
                    "KB_S": cols[10],
                    "TPA_Jml": cols[11],
                    "TPA_N": cols[12],
                    "TPA_S": cols[13],
                    "SPS_Jml": cols[14],
                    "SPS_N": cols[15],
                    "SPS_S": cols[16],
                    "PKBM_Jml": cols[17],
                    "PKBM_N": cols[18],
                    "PKBM_S": cols[19],
                    "SKB_Jml": cols[20],
                    "SKB_N": cols[21],
                    "SKB_S": cols[22],
                    "SD_Jml": cols[23],
                    "SD_N": cols[24],
                    "SD_S": cols[25],
                    "SMP_Jml": cols[26],
                    "SMP_N": cols[27],
                    "SMP_S": cols[28],
                    "SMA_Jml": cols[29],
                    "SMA_N": cols[30],
                    "SMA_S": cols[31],
                    "SMK_Jml": cols[32],
                    "SMK_N": cols[33],
                    "SMK_S": cols[34],
                    "SLB_Jml": cols[35],
                    "SLB_N": cols[36],
                    "SLB_S": cols[37],
                    }
                )

            except Exception as e:
                self.logger.error(f"Error scraping kecamatan in {kabupaten['nama']}: {e}")

    async def scrape_sekolah(self, page):
        """
        Scrape sekolah level data for all unprocessed kecamatan.
        
        :param page: Playwright page object
        """
        kecamatan_list = self.db.get_unprocessed_kecamatan()

        for kecamatan in kecamatan_list:
            try:
                await page.goto(f"https://dapo.kemdikbud.go.id{kecamatan['url']}")

                await self._process_page_rows(
                    page,  # Corrected parameter order
                    "table#dataTables",  # Consistent table selector
                    "#dataTables tbody tr", 
                    self.db.insert_sekolah,
                    parent_id_key="sekolah_id", 
                    parent_id=kecamatan["id"],
                    progress_update_key="kecamatan", 
                    progress_update_id=kecamatan["id"],
                    data_mapping_func=lambda cols: {
                        "NamaSekolah": cols[0],
                        "NPSN": cols[0] if len(cols) > 0 else "",
                        "BP": cols[1] if len(cols) > 1 else "",
                        "Status": cols[2] if len(cols) > 2 else "",
                        "Last_Sync": cols[3] if len(cols) > 3 else "",
                        "Jml_Sync": cols[4] if len(cols) > 4 else "",
                        "PD": cols[5] if len(cols) > 5 else "",
                        "Rombel": cols[6] if len(cols) > 6 else "",
                        "Guru": cols[7] if len(cols) > 7 else "",
                        "Pegawai": cols[8] if len(cols) > 8 else "",
                        "Kelas": cols[9] if len(cols) > 9 else "",
                        "Lab": cols[10] if len(cols) > 10 else "",
                        "Perpus": cols[11] if len(cols) > 11 else "",
                    }
                )

            except Exception as e:
                self.logger.error(f"Error scraping sekolah in {kecamatan['nama']}: {e}")