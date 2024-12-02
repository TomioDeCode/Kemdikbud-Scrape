import os
from supabase import create_client, Client

class DatabaseManager:
    def __init__(self, supabase_url, supabase_key):
        self.url = supabase_url
        self.key = supabase_key
        self.client = create_client(self.url, self.key)

    def get_unprocessed_provinsi(self):
        try:
            response = self.client.table('provinsi').select('*').eq('progress', 0).execute()
            return response.data
        except Exception as e:
            print(f"Error fetching unprocessed nasional: {e}")
            return []

    def get_unprocessed_kabupaten(self):
        try:
            response = self.client.table('kabupaten').select('*').eq('progress', 0).execute()
            return response.data
        except Exception as e:
            print(f"Error fetching unprocessed kabupaten: {e}")
            return []

    def get_unprocessed_kecamatan(self):
        try:
            response = self.client.table('kecamatan').select('*').eq('progress', 0).execute()
            return response.data
        except Exception as e:
            print(f"Error fetching unprocessed kecamatan: {e}")
            return []

    def insert_nasional(self, data):
        try:
            quoted_data = {}
            for key, value in data.items():
                if ' ' in key:
                    quoted_data[f'"{key}"'] = value
                else:
                    quoted_data[key] = value

            response = self.client.table('nasional').insert(quoted_data).execute()
            return response.data[0]['id'] if response.data else None
        except Exception as e:
            print(f"Error inserting nasional data: {e}")
            return None

    def insert_provinsi(self, data):
        try:
            existing = self.client.table('provinsi').select('*').eq('nama', data['nama']).execute()

            if existing.data:
                return existing.data[0]['id']

            quoted_data = {}
            for key, value in data.items():
                if ' ' in key:
                    quoted_data[f'"{key}"'] = value
                else:
                    quoted_data[key] = value

            response = self.client.table('provinsi').insert(quoted_data).execute()
            return response.data[0]['id'] if response.data else None
        except Exception as e:
            print(f"Error inserting provinsi data: {e}")
            return None

    def insert_kabupaten(self, data):
        try:
            quoted_data = {}
            for key, value in data.items():
                if ' ' in key:
                    quoted_data[f'"{key}"'] = value
                else:
                    quoted_data[key] = value

            response = self.client.table('kabupaten').insert(quoted_data).execute()
            return response.data[0]['id'] if response.data else None
        except Exception as e:
            print(f"Error inserting kabupaten data: {e}")
            return None

    def insert_kecamatan(self, data):
        try:
            quoted_data = {}
            for key, value in data.items():
                if ' ' in key:
                    quoted_data[f'"{key}"'] = value
                else:
                    quoted_data[key] = value

            response = self.client.table('kecamatan').insert(quoted_data).execute()
            return response.data[0]['id'] if response.data else None
        except Exception as e:
            print(f"Error inserting kecamatan data: {e}")
            return None

    def insert_sekolah(self, data):
        try:
            quoted_data = {}
            for key, value in data.items():
                if ' ' in key:
                    quoted_data[f'"{key}"'] = value
                else:
                    quoted_data[key] = value

            response = self.client.table('sekolah').insert(quoted_data).execute()
            return response.data[0]['id'] if response.data else None
        except Exception as e:
            print(f"Error inserting sekolah data: {e}")
            return None

    def update_progress(self, table, record_id):
        try:
            response = self.client.table(table).update({'progress': 1}).eq('id', record_id).execute()
            return True
        except Exception as e:
            print(f"Error updating progress for {table}: {e}")
            return False