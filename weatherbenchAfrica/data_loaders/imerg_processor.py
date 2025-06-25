import os
import requests
from datetime import datetime, timedelta
import xarray as xr
from dotenv import load_dotenv

class ImergProcessor:

    def __init__(self, start_date, end_date, base_url, auth, country_name, output_dir):
        self.start_date = datetime.strptime(start_date, "%Y-%m-%d")
        self.end_date = datetime.strptime(end_date, "%Y-%m-%d")
        self.base_url = base_url
        self.auth = auth
        self.country_name = country_name
        self.output_dir = output_dir
        os.makedirs(self.output_dir, exist_ok=True)

    def run(self, lat_north, lat_south, lon_east, lon_west):
        for date in self._date_range():
            filename, url = self._build_url_and_filename(date)
            dest_path = os.path.join(self.output_dir, filename.replace(".nc4", f"_{self.country_name}.nc4"))

            if os.path.exists(dest_path):
                print(f"File already exists: {dest_path}")
                continue

            try:
                temp_path = self._download_file(url, filename)
                self._process_file(temp_path, dest_path, lat_north, lat_south, lon_east, lon_west)
            except Exception as e:
                print(f"Error processing {filename}: {e}")

    def _date_range(self):
        current = self.start_date
        while current <= self.end_date:
            yield current
            current += timedelta(days=1)

    def _build_url_and_filename(self, date):
        date_str = date.strftime("%Y%m%d")
        year = date.strftime("%Y")
        month = date.strftime("%m")
        filename = f"3B-DAY.MS.MRG.3IMERG.{date_str}-S000000-E235959.V07B.nc4"
        url = f"{self.base_url}/{year}/{month}/{filename}"
        return filename, url

    def _download_file(self, url, filename):
        print(f"Downloading: {filename}")
        response = requests.get(url, auth=self.auth, stream=True, timeout=60)

        if response.status_code != 200:
            raise Exception(f"Download failed with status {response.status_code}")

        temp_path = "temp.nc4"
        with open(temp_path, "wb") as f:
            for chunk in response.iter_content(8192):
                f.write(chunk)
        return temp_path

    def _process_file(self, temp_path, dest_path, lat_north, lat_south, lon_east, lon_west):
        ds = xr.open_dataset(temp_path)
        lat_slice = slice(lat_south, lat_north) if ds.lat[0] < ds.lat[-1] else slice(lat_north, lat_south)
        ds_processed = ds.sel(lat=lat_slice, lon=slice(lon_west, lon_east))

        precip_var = 'precipitation' if 'precipitation' in ds_processed.data_vars else list(ds_processed.data_vars)[0]
        if ds_processed[precip_var].count().item() == 0:
            print("No precipitation data found in file.")
        else:
            ds_processed.to_netcdf(dest_path)
            print(f"Saved data to: {dest_path}")

        ds.close()
        os.remove(temp_path)

def main():
    load_dotenv()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    data_dir = os.path.abspath(os.path.join(script_dir, "..", "data"))

    processor = ImergProcessor(
        start_date=os.getenv("START_DATE"),
        end_date=os.getenv("END_DATE"),
        base_url=os.getenv("BASE_URL"),
        auth=(os.getenv("GESDISC_USERNAME"), os.getenv("GESDISC_TOKEN")),
        country_name=os.getenv("COUNTRY_NAME"),
        output_dir=data_dir
    )

    processor.run(
        lat_north=float(os.getenv("LAT_NORTH")),
        lat_south=float(os.getenv("LAT_SOUTH")),
        lon_east=float(os.getenv("LON_EAST")),
        lon_west=float(os.getenv("LON_WEST")),
    )

if __name__ == "__main__":
    main()