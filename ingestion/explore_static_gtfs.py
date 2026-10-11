import io
import zipfile
import requests

URL = "https://mtd.dev/gtfs.zip"  # from the MTD developer page

response = requests.get(URL, timeout=60)
response.raise_for_status()

print(f"Zip size: {len(response.content) / 1024 / 1024:.2f} MB")
print("ETag:", response.headers.get("ETag"))
print("Last-Modified:", response.headers.get("Last-Modified"))

with zipfile.ZipFile(io.BytesIO(response.content)) as z:
    for name in z.namelist():
        with z.open(name) as f:
            lines = f.read().decode("utf-8-sig").splitlines()
        print(f"\n=== {name} === ({len(lines) - 1} rows)")
        print("Columns:", lines[0])
        if len(lines) > 1:
            print("Example:", lines[1])