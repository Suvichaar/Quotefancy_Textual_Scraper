import streamlit as st
import pandas as pd
import time
import csv
import io
import re
import json
import datetime
import base64
import os
import shutil
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter, Retry
import boto3
from simple_image_download import simple_image_download as simp
from openai import AzureOpenAI
from azure.storage.blob import (
    BlobServiceClient,
    generate_blob_sas,
    BlobSasPermissions,
    ContentSettings
)

st.set_page_config(page_title="Quote Utility Toolkit", layout="wide")

tab1, tab2, tab3, tab4, tab5, tab6 , tab7 = st.tabs([
    "🕸️ Scrape Quotes from QuoteFancy",
    "📊 Structure Quotes by Author",
    "🧠 Quote Metadata Generator & Azure OpenAI JSONL Creator ",
    "📦 Azure Batch Result Fetcher & Blob Uploader",
    "👤 Distinct Author Extractor",
    "🖼️ Bulk Image Downloader + S3 Uploader + CDN Transformer",
    "📅 Merge Metadata into Structured CSV"
])

# ------------------- TAB 1 -------------------
with tab1:
    st.title("📝 QuoteFancy Scraper")
    USER_AGENT = (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/90.0.4430.93 Safari/537.36"
    )
    REQUEST_TIMEOUT = 10
    DELAY_BETWEEN_PAGES = 1
    MAX_PAGES = 10
    
    def create_session_with_retries():
        session = requests.Session()
        session.headers.update({
            'User-Agent': USER_AGENT,
            'Accept-Language': 'en-US,en;q=0.9'
        })
        retries = Retry(
            total=3,
            backoff_factor=0.3,
            status_forcelist=[500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS"]
        )
        adapter = HTTPAdapter(max_retries=retries)
        session.mount("http://", adapter)
        session.mount("https://", adapter)
        return session
    
    def extract_slug_from_url(url):
        parsed = urlparse(url)
        path = parsed.path.strip("/")
        return path.split("/")[0] if path else ""
    
    def scrape_quotes_for_slug(slug, max_pages=MAX_PAGES):
        session = create_session_with_retries()
        rows, serial_number = [], 1
    
        for page_number in range(1, max_pages + 1):
            page_url = f"https://quotefancy.com/{slug}/page/{page_number}"
            try:
                response = session.get(page_url, timeout=REQUEST_TIMEOUT)
                response.raise_for_status()
            except requests.RequestException:
                break
    
            soup = BeautifulSoup(response.content, "html.parser")
            containers = soup.find_all("div", class_="q-wrapper")
            if not containers:
                break
    
            for container in containers:
                quote_div = container.find("div", class_="quote-a")
                quote_text = quote_div.get_text(strip=True) if quote_div else container.find("a", class_="quote-a").get_text(strip=True)
    
                quote_link = ""
                if quote_div and quote_div.find("a"):
                    quote_link = quote_div.find("a").get("href", "")
                elif container.find("a", class_="quote-a"):
                    quote_link = container.find("a", class_="quote-a").get("href", "")
    
                author_div = container.find("div", class_="author-p bylines")
                if author_div:
                    author_text = author_div.get_text(strip=True).replace("by ", "").strip()
                else:
                    author_p = container.find("p", class_="author-p")
                    author_text = author_p.find("a").get_text(strip=True) if author_p and author_p.find("a") else "Anonymous"
    
                rows.append([serial_number, quote_text, quote_link, author_text])
                serial_number += 1
    
            time.sleep(DELAY_BETWEEN_PAGES)
    
        return rows
    
    def convert_to_csv_buffer(rows):
        output = io.StringIO()
        writer = csv.writer(output)
        writer.writerow(["Serial No", "Quote", "Link", "Author"])
        writer.writerows(rows)
        return output.getvalue()
    
    input_urls = st.text_area("Enter QuoteFancy URLs (comma separated):")
    filename_prefix = st.text_input("Filename prefix (without extension)", "quotes")
    
    if st.button("Start Scraping", key="scrape_button"):
        if not input_urls or not filename_prefix:
            st.error("Please provide both URLs and filename prefix.")
        else:
            url_list = [url.strip() for url in input_urls.split(",") if url.strip()]
            all_quotes = []
            for url in url_list:
                slug = extract_slug_from_url(url)
                st.write(f"🔍 Scraping: `{slug}`")
                all_quotes.extend(scrape_quotes_for_slug(slug))
    
            if all_quotes:
                csv_data = convert_to_csv_buffer(all_quotes)
                timestamp = int(time.time())
                full_filename = f"{filename_prefix}_{timestamp}.csv"
                st.success(f"✅ Scraped {len(all_quotes)} quotes.")
                st.download_button("📥 Download CSV", data=csv_data, file_name=full_filename, mime='text/csv')
            else:
                st.warning("⚠️ No quotes scraped.")

# ------------------- TAB 2 -------------------
with tab2:
    st.header("📊 Structure Quotes by Author")
    file = st.file_uploader("Upload CSV with 'Quote' and 'Author' columns", type="csv")
    if file:
        df = pd.read_csv(file)
        if 'Quote' not in df.columns or 'Author' not in df.columns:
            st.error("Missing required columns")
        else:
            df = df[df['Quote'].apply(lambda x: isinstance(x, str) and len(x.strip()) <= 180)]
            groups = []
            for author, group in df.groupby('Author'):
                quotes = group['Quote'].dropna().tolist()[:8]
                quotes += ['NA'] * (8 - len(quotes))
                groups.append(quotes + [author])
            columns = [f"s{i}paragraph1" for i in range(2, 10)] + ['Author']
            final = pd.DataFrame(groups, columns=columns)
            ts = int(time.time())
            st.download_button("Download Structured CSV", final.to_csv(index=False), file_name=f"structured_quotes_{ts}.csv")

# ------------------- TAB 3 -------------------
with tab3:
    
    client = AzureOpenAI(
        api_key=st.secrets["azure_openai_api_key"],
        api_version="2025-03-01-preview",
        azure_endpoint="https://suvichaarai008818057333687.cognitiveservices.azure.com"
        )
    deployment_model = "gpt-4o-global-batch"
    
    # ============================ 🎯 Title ============================
    st.title("🧠 Quote Metadata Generator & Azure OpenAI JSONL Creator")
    
    # ============================ 📤 File Upload ============================
    uploaded_file = st.file_uploader("Upload your CSV with Author + s2paragraph1 to s9paragraph1", type=["csv"])
    
    if uploaded_file:
        ts = str(int(time.time()))
        df = pd.read_csv(uploaded_file)
        df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)
        df.replace("NA", pd.NA, inplace=True)
    
        # Separate clean and removed
        clean = df.dropna()
        removed = df[df.isna().any(axis=1)]
    
        # Save cleaned CSV
        cleaned_csv = f"cleaned_data_{ts}.csv"
        removed_csv = f"removed_data_{ts}.csv"
        clean.to_csv(cleaned_csv, index=False)
        removed.to_csv(removed_csv, index=False)
    
        st.download_button("📥 Download Cleaned CSV", data=clean.to_csv(index=False), file_name=cleaned_csv, mime="text/csv")
        st.download_button("📥 Download Removed Rows CSV", data=removed.to_csv(index=False), file_name=removed_csv, mime="text/csv")
    
        # ============================ 🆔 Generate custom_id ============================
        author_map, counter = {}, {}
        ids, gcount = [], 1
        for _, row in clean.iterrows():
            a = row['Author'].strip()
            k = a.replace(" ", "_")
            if a not in author_map:
                author_map[a], counter[a] = gcount, 1
                gcount += 1
            else:
                counter[a] += 1
            ids.append(f"{author_map[a]}-{k}-{counter[a]}")
        clean["custom_id"] = ids
        final = clean[["custom_id"] + [c for c in clean.columns if c != "custom_id"]]
    
        structured_csv = f"structured-data-id_{ts}.csv"
        final.to_csv(structured_csv, index=False)
        st.download_button("📥 Download Structured CSV", data=final.to_csv(index=False), file_name=structured_csv, mime="text/csv")
    
        # ============================ 🔄 Generate JSONL ============================
        payloads = []
        for _, row in final.iterrows():
            quotes = [row.get(f"s{i}paragraph1", '') for i in range(2, 10)]
            block = "\n".join(f"- {q}" for q in quotes if q and q != "NA")
            author = row['Author']
            prompt = f"You're given a series of quotes by {author}.\nUse them to generate metadata for a web story.\nQuotes:\n{block}\n\nPlease respond ONLY in this exact JSON format:\n{{\n  \"storytitle\": \"...\",\n  \"metadescription\": \"...\",\n  \"metakeywords\": \"...\"\n}}"
            payloads.append({
                "custom_id": row["custom_id"],
                "method": "POST",
                "url": "/chat/completions",
                "body": {
                    "model": deployment_model,
                    "messages": [
                        {"role": "system", "content": "You are a creative and SEO-savvy content writer."},
                        {"role": "user", "content": prompt}
                    ]
                }
            })
    
        jsonl_filename = f"quotefancy_azure_batch_{ts}.jsonl"
        jsonl_str = '\n'.join(json.dumps(record) for record in payloads)
        st.download_button("📥 Download JSONL File", data=jsonl_str, file_name=jsonl_filename, mime="application/jsonl")
    
        # ============================ ⬆️ Upload JSONL to Azure ============================
        with open(jsonl_filename, "w") as f:
            f.write(jsonl_str)
        with open(jsonl_filename, "rb") as f:
            batch_file = client.files.create(
                file=f,
                purpose="batch",
                extra_body={"expires_after": {"seconds": 1209600, "anchor": "created_at"}}
            )
        file_id = batch_file.id
        st.success("✅ File uploaded to Azure.")
        st.json(batch_file.model_dump())
    
        # ============================ 🚀 Submit Batch Job ============================
        batch_job = client.batches.create(
            input_file_id=file_id,
            endpoint="/chat/completions",
            completion_window="24h",
            extra_body={"output_expires_after": {"seconds": 1209600, "anchor": "created_at"}}
        )
        batch_id = batch_job.id
        st.success(f"🚀 Batch job submitted successfully! Batch ID: {batch_id}")
    
        # ============================ 💾 Save Tracking Info ============================
        tracking_info = {
            "ts": ts,
            "batch_id": batch_id,
            "file_id": file_id,
            "jsonl_file": jsonl_filename,
            "csv_file": structured_csv
        }
        track_filename = f"azure_batch_tracking_{ts}.json"
        st.download_button("📥 Download Tracking JSON", data=json.dumps(tracking_info, indent=2), file_name=track_filename, mime="application/json")
    
        st.info("✅ You can now close the app or monitor the batch using the Batch ID.")

# ------------------- TAB 4 -------------------
# ============================ 🔐 Azure Credentials ============================
with tab4:
    AZURE_OPENAI_API_KEY = st.secrets["azure_openai_api_key"]
    AZURE_STORAGE_CONNECTION_STRING = st.secrets["azure_blob_connection_string"]
    AZURE_STORAGE_CONTAINER = "suvichaarbatch1"
    AZURE_BLOB_ACCOUNT_NAME = "suvichaarblob"
    AZURE_BLOB_ACCOUNT_KEY = st.secrets["azure_blob_account_key"]
    
    client = AzureOpenAI(
        api_key=AZURE_OPENAI_API_KEY,
        api_version="2025-03-01-preview",
        azure_endpoint="https://suvichaarai008818057333687.cognitiveservices.azure.com"
    )
    
    # ============================ 🎯 UI ============================
    st.title("📦 Azure Batch Result Fetcher & Blob Uploader")
    uploaded_file = st.file_uploader("📤 Upload your `azure_batch_tracking_*.json` file", type=["json"])
    
    if uploaded_file:
        tracking_info = json.load(uploaded_file)
        batch_id = tracking_info.get("batch_id")
        ts = tracking_info.get("ts")
        output_filename = f"batch_results_{ts}.jsonl"
    
        st.info(f"🔍 Checking status for Batch ID: `{batch_id}`...")
        batch_job = client.batches.retrieve(batch_id)
        status = batch_job.status
        st.write(f"📊 Batch Status: **{status}**")
    
        if status != "completed":
            st.warning("⚠️ Batch not ready yet. Please try again later.")
        else:
            output_file_id = batch_job.output_file_id or batch_job.error_file_id
            if not output_file_id:
                st.error("❌ No output or error file found in batch job.")
            else:
                # ============================ 📥 Download Batch Output ============================
                file_response = client.files.content(output_file_id)
                raw_lines = file_response.text.strip().split('\n')
                with open(output_filename, "w") as f:
                    for line in raw_lines:
                        f.write(line + "\n")
    
                with open(output_filename, "rb") as f:
                    st.download_button("📥 Download Results JSONL", data=f, file_name=output_filename, mime="application/jsonl")
    
                # ============================ ☁️ Upload to Azure Blob ============================
                blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
                container_client = blob_service_client.get_container_client(AZURE_STORAGE_CONTAINER)
    
                with open(output_filename, "rb") as data:
                    container_client.upload_blob(
                        name=output_filename,
                        data=data,
                        overwrite=True,
                        content_settings=ContentSettings(content_type="application/json")
                    )
    
                # ============================ 🔗 Generate SAS URL ============================
                sas_token = generate_blob_sas(
                    account_name=AZURE_BLOB_ACCOUNT_NAME,
                    container_name=AZURE_STORAGE_CONTAINER,
                    blob_name=output_filename,
                    account_key=AZURE_BLOB_ACCOUNT_KEY,
                    permission=BlobSasPermissions(read=True),
                    expiry=datetime.datetime.utcnow() + datetime.timedelta(days=1)
                )
    
                blob_url = f"https://{AZURE_BLOB_ACCOUNT_NAME}.blob.core.windows.net/{AZURE_STORAGE_CONTAINER}/{output_filename}?{sas_token}"
                st.success("✅ File uploaded to Azure Blob Storage.")
                st.markdown(f"📎 [Click here to download from Azure Blob]({blob_url})", unsafe_allow_html=True)

# ------------------- TAB 5 -------------------
with tab5:
    st.header("👤 Distinct Author Extractor")
    file = st.file_uploader("Upload CSV with 'Author' column", type="csv", key="auth_csv")
    if file:
        df = pd.read_csv(file)
        if 'Author' not in df.columns:
            st.error("Missing 'Author' column")
        else:
            authors = ', '.join(sorted(df['Author'].dropna().unique()))
            st.text_area("Distinct Authors", authors, height=200)

# ------------------- TAB 6 -------------------
with tab6:
    aws_access_key = st.secrets["aws_access_key"]
    aws_secret_key = st.secrets["aws_secret_key"]
    region_name = "ap-south-1"
    bucket_name = "suvichaarapp"
    s3_prefix = "media/"
    cdn_base_url = "https://cdn.suvichaar.org/"
    
    # ============================ 🎯 UI ============================
    st.title("🖼️ Bulk Image Downloader + S3 Uploader + CDN Transformer")
    
    keywords_input = st.text_input("Enter keywords (comma-separated)", "cat,dog,car")
    count = st.number_input("Number of images per keyword", min_value=1, max_value=50, value=5)
    filename_input = st.text_input("Output CSV filename", "image_links")
    
    # ============================ 🚀 Main Process ============================
    if st.button("🚀 Download, Upload, and Transform"):
        if os.path.exists("simple_images"):
            shutil.rmtree("simple_images")
    
        response = simp.simple_image_download()
        keywords = [k.strip() for k in keywords_input.split(",") if k.strip()]
    
        for keyword in keywords:
            st.write(f"📥 Downloading {count} images for {keyword}")
            response.download(keyword, count)
    
        # Initialize S3 client
        s3 = boto3.client("s3", aws_access_key_id=aws_access_key,
                          aws_secret_access_key=aws_secret_key, region_name=region_name)
    
        results = []
    
        for folder, _, files_ in os.walk("simple_images"):
            for f in files_:
                path = os.path.join(folder, f)
                kf = os.path.basename(folder).replace(" ", "-")
                fname = f.replace(" ", "-")
                key = f"{s3_prefix}{kf}/{fname}"
                try:
                    s3.upload_file(path, bucket_name, key)
                    cdn_url = f"{cdn_base_url}{key}"
                    results.append([kf, fname, cdn_url])
                    st.write(f"✅ Uploaded {fname}")
                except Exception as e:
                    st.error(f"❌ Failed to upload {fname}: {e}")
    
        # Create DataFrame from results
        df = pd.DataFrame(results, columns=["Keyword", "Filename", "CDN_URL"])
    
        # ============================ 🔄 Transform URLs ============================
        st.write("🔄 Transforming CDN URLs...")
    
        template = {
            "bucket": bucket_name,
            "key": "keyValue",
            "edits": {
                "resize": {
                    "width": 720,
                    "height": 1280,
                    "fit": "cover"
                }
            }
        }
    
        transformed_urls = []
    
        for idx, row in df.iterrows():
            media_url = str(row["CDN_URL"]).strip()
            try:
                if not media_url.startswith("https://cdn.suvichaar.org/") and not media_url.startswith("https://media.suvichaar.org/"):
                    raise ValueError(f"Unsupported domain in URL: {media_url}")
    
                if media_url.startswith("https://cdn.suvichaar.org/"):
                    media_url = media_url.replace("https://cdn.suvichaar.org/", "https://media.suvichaar.org/")
    
                key_value = media_url.replace("https://media.suvichaar.org/", "")
                template["key"] = key_value
                encoded = base64.urlsafe_b64encode(json.dumps(template).encode()).decode()
                final_url = f"https://media.suvichaar.org/{encoded}"
                transformed_urls.append(final_url)
            except Exception as e:
                st.error(f"⚠️ Error in row {idx}: {e}")
                transformed_urls.append("ERROR")
    
        df["standardurl"] = transformed_urls
    
        # ============================ 📁 Show + Download ============================
        st.dataframe(df.head())
        output_csv = df.to_csv(index=False)
        st.download_button("📥 Download Image Links CSV", output_csv, file_name=f"{filename_input}.csv", mime="text/csv")
# ------------------- TAB 7 -------------------
with tab7:
    st.header("📅 Merge Metadata into Structured CSV")
    up_csv = st.file_uploader("Upload structured_datawith_id.csv", type="csv", key="tab6csv")
    up_jsonl = st.file_uploader("Upload metadata.jsonl", type="jsonl", key="tab6jsonl")
    if up_csv and up_jsonl:
        try:
            def norm(cid):
                cid = str(cid).strip().lower()
                m = re.match(r"(\d+)-(.+)", cid)
                return f"{int(m.group(1))}-{m.group(2)}" if m else cid

            df = pd.read_csv(up_csv)
            df["custom_id_normalized"] = df["custom_id"].apply(norm)

            meta_map = {}
            for line in up_jsonl.read().decode().splitlines():
                try:
                    obj = json.loads(line)
                    rid = norm(obj.get("custom_id", ""))
                    raw = obj["response"]["body"]["choices"][0]["message"]["content"]
                    clean = re.sub(r"^```json\\s*|\\s*```$", "", raw.strip())
                    meta = json.loads(clean)
                    meta_map[rid] = meta
                except:
                    continue

            df["storytitle"] = df["custom_id_normalized"].map(lambda x: meta_map.get(x, {}).get("storytitle", ""))
            df["metadescription"] = df["custom_id_normalized"].map(lambda x: meta_map.get(x, {}).get("metadescription", ""))
            df["metakeywords"] = df["custom_id_normalized"].map(lambda x: meta_map.get(x, {}).get("metakeywords", ""))
            df.drop(columns=["custom_id_normalized"], inplace=True)

            ts = str(int(time.time()))
            st.download_button("Download Merged CSV", data=df.to_csv(index=False), file_name=f"Textual-Data-Quote-Fancy_{ts}.csv")
        except Exception as e:
            st.error(f"Error: {e}")
