import streamlit as st
import pandas as pd
import time
import csv
import io
import re
import json
import os
import shutil
from urllib.parse import urlparse
from bs4 import BeautifulSoup
import requests
from requests.adapters import HTTPAdapter, Retry
import boto3
from simple_image_download import simple_image_download as simp

st.set_page_config(page_title="Quote Utility Toolkit", layout="wide")

tab1, tab2, tab3, tab4, tab5, tab6 = st.tabs([
    "🕸️ Scrape Quotes from QuoteFancy",
    "📊 Structure Quotes by Author",
    "👤 Distinct Author Extractor",
    "🖼️ Bulk Image Downloader + S3 CDN Uploader",
    "🧠 CSV Cleaner + Azure Batch JSONL Generator",
    "📅 Merge Metadata into Structured CSV"
])

# ------------------- TAB 1 -------------------
with tab1:
    st.header("🕸️ Scrape Quotes from QuoteFancy")
    
    USER_AGENT = "Mozilla/5.0 ... Safari/537.36"
    REQUEST_TIMEOUT = 10
    DELAY_BETWEEN_PAGES = 1

    def create_session():
        session = requests.Session()
        session.headers.update({'User-Agent': USER_AGENT})
        retries = Retry(total=3, backoff_factor=0.3, status_forcelist=[500, 502, 503, 504])
        session.mount("http://", HTTPAdapter(max_retries=retries))
        session.mount("https://", HTTPAdapter(max_retries=retries))
        return session

    def extract_slug(url):
        return urlparse(url).path.strip("/").split("/")[0]

    def scrape(slug, max_pages):
        session = create_session()
        rows, sn = [], 1
        for page in range(1, max_pages + 1):
            url = f"https://quotefancy.com/{slug}/page/{page}"
            try:
                res = session.get(url, timeout=REQUEST_TIMEOUT)
                res.raise_for_status()
            except:
                break
            soup = BeautifulSoup(res.content, "html.parser")
            items = soup.find_all("div", class_="q-wrapper")
            if not items: break
            for item in items:
                qt = item.find("div", class_="quote-a") or item.find("a", class_="quote-a")
                txt = qt.get_text(strip=True) if qt else ""
                href = qt.find("a").get("href", "") if qt and qt.find("a") else ""
                auth = item.find("div", class_="author-p bylines")
                auth = auth.get_text(strip=True).replace("by ", "") if auth else "Anonymous"
                rows.append([sn, txt, href, auth])
                sn += 1
            time.sleep(DELAY_BETWEEN_PAGES)
        return rows

    urls = st.text_area("Enter QuoteFancy URLs (comma separated)")
    max_pages = st.slider("Max Pages per Slug", 1, 20, 10)

    if st.button("Scrape Quotes"):
        if not urls.strip():
            st.error("Enter at least one URL")
        else:
            all_rows = []
            for u in urls.split(','):
                slug = extract_slug(u.strip())
                st.info(f"Scraping {slug}")
                all_rows.extend(scrape(slug, max_pages))
            if all_rows:
                out = io.StringIO()
                writer = csv.writer(out)
                writer.writerow(["Serial No", "Quote", "Link", "Author"])
                writer.writerows(all_rows)
                ts = int(time.time())
                st.download_button("Download CSV", out.getvalue(), file_name=f"quotes_{ts}.csv", mime="text/csv")
            else:
                st.warning("No quotes found")

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
    st.header("👤 Distinct Author Extractor")
    file = st.file_uploader("Upload CSV with 'Author' column", type="csv", key="auth_csv")
    if file:
        df = pd.read_csv(file)
        if 'Author' not in df.columns:
            st.error("Missing 'Author' column")
        else:
            authors = ', '.join(sorted(df['Author'].dropna().unique()))
            st.text_area("Distinct Authors", authors, height=200)

# ------------------- TAB 4 -------------------
with tab4:
    st.header("🖼️ Bulk Image Downloader + S3 CDN Uploader")
    aws_access_key = st.secrets["aws_access_key"]
    aws_secret_key = st.secrets["aws_secret_key"]
    region_name = "ap-south-1"
    bucket_name = "suvichaarapp"
    s3_prefix = "media/"
    cdn_base_url = "https://media.suvichaar.org/"

    s3 = boto3.client("s3", aws_access_key_id=aws_access_key, aws_secret_access_key=aws_secret_key, region_name=region_name)

    keywords_input = st.text_input("Enter keywords", "cat,dog,car")
    count = st.number_input("Images per keyword", 1, 20, 5)
    filename_input = st.text_input("Output CSV filename", "image_links")

    if st.button("Download & Upload Images"):
        if os.path.exists("simple_images"): shutil.rmtree("simple_images")
        for keyword in [k.strip() for k in keywords_input.split(",") if k.strip()]:
            st.write(f"Downloading {count} images for {keyword}")
            simp.simple_image_download().download(keyword, count)

        results = []
        for folder, _, files_ in os.walk("simple_images"):
            for f in files_:
                path = os.path.join(folder, f)
                kf = os.path.basename(folder).replace(" ", "-")
                fname = f.replace(" ", "-")
                key = f"{s3_prefix}{kf}/{fname}"
                try:
                    s3.upload_file(path, bucket_name, key)
                    results.append([kf, fname, f"{cdn_base_url}{key}"])
                except Exception as e:
                    st.error(f"Failed for {fname}: {e}")
        out = io.StringIO()
        csv.writer(out).writerows([["Keyword", "Filename", "CDN_URL"]] + results)
        st.download_button("Download CDN CSV", out.getvalue(), file_name=f"{filename_input}.csv")

# ------------------- TAB 5 -------------------
with tab5:
    st.header("🧠 CSV Cleaner + Azure Batch JSONL Generator")
    upload = st.file_uploader("Upload CSV with Author + s2paragraph1 to s9paragraph1", type="csv", key="csv5")
    if upload:
        df = pd.read_csv(upload)
        df.replace(r'^\s*$', pd.NA, regex=True, inplace=True)
        df.replace("NA", pd.NA, inplace=True)
        clean = df.dropna()
        removed = df[df.isna().any(axis=1)]
        ts = str(int(time.time()))
        st.download_button("Cleaned CSV", clean.to_csv(index=False), file_name=f"cleaned_data_{ts}.csv")
        st.download_button("Removed Rows CSV", removed.to_csv(index=False), file_name=f"removed_data_{ts}.csv")

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
        st.download_button("Structured CSV", final.to_csv(index=False), file_name=f"structured_datawith_id_{ts}.csv")

        payloads = []
        for _, row in final.iterrows():
            quotes = [row.get(f"s{i}paragraph1", '') for i in range(2, 10)]
            block = "\n".join(f"- {q}" for q in quotes if q and q != "NA")
            author = row['Author']
            prompt = f"You're given a series of quotes by {author}.\nUse them to generate metadata for a web story.\nQuotes:\n{block}\n\nPlease respond ONLY in this exact JSON format:\n{{\n  \"storytitle\": \"...\",\n  \"metadescription\": \"...\",\n  \"metakeywords\": \"...\"\n}}"
            payloads.append({"custom_id": row["custom_id"], "method": "POST", "url": "/chat/completions",
                              "body": {"model": "gpt-4o-global-batch", "messages": [
                                  {"role": "system", "content": "You are a creative and SEO-savvy content writer."},
                                  {"role": "user", "content": prompt}]}})

        jsonl_str = "\n".join(json.dumps(p) for p in payloads)
        st.download_button("Download JSONL Batch", data=jsonl_str, file_name=f"quotefancy_azure_batch_{ts}.jsonl")

# ------------------- TAB 6 -------------------
with tab6:
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
