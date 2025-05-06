# 📝 QuoteFancy Scraper

A simple yet powerful Streamlit web app to scrape quotes from [QuoteFancy.com](https://quotefancy.com/) based on author URLs. Built to support quote collection, structuring, and preparation for downstream NLP or metadata generation tasks.

---

## 🚀 Features

- 🔗 Input multiple QuoteFancy URLs (comma-separated)
- 🧠 Automatically extracts quotes and author names
- 📄 Exports a clean CSV file for further use
- 💾 Customizable output filename
- ⚡ Fast, minimal, and easy-to-use interface
- 🎯 Ideal for AI content generation pipelines

---

## 📷 Interface Overview

![QuoteFancy Scraper Screenshot]<img width="2240" alt="Screenshot 2025-05-06 at 9 32 55 PM" src="https://github.com/user-attachments/assets/67dce0f1-0a23-49fa-b8c9-f06ae7e9365c" />
 <!-- Replace with your own if hosted -->

- **Enter QuoteFancy URLs**: Paste multiple URLs separated by commas
- **Filename Prefix**: Choose your output file’s base name (e.g., `quotes.csv`)
- **Start Scraping**: One click to fetch, parse, and download!

---

## 💻 How to Run Locally

### 1. Clone this repository

```bash
git clone https://github.com/your-username/quotefancy-scraper.git
cd quotefancy-scraper
```

### 2. Install dependencies

```bash
pip install streamlit pandas requests beautifulsoup4
```

### 3. Run the Streamlit app

```bash
streamlit run app.py
```

📁 Output

After scraping, the app provides a CSV with:
	•	Author
	•	Quote
	•	Source URL

Perfect for feeding into content pipelines, LLM metadata prompts, or social quote sharing tools.

🔧 Example Input

```bash
[streamlit run app.py](https://quotefancy.com/a-j-cronin-quotes, https://quotefancy.com/a-j-hawk-quotes)
```

📌 Related Tools

This app integrates well with:
	•	✅ Quote Structurer
	•	🧠 Metadata Prompt Generator (Azure OpenAI JSONL Creator)
	•	📤 Azure Batch Result Fetcher & Blob Uploader
	•	🖼️ Bulk Image Downloader + S3 CDN Uploader


👤 Author

Developed by Kumar Mayank
📧 krmayank2002@gmail.com

📄 License

MIT License © 2025 Kumar Mayank

