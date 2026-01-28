import streamlit as st
from scraper import scrape_jumia_category
from io import BytesIO

st.set_page_config(page_title="Jumia Scraper", layout="centered")

st.title("🛒 Jumia Multi-Page Scraper")
st.write("Scrape **all products** from any Jumia country category.")

url = st.text_input(
    "Enter Jumia Category URL",
    placeholder="https://www.jumia.co.ke/phones-tablets/"
)

log_box = st.empty()

logs = []


def log(msg):
    logs.append(msg)
    log_box.code("\n".join(logs[-15:]))


if st.button("🚀 Start Scraping"):
    if not url:
        st.error("Please enter a category URL")
    else:
        logs.clear()
        with st.spinner("Scraping in progress..."):
            df = scrape_jumia_category(url, log_callback=log)

        st.success(f"✅ Scraping complete! {len(df)} products found")

        # Download CSV
        buffer = BytesIO()
        df.to_csv(buffer, index=False)
        buffer.seek(0)

        st.download_button(
            label="⬇️ Download CSV",
            data=buffer,
            file_name="jumia_products.csv",
            mime="text/csv"
        )

        st.dataframe(df.head(20))
