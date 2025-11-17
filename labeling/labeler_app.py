import streamlit as st
import json
import pandas as pd
from typing import List, Dict, Any


class JsonLabelingApp:

    def __init__(self):
        st.set_page_config(page_title="JSON Labeling Tool", layout="wide")
        self._setup_session_state()
        self.data: List[Dict[str, Any]] = []
        self.run_app()

    def _setup_session_state(self):
        defaults = {
            "index": 0,
            "labels": [],
            "meta": [],
            "expanded": False,
            "saved": False,
        }
        for k, v in defaults.items():
            if k not in st.session_state:
                st.session_state[k] = v

    def run_app(self):
        st.title("JSON/CSV Labeling App")

        uploaded_file = st.file_uploader("Upload JSON or CSV", type=["json", "csv"])

        if uploaded_file:
            self.load_data(uploaded_file)
            self.display_entry()
            self.display_controls()
            self.display_progress()
            self.display_save_section()
        else:
            st.info("Upload a JSON or CSV file containing article data to start labeling.")

    def load_data(self, uploaded_file):
        file_type = uploaded_file.name.split(".")[-1].lower()

        if file_type == "json":
            self.data = json.load(uploaded_file)
        else:
            df = pd.read_csv(uploaded_file)
            self.data = df.to_dict(orient="records")

        if not isinstance(self.data, list):
            st.error("Uploaded data must be a list of objects or CSV rows.")
            st.stop()

        if len(st.session_state.labels) != len(self.data):
            st.session_state.labels = [None] * len(self.data)

        if len(st.session_state.meta) != len(self.data):
            st.session_state.meta = [self._empty_meta(item) for item in self.data]

    def _empty_meta(self, item):
        desc = item.get("description", "") or ""

        length_bucket = (
            "short" if len(desc) < 150 else
            "medium" if len(desc) < 400 else
            "long"
        )

        return {
            "ai_group": "Unknown",
            "source_type": "Unknown",
            "region": "Unknown",
            "article_type": "Unknown",
            "org_type": "Unknown",
            "length_bucket": length_bucket
        }

    def display_entry(self):
        idx = st.session_state.index
        item = self.data[idx]

        st.markdown(f"### Entry {idx+1}/{len(self.data)}")
        st.write(f"**Title:** {item.get('title','N/A')}")
        st.write(f"**URL:** {item.get('url','N/A')}")

        desc = item.get("description", "")
        st.write("**Description:**")

        if st.session_state.expanded:
            st.write(desc)
        else:
            st.write(desc[:150] + ("..." if len(desc) > 150 else ""))

        st.button(
            "Show more" if not st.session_state.expanded else "Show less",
            on_click=lambda: st.session_state.update(expanded=not st.session_state.expanded)
        )

        st.subheader("Metadata Labels")
        meta = st.session_state.meta[idx]

        col1, col2, col3 = st.columns(3)

        with col1:
            meta["ai_group"] = st.selectbox(
                "AI Group", ["AI", "Non-AI", "Unknown"],
                index=["AI", "Non-AI", "Unknown"].index(meta["ai_group"])
            )
            meta["source_type"] = st.selectbox(
                "Source Type",
                ["Major media", "Blog", "Corporate PR", "Other", "Unknown"],
                index=["Major media", "Blog", "Corporate PR", "Other", "Unknown"].index(meta["source_type"])
            )

        with col2:
            meta["region"] = st.selectbox(
                "Region",
                ["US", "Europe", "India", "SEA", "LatAm", "Other", "Unknown"],
                index=["US", "Europe", "India", "SEA", "LatAm", "Other", "Unknown"].index(meta["region"])
            )
            meta["article_type"] = st.selectbox(
                "Article Type",
                ["Listicle", "Launch/Announcement", "How-to/Tutorial", "Opinion",
                 "Analysis/Deep dive", "Other", "Unknown"],
                index=["Listicle", "Launch/Announcement", "How-to/Tutorial", "Opinion",
                       "Analysis/Deep dive", "Other", "Unknown"].index(meta["article_type"])
            )

        with col3:
            meta["org_type"] = st.selectbox(
                "Org Type", ["Startup", "MNC", "Unknown"],
                index=["Startup", "MNC", "Unknown"].index(meta["org_type"])
            )
            st.write(f"**Length Bucket:** {meta['length_bucket']}")

        st.session_state.meta[idx] = meta

    def display_controls(self):
        idx = st.session_state.index
        total = len(self.data)

        col1, col2 = st.columns(2)
        with col1:
            st.button("Relevant (1)", on_click=self._label_and_next, args=(1,))
        with col2:
            st.button("Irrelevant (0)", on_click=self._label_and_next, args=(0,))

        col_prev, col_next = st.columns(2)

        with col_prev:
            st.button("⬅️ Previous", disabled=idx == 0,
                      on_click=lambda: st.session_state.update(index=idx - 1, expanded=False))

        with col_next:
            st.button("Next", disabled=idx >= total - 1,
                      on_click=lambda: st.session_state.update(index=idx + 1, expanded=False))

    def _label_and_next(self, value):
        idx = st.session_state.index
        st.session_state.labels[idx] = value
        st.session_state.expanded = False
        if idx < len(self.data) - 1:
            st.session_state.index += 1

    def display_progress(self):
        labeled = sum(l is not None for l in st.session_state.labels)
        total = len(self.data)
        st.progress(labeled / total)
        st.write(f"Labeled: {labeled}/{total}")

    def display_save_section(self):
        if st.button("Save"):
            for i, item in enumerate(self.data):
                item["label"] = st.session_state.labels[i]
                item.update(st.session_state.meta[i])

            st.session_state.saved = True
            st.success("Saved! Download your labeled files below.")

        if st.session_state.saved:
            col1, col2 = st.columns(2)
            with col1:
                st.download_button("Download JSON", self._to_json(), "labeled_output.json")

            with col2:
                st.download_button("Download CSV", self._to_csv(), "labeled_output.csv")

    def _to_json(self):
        return json.dumps(self.data, indent=2).encode("utf-8")

    def _to_csv(self):
        df = pd.DataFrame(self.data)
        return df.to_csv(index=False).encode("utf-8")


if __name__ == "__main__":
    JsonLabelingApp()