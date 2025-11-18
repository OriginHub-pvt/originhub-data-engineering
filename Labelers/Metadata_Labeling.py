import streamlit as st
import json
import pandas as pd


class BiasLabelingApp:

    def __init__(self):
        st.set_page_config(page_title="Metadata Completion App", layout="wide")
        self._setup_session_state()
        self.data = []
        self.run()

    def _setup_session_state(self):
        defaults = {
            "index": 0,
            "meta": [],
            "expanded": False,
            "saved": False,
        }
        for key, val in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = val

    def run(self):
        st.title("Add Missing Metadata to Labeled Dataset")

        file = st.file_uploader("Upload labeled JSON or CSV", type=["json", "csv"])

        if file:
            self.load_data(file)
            self.display_entry()
            self.display_controls()
            self.display_progress()
            self.save_section()
        else:
            st.info("Upload a JSON or CSV file that already contains labels.")

    # -----------------------------------------------------
    # Load JSON/CSV
    # -----------------------------------------------------
    def load_data(self, file):
        ext = file.name.split(".")[-1].lower()

        if ext == "json":
            self.data = json.load(file)
        else:
            df = pd.read_csv(file)
            self.data = df.to_dict(orient="records")

        if len(st.session_state.meta) != len(self.data):
            st.session_state.meta = [self._init_meta(item) for item in self.data]

    # -----------------------------------------------------
    # Initialize metadata (Unknown for all)
    # -----------------------------------------------------
    def _init_meta(self, item):
        desc = item.get("description", "") or ""

        # Auto-fill but still editable by user later
        length_bucket = (
            "short" if len(desc) <= 300 else
            "medium" if len(desc) <= 800 else
            "long"
        )

        return {
            "ai_group": "Unknown",
            "source_type": "Other",
            "region": "Unknown",
            "article_type": "Other",
            "org_type": "Unknown",
            "length_bucket": length_bucket
        }

    # -----------------------------------------------------
    # Display current entry
    # -----------------------------------------------------
    def display_entry(self):
        idx = st.session_state.index
        item = self.data[idx]

        st.markdown(f"### Entry {idx+1}/{len(self.data)}")

        # Show the existing label
        label = item.get("label", None)
        if label == 1:
            st.success("Label: 1 (Relevant)")
        elif label == 0:
            st.error("Label: 0 (Irrelevant)")
        else:
            st.warning("Label: Unknown / Missing")

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

        # --------------------------
        # Metadata fields
        # --------------------------
        st.subheader("Metadata Fields (Manual Selection)")

        meta = st.session_state.meta[idx]

        col1, col2 = st.columns(2)

        with col1:
            meta["ai_group"] = st.selectbox(
                "AI Group",
                ["AI", "Non-AI", "Unknown"],
                index=["AI", "Non-AI", "Unknown"].index(meta["ai_group"])
            )

            meta["region"] = st.selectbox(
                "Region",
                ["US", "Europe", "India", "SEA", "LatAm", "Unknown"],
                index=["US", "Europe", "India", "SEA", "LatAm", "Unknown"].index(meta["region"])
            )

            meta["org_type"] = st.selectbox(
                "Org Type",
                ["Startup", "MNC", "Unknown"],
                index=["Startup", "MNC", "Unknown"].index(meta["org_type"])
            )

        with col2:
            meta["source_type"] = st.selectbox(
                "Source Type",
                ["Major media", "Blog", "Corporate PR", "Other"],
                index=["Major media", "Blog", "Corporate PR", "Other"].index(meta["source_type"])
            )

            meta["article_type"] = st.selectbox(
                "Article Type",
                ["Listicle", "Launch/Announcement", "How-to/Tutorial", "Opinion",
                "Analysis/Deep dive", "Other"],
                index=["Listicle", "Launch/Announcement", "How-to/Tutorial", "Opinion",
                    "Analysis/Deep dive", "Other"].index(meta["article_type"])
            )

            meta["length_bucket"] = st.selectbox(
                "Length Bucket",
                ["short", "medium", "long"],
                index=["short", "medium", "long"].index(meta["length_bucket"])
            )

        st.session_state.meta[idx] = meta

    # -----------------------------------------------------
    # Navigation
    # -----------------------------------------------------
    def display_controls(self):
        idx = st.session_state.index
        total = len(self.data)

        col_prev, col_next = st.columns(2)

        with col_prev:
            st.button("⬅️ Previous", disabled=idx == 0,
                    on_click=lambda: st.session_state.update(index=idx - 1, expanded=False))

        with col_next:
            st.button("Next ➡️", disabled=idx >= total - 1,
                    on_click=lambda: st.session_state.update(index=idx + 1, expanded=False))

    # -----------------------------------------------------
    # Progress
    # -----------------------------------------------------
    def display_progress(self):
        filled = sum(
            m["ai_group"] != "Unknown" or
            m["source_type"] != "Other" or
            m["region"] != "Unknown" or
            m["article_type"] != "Other" or
            m["org_type"] != "Unknown"
            for m in st.session_state.meta
        )
        total = len(self.data)

        st.progress(filled / total)
        st.write(f"Metadata filled: {filled}/{total}")

    # -----------------------------------------------------
    # Save + download
    # -----------------------------------------------------
    def save_section(self):
        if st.button("Save Metadata"):
            for i, item in enumerate(self.data):
                item.update(st.session_state.meta[i])

            st.session_state.saved = True
            st.success("Metadata added successfully!")

        if st.session_state.saved:
            col1, col2 = st.columns(2)
            with col1:
                st.download_button(
                    "Download JSON",
                    json.dumps(self.data, indent=2).encode("utf-8"),
                    "metadata_output.json"
                )
            with col2:
                df = pd.DataFrame(self.data)
                st.download_button(
                    "Download CSV",
                    df.to_csv(index=False).encode("utf-8"),
                    "metadata_output.csv"
                )


if __name__ == "__main__":
    BiasLabelingApp()
