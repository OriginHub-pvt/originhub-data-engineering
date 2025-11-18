import streamlit as st
import json
import pandas as pd
import io
from typing import List, Dict, Any


class JsonLabelingApp:
    """
    A modular Streamlit application for labeling a list of JSON objects.
    Supports manual labeling, navigation, preview toggling, and downloading results.
    """

    def __init__(self):
        """Initialize app state and configuration."""
        st.set_page_config(page_title="JSON Labeling Tool", layout="wide")
        self._setup_session_state()
        self.data: List[Dict[str, Any]] = []
        self.run_app()

    def _setup_session_state(self) -> None:
        """Ensure necessary session variables exist."""
        defaults = {
            "index": 0,
            "labels": [],
            "expanded": False,
            "saved": False,
        }
        for key, value in defaults.items():
            if key not in st.session_state:
                st.session_state[key] = value

    def run_app(self) -> None:
        """Main app execution flow."""
        st.title("JSON Labeling App")
        uploaded_file = st.file_uploader("Upload your JSON file", type=["json"])

        if uploaded_file:
            self.load_json(uploaded_file)
            self.display_entry()
            self.display_controls()
            self.display_progress()
            self.display_save_section()
        else:
            st.info("Upload a JSON file containing a list of items to start labeling.")

    def load_json(self, uploaded_file) -> None:
        """Load JSON data and validate it."""
        self.data = json.load(uploaded_file)
        if not isinstance(self.data, list):
            st.error("JSON must be a list of objects!")
            st.stop()

        # Initialize labels if first time loading
        if len(st.session_state.labels) != len(self.data):
            st.session_state.labels = [None] * len(self.data)

    def display_entry(self) -> None:
        """Display the current JSON entry and description preview."""
        idx = st.session_state.index
        total = len(self.data)
        item = self.data[idx]

        st.markdown(f"### Entry {idx + 1} / {total}")
        st.markdown(f"**Title:** {item.get('title', 'N/A')}")
        st.markdown(f"**URL:** {item.get('url', 'N/A')}")

        desc = item.get("description", "")
        st.markdown("**Description:**")
        self._display_description(desc, idx)

    def _display_description(self, desc: str, idx: int) -> None:
        """Show either preview or full description text."""
        if st.session_state.expanded:
            st.write(desc)
        else:
            preview = desc[:100] + ("..." if len(desc) > 100 else "")
            st.write(preview)

        # Show more / less (always visible, disabled when short)
        if st.session_state.expanded:
            st.button(
                "Show less",
                key=f"less_{idx}",
                on_click=lambda: st.session_state.update(expanded=False),
                disabled=len(desc) <= 100,
            )
        else:
            st.button(
                "Show more",
                key=f"more_{idx}",
                on_click=lambda: st.session_state.update(expanded=True),
                disabled=len(desc) <= 100,
            )

    def display_controls(self) -> None:
        """Render labeling and navigation buttons."""
        idx = st.session_state.index
        total = len(self.data)

        # Label Buttons
        col1, col2 = st.columns(2)
        with col1:
            st.button("Relevant (1)", on_click=self._label_and_next, args=(1,), key=f"rel_{idx}")
        with col2:
            st.button("Irrelevant (0)", on_click=self._label_and_next, args=(0,), key=f"irrel_{idx}")

        # Navigation Buttons
        col_prev, col_next = st.columns(2)
        with col_prev:
            st.button(
                "⬅️ Previous",
                disabled=idx == 0,
                on_click=lambda: st.session_state.update(index=st.session_state.index - 1, expanded=False),
            )
        with col_next:
            st.button(
                "Next",
                disabled=idx >= total - 1,
                on_click=lambda: st.session_state.update(index=st.session_state.index + 1, expanded=False),
            )

    def _label_and_next(self, label_value: int) -> None:
        """
        Assign a label to the current entry and move to the next one.
        Handles empty data safely.
        """
        idx = st.session_state.index

        # If there's no data, exit early
        if not hasattr(self, "data") or len(self.data) == 0:
            return

        # Ensure labels list has correct length
        if idx >= len(st.session_state.labels):
            st.session_state.labels = [None] * len(self.data)

        st.session_state.labels[idx] = label_value
        st.session_state.expanded = False
        if idx < len(self.data) - 1:
            st.session_state.index += 1


    def display_progress(self) -> None:
        """Show progress bar and count of labeled entries."""
        labeled_count = sum(l is not None for l in st.session_state.labels)
        total = len(self.data)
        st.progress(labeled_count / total)
        st.write(f"Labeled: {labeled_count}/{total}")

    def display_save_section(self) -> None:
        """Provide Save and Download controls after labeling."""
        if st.button("Save Labels"):
            for i, label in enumerate(st.session_state.labels):
                self.data[i]["label"] = label
            st.session_state.saved = True
            st.success("Labels processed! You can now download the files below")

        if st.session_state.saved:
            self._render_download_buttons()

    def _render_download_buttons(self) -> None:
        """Render download buttons for JSON and CSV output."""
        json_bytes = self._generate_json_bytes(self.data)
        csv_bytes = self._generate_csv_bytes(self.data)

        colj, colc = st.columns(2)
        with colj:
            st.download_button(
                "Download JSON",
                data=json_bytes,
                file_name="labeled_output.json",
                mime="application/json",
            )
        with colc:
            st.download_button(
                "Download CSV",
                data=csv_bytes,
                file_name="labeled_output.csv",
                mime="text/csv",
            )

    @staticmethod
    def _generate_json_bytes(data: List[Dict[str, Any]]) -> bytes:
        """Generate labeled JSON file bytes."""
        buffer = io.StringIO()
        json.dump(data, buffer, indent=2, ensure_ascii=False)
        return buffer.getvalue().encode("utf-8")

    @staticmethod
    def _generate_csv_bytes(data: List[Dict[str, Any]]) -> bytes:
        """Generate labeled CSV file bytes."""
        buffer = io.StringIO()
        pd.DataFrame(data).to_csv(buffer, index=False, encoding="utf-8")
        return buffer.getvalue().encode("utf-8")


if __name__ == "__main__":
    JsonLabelingApp()