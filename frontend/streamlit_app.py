"""
streamlit_app.py — RAG Chatbot Frontend (v3)
Clean two-panel layout: sidebar for docs, main area for chat.
Design: deep navy + electric indigo, Inter typography, glass-morphism cards.
"""
import os
from datetime import datetime
import requests
import streamlit as st

API_URL = os.getenv("API_URL", "http://localhost:8000")

st.set_page_config(
    page_title="DocChat · RAG Assistant",
    page_icon="⚡",
    layout="wide",
    initial_sidebar_state="expanded",
)

# ── Session state ─────────────────────────────────────────────────────────────
for k, v in {
    "token":         None,
    "user":          None,
    "messages":      [],
    "selected_file": None,
    "show_chunks":   False,
    "top_k":         5,
    "theme":         "dark",
}.items():
    if k not in st.session_state:
        st.session_state[k] = v

# ── CSS ───────────────────────────────────────────────────────────────────────
def inject_css(theme: str):
    if theme == "dark":
        css_vars = """
        --bg-base:      #080d1a;
        --bg-surface:   #0e1629;
        --bg-elevated:  #151f35;
        --bg-card:      rgba(21,31,53,0.85);
        --border:       rgba(99,120,200,0.18);
        --border-focus: rgba(109,93,252,0.6);
        --accent:       #6d5dfc;
        --accent-dim:   rgba(109,93,252,0.15);
        --accent-glow:  rgba(109,93,252,0.35);
        --accent2:      #a78bfa;
        --text-primary: #e8ecf4;
        --text-secondary:#8892aa;
        --text-muted:   #5a6278;
        --user-bg:      rgba(109,93,252,0.12);
        --bot-bg:       #131c30;
        --success:      #22d3a5;
        --warning:      #f59e0b;
        --error:        #f87171;
        --tag-bg:       rgba(109,93,252,0.14);
        --scrollbar:    rgba(109,93,252,0.3);
        """
    else:
        css_vars = """
        --bg-base:      #f0f2fa;
        --bg-surface:   #e8eaf6;
        --bg-elevated:  #ffffff;
        --bg-card:      rgba(255,255,255,0.92);
        --border:       rgba(109,93,252,0.15);
        --border-focus: rgba(109,93,252,0.5);
        --accent:       #5b4de8;
        --accent-dim:   rgba(91,77,232,0.1);
        --accent-glow:  rgba(91,77,232,0.25);
        --accent2:      #7c6ef5;
        --text-primary: #1a1f35;
        --text-secondary:#4b5270;
        --text-muted:   #8892aa;
        --user-bg:      rgba(91,77,232,0.08);
        --bot-bg:       #ffffff;
        --success:      #059669;
        --warning:      #d97706;
        --error:        #dc2626;
        --tag-bg:       rgba(91,77,232,0.1);
        --scrollbar:    rgba(91,77,232,0.25);
        """

    st.markdown(f"""
<style>
@import url('https://fonts.googleapis.com/css2?family=Inter:wght@300;400;500;600;700&family=JetBrains+Mono:wght@400;500&display=swap');

:root {{ {css_vars} }}

/* ── Base ── */
html, body, [data-testid="stApp"], [data-testid="stAppViewContainer"] {{
    background: var(--bg-base) !important;
    color: var(--text-primary) !important;
    font-family: 'Inter', sans-serif !important;
}}
[data-testid="stMain"],
[data-testid="stMainBlockContainer"],
[data-testid="stVerticalBlock"],
[data-testid="stVerticalBlockBorderWrapper"],
.main, .block-container {{
    background: var(--bg-base) !important;
    background-color: var(--bg-base) !important;
}}

/* ── Streamlit top toolbar (Deploy bar + kebab menu) ── */
[data-testid="stToolbar"],
[data-testid="stHeader"],
[data-testid="stDecoration"],
header[data-testid="stHeader"],
.stAppDeployButton,
#MainMenu {{
    background: var(--bg-surface) !important;
    background-color: var(--bg-surface) !important;
    border-bottom: 1px solid var(--border) !important;
}}
/* Toolbar icon buttons */
[data-testid="stToolbar"] button,
[data-testid="stToolbarActions"] button,
[data-testid="stHeader"] button {{
    color: var(--text-secondary) !important;
    background: transparent !important;
    border: none !important;
    box-shadow: none !important;
}}
[data-testid="stToolbar"] button:hover,
[data-testid="stHeader"] button:hover {{
    color: var(--text-primary) !important;
    background: var(--accent-dim) !important;
}}
/* "Deploy" button text */
[data-testid="stAppDeployButton"] span,
[data-testid="stAppDeployButton"] p {{
    color: var(--text-secondary) !important;
}}

/* ── Bottom chat input bar background ── */
[data-testid="stBottom"],
[data-testid="stBottom"] > div {{
    background: var(--bg-base) !important;
    border-top: 1px solid var(--border) !important;
}}

/* ── Popover / dropdown menus (toolbar kebab) ── */
[data-testid="stPopover"],
[data-baseweb="popover"],
[data-baseweb="menu"],
[role="listbox"],
[role="menu"] {{
    background: var(--bg-elevated) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
}}
[data-baseweb="menu"] li,
[role="option"],
[role="menuitem"] {{
    background: transparent !important;
    color: var(--text-primary) !important;
}}
[data-baseweb="menu"] li:hover,
[role="option"]:hover {{
    background: var(--accent-dim) !important;
}}

/* ── Toast / snackbar notifications ── */
[data-testid="stToast"],
[data-baseweb="toast"] {{
    background: var(--bg-elevated) !important;
    border: 1px solid var(--border) !important;
    color: var(--text-primary) !important;
    border-radius: 8px !important;
}}

/* ── Sidebar ── */
[data-testid="stSidebar"] > div:first-child {{
    background: var(--bg-surface) !important;
    border-right: 1px solid var(--border) !important;
    padding: 0 !important;
}}
[data-testid="stSidebar"] * {{ color: var(--text-primary) !important; }}

/* ── Typography ── */
h1, h2, h3, h4 {{ color: var(--text-primary) !important; font-weight: 600 !important; letter-spacing: -0.02em !important; }}
p, span, li, label, div {{ color: var(--text-primary) !important; }}
.stMarkdown p {{ line-height: 1.7 !important; }}

/* ── Page header ── */
.page-header {{
    padding: 1.5rem 0 1rem;
    border-bottom: 1px solid var(--border);
    margin-bottom: 1.25rem;
}}
.page-header h1 {{
    font-size: 1.6rem !important;
    font-weight: 700 !important;
    background: linear-gradient(135deg, var(--accent2), var(--accent));
    -webkit-background-clip: text !important;
    -webkit-text-fill-color: transparent !important;
    background-clip: text !important;
    margin: 0 !important;
}}
.page-header p {{
    font-size: 0.82rem !important;
    color: var(--text-secondary) !important;
    margin: 0.25rem 0 0 !important;
}}

/* ── Sidebar header ── */
.sidebar-brand {{
    padding: 1.2rem 1.2rem 0.8rem;
    border-bottom: 1px solid var(--border);
    margin-bottom: 0.5rem;
}}
.sidebar-brand .brand-name {{
    font-size: 1rem;
    font-weight: 700;
    letter-spacing: -0.01em;
}}
.sidebar-brand .brand-sub {{
    font-size: 0.72rem;
    color: var(--text-secondary) !important;
    margin-top: 2px;
}}

/* ── Section labels ── */
.section-label {{
    font-size: 0.68rem;
    font-weight: 600;
    letter-spacing: 0.08em;
    text-transform: uppercase;
    color: var(--text-muted) !important;
    padding: 0.9rem 1.2rem 0.35rem;
}}

/* ── Status bar ── */
.status-bar {{
    display: flex;
    align-items: center;
    gap: 0.5rem;
    padding: 0.5rem 1.2rem;
    margin: 0 0.8rem 0.4rem;
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    border-radius: 8px;
    font-size: 0.75rem;
    color: var(--text-secondary) !important;
}}
.dot-ok  {{ width:7px;height:7px;border-radius:50%;background:var(--success);flex-shrink:0; box-shadow: 0 0 6px var(--success); }}
.dot-err {{ width:7px;height:7px;border-radius:50%;background:var(--error);flex-shrink:0; }}

/* ── Stat pills ── */
.stats-row {{
    display: flex;
    gap: 0.5rem;
    padding: 0 1.2rem 0.8rem;
}}
.stat-pill {{
    flex: 1;
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    border-radius: 8px;
    padding: 0.5rem 0.6rem;
    text-align: center;
}}
.stat-val {{
    font-size: 1.1rem;
    font-weight: 700;
    color: var(--accent2) !important;
    line-height: 1;
}}
.stat-lbl {{
    font-size: 0.62rem;
    color: var(--text-muted) !important;
    text-transform: uppercase;
    letter-spacing: 0.05em;
    margin-top: 3px;
}}

/* ── Upload zone — kill every white layer Streamlit injects ── */
[data-testid="stFileUploader"],
[data-testid="stFileUploader"] > div,
[data-testid="stFileUploader"] > div > div,
[data-testid="stFileUploaderDropzone"],
[data-testid="stFileUploaderDropzoneInstructions"] {{
    background: var(--bg-elevated) !important;
    background-color: var(--bg-elevated) !important;
    border-color: var(--border-focus) !important;
    color: var(--text-primary) !important;
}}
[data-testid="stFileUploader"] {{
    border: 1.5px dashed var(--border-focus) !important;
    border-radius: 10px !important;
    padding: 0.5rem !important;
    transition: border-color 0.2s !important;
}}
[data-testid="stFileUploader"]:hover {{
    border-color: var(--accent) !important;
}}
/* Inner drop zone white box */
[data-testid="stFileUploaderDropzone"] {{
    background: var(--bg-elevated) !important;
    border: none !important;
    border-radius: 8px !important;
    padding: 1rem 0.5rem !important;
}}
/* The instructional text lines */
[data-testid="stFileUploaderDropzoneInstructions"] span,
[data-testid="stFileUploaderDropzoneInstructions"] p,
[data-testid="stFileUploaderDropzoneInstructions"] div,
[data-testid="stFileUploaderDropzoneInstructions"] small {{
    color: var(--text-secondary) !important;
}}
/* "Drag and drop" headline text */
[data-testid="stFileUploaderDropzoneInstructions"] > div > span {{
    color: var(--text-primary) !important;
    font-weight: 500 !important;
}}
/* "Limit 200MB" small text */
[data-testid="stFileUploaderDropzoneInstructions"] > div > small,
[data-testid="stFileUploaderDropzoneInstructions"] small {{
    color: var(--text-muted) !important;
    font-size: 0.72rem !important;
}}
/* Browse files button */
[data-testid="stFileUploader"] button,
[data-testid="stFileUploaderDropzone"] button {{
    background: var(--bg-surface) !important;
    color: var(--text-primary) !important;
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
    font-size: 0.78rem !important;
    padding: 0.3rem 0.8rem !important;
    box-shadow: none !important;
    transform: none !important;
}}
[data-testid="stFileUploader"] button:hover {{
    border-color: var(--accent) !important;
    background: var(--accent-dim) !important;
    color: var(--accent2) !important;
}}
/* Uploaded file chip that appears after selection */
[data-testid="stFileUploaderFile"],
[data-testid="stFileUploaderFile"] > div {{
    background: var(--bg-surface) !important;
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
    color: var(--text-primary) !important;
}}
[data-testid="stFileUploaderFile"] span,
[data-testid="stFileUploaderFile"] p {{
    color: var(--text-primary) !important;
}}

/* ── File cards ── */
.file-card {{
    background: var(--bg-elevated);
    border: 1px solid var(--border);
    border-radius: 9px;
    padding: 0.6rem 0.8rem;
    margin: 0 0 0.45rem;
    cursor: pointer;
    transition: all 0.15s ease;
    position: relative;
    overflow: hidden;
}}
.file-card::before {{
    content: '';
    position: absolute;
    left: 0; top: 0; bottom: 0;
    width: 3px;
    background: transparent;
    transition: background 0.15s;
}}
.file-card.active {{
    border-color: var(--accent);
    background: var(--accent-dim);
}}
.file-card.active::before {{
    background: var(--accent);
}}
.file-card-row {{
    display: flex;
    align-items: center;
    gap: 0.5rem;
}}
.file-icon {{
    font-size: 1.05rem;
    flex-shrink: 0;
}}
.file-name {{
    font-size: 0.8rem;
    font-weight: 500;
    white-space: nowrap;
    overflow: hidden;
    text-overflow: ellipsis;
    flex: 1;
}}
.file-meta {{
    font-size: 0.68rem;
    color: var(--text-muted) !important;
    margin-top: 3px;
    display: flex;
    gap: 0.75rem;
}}
.file-badge {{
    display: inline-flex;
    align-items: center;
    background: var(--tag-bg);
    color: var(--accent2) !important;
    border-radius: 4px;
    padding: 1px 6px;
    font-size: 0.65rem;
    font-family: 'JetBrains Mono', monospace;
    font-weight: 500;
    flex-shrink: 0;
}}

/* ── Active filter banner ── */
.filter-banner {{
    background: var(--accent-dim);
    border: 1px solid var(--accent);
    border-radius: 8px;
    padding: 0.55rem 0.9rem;
    font-size: 0.8rem;
    display: flex;
    align-items: center;
    gap: 0.5rem;
    margin-bottom: 1rem;
}}
.filter-banner span {{ color: var(--accent2) !important; font-weight: 500; }}

/* ── Chat messages ── */
[data-testid="stChatMessage"] {{
    background: transparent !important;
    border: none !important;
    padding: 0.35rem 0 !important;
}}
.user-bubble {{
    background: var(--user-bg);
    border: 1px solid var(--border);
    border-radius: 14px 14px 4px 14px;
    padding: 0.75rem 1rem;
    max-width: 78%;
    margin-left: auto;
    font-size: 0.88rem;
    line-height: 1.65;
}}
.bot-bubble {{
    background: var(--bot-bg);
    border: 1px solid var(--border);
    border-radius: 4px 14px 14px 14px;
    padding: 0.85rem 1.05rem;
    max-width: 84%;
    font-size: 0.88rem;
    line-height: 1.75;
    box-shadow: 0 2px 12px rgba(0,0,0,0.08);
}}

/* ── Fix Streamlit markdown white overrides inside chat ── */
[data-testid="stChatMessage"] pre,
[data-testid="stChatMessage"] code,
[data-testid="stChatMessage"] .stMarkdown pre,
[data-testid="stChatMessage"] .stMarkdown code {{
    background: var(--bg-base) !important;
    color: var(--accent2) !important;
    border: 1px solid var(--border) !important;
    border-radius: 6px !important;
}}
[data-testid="stChatMessage"] table,
[data-testid="stChatMessage"] .stMarkdown table {{
    background: var(--bg-elevated) !important;
    border-collapse: collapse !important;
    width: 100% !important;
    border-radius: 8px !important;
    overflow: hidden !important;
    border: 1px solid var(--border) !important;
}}
[data-testid="stChatMessage"] th,
[data-testid="stChatMessage"] .stMarkdown th {{
    background: var(--bg-surface) !important;
    color: var(--accent2) !important;
    font-weight: 600 !important;
    font-size: 0.78rem !important;
    letter-spacing: 0.03em !important;
    padding: 0.5rem 0.75rem !important;
    border-bottom: 1px solid var(--border) !important;
    text-align: left !important;
}}
[data-testid="stChatMessage"] td,
[data-testid="stChatMessage"] .stMarkdown td {{
    background: transparent !important;
    color: var(--text-primary) !important;
    font-size: 0.8rem !important;
    padding: 0.45rem 0.75rem !important;
    border-bottom: 1px solid var(--border) !important;
}}
[data-testid="stChatMessage"] tr:last-child td {{
    border-bottom: none !important;
}}
[data-testid="stChatMessage"] tr:hover td {{
    background: var(--accent-dim) !important;
}}
/* Inline code */
[data-testid="stChatMessage"] p code,
[data-testid="stChatMessage"] li code {{
    background: var(--bg-base) !important;
    color: var(--accent2) !important;
    border: 1px solid var(--border) !important;
    border-radius: 4px !important;
    padding: 1px 5px !important;
    font-family: 'JetBrains Mono', monospace !important;
    font-size: 0.82em !important;
}}
/* Blockquotes */
[data-testid="stChatMessage"] blockquote {{
    background: var(--accent-dim) !important;
    border-left: 3px solid var(--accent) !important;
    padding: 0.5rem 0.75rem !important;
    border-radius: 0 6px 6px 0 !important;
    margin: 0.5rem 0 !important;
}}
/* Headings inside chat */
[data-testid="stChatMessage"] h1,
[data-testid="stChatMessage"] h2,
[data-testid="stChatMessage"] h3,
[data-testid="stChatMessage"] h4 {{
    color: var(--accent2) !important;
    border-bottom: 1px solid var(--border) !important;
    padding-bottom: 0.25rem !important;
    margin-top: 0.75rem !important;
}}
/* Lists */
[data-testid="stChatMessage"] ul,
[data-testid="stChatMessage"] ol {{
    padding-left: 1.5rem !important;
}}
[data-testid="stChatMessage"] li {{
    color: var(--text-primary) !important;
    font-size: 0.87rem !important;
    line-height: 1.7 !important;
}}
/* Strong / bold */
[data-testid="stChatMessage"] strong {{
    color: var(--text-primary) !important;
    font-weight: 600 !important;
}}
/* Horizontal rules */
[data-testid="stChatMessage"] hr {{
    border-color: var(--border) !important;
    margin: 0.6rem 0 !important;
}}
/* General white background kill-switch for everything inside chat */
[data-testid="stChatMessage"] [style*="background: white"],
[data-testid="stChatMessage"] [style*="background-color: white"],
[data-testid="stChatMessage"] [style*="background:#fff"],
[data-testid="stChatMessage"] [style*="background: #fff"] {{
    background: var(--bg-elevated) !important;
}}
.msg-footer {{
    display: flex;
    align-items: center;
    flex-wrap: wrap;
    gap: 0.4rem;
    margin-top: 0.5rem;
}}
.msg-time {{
    font-size: 0.68rem;
    color: var(--text-muted) !important;
    font-family: 'JetBrains Mono', monospace;
}}
.msg-model {{
    font-size: 0.68rem;
    color: var(--accent2) !important;
    background: var(--tag-bg);
    padding: 1px 7px;
    border-radius: 4px;
    font-family: 'JetBrains Mono', monospace;
}}
.src-pill {{
    display: inline-flex;
    align-items: center;
    gap: 3px;
    background: var(--tag-bg);
    color: var(--accent2) !important;
    border: 1px solid var(--border-focus);
    border-radius: 20px;
    padding: 2px 10px;
    font-size: 0.7rem;
    font-weight: 500;
}}

/* ── Score bar ── */
.score-wrap {{ margin: 4px 0 8px; }}
.score-label {{ font-size: 0.7rem; color: var(--text-muted) !important; margin-bottom: 3px; font-family: 'JetBrains Mono', monospace; }}
.score-track {{ height: 3px; background: var(--border); border-radius: 3px; }}
.score-fill  {{ height: 3px; background: linear-gradient(90deg, var(--accent), var(--accent2)); border-radius: 3px; }}
.chunk-text  {{ font-size: 0.78rem; color: var(--text-secondary) !important; line-height: 1.6; margin: 6px 0 0; font-family: 'Inter', sans-serif; }}

/* ── Chat input ── */
[data-testid="stChatInput"] {{
    background: var(--bg-elevated) !important;
    border: 1.5px solid var(--border) !important;
    border-radius: 12px !important;
    transition: border-color 0.2s !important;
}}
[data-testid="stChatInput"]:focus-within {{
    border-color: var(--accent) !important;
    box-shadow: 0 0 0 3px var(--accent-glow) !important;
}}
[data-testid="stChatInput"] textarea {{
    background: transparent !important;
    color: var(--text-primary) !important;
    font-family: 'Inter', sans-serif !important;
}}

/* ── Buttons ── */
.stButton > button {{
    background: linear-gradient(135deg, var(--accent), #5046c8) !important;
    color: #fff !important;
    border: none !important;
    border-radius: 8px !important;
    font-weight: 500 !important;
    font-family: 'Inter', sans-serif !important;
    letter-spacing: 0.01em !important;
    transition: all 0.15s !important;
    box-shadow: 0 2px 8px var(--accent-glow) !important;
}}
.stButton > button:hover {{
    transform: translateY(-1px) !important;
    box-shadow: 0 4px 14px var(--accent-glow) !important;
}}
.stButton > button[kind="secondary"] {{
    background: var(--bg-elevated) !important;
    color: var(--text-primary) !important;
    border: 1px solid var(--border) !important;
    box-shadow: none !important;
}}
.stButton > button[kind="secondary"]:hover {{
    border-color: var(--accent) !important;
    box-shadow: none !important;
    transform: none !important;
}}

/* ── Select / radio / slider ── */
.stSelectbox > div > div,
.stRadio > div {{
    background: var(--bg-elevated) !important;
    border-color: var(--border) !important;
    font-family: 'Inter', sans-serif !important;
}}
.stSlider {{ padding: 0 0.5rem; }}

/* ── Expander ── */
[data-testid="stExpander"] {{
    background: var(--bg-elevated) !important;
    border: 1px solid var(--border) !important;
    border-radius: 8px !important;
}}

/* ── Divider ── */
hr {{ border-color: var(--border) !important; margin: 0.5rem 0 !important; }}

/* ── Alerts ── */
.stAlert {{ border-radius: 8px !important; font-size: 0.82rem !important; }}

/* ── Empty state ── */
.empty-state {{
    text-align: center;
    padding: 3rem 2rem;
    color: var(--text-muted) !important;
}}
.empty-icon {{ font-size: 2.8rem; margin-bottom: 0.75rem; }}
.empty-title {{ font-size: 1rem; font-weight: 600; color: var(--text-secondary) !important; margin-bottom: 0.35rem; }}
.empty-sub {{ font-size: 0.8rem; line-height: 1.6; }}

/* ── Scrollbar ── */
::-webkit-scrollbar {{ width: 5px; height: 5px; }}
::-webkit-scrollbar-track {{ background: transparent; }}
::-webkit-scrollbar-thumb {{ background: var(--scrollbar); border-radius: 3px; }}

/* ── Metric override ── */
[data-testid="stMetricValue"] {{ color: var(--accent2) !important; }}
[data-testid="stMetricLabel"] {{ color: var(--text-muted) !important; }}

/* ── Toggle ── */
[data-testid="stToggle"] {{ color: var(--text-primary) !important; }}
</style>
""", unsafe_allow_html=True)

inject_css(st.session_state.theme)

# ── Helpers ───────────────────────────────────────────────────────────────────
def fmt_size(b):
    if not b: return "—"
    if b < 1024: return f"{b} B"
    if b < 1_048_576: return f"{b/1024:.1f} KB"
    return f"{b/1_048_576:.1f} MB"

def fmt_dt(iso):
    try:
        dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
        return dt.strftime("%b %d, %H:%M")
    except: return "—"

def ext_icon(name):
    e = name.rsplit(".", 1)[-1].upper() if "." in name else "?"
    return {"PDF":"📕","DOCX":"📘","TXT":"📄","CSV":"📊",
            "XLSX":"📗","JSON":"🗂","PPTX":"📙","MD":"📝"}.get(e, "📄")

def auth_headers():
    if st.session_state.token:
        return {"Authorization": f"Bearer {st.session_state.token}"}
    return {}

def api_get(path):
    try:
        return requests.get(
            f"{API_URL}{path}",
            headers=auth_headers(),
            timeout=8
        ).json()
    except Exception as e:
        return {"error": str(e)}

def api_post(path, **kw):
    try:
        headers = kw.pop("headers", {})
        headers.update(auth_headers())
        r = requests.post(
            f"{API_URL}{path}",
            headers=headers,
            timeout=120,
            **kw
        )
        return r.json(), r.status_code
    except Exception as e:
        return {"error": str(e)}, 500

def api_delete(path):
    try:
        return requests.delete(
            f"{API_URL}{path}",
            headers=auth_headers(),
            timeout=10
        ).json()
    except Exception as e:
        return {"error": str(e)}

def load_chat_history():
    data = api_get("/history")
    history = data.get("history", [])
    messages = []

    for item in reversed(history[-20:]):
        q = item.get("question", "")
        a = item.get("answer", "")
        ts = fmt_dt(item.get("created_at", ""))

        if q:
            messages.append({"role": "user", "content": q, "ts": ts})

        if a:
            messages.append({
                "role": "assistant",
                "content": a,
                "ts": ts,
                "sources": [item.get("file_name")] if item.get("file_name") else [],
                "chunks": [],
                "model": "",
                "sql": item.get("generated_sql"),
                "table_name": item.get("table_name"),
                "file_type": item.get("file_type"),
                "row_count": None,
            })

    st.session_state.messages = messages
# ── Auth screen ────────────────────────────────────────────────────────────────
if not st.session_state.token:
    st.markdown("""
<div class="page-header">
  <h1>🔐 Login to DocChat</h1>
  <p>Please login or register to access your private documents and chat history.</p>
</div>
""", unsafe_allow_html=True)

    login_tab, register_tab = st.tabs(["Login", "Register"])

    with login_tab:
        email = st.text_input("Email", key="login_email")
        password = st.text_input("Password", type="password", key="login_password")

        if st.button("Login", type="primary", use_container_width=True):
            data, status = api_post("/auth/login-json", json={
                "email": email,
                "password": password
            })

            if status == 200:
                st.session_state.token = data["access_token"]
                st.session_state.user = data["user"]
                st.session_state.messages = []
                load_chat_history()
                st.success("Login successful")
                st.rerun()
            else:
                st.error(data.get("detail", "Login failed"))

    with register_tab:
        name = st.text_input("Name", key="reg_name")
        reg_email = st.text_input("Email", key="reg_email")
        reg_password = st.text_input("Password", type="password", key="reg_password")

        if st.button("Register", type="primary", use_container_width=True):
            data, status = api_post("/auth/register", json={
                "name": name,
                "email": reg_email,
                "password": reg_password
            })

            if status == 200:
                st.session_state.token = data["access_token"]
                st.session_state.user = data["user"]
                st.session_state.messages = []
                st.success("Registration successful")
                st.rerun()
            else:
                st.error(data.get("detail", "Registration failed"))

    st.stop()

# ── Sidebar ───────────────────────────────────────────────────────────────────
with st.sidebar:

    # Brand
    st.markdown("""
<div class="sidebar-brand">
  <div class="brand-name">⚡ DocChat</div>
  <div class="brand-sub">RAG · Groq · dbt</div>
</div>
""", unsafe_allow_html=True)

    # Current user + logout
    if st.session_state.user:
        st.markdown(f"""
<div class="status-bar">
  <span class="dot-ok"></span>
  <span style="color:var(--text-secondary)">Logged in as {st.session_state.user.get('name', '')}</span>
</div>
""", unsafe_allow_html=True)

    c_logout1, c_logout2 = st.columns(2)
    with c_logout1:
        if st.button("↻ History", type="secondary", use_container_width=True):
            load_chat_history()
            st.rerun()
    with c_logout2:
        if st.button("Logout", type="secondary", use_container_width=True):
            st.session_state.token = None
            st.session_state.user = None
            st.session_state.messages = []
            st.session_state.selected_file = None
            st.rerun()

    # Theme toggle
    c1, c2 = st.columns(2)
    with c1:
        if st.button("🌙 Dark", use_container_width=True,
                     type="primary" if st.session_state.theme == "dark" else "secondary"):
            st.session_state.theme = "dark"; st.rerun()
    with c2:
        if st.button("☀️ Light", use_container_width=True,
                     type="primary" if st.session_state.theme == "light" else "secondary"):
            st.session_state.theme = "light"; st.rerun()

    st.markdown('<div style="height:0.6rem"></div>', unsafe_allow_html=True)

    # Health + Stats
    health = api_get("/health")
    stats  = api_get("/stats")
    pg_ok  = health.get("postgres") == "ok"

    dot_cls = "dot-ok" if pg_ok else "dot-err"
    pg_label = "Database connected" if pg_ok else "Database offline"
    st.markdown(f"""
<div class="status-bar">
  <span class="{dot_cls}"></span>
  <span style="color:var(--text-secondary)">{pg_label}</span>
</div>
<div class="stats-row">
  <div class="stat-pill">
    <div class="stat-val">{stats.get('pg_files', stats.get('total_files', 0))}</div>
    <div class="stat-lbl">Files</div>
  </div>
  <div class="stat-pill">
    <div class="stat-val">{stats.get('pg_raw_chunks', 0)}</div>
    <div class="stat-lbl">Chunks</div>
  </div>
  <div class="stat-pill">
    <div class="stat-val">{stats.get('total_vectors', 0)}</div>
    <div class="stat-lbl">Vectors</div>
  </div>
</div>
""", unsafe_allow_html=True)

    st.markdown('<div class="section-label">Upload Document</div>', unsafe_allow_html=True)

    with st.container():
        uploaded = st.file_uploader(
            "Drop a file or click to browse",
            type=["pdf","docx","txt","csv","xlsx","xls","json","pptx","md"],
            label_visibility="collapsed",
        )

    if uploaded:
        st.markdown(f'<div style="font-size:0.75rem;color:var(--text-secondary);padding:0.3rem 0.2rem">📎 {uploaded.name} · {fmt_size(len(uploaded.getvalue()))}</div>', unsafe_allow_html=True)
        if st.button("⬆ Ingest File", type="primary", use_container_width=True):
            with st.spinner(f"Processing…"):
                data,status = api_post("/upload", files={"file": (uploaded.name, uploaded.getvalue())})
                st.write("STATUS:", status)
                st.write("DATA:", data)
                if status == 200:
                    if data.get("status") == "success":
                        file_type = data.get("file_type", "unknown")

                        if file_type == "structured":
                            st.success(f"✓ Structured file uploaded: {data.get('file')}")
                        elif file_type == "unstructured":
                            st.success(
                                f"✓ Unstructured file ingested · "
                                f"{data.get('chunks', 0)} chunks · "
                                f"{data.get('embedded', 0)} embeddings"
                            )
                        else:
                            st.success(f"✓ Uploaded: {data.get('file')}")
                    elif data.get("status") == "duplicate":
                        st.warning("Already ingested — skipped.")
                    else:
                        st.error(data.get("detail", "Upload failed"))
                else:
                    st.error(data.get("detail", data.get("error", "Server error")))

    # File list
    st.markdown('<div class="section-label">Documents</div>', unsafe_allow_html=True)

    files_resp = api_get("/files")
    files      = files_resp.get("files", [])
    if files and isinstance(files[0], str):
        files = [
            {
                "name": f,
                "file_type": "unknown",
                "chunks": "?",
                "size": 0,
                "uploaded_at": None
            }
            for f in files
        ]

    if files:
        file_names = [f["name"] for f in files]

        # Hidden radio for selection logic (visually replaced by cards below)
        # We use a real selectbox but hide its label to keep the card UI
        sel_idx = 0
        opts = ["All files"] + file_names
        if st.session_state.selected_file in file_names:
            sel_idx = file_names.index(st.session_state.selected_file) + 1

        selected = st.radio(
            "Select file",
            options=opts,
            index=sel_idx,
            label_visibility="collapsed",
        )
        st.session_state.selected_file = None if selected == "All files" else selected

        # Render cards below the radio for visual feedback
        for f in files:
            icon = ext_icon(f["name"])
            is_sel = f["name"] == st.session_state.selected_file
            cls = "file-card active" if is_sel else "file-card"

            chunks = f.get("chunks", "?")
            size = fmt_size(f.get("size", 0))
            upd = fmt_dt(f.get("uploaded_at", ""))
            file_type = f.get("file_type", "unknown")

            if file_type == "structured":
                type_badge = "📊 structured"
            elif file_type == "unstructured":
                type_badge = "📄 unstructured"
            else:
                type_badge = "❔ unknown"

            st.markdown(f"""
        <div class="{cls}">
        <div class="file-card-row">
            <span class="file-icon">{icon}</span>
            <span class="file-name">{f['name']}</span>
            <span class="file-badge">{type_badge}</span>
        </div>
        <div class="file-meta">
            <span>{chunks} chunks</span>
            <span>{size}</span>
            <span>{upd}</span>
        </div>
        </div>
        """, unsafe_allow_html=True)

        # Delete
        st.markdown('<div class="section-label">Remove Document</div>', unsafe_allow_html=True)
        del_file = st.selectbox("File to remove", file_names, label_visibility="collapsed")
        if st.button("🗑 Remove", type="secondary", use_container_width=True):
            api_delete(f"/files/{del_file}")
            if st.session_state.selected_file == del_file:
                st.session_state.selected_file = None
            st.rerun()
    else:
        st.markdown("""
<div style="padding:1.2rem;text-align:center">
  <div style="font-size:1.5rem;margin-bottom:0.4rem">📂</div>
  <div style="font-size:0.78rem;color:var(--text-muted)">No documents yet.<br>Upload a file to get started.</div>
</div>
""", unsafe_allow_html=True)

    # Settings
    st.markdown('<div class="section-label">Settings</div>', unsafe_allow_html=True)
    st.session_state.top_k = st.slider("Chunks to retrieve", 1, 10,
                                        st.session_state.top_k, label_visibility="visible")
    st.session_state.show_chunks = st.toggle("Show retrieved chunks",
                                              st.session_state.show_chunks)

    col_a, col_b = st.columns(2)
    with col_a:
        if st.button("⟳ Run dbt", type="secondary", use_container_width=True):
            with st.spinner("Running…"):
                data, _ = api_post("/dbt/run")
            st.info(data.get("dbt_status", "done"))
    with col_b:
        if st.button("✕ Clear chat", type="secondary", use_container_width=True):
            st.session_state.messages = []
            st.rerun()

    st.markdown('<div style="height:1rem"></div>', unsafe_allow_html=True)

# ── Main content ──────────────────────────────────────────────────────────────
st.markdown("""
<div class="page-header">
  <h1>Document Chat</h1>
  <p>Ask anything about your uploaded documents — answers grounded in your content</p>
</div>
""", unsafe_allow_html=True)

if st.session_state.selected_file:
    st.markdown(f"""
<div class="filter-banner">
  🔍 Scoped to: <span>{st.session_state.selected_file}</span>
  &nbsp;·&nbsp;
  <span style="color:var(--text-muted);font-weight:400;font-size:0.75rem">
    Answers limited to this document
  </span>
</div>
""", unsafe_allow_html=True)

# Render chat history
if not st.session_state.messages:
    st.markdown("""
<div class="empty-state">
  <div class="empty-icon">💬</div>
  <div class="empty-title">Start a conversation</div>
  <div class="empty-sub">
    Upload a document in the sidebar,<br>
    then ask a question below.
  </div>
</div>
""", unsafe_allow_html=True)

for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        bubble_cls = "user-bubble" if msg["role"] == "user" else "bot-bubble"
        content_html = msg["content"].replace("\n", "<br>")
        st.markdown(f'<div class="{bubble_cls}">{content_html}</div>', unsafe_allow_html=True)

        # Footer: time + model
        footer_parts = []
        if msg.get("ts"):
            footer_parts.append(f'<span class="msg-time">{msg["ts"]}</span>')
        if msg.get("model"):
            footer_parts.append(f'<span class="msg-model">{msg["model"]}</span>')
        if msg.get("sources"):
            pills = "".join(
                f'<span class="src-pill">📎 {s}</span>'
                for s in msg["sources"]
            )
            footer_parts.append(pills)
        if footer_parts:
            st.markdown(
                f'<div class="msg-footer">{"".join(footer_parts)}</div>',
                unsafe_allow_html=True,
            )

        # Chunks expander
        if msg.get("chunks") and st.session_state.show_chunks:
            with st.expander(f"🔍 {len(msg['chunks'])} source chunks"):
                for i, c in enumerate(msg["chunks"], 1):
                    pct = int(c.get("score", 0) * 100)
                    st.markdown(f"""
<div class="score-wrap">
  <div class="score-label">Chunk {i} · {c['file_name']} · {c['score']:.3f}</div>
  <div class="score-track"><div class="score-fill" style="width:{pct}%"></div></div>
  <div class="chunk-text">{c['chunk_text'][:320]}{"…" if len(c['chunk_text']) > 320 else ""}</div>
</div>
""", unsafe_allow_html=True)
                    if i < len(msg["chunks"]):
                        st.markdown('<hr style="margin:0.6rem 0">', unsafe_allow_html=True)
        if msg.get("file_type") == "structured" and msg.get("sql"):
            with st.expander("📊 Structured Query Details"):
                st.code(msg.get("sql"), language="sql")
                st.write("Table:", msg.get("table_name"))

# ── Input ─────────────────────────────────────────────────────────────────────
if question := st.chat_input("Ask a question about your documents…"):
    ts_now = datetime.now().strftime("%H:%M")

    st.session_state.messages.append({"role": "user", "content": question, "ts": ts_now})
    with st.chat_message("user"):
        st.markdown(f'<div class="user-bubble">{question}</div>', unsafe_allow_html=True)
        st.markdown(f'<div class="msg-footer"><span class="msg-time">{ts_now}</span></div>', unsafe_allow_html=True)

    history = [
        {"role": m["role"], "content": m["content"]}
        for m in st.session_state.messages[-6:]
        if m["role"] in ("user", "assistant")
    ]

    with st.chat_message("assistant"):
        with st.spinner("Thinking…"):
            data, status = api_post("/chat", json={
                "question":     question,
                "file_name":    st.session_state.selected_file,
                "top_k":        st.session_state.top_k,
                "chat_history": history,
            })

        if status == 200:
            ans        = data.get("answer", "No answer returned.")
            sources    = data.get("sources", [])
            chunks     = data.get("chunks", [])
            model      = data.get("model", "")
            sql        = data.get("sql")
            table_name = data.get("table_name")
            row_count  = data.get("row_count")
            file_type  = data.get("file_type")
            ts_ans     = datetime.now().strftime("%H:%M")

            content_html = ans.replace("\n", "<br>")
            st.markdown(f'<div class="bot-bubble">{content_html}</div>', unsafe_allow_html=True)
            if file_type == "structured" and sql:
                with st.expander("📊 Structured Query Details"):
                    st.code(sql, language="sql")
                    st.write("Table:", table_name)
                    st.write("Rows Returned:", row_count)            

            footer_parts = [f'<span class="msg-time">{ts_ans}</span>']
            if model:
                footer_parts.append(f'<span class="msg-model">{model}</span>')
            if sources:
                footer_parts += [f'<span class="src-pill">📎 {s}</span>' for s in sources]
            st.markdown(
                f'<div class="msg-footer">{"".join(footer_parts)}</div>',
                unsafe_allow_html=True,
            )

            if chunks and st.session_state.show_chunks:
                with st.expander(f"🔍 {len(chunks)} source chunks"):
                    for i, c in enumerate(chunks, 1):
                        pct = int(c.get("score", 0) * 100)
                        st.markdown(f"""
<div class="score-wrap">
  <div class="score-label">Chunk {i} · {c['file_name']} · {c['score']:.3f}</div>
  <div class="score-track"><div class="score-fill" style="width:{pct}%"></div></div>
  <div class="chunk-text">{c['chunk_text'][:320]}{"…" if len(c['chunk_text']) > 320 else ""}</div>
</div>
""", unsafe_allow_html=True)
                        if i < len(chunks):
                            st.markdown('<hr style="margin:0.6rem 0">', unsafe_allow_html=True)

            st.session_state.messages.append({
                "role": "assistant",
                "content": ans,
                "ts": ts_ans,
                "sources": sources,
                "chunks": chunks,
                "model": model,
                "sql": sql,
                "table_name": table_name,
                "row_count": row_count,
                "file_type": file_type,
            })
        else:
            err = f"❌ {data.get('detail', data.get('error', 'Something went wrong.'))}"
            st.markdown(f'<div class="bot-bubble">{err}</div>', unsafe_allow_html=True)
            st.session_state.messages.append({
                "role": "assistant", "content": err,
                "ts": datetime.now().strftime("%H:%M"),
            })