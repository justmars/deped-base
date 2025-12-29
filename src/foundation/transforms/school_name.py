import re


# =========================================================
# 0. PRE-CLEAN (handle malformed punctuation BEFORE anything else)
# =========================================================
def pre_clean(name: str) -> str:
    """
    Fix messy punctuation, stray letters, repeated periods, malformed Nat'l,
    Agro-Ind'l, spacing around dots, etc.
    """
    if not name:
        return name

    # Collapse repeated dots: "Nat'l.." -> "Nat'l."
    name = re.sub(r"\.{2,}", ".", name)

    # Remove spaces before punctuation: "Natl ." -> "Natl."
    name = re.sub(r"\s+([.,&\-])", r"\1", name)

    # Normalize apostrophes
    name = name.replace("’", "'").replace("`", "'")

    # Normalize malformed Nat'l variants into "Natl"
    # Handles Nat'l., Nat'l.l, Nat'l.l., NAT'L.L, etc.
    name = re.sub(r"(?i)\bnat(?:'|’)?l(?:\.?l?)+", "Natl", name)
    name = re.sub(r"(?i)\bnat(?:'|’)?l\.?", "Natl", name)

    # Normalize Agro–Ind'l (with any hyphen type)
    name = re.sub(r"(?i)\bAgro\s*[-–]\s*Ind'?l\.?", "Agricultural–Industrial", name)

    return name


# =========================================================
# 1. SPACING & BASIC CLEANUP
# =========================================================
def clean_spacing(name: str) -> str:
    name = name.strip()
    name = re.sub(r"\s+", " ", name)
    name = name.replace("–", "-").replace("—", "-")
    name = re.sub(r"\s*-\s*", "-", name)
    return name


# =========================================================
# 2. CASE NORMALIZATION
# =========================================================
def standardize_case(name: str) -> str:
    name = name.title()

    abbreviations = [
        "ES",
        "PS",
        "CES",
        "MES",
        "CS",
        "SPED",
        "JHS",
        "SHS",
        "NHS",
        "MHS",
        "HS",
        "IS",
        "NATL",
    ]
    for abbr in abbreviations:
        name = re.sub(rf"\b{abbr.title()}\b", abbr, name)

    name = re.sub(r"\bSped\b", "SPED", name)
    return name


# =========================================================
# 3. ABBREVIATION EXPANSION (ALL RULES)
# =========================================================
ABBREV_MAP = {
    # Elementary/Primary
    r"\bES\b": "Elementary School",
    r"\bE/S\b": "Elementary School",
    r"\bElem\.?( School)?\b": "Elementary School",
    r"\bElem\b": "Elementary School",
    r"\bPS\b": "Primary School",
    # Central
    r"\bC/S\b": "Central School",
    r"\bCS\b": "Central School",
    r"\bCES\b": "Central Elementary School",
    # Memorial Elementary
    r"\bMES\b": "Memorial Elementary School",
    r"\bMem\.?\b": "Memorial",
    r"\bMeml\.?\b": "Memorial",
    # High school levels
    r"\bJHS\b": "Junior High School",
    r"\bSHS\b": "Senior High School",
    r"\bNHS\b": "National High School",
    r"\bMHS\b": "Memorial High School",
    # Integrated School must be BEFORE HS
    r"\bIS\b": "Integrated School",
    # Generic HS
    r"\bHS\b": "High School",
    # National (merged to canonical token)
    r"(?i)\bNatl\b": "National",
    r"(?i)\bNational\b": "National",
    # NEW: Agricultural / Industrial / Vocational
    r"(?i)\bVoc'l\.?\b": "Vocational",
    r"(?i)\bInd'l\.?\b": "Industrial",
    r"(?i)\bAgro\b": "Agricultural",
    r"(?i)\bAgro-\b": "Agricultural-",
}


def expand_abbreviations(name: str) -> str:
    for pattern, repl in ABBREV_MAP.items():
        name = re.sub(pattern, repl, name)
    return name


# =========================================================
# 4. ROMAN NUMERAL NORMALIZATION
# =========================================================
ROMAN_NUMERALS = {
    "i": "I",
    "ii": "II",
    "iii": "III",
    "iv": "IV",
    "v": "V",
    "vi": "VI",
    "vii": "VII",
    "viii": "VIII",
    "ix": "IX",
    "x": "X",
}


def normalize_roman_numerals(name: str) -> str:
    tokens = name.split()
    out = []
    for t in tokens:
        clean = t.lower().replace(".", "")
        out.append(ROMAN_NUMERALS.get(clean, t))
    return " ".join(out)


# =========================================================
# 5. MEMORIAL NORMALIZATION
# =========================================================
def normalize_memorial(name: str) -> str:
    name = re.sub(r"Memorial\.", "Memorial", name, flags=re.IGNORECASE)

    # Remove duplicates
    dup = [
        "Memorial Elementary School",
        "Memorial High School",
    ]
    for d in dup:
        name = re.sub(rf"({d})(\s+\1)+", r"\1", name, flags=re.IGNORECASE)

    return name


# =========================================================
# 6. SCHOOL TYPE POSITIONING
# =========================================================
def normalize_school_type_position(name: str) -> str:
    school_types = [
        "Elementary School",
        "Primary School",
        "Central School",
        "Central Elementary School",
        "Memorial Elementary School",
        "Community School",
        "High School",
        "Junior High School",
        "Senior High School",
        "National High School",
        "Memorial High School",
        "Integrated School",
    ]

    # split suffix
    if "," in name:
        main, suf = name.split(",", 1)
        return f"{normalize_school_type_position(main.strip())}, {suf.strip()}"

    if any(name.endswith(t) for t in school_types):
        return name

    if re.search(r"\bElementary\b", name):
        return re.sub(r"Elementary$", "Elementary School", name)

    if re.search(r"\bHigh\b", name):
        return re.sub(r"High$", "High School", name)

    if re.search(r"\bIntegrated\b", name):
        return re.sub(r"Integrated$", "Integrated School", name)

    return name


# =========================================================
# 7. MULTI-SITE HYPHEN NORMALIZATION
# =========================================================
def normalize_multi_location_names(name: str) -> str:
    name = name.replace("-", "–")
    name = re.sub(r"\s*–\s*", "–", name)
    return name


# =========================================================
# 8. FINAL CLEANUP
# =========================================================
def finalize_format(name: str) -> str:
    patterns = [
        "Elementary School",
        "High School",
        "Integrated School",
        "Central School",
        "Central Elementary School",
        "Memorial Elementary School",
        "Memorial High School",
    ]
    for p in patterns:
        name = re.sub(rf"({p})(\s+\1)+", r"\1", name, flags=re.IGNORECASE)
    return name.strip()


# =========================================================
# 9. MAIN PIPELINE
# =========================================================
def clean_school_name(raw: str) -> str:
    name = raw or ""
    name = pre_clean(name)
    name = clean_spacing(name)
    name = standardize_case(name)
    name = expand_abbreviations(name)
    name = normalize_roman_numerals(name)
    name = normalize_memorial(name)
    name = normalize_school_type_position(name)
    name = normalize_multi_location_names(name)
    name = finalize_format(name)
    return name
