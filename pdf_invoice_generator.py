from __future__ import annotations

import csv
import json
import os
import re
import subprocess
import sys
from io import StringIO
from pathlib import Path
from typing import Any

from weasyprint import CSS, HTML
from weasyprint.text.fonts import FontConfiguration


BASE_DIR = Path(__file__).resolve().parent
DATA_DIR = BASE_DIR / "data"
TEMPLATES_DIR = BASE_DIR / "templates"
OUTPUT_DIR = BASE_DIR / "output"
EXPECTED_CONTEXT_KEYS = {
    "invoice_id",
    "supplier",
    "buyer",
    "total_amount",
    "basis",
    "bank_name",
    "bik",
    "bank_account",
    "inn",
    "kpp",
    "company_name",
    "recipient_account",
}


def ensure_directories() -> None:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    TEMPLATES_DIR.mkdir(parents=True, exist_ok=True)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)


def list_data_files() -> list[Path]:
    files = [*DATA_DIR.glob("*.csv"), *DATA_DIR.glob("*.json")]
    return sorted(files, key=lambda p: p.name.lower())


def list_template_files() -> list[Path]:
    files = [*TEMPLATES_DIR.glob("*.html"), *TEMPLATES_DIR.glob("*.htm")]
    return sorted(files, key=lambda p: p.name.lower())


def print_numbered_menu(title: str, options: list[str]) -> None:
    print(f"\n{title}")
    print("-" * len(title))
    for index, option in enumerate(options, start=1):
        print(f"{index}. {option}")


def choose_from_menu(prompt: str, options: list[Any]) -> Any:
    while True:
        value = input(f"{prompt} [1-{len(options)}]: ").strip()
        if value.isdigit():
            index = int(value)
            if 1 <= index <= len(options):
                return options[index - 1]
        print("Некорректный выбор. Попробуйте снова.")


def read_text_with_fallbacks(path: Path) -> str:
    encodings = ("utf-8-sig", "utf-8", "cp1251")
    for encoding in encodings:
        try:
            return path.read_text(encoding=encoding)
        except UnicodeDecodeError:
            continue
    raise UnicodeDecodeError("unknown", b"", 0, 1, f"Не удалось прочитать файл: {path}")


def parse_csv_file(path: Path) -> list[dict[str, Any]]:
    content = read_text_with_fallbacks(path)
    sample = "\n".join(content.splitlines()[:10])
    delimiter = ";"
    try:
        dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
        delimiter = dialect.delimiter
    except csv.Error:
        if sample.count(",") > sample.count(";"):
            delimiter = ","

    rows: list[dict[str, Any]] = []

    # Try standard table parsing first.
    dict_reader = csv.DictReader(StringIO(content), delimiter=delimiter)
    for row in dict_reader:
        normalized = {str(key).strip(): (value if value is not None else "") for key, value in row.items() if key}
        if any(str(value).strip() for value in normalized.values()):
            rows.append(normalized)

    fallback_record = parse_semistructured_invoice_csv(content, delimiter)
    structured_score = max((score_record_keys(record) for record in rows), default=0)
    fallback_score = score_record_keys(fallback_record) if fallback_record else 0

    # For spreadsheet-form CSV, fallback parser usually extracts more semantic fields
    # than DictReader with accidental headers from the first data row.
    if fallback_record and fallback_score >= 2 and fallback_score > structured_score:
        return [fallback_record]
    if rows and any(extract_invoice_id(record) for record in rows):
        return rows
    if fallback_record:
        return [fallback_record]
    return rows


def parse_semistructured_invoice_csv(content: str, delimiter: str) -> dict[str, str]:
    record: dict[str, str] = {}
    raw_rows = list(csv.reader(StringIO(content), delimiter=delimiter))

    for row in raw_rows:
        cells = [cell.strip().strip('"') for cell in row if cell and cell.strip()]
        if not cells:
            continue
        row_text = " ".join(cells)
        compact = re.sub(r"\s+", " ", row_text).strip()
        lowered = compact.lower()

        invoice_match = re.search(r"(?:счет|сч[её]т)\s*№\s*([A-Za-zА-Яа-я0-9\-/]+)", compact, flags=re.IGNORECASE)
        if invoice_match and "invoice_id" not in record:
            record["invoice_id"] = invoice_match.group(1).strip()

        if "bank_name" not in record and "бик" in lowered:
            bank_name = re.split(r"\bбик\b", compact, maxsplit=1, flags=re.IGNORECASE)[0].strip()
            if bank_name:
                record["bank_name"] = bank_name
            bik_match = re.search(r"\bбик\b\D*([0-9]{6,12})", compact, flags=re.IGNORECASE)
            if bik_match:
                record["bik"] = bik_match.group(1).strip()

        if "bank_account" not in record and re.search(r"(?:^|\s)сч\.\s*№", lowered):
            account_match = re.search(r"([0-9]{12,})", compact)
            if account_match:
                record["bank_account"] = account_match.group(1).strip()

        if "inn" not in record and "инн" in lowered:
            inn_match = re.search(r"\bинн\b\D*([0-9]{10,12})", compact, flags=re.IGNORECASE)
            if inn_match:
                record["inn"] = inn_match.group(1).strip()
        if "kpp" not in record and "кпп" in lowered:
            kpp_match = re.search(r"\bкпп\b\D*([0-9]{8,10})", compact, flags=re.IGNORECASE)
            if kpp_match:
                record["kpp"] = kpp_match.group(1).strip()
        if "recipient_account" not in record and "инн" in lowered and "кпп" in lowered and "сч." in lowered:
            account_matches = re.findall(r"([0-9]{12,})", compact)
            if account_matches:
                record["recipient_account"] = account_matches[-1].strip()

        if "company_name" not in record and re.search(r"\bооо\b", lowered):
            record["company_name"] = compact

        if lowered.startswith("поставщик"):
            record["supplier"] = compact.split(":", 1)[-1].strip()
        elif lowered.startswith("покупатель"):
            record["buyer"] = compact.split(":", 1)[-1].strip()
        elif "всего к оплате" in lowered:
            amount_match = re.search(r"(\d[\d\s.,]*)$", compact)
            record["total_amount"] = amount_match.group(1).strip() if amount_match else compact
        elif "основание" in lowered:
            record["basis"] = compact.split(":", 1)[-1].strip()

    return record


def parse_json_file(path: Path) -> list[dict[str, Any]]:
    raw = read_text_with_fallbacks(path)
    payload = json.loads(raw)

    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]

    if isinstance(payload, dict):
        for candidate_key in ("invoices", "items", "data", "records"):
            value = payload.get(candidate_key)
            if isinstance(value, list):
                return [item for item in value if isinstance(item, dict)]
        return [payload]

    return []


def load_data_file(path: Path) -> list[dict[str, Any]]:
    if path.suffix.lower() == ".csv":
        return parse_csv_file(path)
    if path.suffix.lower() == ".json":
        return parse_json_file(path)
    return []


def normalize_key(key: str) -> str:
    return re.sub(r"[\s_\-]+", "", key.lower())


def score_record_keys(record: dict[str, Any]) -> int:
    if not record:
        return 0
    normalized_expected = {normalize_key(key) for key in EXPECTED_CONTEXT_KEYS}
    score = 0
    for key, value in record.items():
        if normalize_key(str(key)) in normalized_expected and str(value).strip():
            score += 1
    return score


def extract_invoice_id(record: dict[str, Any]) -> str | None:
    prioritized = (
        "invoice_id",
        "invoiceid",
        "invoice",
        "id",
        "number",
        "invoice_number",
        "номер",
        "номерсчета",
        "счет",
        "счёт",
        "счетномер",
        "счётномер",
    )
    normalized_map = {normalize_key(k): k for k in record.keys()}

    for preferred in prioritized:
        key = normalized_map.get(normalize_key(preferred))
        if key and str(record.get(key, "")).strip():
            return str(record[key]).strip()

    for key, value in record.items():
        normalized = normalize_key(key)
        if ("invoice" in normalized or "счет" in normalized or "счёт" in normalized) and str(value).strip():
            return str(value).strip()
        if str(value).strip():
            match = re.search(r"(?:счет|сч[её]т)\s*№\s*([A-Za-zА-Яа-я0-9\-/]+)", str(value), flags=re.IGNORECASE)
            if match:
                return match.group(1).strip()

    return None


def to_display_value(value: Any) -> str:
    if value is None:
        return ""
    if isinstance(value, (dict, list)):
        return json.dumps(value, ensure_ascii=False)
    return str(value)


def build_context(record: dict[str, Any]) -> dict[str, str]:
    context = {key: to_display_value(value) for key, value in record.items()}
    invoice_id = extract_invoice_id(record)
    if invoice_id:
        context["invoice_id"] = invoice_id
    return context


def render_template(template_html: str, context: dict[str, str]) -> str:
    rendered = template_html

    # Support placeholders: {{ key }} and {key}
    for key, value in context.items():
        rendered = re.sub(r"\{\{\s*" + re.escape(key) + r"\s*\}\}", value, rendered)
        rendered = rendered.replace("{" + key + "}", value)

    # Replace unreplaced {{...}} with empty string.
    rendered = re.sub(r"\{\{\s*[\w\.\-]+\s*\}\}", "", rendered)
    return rendered


def detect_font_paths() -> list[Path]:
    candidates = [
        # Windows common locations
        Path("C:/Windows/Fonts/DejaVuSans.ttf"),
        Path("C:/Windows/Fonts/dejavusans.ttf"),
        Path("C:/Windows/Fonts/Roboto-Regular.ttf"),
        # macOS common locations
        Path("/Library/Fonts/DejaVuSans.ttf"),
        Path("/Library/Fonts/Roboto-Regular.ttf"),
        Path("/System/Library/Fonts/Supplemental/Arial Unicode.ttf"),
    ]
    return [path for path in candidates if path.exists()]


def build_font_css() -> str:
    font_paths = detect_font_paths()
    face_rules: list[str] = []
    family_name = "AppFont"

    if font_paths:
        for font_path in font_paths:
            safe_uri = font_path.resolve().as_uri()
            face_rules.append(
                f"@font-face {{ font-family: '{family_name}'; src: url('{safe_uri}'); font-style: normal; }}"
            )
        font_family = f"'{family_name}', 'DejaVu Sans', 'Roboto', 'Arial', sans-serif"
    else:
        font_family = "'DejaVu Sans', 'Roboto', 'Arial', sans-serif"

    return "\n".join(
        [
            *face_rules,
            f"body {{ font-family: {font_family}; }}",
            "* { font-family: inherit; }",
        ]
    )


def save_pdf(html_content: str, output_path: Path) -> None:
    font_config = FontConfiguration()
    css = CSS(string=build_font_css(), font_config=font_config)
    HTML(string=html_content, base_url=str(BASE_DIR)).write_pdf(
        str(output_path), stylesheets=[css], font_config=font_config
    )


def open_pdf(path: Path) -> None:
    if sys.platform.startswith("win"):
        os.startfile(str(path))  # type: ignore[attr-defined]
        return
    if sys.platform == "darwin":
        subprocess.run(["open", str(path)], check=False)
        return
    subprocess.run(["xdg-open", str(path)], check=False)


def choose_invoice(records: list[dict[str, Any]]) -> dict[str, Any]:
    records_with_ids: list[tuple[str, dict[str, Any]]] = []
    for record in records:
        invoice_id = extract_invoice_id(record)
        if invoice_id:
            records_with_ids.append((invoice_id, record))

    if not records_with_ids:
        index_options = [f"Запись #{index + 1}" for index in range(len(records))]
        print_numbered_menu("Invoice id не найден, выберите запись", index_options)
        selected_label = choose_from_menu("Выберите запись", index_options)
        selected_index = index_options.index(selected_label)
        return records[selected_index]

    unique_ids: list[str] = []
    for invoice_id, _record in records_with_ids:
        if invoice_id not in unique_ids:
            unique_ids.append(invoice_id)

    if len(unique_ids) == 1:
        selected_id = unique_ids[0]
        print(f"\nНайден один invoice id: {selected_id}. Выбор выполнен автоматически.")
    else:
        print_numbered_menu("Доступные чеки (invoice id)", unique_ids)
        selected_id = choose_from_menu("Выберите invoice id", unique_ids)

    for invoice_id, record in records_with_ids:
        if invoice_id == selected_id:
            return record

    raise ValueError("Не удалось найти выбранный invoice id.")


def main() -> None:
    ensure_directories()

    data_files = list_data_files()
    template_files = list_template_files()

    print("\n=== Генератор PDF по шаблону ===")
    if not data_files:
        print(f"В директории '{DATA_DIR}' нет CSV/JSON файлов.")
        return
    if not template_files:
        print(f"В директории '{TEMPLATES_DIR}' нет HTML-шаблонов.")
        return

    print_numbered_menu("Доступные файлы с данными", [file.name for file in data_files])
    selected_data_file = choose_from_menu("Выберите файл данных", data_files)

    print_numbered_menu("Доступные HTML-шаблоны", [file.name for file in template_files])
    selected_template_file = choose_from_menu("Выберите HTML-шаблон", template_files)

    records = load_data_file(selected_data_file)
    if not records:
        print(f"Файл '{selected_data_file.name}' не содержит подходящих записей.")
        return

    selected_record = choose_invoice(records)
    context = build_context(selected_record)

    template_html = read_text_with_fallbacks(selected_template_file)
    rendered_html = render_template(template_html, context)

    invoice_id = context.get("invoice_id", "invoice")
    safe_invoice_id = re.sub(r"[^\w\-\.]+", "_", invoice_id, flags=re.UNICODE)
    output_file = OUTPUT_DIR / f"{safe_invoice_id}.pdf"

    save_pdf(rendered_html, output_file)
    print(f"\nPDF успешно создан: {output_file}")
    open_pdf(output_file)


if __name__ == "__main__":
    try:
        main()
    except KeyboardInterrupt:
        print("\nОперация отменена пользователем.")
    except Exception as error:
        print(f"\nОшибка: {error}")
