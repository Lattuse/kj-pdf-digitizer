from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import FileResponse
from fastapi.middleware.cors import CORSMiddleware
import pdfplumber
import re
import pandas as pd
from pathlib import Path
from collections import defaultdict
import tempfile
from fastapi.responses import StreamingResponse
from io import BytesIO
import os
import sys
import uvicorn
sys.stdout.reconfigure(encoding="utf-8")


# fastapi

app = FastAPI(title="BI PDF to XLSX Digitizer")

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],   # для Builder / React, мало ли
    allow_methods=["*"],
    allow_headers=["*"],
)


# regex

OBJECT_RE = re.compile(
    r"(Км-\d+|СЖм-\d+|СТм-\d+|Бм-\d+|Пм-\d+)\s+(\d+)"
)

SPEC_ELEMENTS_RE = re.compile(
    r"Спецификация\s+элементов\s+(Км-\d+|СЖм-\d+|СТм-\d+|Бм-\d+|Пм-\d+)",
    re.IGNORECASE
)

SPEC_HEADER_RE = re.compile(
    r"(Колонна|Балка|Плита|Стены\s+монолитные|Стена)\s+"
    r"(Км-\d+|СЖм-\d+|СТм-\d+|Бм-\d+|Пм-\d+)",
    re.IGNORECASE
)

EMBEDDED_LINE_RE = re.compile(
    r"Изделие\s+закладное\s+(ЗД-\d+)\s+(\d+)",
    re.IGNORECASE
)

EMBEDDED_HEADER_RE = re.compile(
    r"Изделие\s+закладное\s+(ЗД-\d+)",
    re.IGNORECASE
)

LINE_RE = re.compile(
    r"""
    (?P<fullname>
        (?:ГОСТ\s*\d+[-–]\d+\s*)?
        Пруток\s+
        .*?
        [Ø∅]?\s*\d+\s*[xх]\s*\d+
        (?:-\s*A\d{3}С?)?
    )
    \s+
    (?P<qty>\d+)
    """,
    re.VERBOSE | re.IGNORECASE
)

SIZE_RE = re.compile(
    r"[Ø∅]?\s*(?P<diameter>\d+)\s*[xх]\s*(?P<length>\d+)",
    re.IGNORECASE
)


# digitization logic

def extract_object_counts(pdf):
    text = pdf.pages[0].extract_text() or ""
    return {m: int(q) for m, q in OBJECT_RE.findall(text)}


def digitize(pdf, object_counts):
    rows = defaultdict(lambda: {
        "Прутки напрямую": 0,
        "Прутки в ЗД": 0,
        "Кол-во ЗД": 0
    })

    current_element = None
    embedded_qty = 0
    in_embedded = False

    for page_index, page in enumerate(pdf.pages):
        if page_index == 0:
            continue

        text = page.extract_text()
        if not text:
            continue

        for line in text.splitlines():
            if not line or not line.strip():
                continue

            spec_elements = SPEC_ELEMENTS_RE.search(line)
            if spec_elements:
                current_element = spec_elements.group(1)
                in_embedded = False
                embedded_qty = 0
                continue

            header = SPEC_HEADER_RE.search(line)
            if header:
                current_element = header.group(2)
                in_embedded = False
                embedded_qty = 0
                continue

            if not current_element:
                continue

            embedded_line = EMBEDDED_LINE_RE.search(line)
            if embedded_line:
                embedded_qty = int(embedded_line.group(2))
                continue

            if EMBEDDED_HEADER_RE.search(line):
                in_embedded = True
                continue

            line_match = LINE_RE.search(line)
            if not line_match:
                continue

            fullname = line_match.group("fullname").strip()
            qty = int(line_match.group("qty"))

            size = SIZE_RE.search(fullname)
            if not size:
                continue

            diameter = int(size.group("diameter"))
            length = int(size.group("length"))

            key = (current_element, fullname, diameter, length)

            if in_embedded:
                rows[key]["Прутки в ЗД"] += qty
                rows[key]["Кол-во ЗД"] = embedded_qty
            else:
                rows[key]["Прутки напрямую"] += qty

    result_rows = []
    for (element, fullname, d, l), c in rows.items():
        obj_qty = object_counts.get(element, 1)

        total = (
            c["Прутки напрямую"] * obj_qty
            + c["Прутки в ЗД"] * c["Кол-во ЗД"] * obj_qty
        )

        result_rows.append({
            "Элемент": element,
            "Наименование": fullname,
            "Диаметр": d,
            "Длина, мм": l,
            "Прутки напрямую": c["Прутки напрямую"],
            "Прутки в ЗД": c["Прутки в ЗД"],
            "Количество объектов": obj_qty,
            "Количество ЗД": c["Кол-во ЗД"],
            "Общее количество": total
        })

    return result_rows


# API

@app.post("/process-pdf")
async def process_pdf(file: UploadFile = File(...)):
    if not file.filename.lower().endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Нужен PDF файл")

    pdf_bytes = await file.read()

    with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_pdf:
        tmp_pdf.write(pdf_bytes)
        pdf_path = tmp_pdf.name

    try:
        with pdfplumber.open(pdf_path) as pdf:
            object_counts = extract_object_counts(pdf)
            rows = digitize(pdf, object_counts)

        df = pd.DataFrame(rows)

        output = BytesIO()
        df.to_excel(output, index=False)
        output.seek(0)

        return StreamingResponse(
            output,
            media_type="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            headers={
                "Content-Disposition": "attachment; filename=digitized_specifications.xlsx"
            }
        )

    finally:
        os.remove(pdf_path)
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0")

