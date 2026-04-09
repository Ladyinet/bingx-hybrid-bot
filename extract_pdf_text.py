from pathlib import Path
import re
import zlib


def parse_objects(raw_text: str) -> dict[int, str]:
    return {int(m.group(1)): m.group(2) for m in re.finditer(r"(\d+) 0 obj(.*?)endobj", raw_text, re.S)}


def build_font_maps(raw_text: str, objects: dict[int, str]) -> dict[str, dict[int, str]]:
    font_to_obj: dict[str, int] = {}
    for m in re.finditer(r"/([A-Z]?F\d+)\s+(\d+)\s+0\s+R", raw_text):
        font_to_obj[m.group(1)] = int(m.group(2))

    font_maps: dict[str, dict[int, str]] = {}
    for font_name, obj_num in font_to_obj.items():
        obj = objects.get(obj_num, "")
        m = re.search(r"/ToUnicode\s+(\d+)\s+0\s+R", obj)
        cmap: dict[int, str] = {}
        if not m:
            font_maps[font_name] = cmap
            continue

        cmap_obj = int(m.group(1))
        sm = re.search(rf"{cmap_obj} 0 obj(.*?)endobj", raw_text, re.S)
        if not sm:
            font_maps[font_name] = cmap
            continue

        stream_match = re.search(r"stream\r?\n(.*?)\r?\nendstream", sm.group(1), re.S)
        if not stream_match:
            font_maps[font_name] = cmap
            continue

        raw_stream = stream_match.group(1).encode("latin1", errors="ignore")
        try:
            decoded = zlib.decompress(raw_stream).decode("latin1", errors="ignore")
        except Exception:
            font_maps[font_name] = cmap
            continue

        for mm in re.finditer(r"<([0-9A-F]+)>\s*<([0-9A-F]+)>", decoded):
            src, dst = mm.groups()
            if len(src) <= 4 and len(dst) % 4 == 0:
                try:
                    cmap[int(src, 16)] = bytes.fromhex(dst).decode("utf-16-be")
                except Exception:
                    pass

        for mm in re.finditer(r"<([0-9A-F]+)>\s*<([0-9A-F]+)>\s*\[(.*?)\]", decoded, re.S):
            start, end, arr = mm.groups()
            code = int(start, 16)
            end_code = int(end, 16)
            for val in re.findall(r"<([0-9A-F]+)>", arr):
                if code > end_code:
                    break
                try:
                    cmap[code] = bytes.fromhex(val).decode("utf-16-be")
                except Exception:
                    pass
                code += 1

        for mm in re.finditer(r"<([0-9A-F]+)>\s*<([0-9A-F]+)>\s*<([0-9A-F]+)>", decoded):
            start, end, base = mm.groups()
            s, e, b = int(start, 16), int(end, 16), int(base, 16)
            for offs, code in enumerate(range(s, e + 1)):
                try:
                    cmap[code] = chr(b + offs)
                except Exception:
                    pass

        font_maps[font_name] = cmap

    return font_maps


def decode_literal(token: str) -> str:
    s = token[1:-1]
    out: list[str] = []
    i = 0
    while i < len(s):
        ch = s[i]
        if ch != "\\":
            out.append(ch)
            i += 1
            continue
        i += 1
        if i >= len(s):
            break
        ch = s[i]
        if ch in "nrtbf":
            out.append({"n": "\n", "r": "\r", "t": "\t", "b": "\b", "f": "\f"}[ch])
            i += 1
        elif ch in "()\\":
            out.append(ch)
            i += 1
        elif ch in "01234567":
            oct_digits = ch
            i += 1
            for _ in range(2):
                if i < len(s) and s[i] in "01234567":
                    oct_digits += s[i]
                    i += 1
            out.append(chr(int(oct_digits, 8)))
        else:
            out.append(ch)
            i += 1
    return "".join(out)


def decode_hex(hex_str: str, font_name: str, font_maps: dict[str, dict[int, str]]) -> str:
    cmap = font_maps.get(font_name, {})
    if not cmap:
        try:
            return bytes.fromhex(hex_str).decode("utf-16-be")
        except Exception:
            return ""
    out: list[str] = []
    for i in range(0, len(hex_str), 4):
        chunk = hex_str[i : i + 4]
        if len(chunk) == 4:
            out.append(cmap.get(int(chunk, 16), ""))
    return "".join(out)


def decode_show(expr: str, font_name: str, font_maps: dict[str, dict[int, str]]) -> str:
    expr = expr.strip()
    pieces: list[str] = []
    if expr.endswith("TJ"):
        arr = expr[:-2]
        for m in re.finditer(r"<([0-9A-F]+)>", arr):
            pieces.append(decode_hex(m.group(1), font_name, font_maps))
        for m in re.finditer(r"\((?:\\.|[^\\)])*\)", arr):
            pieces.append(decode_literal(m.group(0)))
    else:
        body = expr[:-2].strip()
        if body.startswith("<"):
            pieces.append(decode_hex(body[1:-1], font_name, font_maps))
        elif body.startswith("("):
            pieces.append(decode_literal(body))
    return "".join(pieces)


def extract_text(pdf_path: Path) -> str:
    data = pdf_path.read_bytes()
    raw_text = data.decode("latin1", errors="ignore")
    objects = parse_objects(raw_text)
    font_maps = build_font_maps(raw_text, objects)

    out_lines: list[str] = []
    current_font = "F1"

    for m in re.finditer(rb"stream\r?\n(.*?)\r?\nendstream", data, re.S):
        try:
            decoded = zlib.decompress(m.group(1)).decode("latin1", errors="ignore")
        except Exception:
            continue
        if "BT" not in decoded or ("Tj" not in decoded and "TJ" not in decoded):
            continue

        line = ""
        token_re = re.compile(
            r"/[A-Z]?F\d+\s+[\d.]+\s+Tf|"
            r"\[(?:.|\n|\r)*?\]\s*TJ|"
            r"\((?:\\.|[^\\)])*\)\s*Tj|"
            r"<[^>]+>\s*Tj|"
            r"ET|"
            r"[-\d.]+\s+[-\d.]+\s+Tm|"
            r"[-\d.]+\s+[-\d.]+\s+Td|"
            r"[-\d.]+\s+[-\d.]+\s+TD|"
            r"T\*"
        )
        for tok_match in token_re.finditer(decoded):
            tok = tok_match.group(0)
            font_match = re.fullmatch(r"/([A-Z]?F\d+)\s+[\d.]+\s+Tf", tok)
            if font_match:
                current_font = font_match.group(1)
                continue

            if tok == "ET" or tok == "T*" or tok.endswith(" Tm") or tok.endswith(" Td") or tok.endswith(" TD"):
                if line.strip():
                    out_lines.append(re.sub(r"\s+", " ", line.strip()))
                line = ""
                continue

            piece = decode_show(tok, current_font, font_maps).replace("\r", " ").replace("\n", " ")
            if piece:
                if line and not line.endswith(" ") and not piece.startswith((" ", ".", ",", ";", ":", ")")):
                    line += " "
                line += piece

        if line.strip():
            out_lines.append(re.sub(r"\s+", " ", line.strip()))

    return "\n".join(line for line in out_lines if line)


if __name__ == "__main__":
    pdf_path = next(Path(".").glob("*.pdf"))
    print(extract_text(pdf_path))
