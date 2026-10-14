#!/usr/bin/env python3
"""
Full Wayback Machine grab of a site, with built-in metadata extraction.

Requires: requests, beautifulsoup4, Pillow (>=9.4), pypdf

- Enumerates every capture (all subdomains/paths) via the CDX API
- Keeps only the oldest capture of each distinct content digest per URL
- Fetches raw original bytes (id_ modifier: no toolbar, no rewritten links)
- Crawls links found in downloaded HTML/CSS to discover pages the CDX
  domain listing missed, and pulls their full histories too
- Extracts metadata inline:
    HTML  : title, <meta>, <link>, comments, headings, img alt/title/size,
            JSON-LD, links, plus a plain-text sidecar
    Images: format/size, EXIF (+Exif/GPS/Interop IFDs), IPTC, XMP, comments
    PDF   : document info, XMP, page count, text sidecar
    Office: docProps (core/app/custom) from docx/xlsx/pptx
    CSS/JS/SVG/XML: embedded comments
- Saves original HTTP headers (Last-Modified, ETag, Server...) per file
- Resumable: rerun and it picks up where it left off

Usage: python wayback_grab.py [domain] [outdir]
"""
import hashlib
import io
import json
import re
import sys
import threading
import time
import zipfile
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from urllib.parse import unquote, urldefrag, urljoin, urlsplit

# Third-party deps, imported up front so a missing one fails immediately
# instead of silently producing empty metadata mid-run.
import requests
from bs4 import BeautifulSoup, Comment
from PIL import ExifTags, Image, IptcImagePlugin
from pypdf import PdfReader

# ---------------------------------------------------------------- config
DOMAIN = (sys.argv[1] if len(sys.argv) > 1 else "mdvaden.com").lower()
OUT = Path(sys.argv[2] if len(sys.argv) > 2 else "mdvaden_archive")
WORKERS = 4              # keep low; archive.org rate-limits aggressively
MAX_RETRIES = 6
CDX = "https://web.archive.org/cdx/search/cdx"

FILES, TEXT = OUT / "files", OUT / "text"
MANIFEST, FAILED = OUT / "manifest.jsonl", OUT / "failed.jsonl"
lock = threading.Lock()

session = requests.Session()
session.headers["User-Agent"] = "personal-archive-research/1.0 (wayback_grab.py)"


# ---------------------------------------------------------------- network
def get_with_retry(url, **kw):
    delay = 5
    for _ in range(MAX_RETRIES):
        try:
            r = session.get(url, timeout=60, **kw)
            if r.status_code == 200:
                return r
            if r.status_code in (404, 410):
                return None
        except requests.RequestException:
            pass
        time.sleep(delay)
        delay = min(delay * 2, 120)
    raise RuntimeError(f"gave up on {url}")


def list_captures(url, match):
    """Yield CDX rows (dicts) for a URL or whole domain, handling paging."""
    params = {
        "url": url, "matchType": match, "output": "json",
        "fl": "timestamp,original,mimetype,statuscode,digest,length,urlkey",
        "filter": "statuscode:200", "limit": 50000, "showResumeKey": "true",
    }
    resume = None
    while True:
        p = dict(params)
        if resume:
            p["resumeKey"] = resume
        r = get_with_retry(CDX, params=p)
        rows = r.json() if (r is not None and r.text.strip()) else []
        resume = None
        if len(rows) >= 2 and rows[-2] == []:
            resume = rows[-1][0]
            rows = rows[:-2]
        header = ["timestamp", "original", "mimetype", "statuscode",
                  "digest", "length", "urlkey"]
        if rows and rows[0][0] == "timestamp":
            header, rows = rows[0], rows[1:]
        for row in rows:
            yield dict(zip(header, row))
        if not resume:
            break


def pick_oldest_unique(rows):
    """Per urlkey, keep the oldest capture of each distinct digest."""
    rows = sorted(rows, key=lambda r: (r["urlkey"], r["timestamp"]))
    seen, keep = set(), []
    for r in rows:
        k = (r["urlkey"], r["digest"])
        if k not in seen:
            seen.add(k)
            keep.append(r)
    return keep


def url_key(u):
    s = urlsplit(u)
    host = (s.hostname or "").lower()
    host = host[4:] if host.startswith("www.") else host
    return f"{host}{unquote(s.path) or '/'}" + (f"?{s.query}" if s.query else "")


def in_domain(u):
    s = urlsplit(u)
    h = (s.hostname or "").lower()
    return s.scheme in ("http", "https") and (h == DOMAIN or h.endswith("." + DOMAIN))


# ---------------------------------------------------------------- paths
def safe_seg(s):
    s = re.sub(r'[<>:"|?*\\\x00-\x1f]', "_", s)
    return s[:150] or "_"


def local_path(url, ts, ctype):
    u = urlsplit(url)
    host = (u.hostname or "unknown").lower()
    path = unquote(u.path)
    segs = [safe_seg(s) for s in path.split("/") if s]
    if path.endswith("/") or not segs:
        name, dirs = "index", segs
    else:
        name, dirs = segs[-1], segs[:-1]
    if u.query:
        name += "__q" + hashlib.sha1(u.query.encode()).hexdigest()[:8]
    if "." not in name and "html" in ctype.lower():
        name += ".html"
    return FILES.joinpath(host, *dirs, f"{ts}__{name}")


# ---------------------------------------------------------------- metadata
def jsonable(v):
    if isinstance(v, bytes):
        try:
            return v.decode("utf-8").rstrip("\x00")
        except UnicodeDecodeError:
            return {"hex": v[:256].hex(), "len": len(v)}
    if v is None or isinstance(v, (str, int, bool, float)):
        return v
    if isinstance(v, dict):
        return {str(k): jsonable(x) for k, x in v.items()}
    if isinstance(v, (list, tuple, set)):
        return [jsonable(x) for x in v]
    try:
        return float(v)
    except Exception:
        return str(v)


XMP_RE = re.compile(rb"<\?xpacket begin.*?<\?xpacket end[^>]*\?>", re.S)
XMP_RE2 = re.compile(rb"<x:xmpmeta.*?</x:xmpmeta>", re.S)


def find_xmp(data):
    m = XMP_RE.search(data) or XMP_RE2.search(data)
    return m.group(0).decode("utf-8", "replace")[:300_000] if m else None


def image_meta(data):
    m = {}
    with Image.open(io.BytesIO(data)) as im:
        m.update(format=im.format, size=im.size, mode=im.mode,
                 n_frames=getattr(im, "n_frames", 1))
        skip = {"exif", "icc_profile", "photoshop", "xmp", "XML:com.adobe.xmp"}
        m["info"] = {k: v for k, v in im.info.items() if k not in skip}
        try:
            ex = im.getexif()
            if ex:
                d = {ExifTags.TAGS.get(k, str(k)): v for k, v in ex.items()}
                for ifd, name, tags in ((ExifTags.IFD.Exif, "Exif", ExifTags.TAGS),
                                        (ExifTags.IFD.GPSInfo, "GPS", ExifTags.GPSTAGS),
                                        (ExifTags.IFD.Interop, "Interop", ExifTags.TAGS)):
                    try:
                        sub = ex.get_ifd(ifd)
                    except Exception:
                        sub = {}
                    if sub:
                        d[name] = {tags.get(k, str(k)): v for k, v in sub.items()}
                m["exif"] = d
        except Exception as e:
            m["exif_error"] = str(e)
        try:
            iptc = IptcImagePlugin.getiptcinfo(im)
            if iptc:
                m["iptc"] = {f"{a}:{b}": v for (a, b), v in iptc.items()}
        except Exception:
            pass
    return m


COMMENT_RES = {
    "css": re.compile(r"/\*.*?\*/", re.S),
    "js": re.compile(r"/\*.*?\*/|(?<![:\"'\\])//[^\n]*", re.S),
    "xml": re.compile(r"<!--.*?-->", re.S),
}


def html_meta(data, base_url):
    soup = BeautifulSoup(data, "html.parser")
    m = {
        "title": soup.title.get_text(strip=True) if soup.title else None,
        "html_attrs": dict(soup.html.attrs) if soup.html else {},
        "meta_tags": [dict(t.attrs) for t in soup.find_all("meta")],
        "link_tags": [dict(t.attrs) for t in soup.find_all("link")],
        "comments": [str(c).strip() for c in
                     soup.find_all(string=lambda s: isinstance(s, Comment))],
        "headings": [[h.name, h.get_text(" ", strip=True)]
                     for h in soup.find_all(re.compile(r"^h[1-6]$"))],
        "images": [{k: i.get(k) for k in
                    ("src", "alt", "title", "width", "height", "longdesc")
                    if i.get(k) is not None} for i in soup.find_all("img")],
        "json_ld": [s.get_text() for s in
                    soup.find_all("script", type="application/ld+json")],
        "title_attrs": [t["title"] for t in soup.find_all(title=True)][:500],
    }
    # URLs to follow
    found = set()
    for tag, attr in (("a", "href"), ("area", "href"), ("img", "src"),
                      ("link", "href"), ("script", "src"), ("iframe", "src"),
                      ("frame", "src"), ("source", "src"), ("embed", "src"),
                      ("object", "data"), ("body", "background"),
                      ("td", "background"), ("table", "background")):
        for t in soup.find_all(tag):
            v = t.get(attr)
            if v:
                found.add(v)
    for t in soup.find_all(srcset=True):
        for part in t["srcset"].split(","):
            if part.strip():
                found.add(part.strip().split()[0])
    for t in soup.find_all(style=True):
        for v in re.findall(r"url\(['\"]?([^)'\"]+)", t["style"]):
            found.add(v)
    m["_urls"] = found
    for s in soup(["script", "style"]):
        s.decompose()
    text = soup.get_text("\n", strip=True)
    return m, text


def pdf_meta(data):
    r = PdfReader(io.BytesIO(data))
    m = {"pages": len(r.pages), "info": dict(r.metadata or {}),
         "xmp": find_xmp(data)}
    text = []
    for p in r.pages:
        try:
            text.append(p.extract_text() or "")
        except Exception:
            pass
    return m, "\n\n".join(text)


def office_meta(data):
    m = {}
    with zipfile.ZipFile(io.BytesIO(data)) as z:
        for n in ("docProps/core.xml", "docProps/app.xml", "docProps/custom.xml"):
            if n in z.namelist():
                m[n] = z.read(n).decode("utf-8", "replace")
    return m


def extract(data, ctype, url):
    """Return (meta_dict, text_or_None, discovered_urls)."""
    ctype = ctype.lower()
    name = urlsplit(url).path.lower()
    meta, text, urls = {}, None, set()
    try:
        head = data[:2048].lstrip().lower()
        if ("html" in ctype or head.startswith((b"<!doctype html", b"<html"))):
            meta, text = html_meta(data, url)
            urls = meta.pop("_urls")
            meta["kind"] = "html"
        elif data[:4] == b"%PDF":
            meta, text = pdf_meta(data)
            meta["kind"] = "pdf"
        elif data[:2] == b"PK" and name.endswith((".docx", ".xlsx", ".pptx")):
            meta = office_meta(data)
            meta["kind"] = "office"
        elif ctype.startswith("image/") and "svg" not in ctype:
            meta = image_meta(data)
            meta["xmp"] = find_xmp(data)
            meta["kind"] = "image"
        elif "css" in ctype or name.endswith(".css"):
            txt = data.decode("utf-8", "replace")
            meta = {"kind": "css", "comments": COMMENT_RES["css"].findall(txt)}
            urls = set(re.findall(r"url\(['\"]?([^)'\"]+)", txt))
            urls |= set(re.findall(r"@import\s+['\"]([^'\"]+)", txt))
        elif "javascript" in ctype or name.endswith(".js"):
            txt = data.decode("utf-8", "replace")
            meta = {"kind": "js", "comments": COMMENT_RES["js"].findall(txt)[:500]}
        elif "xml" in ctype or "svg" in ctype or name.endswith((".svg", ".xml")):
            txt = data.decode("utf-8", "replace")
            meta = {"kind": "xml", "comments": COMMENT_RES["xml"].findall(txt),
                    "xmp": find_xmp(data)}
        elif ctype.startswith("text/"):
            meta = {"kind": "text"}
            text = data.decode("utf-8", "replace")
        else:
            meta = {"kind": "other", "xmp": find_xmp(data)}
    except Exception as e:
        meta = {"kind": meta.get("kind", "unknown"), "extract_error": repr(e)}

    abs_urls = set()
    for u in urls:
        u = urldefrag(urljoin(url, u.strip()))[0]
        if in_domain(u):
            abs_urls.add(u)
    return jsonable(meta), text, sorted(abs_urls)


# ---------------------------------------------------------------- fetching
def fetch_one(row):
    ts, orig = row["timestamp"], row["original"]
    wb_url = f"https://web.archive.org/web/{ts}id_/{orig}"
    r = get_with_retry(wb_url, allow_redirects=True)
    if r is None:
        raise RuntimeError("404/410 from wayback")
    data = r.content
    ctype = r.headers.get("Content-Type", row["mimetype"]).split(";")[0].strip()
    dest = local_path(orig, ts, ctype)
    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_name(dest.name + ".part")
    tmp.write_bytes(data)
    tmp.replace(dest)

    meta, text, found = extract(data, ctype, orig)
    text_path = None
    if text and text.strip():
        text_path = TEXT / (str(dest.relative_to(FILES)) + ".txt")
        text_path.parent.mkdir(parents=True, exist_ok=True)
        text_path.write_text(text, encoding="utf-8")

    orig_headers = {k[len("x-archive-orig-"):].lower(): v
                    for k, v in r.headers.items()
                    if k.lower().startswith("x-archive-orig-")}
    return {
        "url": orig, "timestamp": ts, "wayback_url": wb_url,
        "cdx_digest": row["digest"], "content_type": ctype,
        "saved_as": str(dest.relative_to(OUT)),
        "text_file": str(text_path.relative_to(OUT)) if text_path else None,
        "bytes": len(data), "sha256": hashlib.sha256(data).hexdigest(),
        "orig_headers": orig_headers, "metadata": meta,
        "discovered_urls": found,
    }


def load_manifest():
    done, links = set(), set()
    if MANIFEST.exists():
        for line in MANIFEST.read_text().splitlines():
            try:
                m = json.loads(line)
                done.add((m["url"], m["timestamp"]))
                links.update(m.get("discovered_urls", []))
            except Exception:
                pass
    return done, links


def main():
    OUT.mkdir(parents=True, exist_ok=True)
    print(f"Listing captures for {DOMAIN} (all subdomains and paths) ...")
    rows = list(list_captures(DOMAIN, "domain"))
    print(f"  {len(rows)} captures with status 200")

    known = {url_key(r["original"]) for r in rows}
    queried = set()
    done, candidates = load_manifest()
    todo = pick_oldest_unique(rows)
    print(f"  {len(todo)} unique versions after dedup\n")

    ok = bad = 0
    with ThreadPoolExecutor(WORKERS) as ex, \
         open(MANIFEST, "a") as mf, open(FAILED, "a") as ff:

        def run(batch):
            nonlocal ok, bad
            batch = [r for r in batch if (r["original"], r["timestamp"]) not in done]
            print(f"Downloading {len(batch)} files ...")
            futs = [(r, ex.submit(fetch_one, r)) for r in batch]
            for i, (row, fut) in enumerate(futs, 1):
                try:
                    rec = fut.result()
                    done.add((rec["url"], rec["timestamp"]))
                    candidates.update(rec["discovered_urls"])
                    with lock:
                        mf.write(json.dumps(rec) + "\n")
                        mf.flush()
                    ok += 1
                except Exception as e:
                    with lock:
                        ff.write(json.dumps({**row, "error": str(e)}) + "\n")
                        ff.flush()
                    bad += 1
                if i % 25 == 0 or i == len(futs):
                    print(f"  [{i}/{len(futs)}] ok={ok} failed={bad}")

        run(todo)

        # Crawl: find same-domain URLs referenced by pages but absent from CDX listing
        round_no = 0
        while True:
            new = {u for u in candidates
                   if url_key(u) not in known and url_key(u) not in queried}
            if not new:
                break
            round_no += 1
            print(f"\nCrawl round {round_no}: {len(new)} newly referenced URLs")
            for u in new:
                queried.add(url_key(u))

            def lookup(u):
                try:
                    return list(list_captures(u, "exact"))
                except Exception:
                    return []

            found_rows = []
            for result in ex.map(lookup, sorted(new)):
                found_rows.extend(result)
            for r in found_rows:
                known.add(url_key(r["original"]))
            batch = pick_oldest_unique(found_rows)
            print(f"  {len(batch)} archived versions found")
            if batch:
                run(batch)

    print(f"\nDone. {ok} downloaded, {bad} failed (see failed.jsonl; rerun to retry).")
    print(f"Files: {FILES}\nExtracted text: {TEXT}\nMetadata + headers: {MANIFEST}")


if __name__ == "__main__":
    main()
