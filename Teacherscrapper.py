from __future__ import annotations

import argparse
import csv
import json
import re
import time
from collections import deque
from typing import Dict, List, Optional
from urllib.parse import urljoin, urlparse

import requests
from bs4 import BeautifulSoup
try:
	import certifi
except Exception:
	certifi = None


DEFAULT_USER_AGENT = "Mozilla/5.0 (compatible; Teacherscrapper/1.0; +https://github.com/)"


def normalize_text(x: Optional[str]) -> str:
	if not x:
		return ""
	return re.sub(r"\s+", " ", x).strip()


def find_label_value_pairs(soup: BeautifulSoup) -> Dict[str, str]:
	pairs: Dict[str, str] = {}

	# <dl><dt>Label</dt><dd>Value</dd></dl>
	for dl in soup.find_all("dl"):
		dts = dl.find_all("dt")
		dds = dl.find_all("dd")
		for dt, dd in zip(dts, dds):
			k = normalize_text(dt.get_text()).lower().rstrip(":")
			v = normalize_text(dd.get_text())
			if k:
				pairs[k] = v

	# tables
	for table in soup.find_all("table"):
		# assume two-column table or th/td pairs
		for row in table.find_all("tr"):
			cols = row.find_all(["th", "td"])
			if len(cols) >= 2:
				k = normalize_text(cols[0].get_text()).lower().rstrip(":")
				v = normalize_text(cols[1].get_text())
				if k:
					pairs[k] = v

	# paragraphs that look like 'Label: value'
	for p in soup.find_all(["p", "li"]):
		text = normalize_text(p.get_text())
		if ":" in text:
			k, v = text.split(":", 1)
			k = k.lower().strip()
			v = v.strip()
			if len(k) < 40 and len(v) > 0:
				pairs.setdefault(k, v)

	return pairs


def extract_emails(text: str) -> List[str]:
	return list(set(re.findall(r"[A-Za-z0-9._%+-]+@[A-Za-z0-9.-]+\.[A-Za-z]{2,}", text)))


def extract_phones(text: str) -> List[str]:
	# simple phone matcher (international and local digits + separators)
	phones = re.findall(r"\+?\d[\d ()-]{5,}\d", text)
	# clean
	cleaned = [re.sub(r"\s+", " ", p).strip() for p in phones]
	return list(dict.fromkeys(cleaned))


class TeacherScraper:
	def __init__(self, start_url: str, user_agent: str = DEFAULT_USER_AGENT, delay: float = 0.5, max_pages: int = 1000):
		parsed = urlparse(start_url)
		self.start_url = start_url
		self.domain = parsed.netloc
		self.scheme = parsed.scheme or "https"
		self.session = requests.Session()
		self.session.headers.update({"User-Agent": user_agent})
		# if certifi is available, prefer its CA bundle
		if certifi is not None:
			self.session.verify = certifi.where()
		self.delay = float(delay)
		self.max_pages = int(max_pages)
		self.keywords = ["teacher", "faculty", "staff", "people", "personnel", "profile", "academic", "dept", "division",
                         "fse", "fsis", "fbs", "fah", "law", "dis", "shis", "qsis", "cse", "eee", "cce"]

	def is_same_domain(self, url: str) -> bool:
		try:
			p = urlparse(url)
			return p.netloc == self.domain or p.netloc.endswith("." + self.domain)
		except Exception:
			return False

	def get_links(self, soup: BeautifulSoup, base_url: str) -> List[str]:
		out = []
		for a in soup.find_all("a", href=True):
			href = a["href"].strip()
			if href.startswith("mailto:") or href.startswith("tel:"):
				continue
			full = urljoin(base_url, href)
			if self.is_same_domain(full):
				out.append(full.split("#")[0])
		return out

	def is_potential_relevant(self, url: str) -> bool:
		lower = url.lower()
		return any(k in lower for k in self.keywords)

	def looks_like_profile(self, url: str, soup: BeautifulSoup) -> bool:
		# heuristics: url contains faculty/teacher/people/staff/profile
		lower = url.lower()
		if any(k in lower for k in self.keywords):
			return True

		# page contains likely labels
		text = (soup.get_text() or "").lower()
		if "research" in text or "designation" in text or "email" in text:
			# also require a 'name' in header
			if soup.find(["h1", "h2"]) is not None:
				return True

		return False

	def extract_profile(self, url: str, soup: BeautifulSoup) -> Optional[Dict[str, object]]:
		# extract name
		name = ""
		if soup.title and soup.title.string:
			name = normalize_text(soup.title.string)

		# prefer h1/h2 contents
		for tag in ("h1", "h2", "h3"):
			t = soup.find(tag)
			if t and len(t.get_text(strip=True)) > 2:
				name = normalize_text(t.get_text())
				break

		pairs = find_label_value_pairs(soup)

		# common label keys mapping
		def get_pair(*keys, fallback: str = ""):
			for k in keys:
				if k in pairs:
					return pairs[k]
			return fallback

		designation = get_pair("designation", "position", "post")
		department = get_pair("department", "dept", "division")
		research = get_pair("research interests", "research", "area of interest", "research area")
		office = get_pair("office", "room", "office no", "office address")

		text = soup.get_text(" ")
		emails = extract_emails(text)
		phones = extract_phones(text)

		# image
		img = None
		imgtag = soup.find("img")
		if imgtag and imgtag.get("src"):
			img = urljoin(url, imgtag.get("src"))

		# if we don't find a name or email/phone at all, not a profile
		if not name and not emails and not phones and not designation:
			return None

		profile = {
			"name": name,
			"designation": designation,
			"department": department,
			"research_interests": research,
			"office": office,
			"emails": emails,
			"phones": phones,
			"image": img,
			"profile_url": url,
		}

		return profile

	def crawl(self, limit: Optional[int] = None, verbose: bool = True) -> List[Dict[str, object]]:
		limit = limit or self.max_pages
		seen = set()
		q = deque([self.start_url])
		results: List[Dict[str, object]] = []

		while q and len(seen) < limit and len(results) < 10000:
			url = q.popleft()
			if url in seen:
				continue
			seen.add(url)
			try:
				time.sleep(self.delay)
				r = self.session.get(url, timeout=15)
			except requests.exceptions.SSLError as e:
				if verbose:
					print(f"SSL error fetching {url}: {e}. Retrying with SSL verification disabled (insecure).")
				self.session.verify = False
				try:
					r = self.session.get(url, timeout=15)
				except Exception as retry_e:
					if verbose:
						print(f"Retry failed for {url}: {retry_e}")
					continue
			except Exception as e:
				if verbose:
					print(f"failed to fetch {url}: {e}")
				continue

			if r.status_code != 200 or 'text/html' not in r.headers.get('Content-Type', ''):
				continue

			soup = BeautifulSoup(r.text, "lxml")

			# collect links
			for link in self.get_links(soup, url):
				if link not in seen:
					if len(seen) + len(q) >= limit:
						break
					if self.is_potential_relevant(link):
						q.appendleft(link)  # prioritize relevant links
					else:
						q.append(link)

			# if looks like profile, attempt extract
			if self.looks_like_profile(url, soup):
				profile = self.extract_profile(url, soup)
				if profile:
					results.append(profile)
					if verbose:
						print(f"Found profile: {profile.get('name') or profile.get('emails')}")

		return results


def save_results(results: List[Dict[str, object]], out_path: str, fmt: str = "csv") -> None:
	fmt = fmt.lower()
	if fmt == "json":
		with open(out_path, "w", encoding="utf-8") as f:
			json.dump(results, f, ensure_ascii=False, indent=2)
		print(f"Saved {len(results)} records to {out_path} (json)")
		return

	# CSV: flatten lists
	fieldnames = [
		"name",
		"designation",
		"department",
		"research_interests",
		"office",
		"emails",
		"phones",
		"image",
		"profile_url",
	]
	with open(out_path, "w", encoding="utf-8", newline="") as f:
		writer = csv.DictWriter(f, fieldnames=fieldnames)
		writer.writeheader()
		for r in results:
			row = {k: "" for k in fieldnames}
			for k in fieldnames:
				v = r.get(k)
				if isinstance(v, list):
					row[k] = "; ".join(v)
				elif v is None:
					row[k] = ""
				else:
					row[k] = str(v)
			writer.writerow(row)

	print(f"Saved {len(results)} records to {out_path} (csv)")


SAMPLE_HTML = '''
<html>
  <head><title>Prof. John Doe - Department of Computer Science</title></head>
  <body>
	<h1>Prof. John Doe</h1>
	<p>Designation: Professor</p>
	<p>Department: Computer Science & Engineering</p>
	<p>Email: john.doe@iiuc.ac.bd</p>
	<p>Phone: +880 1234 567890</p>
	<p>Research interests: Algorithms, Distributed Systems</p>
  </body>
  </html>
'''


def run_test():
	print("Running local extraction tests...")
	soup = BeautifulSoup(SAMPLE_HTML, "lxml")
	s = TeacherScraper("https://www.iiuc.ac.bd/")
	profile = s.extract_profile("https://www.iiuc.ac.bd/faculty/john-doe", soup)
	assert profile is not None, "extract_profile returned None"
	assert profile["name"].lower().startswith("prof. john" ) or "john" in profile["name"].lower()
	assert "john.doe@iiuc.ac.bd" in profile["emails"]
	print("Test passed: sample profile extracted:")
	print(json.dumps(profile, indent=2))


def main():
	parser = argparse.ArgumentParser(description="Scrape teacher profiles from iiuc.ac.bd")
	parser.add_argument("--start-url", default="https://www.iiuc.ac.bd/", help="Start URL (domain will be restricted)")
	parser.add_argument("--output", default="teachers.csv", help="Output file path")
	parser.add_argument("--format", choices=["csv", "json"], default="csv", help="Output format")
	parser.add_argument("--delay", type=float, default=0.5, help="Delay between requests (seconds)")
	parser.add_argument("--max-pages", type=int, default=1000, help="Maximum pages to fetch")
	parser.add_argument("--user-agent", default=DEFAULT_USER_AGENT)
	parser.add_argument("--insecure", action="store_true", help="Skip SSL verification (not recommended)")
	parser.add_argument("--ca-bundle", help="Path to a PEM file with CA certificates to use for verification")
	parser.add_argument("--test", action="store_true", help="Run local extractor tests (no network)")
	args = parser.parse_args()

	if args.test:
		run_test()
		return

	scraper = TeacherScraper(start_url=args.start_url, user_agent=args.user_agent, delay=args.delay, max_pages=args.max_pages)
	if args.insecure:
		scraper.session.verify = False
	if args.ca_bundle:
		scraper.session.verify = args.ca_bundle
	print(f"Starting crawl at {args.start_url} (domain={scraper.domain})")
	results = scraper.crawl(limit=args.max_pages, verbose=True)
	save_results(results, args.output, fmt=args.format)


if __name__ == "__main__":
	main()