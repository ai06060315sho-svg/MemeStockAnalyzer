"""
SEC EDGAR Form 4 インサイダー取引追跡
役員・取締役の自社株買いを検出する
"""
import logging
import requests
import time
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta
from typing import List, Dict, Optional
from config import Config

logger = logging.getLogger('MemeStock.Insider')


class InsiderTracker:
    BASE_URL = "https://efts.sec.gov/LATEST"
    FILING_URL = "https://www.sec.gov/cgi-bin/browse-edgar"
    EDGAR_API = "https://data.sec.gov"

    def __init__(self):
        self.headers = {'User-Agent': Config.SEC_EDGAR_USER_AGENT}
        self._cik_cache = {}
        self._load_cik_map()

    def _load_cik_map(self):
        """SEC公式のticker→CIKマッピングを取得"""
        try:
            resp = requests.get(
                "https://www.sec.gov/files/company_tickers.json",
                headers=self.headers, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                for entry in data.values():
                    ticker = entry.get('ticker', '').upper()
                    cik = str(entry.get('cik_str', ''))
                    if ticker and cik:
                        self._cik_cache[ticker] = cik.zfill(10)
                print(f"[Insider] CIK map loaded: {len(self._cik_cache)} tickers")
        except Exception as e:
            print(f"[Insider] CIK map load error: {e}")

    def get_cik(self, ticker: str) -> Optional[str]:
        """ティッカーからCIKを取得"""
        return self._cik_cache.get(ticker.upper())

    def check_insider_buying(self, ticker: str) -> List[Dict]:
        """指定銘柄のインサイダー買いを取得

        SEC EDGAR Full-Text Search APIで最近のForm 4を検索し、
        購入取引（transactionCode='P'）のみを返す
        """
        cik = self.get_cik(ticker)
        if not cik:
            return []

        results = []
        try:
            # EDGAR Full-Text Search
            start_date = (datetime.now() - timedelta(days=Config.INSIDER_LOOKBACK_DAYS)
                          ).strftime('%Y-%m-%d')
            resp = requests.get(
                f"{self.BASE_URL}/search-index",
                params={
                    'q': f'"form-type"="4"',
                    'dateRange': 'custom',
                    'startdt': start_date,
                    'enddt': datetime.now().strftime('%Y-%m-%d'),
                    'forms': '4',
                    'entityName': ticker,
                },
                headers=self.headers, timeout=15)

            if resp.status_code != 200:
                # フォールバック: EDGAR submissions APIを使用
                results = self._check_via_submissions(cik, ticker)
                return results

            data = resp.json()
            hits = data.get('hits', {}).get('hits', [])

            for hit in hits[:10]:  # 最新10件まで
                filing_url = hit.get('_source', {}).get('file_url', '')
                if filing_url:
                    parsed = self._parse_form4(filing_url, ticker)
                    if parsed:
                        results.extend(parsed)
                time.sleep(0.1)  # SEC rate limit: 10 req/sec

        except Exception as e:
            # フォールバック
            try:
                results = self._check_via_submissions(cik, ticker)
            except Exception as e2:
                print(f"[Insider] {ticker} error: {e2}")

        # 購入のみフィルタ（金額下限あり）
        buys = [r for r in results
                if r.get('transaction_type') == 'Purchase'
                and (r.get('total_value', 0) or 0) >= Config.MIN_INSIDER_BUY_USD]

        return buys

    def _check_via_submissions(self, cik: str, ticker: str) -> List[Dict]:
        """EDGAR Submissions APIでForm 4を取得（フォールバック）"""
        results = []
        try:
            resp = requests.get(
                f"{self.EDGAR_API}/submissions/CIK{cik}.json",
                headers=self.headers, timeout=15)
            if resp.status_code != 200:
                return []

            data = resp.json()
            recent = data.get('filings', {}).get('recent', {})
            forms = recent.get('form', [])
            dates = recent.get('filingDate', [])
            accessions = recent.get('accessionNumber', [])

            cutoff = (datetime.now() - timedelta(days=Config.INSIDER_LOOKBACK_DAYS)
                      ).strftime('%Y-%m-%d')

            for idx, form in enumerate(forms):
                if form != '4':
                    continue
                if idx < len(dates) and dates[idx] < cutoff:
                    continue

                if idx < len(accessions):
                    acc = accessions[idx].replace('-', '')
                    xml_url = (f"{self.EDGAR_API}/Archives/edgar/data/"
                               f"{cik}/{acc}/primary_doc.xml")
                    parsed = self._parse_form4(xml_url, ticker)
                    if parsed:
                        results.extend(parsed)
                    time.sleep(0.1)

        except Exception as e:
            print(f"[Insider] Submissions API error for {ticker}: {e}")

        return results

    def _parse_form4(self, url: str, ticker: str) -> List[Dict]:
        """Form 4 XMLをパースしてインサイダー取引データを抽出"""
        results = []
        try:
            resp = requests.get(url, headers=self.headers, timeout=10)
            if resp.status_code != 200:
                return []

            # XMLパース（名前空間を無視）
            text = resp.text
            # 名前空間を除去（Form 4のXMLは名前空間が不統一）
            import re
            text = re.sub(r'\sxmlns[^"]*"[^"]*"', '', text)

            root = ET.fromstring(text)

            # 報告者情報
            owner = root.find('.//reportingOwner')
            if owner is None:
                return []

            owner_name = ''
            owner_title = ''
            name_elem = owner.find('.//rptOwnerName')
            if name_elem is not None:
                owner_name = name_elem.text or ''
            title_elem = owner.find('.//officerTitle')
            if title_elem is not None:
                owner_title = title_elem.text or ''

            # 取引情報
            for txn in root.findall('.//nonDerivativeTransaction'):
                code_elem = txn.find('.//transactionCode')
                if code_elem is None:
                    continue
                code = code_elem.text or ''

                # P=Purchase(買い), S=Sale(売り), A=Grant(付与)
                if code == 'P':
                    txn_type = 'Purchase'
                elif code == 'S':
                    txn_type = 'Sale'
                else:
                    continue

                shares_elem = txn.find('.//transactionShares/value')
                price_elem = txn.find('.//transactionPricePerShare/value')
                date_elem = txn.find('.//transactionDate/value')

                shares = float(shares_elem.text) if shares_elem is not None and shares_elem.text else 0
                price = float(price_elem.text) if price_elem is not None and price_elem.text else 0
                date = date_elem.text if date_elem is not None else ''

                results.append({
                    'ticker': ticker,
                    'insider_name': owner_name,
                    'insider_title': owner_title,
                    'transaction_type': txn_type,
                    'shares': int(shares),
                    'price_per_share': round(price, 4),
                    'total_value': round(shares * price, 2),
                    'date': date,
                })

        except ET.ParseError as e:
            logger.debug(f"{ticker}: Form4 XML parse error: {e}")
        except Exception as e:
            logger.warning(f"{ticker}: Form4 parse error: {e}")

        return results

    # インサイダー役職のランク（CEO/CFOの購入は重み付けが高い）
    INSIDER_RANK = {
        'CEO': 5, 'Chief Executive Officer': 5,
        'CFO': 4, 'Chief Financial Officer': 4,
        'COO': 4, 'Chief Operating Officer': 4,
        'CTO': 3, 'Chief Technology Officer': 3,
        'Chairman': 5, 'President': 4,
        'Director': 3, 'VP': 2, 'Vice President': 2,
        'SVP': 3, 'EVP': 3,
    }

    def get_insider_rank(self, title: str) -> int:
        """インサイダーの役職からランク(1-5)を返す"""
        if not title:
            return 1
        title_upper = title.upper()
        for key, rank in self.INSIDER_RANK.items():
            if key.upper() in title_upper:
                return rank
        return 1

    def scan_tickers(self, tickers: List[str]) -> List[Dict]:
        """複数銘柄のインサイダー買いを一括チェック"""
        all_buys = []
        for i, ticker in enumerate(tickers):
            buys = self.check_insider_buying(ticker)
            if buys:
                # 役職ランクを付加
                for buy in buys:
                    buy['insider_rank'] = self.get_insider_rank(
                        buy.get('insider_title', ''))
                all_buys.extend(buys)
                logger.info(f"{ticker}: {len(buys)} insider buy(s) found")
            if i > 0 and i % 10 == 0:
                time.sleep(1)  # SEC rate limit対策
        return all_buys
