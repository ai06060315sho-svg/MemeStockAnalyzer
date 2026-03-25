"""
米国株スキャナー
$5以下の銘柄から異常出来高・出来高じわ上げを検出する
"""
import logging
import time
import requests
import yfinance as yf
import pandas as pd
from datetime import datetime, timezone, timedelta
from typing import List, Dict, Optional
from config import Config

logger = logging.getLogger('MemeStock.Scanner')


def yf_retry(func, max_retries=3, base_wait=10):
    """yfinance呼び出しのリトライラッパー（指数バックオフ）"""
    for attempt in range(max_retries):
        try:
            return func()
        except Exception as e:
            err_str = str(e)
            if 'Too Many Requests' in err_str or '429' in err_str:
                wait = base_wait * (2 ** attempt)
                logger.warning(f"Rate limited, waiting {wait}s (attempt {attempt+1}/{max_retries})")
                time.sleep(wait)
            elif attempt < max_retries - 1:
                time.sleep(2)
            else:
                logger.error(f"yfinance call failed after {max_retries} attempts: {e}")
                raise
    return None


class StockScanner:
    def __init__(self, db=None):
        self.db = db
        self.api_key = Config.POLYGON_API_KEY
        self._universe_cache = []
        self._last_universe_update = 0
        self._float_cache = {}  # ティッカー -> {data, timestamp}
        self._float_cache_ttl = 3600 * 4  # 4時間キャッシュ

    # === 銘柄ユニバース構築 ===
    def build_universe(self) -> List[Dict]:
        """NASDAQ APIから$5以下の全米国株を一括取得（数秒で完了）"""
        print("[Scanner] Building stock universe (under $5)...")
        penny_stocks = []

        try:
            resp = requests.get(
                'https://api.nasdaq.com/api/screener/stocks',
                params={'tableType': 'STOCK', 'listingTiers': 'GLOBAL', 'download': 'true'},
                headers={'User-Agent': 'Mozilla/5.0'},
                timeout=30)

            if resp.status_code != 200:
                print(f"[Scanner] NASDAQ API error: {resp.status_code}")
                return []

            data = resp.json()
            rows = data.get('data', {}).get('rows', [])
            print(f"[Scanner] Total US stocks from NASDAQ: {len(rows)}")

            for r in rows:
                try:
                    price_str = r.get('lastsale', '').replace('$', '').replace(',', '')
                    if not price_str:
                        continue
                    price = float(price_str)
                    if 0.01 <= price <= Config.MAX_PRICE:
                        symbol = r.get('symbol', '').strip()
                        # ワラントや権利銘柄を除外
                        if not symbol or len(symbol) > 5 or '/' in symbol or '^' in symbol:
                            continue
                        penny_stocks.append({
                            'ticker': symbol,
                            'name': r.get('name', ''),
                            'last_price': price,
                            'market_cap': None,
                            'sector': r.get('sector', ''),
                            'industry': r.get('industry', ''),
                            'country': r.get('country', ''),
                        })
                except (ValueError, KeyError):
                    continue

        except Exception as e:
            print(f"[Scanner] Universe build error: {e}")

        print(f"[Scanner] Penny stocks (under ${Config.MAX_PRICE}): {len(penny_stocks)}")
        return penny_stocks

    def get_universe(self, force_refresh: bool = False) -> List[str]:
        """キャッシュ付きで銘柄ユニバースを取得"""
        # DBからキャッシュを確認
        if self.db and not force_refresh:
            age = self.db.get_universe_age_hours()
            if age < Config.UNIVERSE_REFRESH_HOURS:
                cached = self.db.get_universe()
                if cached:
                    print(f"[Scanner] Using cached universe: {len(cached)} tickers "
                          f"({age:.1f}h old)")
                    return cached

        # 新規構築（NASDAQ APIで一括取得、数秒で完了）
        penny_stocks = self.build_universe()

        # DBに保存
        if self.db and penny_stocks:
            self.db.save_universe(penny_stocks)

        return [t['ticker'] for t in penny_stocks]

    # === 出来高スパイク検出 ===
    def scan_volume_spikes(self, tickers: List[str] = None) -> List[Dict]:
        """出来高スパイクを検出

        検出パターン:
        1. 出来高スパイク: 今日の出来高が20日平均の3倍以上
        2. 出来高じわ上げ: 3日連続で出来高が増加中（各日1.5倍以上）
        """
        if tickers is None:
            tickers = self.get_universe()

        if not tickers:
            print("[Scanner] No tickers to scan")
            return []

        print(f"[Scanner] Scanning {len(tickers)} tickers for volume spikes...")
        alerts = []
        batch_size = 15  # e2-micro(1GB RAM)対応: 小バッチ+GC
        import gc

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            try:
                # レート制限対策: リトライ付きダウンロード
                data = None
                for _retry in range(3):
                    try:
                        data = yf.download(batch, period='30d', progress=False,
                                           threads=False, ignore_tz=True,
                                           prepost=True)
                        break
                    except Exception as _e:
                        if 'Too Many Requests' in str(_e) or '429' in str(_e):
                            print(f"[Scanner] Rate limited, waiting 30s... (retry {_retry+1}/3)")
                            time.sleep(30)
                        else:
                            raise
                if data is None or data.empty:
                    gc.collect()
                    continue

                volume = data.get('Volume')
                close = data.get('Close')
                high = data.get('High')
                low = data.get('Low')
                if volume is None or close is None:
                    continue

                if isinstance(volume, pd.Series):
                    volume = volume.to_frame()
                    close = close.to_frame()
                    high = high.to_frame() if high is not None else None
                    low = low.to_frame() if low is not None else None

                for sym in batch:
                    try:
                        if sym not in volume.columns:
                            continue

                        vol_series = volume[sym].dropna()
                        close_series = close[sym].dropna()

                        if len(vol_series) < 5:
                            continue

                        current_price = float(close_series.iloc[-1])
                        if current_price > Config.MAX_PRICE or current_price < 0.01:
                            continue

                        # 既に急騰済みの銘柄を除外（天井掴み防止）
                        if len(close_series) >= 2:
                            prev_close_chk = float(close_series.iloc[-2])
                            if prev_close_chk > 0:
                                price_change_pct = (current_price - prev_close_chk) / prev_close_chk * 100
                                if price_change_pct >= 50:
                                    logger.info(f"SKIP {sym}: already +{price_change_pct:.0f}% (ceiling risk)")
                                    continue

                        current_vol = int(vol_series.iloc[-1])
                        avg_vol_20 = int(vol_series.iloc[-21:-1].mean()) if len(vol_series) >= 21 else int(vol_series.iloc[:-1].mean())

                        if avg_vol_20 < Config.MIN_AVG_VOLUME:
                            continue

                        vol_ratio = current_vol / avg_vol_20 if avg_vol_20 > 0 else 0

                        # パターン1: 出来高スパイク
                        if vol_ratio >= Config.VOLUME_SPIKE_MULT:
                            alerts.append({
                                'ticker': sym,
                                'price': current_price,
                                'volume': current_vol,
                                'avg_volume': avg_vol_20,
                                'volume_ratio': round(vol_ratio, 2),
                                'alert_type': 'VOLUME_SPIKE',
                                'detail': f'Volume {vol_ratio:.1f}x above 20-day avg',
                            })

                        # パターン2: 出来高じわ上げ（連続増加）
                        # 実績: 勝率54%, 翌日+6.6%（最も実用的なパターン）
                        # 感度を上げる: 2日連続でも検出、倍率1.3倍以上に緩和
                        if len(vol_series) >= 3:
                            # 3日連続チェック（従来通り）
                            rising_3d = False
                            if len(vol_series) >= 4:
                                recent_3 = vol_series.iloc[-3:]
                                rising_3d = all(
                                    recent_3.iloc[j] >= recent_3.iloc[j - 1] * 1.3
                                    for j in range(1, len(recent_3))
                                )

                            # 2日連続チェック（新規：感度UP）
                            recent_2 = vol_series.iloc[-2:]
                            rising_2d = (recent_2.iloc[1] >= recent_2.iloc[0] * Config.VOLUME_RISING_MULT)

                            if (rising_3d and vol_ratio >= 1.3) or (rising_2d and vol_ratio >= 2.0):
                                existing = [a for a in alerts
                                            if a['ticker'] == sym]
                                if not existing:
                                    days_rising = 3 if rising_3d else 2
                                    alerts.append({
                                        'ticker': sym,
                                        'price': current_price,
                                        'volume': current_vol,
                                        'avg_volume': avg_vol_20,
                                        'volume_ratio': round(vol_ratio, 2),
                                        'alert_type': 'VOLUME_RISING',
                                        'detail': f'Volume rising {days_rising} consecutive days ({vol_ratio:.1f}x avg)',
                                    })

                        # パターン3: 価格急騰（+20%以上）
                        # 実績: 翌日平均-10.3%, 勝率42% → 天井掴みリスク大
                        # +30%以上はスキップ（天井掴み確率高い）
                        # +20-30%は出来高5倍以上の場合のみ記録
                        if len(close_series) >= 2:
                            prev_close = float(close_series.iloc[-2])
                            if prev_close > 0:
                                pct_change = (current_price - prev_close) / prev_close * 100
                                if (pct_change >= Config.PRICE_SPIKE_PCT
                                        and pct_change < 30  # +30%以上は除外
                                        and current_vol >= Config.PRICE_SPIKE_MIN_VOLUME
                                        and vol_ratio >= 5):  # 出来高5倍以上のみ
                                    existing = [a for a in alerts
                                                if a['ticker'] == sym]
                                    if not existing:
                                        alerts.append({
                                            'ticker': sym,
                                            'price': current_price,
                                            'volume': current_vol,
                                            'avg_volume': avg_vol_20,
                                            'volume_ratio': round(vol_ratio, 2),
                                            'price_change_pct': round(pct_change, 1),
                                            'alert_type': 'PRICE_SPIKE',
                                            'detail': f'+{pct_change:.1f}% today with {vol_ratio:.1f}x volume (ceiling risk)',
                                        })

                    except Exception:
                        continue

            except Exception as e:
                print(f"[Scanner] Scan batch error: {e}")
                continue
            finally:
                # メモリ解放（e2-micro対応）
                data = None
                gc.collect()

            time.sleep(3)  # バッチ間待機

        # ユニバースから業種情報を付加
        if self.db:
            import sqlite3 as _sq
            conn = self.db._get_conn()
            try:
                conn.row_factory = _sq.Row
                for a in alerts:
                    row = conn.execute(
                        "SELECT sector, industry, name FROM stock_universe WHERE ticker = ?",
                        (a['ticker'],)).fetchone()
                    if row:
                        a['sector'] = row['sector'] or ''
                        a['industry'] = row['industry'] or ''
                        if not a.get('company_name'):
                            a['company_name'] = row['name'] or ''
                        from translator import translate_industry
                        a['industry_ja'] = translate_industry(a['industry'])
            finally:
                conn.close()

        # スコア計算してソート
        for a in alerts:
            self.calculate_score(a)
        alerts.sort(key=lambda x: x.get('score', 0), reverse=True)
        print(f"[Scanner] Found {len(alerts)} alerts")
        return alerts

    # === 偽シグナルフィルタ（決算・増資・株式併合除外） ===
    def check_catalyst_filter(self, ticker: str) -> Dict:
        """ミーム以外の急騰要因（決算、増資、株式併合等）を検出して除外判定"""
        result = {
            'has_earnings': False,
            'has_offering': False,
            'has_reverse_split': False,
            'has_split': False,
            'reverse_split_ratio': None,
            'filter_reason': None,
            'should_filter': False,
        }
        try:
            stock = yf_retry(lambda: yf.Ticker(ticker))
            if stock is None:
                return result

            # === 1. 株式併合（リバーススプリット）検出 ===
            # yfinanceのsplitsデータで直近30日以内の併合を検出
            try:
                splits = stock.splits
                if splits is not None and len(splits) > 0:
                    from datetime import date
                    today = date.today()
                    for split_date, ratio in splits.items():
                        # split_dateのdate部分を取得
                        if hasattr(split_date, 'date'):
                            sd = split_date.date()
                        else:
                            sd = split_date
                        days_ago = (today - sd).days

                        # 直近30日以内のスプリット
                        if days_ago <= 30:
                            if ratio < 1.0:
                                # ratio < 1.0 = リバーススプリット（株式併合）
                                # 例: 0.1 = 10株→1株（10:1併合）
                                merge_ratio = int(round(1.0 / ratio))
                                result['has_reverse_split'] = True
                                result['reverse_split_ratio'] = f'{merge_ratio}:1'
                                result['filter_reason'] = (
                                    f'株式併合({merge_ratio}:1) {days_ago}日前 '
                                    f'- 価格上昇は併合による見かけ上の変動')
                                result['should_filter'] = True
                                logger.info(
                                    f"{ticker}: Reverse split detected "
                                    f"({merge_ratio}:1, {days_ago} days ago)")
                            elif ratio > 1.0:
                                # 通常の株式分割（2:1等）
                                result['has_split'] = True
                                if days_ago <= 5:
                                    result['filter_reason'] = (
                                        f'株式分割({ratio:.0f}:1) {days_ago}日前')
                                    result['should_filter'] = True
            except Exception as e:
                logger.debug(f"{ticker}: splits check error: {e}")

            # === 2. 価格履歴パターンで併合を推定 ===
            # splitsデータがない場合でも、急激な価格ジャンプ+出来高急減で推定
            if not result['has_reverse_split']:
                try:
                    hist = stock.history(period='10d', prepost=True)
                    if hist is not None and len(hist) >= 3:
                        closes = hist['Close'].dropna()
                        volumes = hist['Volume'].dropna()
                        if len(closes) >= 2 and len(volumes) >= 2:
                            # 前日比で価格が2倍以上ジャンプ + 出来高が半分以下
                            for i in range(1, len(closes)):
                                prev_close = float(closes.iloc[i-1])
                                curr_close = float(closes.iloc[i])
                                prev_vol = float(volumes.iloc[i-1])
                                curr_vol = float(volumes.iloc[i])

                                if prev_close > 0 and prev_vol > 0:
                                    price_ratio = curr_close / prev_close
                                    vol_ratio = curr_vol / prev_vol

                                    # 価格が2倍以上 + 出来高が半分以下 = 併合の特徴
                                    if price_ratio >= 2.0 and vol_ratio <= 0.5:
                                        estimated_merge = round(price_ratio)
                                        result['has_reverse_split'] = True
                                        result['reverse_split_ratio'] = f'{estimated_merge}:1(推定)'
                                        result['filter_reason'] = (
                                            f'株式併合の可能性({estimated_merge}:1推定) '
                                            f'- 価格{price_ratio:.1f}倍+出来高{vol_ratio:.1%}')
                                        result['should_filter'] = True
                                        logger.info(
                                            f"{ticker}: Possible reverse split detected "
                                            f"(price {price_ratio:.1f}x, vol {vol_ratio:.1%})")
                                        break
                except Exception as e:
                    logger.debug(f"{ticker}: price pattern check error: {e}")

            # === 3. 決算発表チェック（前後2日以内） ===
            try:
                cal = stock.calendar
                if cal is not None:
                    earnings_date = None
                    if isinstance(cal, pd.DataFrame) and 'Earnings Date' in cal.columns:
                        earnings_date = cal['Earnings Date'].iloc[0]
                    elif isinstance(cal, dict):
                        earnings_date = cal.get('Earnings Date')

                    if earnings_date is not None:
                        from datetime import date
                        if hasattr(earnings_date, 'date'):
                            ed = earnings_date.date()
                        else:
                            ed = earnings_date
                        today = date.today()
                        days_diff = abs((today - ed).days) if isinstance(ed, date) else 999
                        if days_diff <= 2:
                            result['has_earnings'] = True
                            if not result['filter_reason']:
                                result['filter_reason'] = f'決算発表({days_diff}日以内)'
                            result['should_filter'] = True
            except Exception:
                pass

            # === 4. ニュースでoffering/splitキーワード検出 ===
            try:
                news_items = stock.news or []
                offering_keywords = ['offering', 'dilution', 'shelf', 'secondary',
                                     'registered direct', 'public offering', 'atm offering']
                split_keywords = ['reverse split', 'reverse stock split', 'r/s',
                                  'stock consolidation', 'share consolidation']

                for item in news_items[:10]:
                    title = (item.get('title', '') or '').lower()

                    # リバーススプリット関連ニュース
                    for kw in split_keywords:
                        if kw in title:
                            if not result['has_reverse_split']:
                                result['has_reverse_split'] = True
                                result['filter_reason'] = f'株式併合ニュース検出: {title[:60]}'
                                result['should_filter'] = True
                            break

                    # 増資関連ニュース
                    for kw in offering_keywords:
                        if kw in title:
                            result['has_offering'] = True
                            if not result['filter_reason']:
                                result['filter_reason'] = f'増資/希薄化ニュース検出'
                            result['should_filter'] = True
                            break
            except Exception:
                pass

        except Exception as e:
            logger.debug(f"{ticker}: catalyst filter error: {e}")

        return result

    # === マルチタイムフレーム出来高確認 ===
    def check_multi_timeframe_volume(self, ticker: str) -> Dict:
        """日足・週足レベルで出来高パターンを確認し、信頼性を評価"""
        result = {
            'weekly_vol_rising': False,
            'daily_vol_sustained': False,
            'confidence_boost': 0,
            'detail': '',
        }
        try:
            stock = yf.Ticker(ticker)

            # 週足データ（8週間）
            weekly = stock.history(period='2mo', interval='1wk', prepost=True)
            if weekly is not None and len(weekly) >= 4:
                weekly_vols = weekly['Volume'].dropna()
                if len(weekly_vols) >= 3:
                    # 直近3週間の出来高が増加傾向か
                    recent_3w = weekly_vols.iloc[-3:]
                    if all(recent_3w.iloc[i] >= recent_3w.iloc[i-1] * 1.1
                           for i in range(1, len(recent_3w))):
                        result['weekly_vol_rising'] = True
                        result['confidence_boost'] += 5
                        result['detail'] += '週足出来高3週連続増 '

                    # 直近週の出来高が4週平均の2倍以上
                    avg_4w = weekly_vols.iloc[-5:-1].mean() if len(weekly_vols) >= 5 else weekly_vols.mean()
                    if avg_4w > 0 and float(weekly_vols.iloc[-1]) >= avg_4w * 2:
                        result['confidence_boost'] += 5
                        result['detail'] += f'週足出来高{float(weekly_vols.iloc[-1])/avg_4w:.1f}x '

            # 日足データ（直近5日で出来高が持続しているか）
            daily = stock.history(period='10d', prepost=True)
            if daily is not None and len(daily) >= 5:
                daily_vols = daily['Volume'].dropna()
                if len(daily_vols) >= 5:
                    avg_vol = daily_vols.iloc[:-1].mean()
                    # 直近3日間すべてが平均以上
                    recent_3d = daily_vols.iloc[-3:]
                    if avg_vol > 0 and all(float(v) >= avg_vol * 1.2 for v in recent_3d):
                        result['daily_vol_sustained'] = True
                        result['confidence_boost'] += 5
                        result['detail'] += '日足出来高3日持続 '

        except Exception:
            pass

        return result

    # === 総合スコアリング ===
    def calculate_score(self, alert: dict, insider_data: dict = None) -> int:
        """アラートの総合スコアを計算（0-100点）

        スコア配分:
          出来高異常    : 最大35点（自動調整可）
          フロート      : 最大25点（自動調整可）
          価格変動      : 最大15点（自動調整可）
          インサイダー  : 最大20点（自動調整可）
          空売り比率    : 最大5点（自動調整可）
          SNSバズ       : 最大15点（新規追加）
          信頼性ボーナス: 最大10点（マルチTF・センチメント等）
        """
        score = 0
        details = []

        # 自動調整された重みをDBから読み込み
        weights = self._load_adjusted_weights()
        vol_max = weights.get('volume_max', 35)
        float_max = weights.get('float_max', 25)
        price_max = weights.get('price_max', 15)
        insider_max = weights.get('insider_max', 20)
        short_max = weights.get('short_max', 5)

        # --- 出来高スコア ---
        vol_ratio = alert.get('volume_ratio', 0)
        if vol_ratio >= 10:
            s = vol_max
        elif vol_ratio >= 5:
            s = int(vol_max * 0.8)
        elif vol_ratio >= 3:
            s = int(vol_max * 0.57)
        elif vol_ratio >= 2:
            s = int(vol_max * 0.34)
        elif vol_ratio >= 1.5:
            s = int(vol_max * 0.14)
        else:
            s = 0
        if s > 0:
            details.append(f'出来高{vol_ratio:.1f}倍: +{s}')
        score += s

        # 出来高じわ上げボーナス（実績: 勝率54%, 翌日+6.6%）
        if alert.get('alert_type') == 'VOLUME_RISING':
            score += 10
            details.append('じわ上げ: +10')

        # 蓄積パターンボーナス（実績: 勝率59%）
        if alert.get('alert_type') == 'ACCUMULATION':
            score += 8
            details.append('買い集め: +8')

        # 出来高底打ち反転ボーナス（実績: 勝率58%, 翌日+2.3%）
        if alert.get('alert_type') == 'VOL_REVERSAL':
            score += 8
            details.append('底打ち反転: +8')

        # --- フロートスコア ---
        float_level = alert.get('float_level', 'NORMAL')
        float_shares = alert.get('float_shares', 0) or 0
        if float_level == 'ULTRA_LOW':
            s = float_max
        elif float_level == 'LOW':
            s = int(float_max * 0.6)
        elif float_shares > 0 and float_shares <= 50_000_000:
            s = int(float_max * 0.2)
        else:
            s = 0
        if s > 0:
            details.append(f'フロート({float_level}): +{s}')
        score += s

        # --- 価格変動スコア（天井掴みリスク考慮） ---
        # 実績データ: +20%以上で検知→翌日平均-5.6%（天井掴みリスク）
        # +10%未満で検知→翌日平均+8.2%（早期検知で有効）
        pct = alert.get('price_change_pct', 0) or 0
        if pct >= 50:
            # 既に+50%以上 → 天井掴みリスク大 → 減点
            s = -10
            details.append(f'!! 既に+{pct:.0f}%急騰(天井リスク): {s}')
        elif pct >= 30:
            # +30-50% → 天井掴みリスク
            s = -5
            details.append(f'!! 既に+{pct:.0f}%上昇(天井リスク): {s}')
        elif pct >= 20:
            s = 0  # 中立（加点も減点もしない）
        elif pct >= 10:
            s = int(price_max * 0.33)
            details.append(f'価格+{pct:.0f}%: +{s}')
        elif pct >= 5:
            # 早期検知 → 加点
            s = int(price_max * 0.5)
            details.append(f'早期検知+{pct:.0f}%: +{s}')
        else:
            s = 0
        score += s

        # プレマーケットボーナス
        if alert.get('alert_type') == 'PREMARKET':
            score += 5
            details.append('プレマーケット: +5')

        # --- インサイダースコア ---
        insider_amount = alert.get('insider_buy_amount', 0) or 0
        if insider_data:
            insider_amount = insider_data.get('total_value', insider_amount)

        if insider_amount >= 100_000:
            s = insider_max
        elif insider_amount >= 50_000:
            s = int(insider_max * 0.75)
        elif insider_amount >= 10_000:
            s = int(insider_max * 0.5)
        elif insider_amount > 0:
            s = int(insider_max * 0.25)
        else:
            s = 0
        # インサイダー役職ランクボーナス（CEO/CFO = +5点）
        insider_rank = alert.get('insider_rank', 0) or 0
        if insider_data:
            insider_rank = insider_data.get('insider_rank', insider_rank)
        if insider_rank >= 4 and s > 0:
            rank_bonus = min(insider_rank, 5)
            s += rank_bonus
            details.append(f'インサイダー${insider_amount:,.0f}(役職{insider_rank}): +{s}')
        elif s > 0:
            details.append(f'インサイダー${insider_amount:,.0f}: +{s}')
        score += s

        # 複合シグナルボーナス
        if alert.get('alert_type') == 'COMBINED':
            score += 10
            details.append('複合ボーナス: +10')

        # --- 空売りスコア ---
        short_pct = alert.get('short_pct', 0) or 0
        if short_pct >= 0.20:
            s = short_max
            details.append(f'空売り{short_pct*100:.0f}%: +{s}')
            score += s
        elif short_pct >= 0.10:
            s = int(short_max * 0.6)
            details.append(f'空売り{short_pct*100:.0f}%: +{s}')
            score += s

        # --- SNSバズスコア（最大15点、新規追加） ---
        buzz = alert.get('buzz_score', 0) or 0
        sentiment = alert.get('sentiment', '')
        if buzz >= 80:
            s = 15
        elif buzz >= 60:
            s = 12
        elif buzz >= 40:
            s = 8
        elif buzz >= 20:
            s = 4
        else:
            s = 0
        # ベアリッシュの場合は減点
        if sentiment == 'BEARISH':
            s = max(s - 8, 0)
        if s > 0:
            sent_label = {'BULLISH': '強気', 'BEARISH': '弱気', 'NEUTRAL': '中立'}.get(sentiment, '')
            details.append(f'SNS({buzz}点{sent_label}): +{s}')
        score += s

        # --- 信頼性ボーナス（マルチTF確認、最大10点） ---
        mtf_boost = alert.get('mtf_confidence_boost', 0) or 0
        if mtf_boost > 0:
            s = min(mtf_boost, 10)
            details.append(f'マルチTF確認: +{s}')
            score += s

        # --- 実績ベースのパターンボーナス ---
        # $1未満 + 出来高10倍以上 → 成功率91%
        alert_price = alert.get('price', 0) or 0
        if alert_price > 0 and alert_price < 1.0 and vol_ratio >= 10:
            score += 20
            details.append(f'$1未満+出来高{vol_ratio:.0f}x(成功率91%): +20')
        # $1未満 + 出来高5倍以上 → 成功率86%
        elif alert_price > 0 and alert_price < 1.0 and vol_ratio >= 5:
            score += 15
            details.append(f'$1未満+出来高{vol_ratio:.0f}x(成功率86%): +15')
        # 出来高20倍以上 → 成功率87%
        elif vol_ratio >= 20:
            score += 15
            details.append(f'出来高{vol_ratio:.0f}x(成功率87%): +15')

        # --- 偽シグナル減点 ---
        if alert.get('has_reverse_split'):
            reason = alert.get('catalyst_filter_reason', '株式併合')
            score = 0
            details = [f'!! {reason} !! (除外)']
        elif alert.get('catalyst_filtered'):
            reason = alert.get('catalyst_filter_reason', '非ミーム要因')
            score = max(score - 20, 0)
            details.append(f'WARNING {reason}: -20')

        # スコア上限100
        score = min(score, 100)

        alert['score'] = score
        alert['score_detail'] = ' | '.join(details)
        return score

    def _load_adjusted_weights(self) -> Dict:
        """DBから自動調整された重みを読み込み（なければデフォルト）"""
        defaults = {
            'volume_max': 35,
            'float_max': 25,
            'price_max': 15,
            'insider_max': 20,
            'short_max': 5,
        }
        if not self.db:
            return defaults
        try:
            import sqlite3, json
            conn = self.db._get_conn()
            try:
                row = conn.execute(
                    "SELECT weights FROM score_weights ORDER BY id DESC LIMIT 1"
                ).fetchone()
                if row and row[0]:
                    loaded = json.loads(row[0])
                    # デフォルトとマージ
                    defaults.update(loaded)
                    return defaults
            except Exception:
                pass
            finally:
                conn.close()
        except Exception:
            pass
        return defaults

    # === スコア自動調整（Phase 2） ===
    # データが十分に溜まったら実行し、各要素の配点を実績ベースで最適化する

    # マイルストーン（この件数の結果データで自動調整を実行）
    AUTO_ADJUST_MILESTONES = [50, 100, 200, 500]

    def auto_adjust_score_weights(self) -> Dict:
        """結果データから各要素の実際の予測力を計算し、配点を最適化する

        実行条件: alert_resultsにN件以上の確定結果がある
        やること:
          1. 出来高倍率帯別の勝率を計算 → 出来高スコアの配点を調整
          2. フロートレベル別の勝率 → フロートスコアの配点を調整
          3. アラートタイプ別の勝率 → ボーナス点の調整
          4. 結果をscore_weightsテーブルに保存
        """
        if not self.db:
            return {'adjusted': False, 'reason': 'DB not available'}

        import sqlite3
        conn = self.db._get_conn()
        try:
            conn.row_factory = sqlite3.Row

            # 確定結果の件数
            total = conn.execute(
                "SELECT COUNT(*) as c FROM alert_results WHERE result != 'PENDING'"
            ).fetchone()['c']

            # マイルストーンチェック
            reached = None
            for ms in self.AUTO_ADJUST_MILESTONES:
                if total >= ms:
                    reached = ms

            if reached is None:
                return {
                    'adjusted': False,
                    'reason': f'データ不足({total}/{self.AUTO_ADJUST_MILESTONES[0]}件必要)',
                    'current_results': total,
                }

            # === 各要素の予測力を分析 ===
            analysis = {}

            # 1. 出来高倍率帯別の勝率
            vol_bands = conn.execute("""
                SELECT
                    CASE
                        WHEN volume_ratio >= 10 THEN '10x+'
                        WHEN volume_ratio >= 5 THEN '5-10x'
                        WHEN volume_ratio >= 3 THEN '3-5x'
                        WHEN volume_ratio >= 1.5 THEN '1.5-3x'
                        ELSE '<1.5x'
                    END as band,
                    COUNT(*) as n,
                    ROUND(AVG(max_gain_pct), 1) as avg_gain,
                    SUM(CASE WHEN result IN ('BIG_WIN','WIN') THEN 1 ELSE 0 END) as wins
                FROM alert_results WHERE result != 'PENDING'
                GROUP BY band ORDER BY avg_gain DESC
            """).fetchall()
            analysis['volume_bands'] = [dict(r) for r in vol_bands]

            # 2. フロートレベル別
            float_bands = conn.execute("""
                SELECT float_level, COUNT(*) as n,
                    ROUND(AVG(max_gain_pct), 1) as avg_gain,
                    SUM(CASE WHEN result IN ('BIG_WIN','WIN') THEN 1 ELSE 0 END) as wins
                FROM alert_results WHERE result != 'PENDING' AND float_level IS NOT NULL
                GROUP BY float_level ORDER BY avg_gain DESC
            """).fetchall()
            analysis['float_bands'] = [dict(r) for r in float_bands]

            # 3. アラートタイプ別
            type_bands = conn.execute("""
                SELECT alert_type, COUNT(*) as n,
                    ROUND(AVG(max_gain_pct), 1) as avg_gain,
                    SUM(CASE WHEN result IN ('BIG_WIN','WIN') THEN 1 ELSE 0 END) as wins
                FROM alert_results WHERE result != 'PENDING'
                GROUP BY alert_type ORDER BY avg_gain DESC
            """).fetchall()
            analysis['type_bands'] = [dict(r) for r in type_bands]

            # 4. スコア帯別の実際の成績（スコアが予測力を持っているか検証）
            score_bands = conn.execute("""
                SELECT
                    CASE
                        WHEN score >= 60 THEN 'high(60+)'
                        WHEN score >= 30 THEN 'mid(30-59)'
                        ELSE 'low(<30)'
                    END as band,
                    COUNT(*) as n,
                    ROUND(AVG(max_gain_pct), 1) as avg_gain,
                    SUM(CASE WHEN result IN ('BIG_WIN','WIN') THEN 1 ELSE 0 END) as wins
                FROM alert_results WHERE result != 'PENDING'
                GROUP BY band ORDER BY avg_gain DESC
            """).fetchall()
            analysis['score_validation'] = [dict(r) for r in score_bands]

            # 5. 新しい配点を計算（勝率に比例して配点を調整）
            new_weights = self._calculate_new_weights(analysis)
            analysis['new_weights'] = new_weights

            # 6. 配点をDBに保存（次回のスコア計算に使用）
            import json
            conn.execute("""
                CREATE TABLE IF NOT EXISTS score_weights (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    milestone INTEGER,
                    total_results INTEGER,
                    weights TEXT,
                    analysis TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("""
                INSERT INTO score_weights (milestone, total_results, weights, analysis)
                VALUES (?, ?, ?, ?)
            """, (reached, total,
                  json.dumps(new_weights, ensure_ascii=False),
                  json.dumps(analysis, ensure_ascii=False)))
            conn.commit()

            print(f"[ScoreAdjust] Weights adjusted at {total} results (milestone={reached})")
            print(f"[ScoreAdjust] New weights: {new_weights}")

            return {
                'adjusted': True,
                'milestone': reached,
                'total_results': total,
                'analysis': analysis,
                'new_weights': new_weights,
            }
        finally:
            conn.close()

    def _calculate_new_weights(self, analysis: Dict) -> Dict:
        """分析結果から新しい配点を計算"""
        weights = {
            'volume_max': 35,
            'float_max': 25,
            'price_max': 15,
            'insider_max': 20,
            'short_max': 5,
        }

        # 出来高倍率の予測力を評価
        vol_bands = analysis.get('volume_bands', [])
        if vol_bands:
            high_vol_gains = [b['avg_gain'] for b in vol_bands
                              if b.get('band') in ('10x+', '5-10x') and b['n'] >= 3]
            low_vol_gains = [b['avg_gain'] for b in vol_bands
                             if b.get('band') in ('<1.5x', '1.5-3x') and b['n'] >= 3]
            if high_vol_gains and low_vol_gains:
                vol_diff = sum(high_vol_gains) / len(high_vol_gains) - \
                           sum(low_vol_gains) / len(low_vol_gains)
                # 出来高の予測力が高いほど配点を上げる
                if vol_diff > 20:
                    weights['volume_max'] = 40
                elif vol_diff < 5:
                    weights['volume_max'] = 25

        # フロートの予測力を評価
        float_bands = analysis.get('float_bands', [])
        if float_bands:
            low_float_gain = next(
                (b['avg_gain'] for b in float_bands
                 if b.get('float_level') in ('ULTRA_LOW', 'LOW') and b['n'] >= 3), None)
            normal_gain = next(
                (b['avg_gain'] for b in float_bands
                 if b.get('float_level') == 'NORMAL' and b['n'] >= 3), None)
            if low_float_gain is not None and normal_gain is not None:
                float_diff = low_float_gain - normal_gain
                if float_diff > 15:
                    weights['float_max'] = 30
                elif float_diff < 3:
                    weights['float_max'] = 15

        return weights

    # === フロート分析 ===
    def get_float_info(self, ticker: str) -> Optional[Dict]:
        """yfinanceから流通株数（フロート）情報を取得（キャッシュ付き・リトライ対応）"""
        # キャッシュチェック
        cached = self._float_cache.get(ticker)
        if cached and (time.time() - cached['ts']) < self._float_cache_ttl:
            return cached['data']

        info = None
        # リトライ付きで情報取得（yfinanceは不安定なため2回試行）
        for attempt in range(2):
            try:
                stock = yf_retry(lambda: yf.Ticker(ticker))
                if stock is None:
                    continue
                info = stock.info
                if info and (info.get('floatShares') or info.get('sharesOutstanding')):
                    break
                time.sleep(1)
            except Exception:
                if attempt == 0:
                    time.sleep(2)
                continue

        if not info:
            # 失敗時は短いTTLでキャッシュ（同じ銘柄の連続リクエスト防止）
            self._float_cache[ticker] = {'data': None, 'ts': time.time() - self._float_cache_ttl + 600}
            return None

        try:
            float_shares = info.get('floatShares')
            shares_outstanding = info.get('sharesOutstanding')
            market_cap = info.get('marketCap')
            short_ratio = info.get('shortRatio')
            short_pct = info.get('shortPercentOfFloat')

            if not float_shares and not shares_outstanding:
                # 失敗キャッシュ（10分間再試行しない）
                self._float_cache[ticker] = {'data': None, 'ts': time.time() - self._float_cache_ttl + 600}
                return None

            effective_float = float_shares or shares_outstanding or 0
            if effective_float <= 0:
                return None

            float_level = 'NORMAL'
            if effective_float <= Config.ULTRA_LOW_FLOAT:
                float_level = 'ULTRA_LOW'
            elif effective_float <= Config.LOW_FLOAT_THRESHOLD:
                float_level = 'LOW'

            result = {
                'ticker': ticker,
                'float_shares': effective_float,
                'shares_outstanding': shares_outstanding,
                'market_cap': market_cap,
                'float_level': float_level,
                'short_ratio': short_ratio,
                'short_percent_of_float': short_pct,
                'company_name': info.get('shortName', ''),
                'sector': info.get('sector', ''),
                'industry': info.get('industry', ''),
                'business_summary': info.get('longBusinessSummary', ''),
            }
            # キャッシュに保存
            self._float_cache[ticker] = {'data': result, 'ts': time.time()}
            return result
        except Exception as e:
            logger.debug(f"{ticker}: float info fetch error: {e}")
            return None

    def enrich_alerts_with_float(self, alerts: List[Dict]) -> List[Dict]:
        """アラートにフロート情報を付加"""
        enriched = []
        for alert in alerts:
            ticker = alert['ticker']
            float_info = self.get_float_info(ticker)
            if float_info:
                alert['float_shares'] = float_info['float_shares']
                alert['float_level'] = float_info['float_level']
                alert['market_cap'] = float_info.get('market_cap')
                alert['short_ratio'] = float_info.get('short_ratio')
                alert['short_pct'] = float_info.get('short_percent_of_float')
                alert['company_name'] = float_info.get('company_name', '')
                alert['sector'] = float_info.get('sector', '')
                alert['industry'] = float_info.get('industry', '')

                # 日本語翻訳
                from translator import translate_sector, translate_industry
                alert['sector_ja'] = translate_sector(alert['sector'])
                alert['industry_ja'] = translate_industry(alert['industry'])

                # フロートが低い銘柄は優先度を上げる
                if float_info['float_level'] in ('LOW', 'ULTRA_LOW'):
                    if 'detail' not in alert:
                        alert['detail'] = ''
                    alert['detail'] += (f" | Float: {float_info['float_shares']/1e6:.1f}M "
                                        f"[{float_info['float_level']}]")
            enriched.append(alert)

        # 低フロート + 高出来高を上位に
        enriched.sort(key=lambda x: (
            0 if x.get('float_level') == 'ULTRA_LOW' else
            1 if x.get('float_level') == 'LOW' else 2,
            -x.get('volume_ratio', 0)
        ))
        return enriched

    # === 出来高蓄積パターン検出（急騰の前兆） ===
    def scan_accumulation(self, tickers: List[str] = None) -> List[Dict]:
        """急騰前の出来高蓄積パターンを検出

        検出パターン:
        1. 静かな蓄積: 5-7日間で出来高が徐々に増加（各日1.2倍以上）
           + 価格がほぼ横ばい（±5%以内）→ 誰かが静かに買い集めている
        2. 出来高底打ち反転: 出来高が長期平均を下回った後に急回復
           → 売り枯れ後の買い開始
        3. 価格収縮+出来高増: 価格レンジが狭まりつつ出来高が増加
           → ブレイクアウト直前のパターン
        """
        if tickers is None:
            tickers = self.get_universe()

        if not tickers:
            return []

        print(f"[Scanner] Scanning {len(tickers)} tickers for accumulation patterns...")
        alerts = []
        batch_size = 100

        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            try:
                data = None
                for _retry in range(3):
                    try:
                        data = yf.download(batch, period='30d', progress=False,
                                           threads=True, ignore_tz=True,
                                           prepost=True)
                        break
                    except Exception as _e:
                        if 'Too Many Requests' in str(_e) or '429' in str(_e):
                            time.sleep(30)
                        else:
                            raise
                if data is None or data.empty:
                    continue

                volume = data.get('Volume')
                close = data.get('Close')
                high = data.get('High')
                low = data.get('Low')
                if volume is None or close is None:
                    continue

                if isinstance(volume, pd.Series):
                    volume = volume.to_frame()
                    close = close.to_frame()
                    high = high.to_frame()
                    low = low.to_frame()

                for sym in batch:
                    try:
                        if sym not in volume.columns:
                            continue

                        vol_s = volume[sym].dropna()
                        close_s = close[sym].dropna()
                        high_s = high[sym].dropna()
                        low_s = low[sym].dropna()

                        if len(vol_s) < 15:
                            continue

                        price = float(close_s.iloc[-1])
                        if price > Config.MAX_PRICE or price < 0.01:
                            continue

                        avg_vol_20 = int(vol_s.iloc[-21:-1].mean()) if len(vol_s) >= 21 else int(vol_s.iloc[:-1].mean())
                        if avg_vol_20 < Config.MIN_AVG_VOLUME:
                            continue

                        # パターン1: 静かな蓄積（出来高増加 + 価格横ばい）
                        # 実績: 勝率59%, 翌日+0.8% → 検出感度を上げる
                        # 緩和: 5日中2日増加でも検出、価格変動±8%まで許容
                        if len(vol_s) >= 5:
                            check_days = min(5, len(vol_s) - 1)
                            recent_n = vol_s.iloc[-check_days:]
                            rising_count = sum(
                                1 for j in range(1, len(recent_n))
                                if recent_n.iloc[j] >= recent_n.iloc[j-1] * 1.1
                            )
                            price_nd_ago = float(close_s.iloc[-check_days])
                            price_change = abs(price - price_nd_ago) / price_nd_ago if price_nd_ago > 0 else 1

                            if rising_count >= 2 and price_change < 0.08:
                                vol_ratio = float(vol_s.iloc[-1]) / avg_vol_20
                                if vol_ratio >= 1.2:
                                    alerts.append({
                                        'ticker': sym,
                                        'price': price,
                                        'volume': int(vol_s.iloc[-1]),
                                        'avg_volume': avg_vol_20,
                                        'volume_ratio': round(vol_ratio, 2),
                                        'alert_type': 'ACCUMULATION',
                                        'detail': (f'出来高5日中{rising_count}日増加 '
                                                   f'(価格変動{price_change*100:.1f}%) '
                                                   f'→ 静かな買い集めの可能性'),
                                        'accumulation_days': rising_count,
                                        'price_stability': round(price_change * 100, 1),
                                    })
                                    continue

                        # パターン2: 出来高底打ち反転
                        # 実績: 勝率58%, 翌日+2.3%
                        # 緩和: 減少判定0.8倍、回復判定1.5倍に
                        if len(vol_s) >= 8:
                            lookback = min(10, len(vol_s) - 1)
                            mid = lookback // 2
                            vol_5d_ago = vol_s.iloc[-(mid+1):-1].mean()
                            vol_10d_ago = vol_s.iloc[-(lookback+1):-(mid+1)].mean()
                            vol_today = float(vol_s.iloc[-1])

                            if vol_10d_ago > 0 and vol_5d_ago > 0:
                                was_declining = vol_5d_ago < vol_10d_ago * 0.8
                                is_recovering = vol_today > vol_5d_ago * 1.5

                                if was_declining and is_recovering:
                                    vol_ratio = vol_today / avg_vol_20
                                    # 出来高が平均の1.5倍未満はノイズとして除外
                                    if vol_ratio < 1.5:
                                        continue
                                    alerts.append({
                                        'ticker': sym,
                                        'price': price,
                                        'volume': int(vol_today),
                                        'avg_volume': avg_vol_20,
                                        'volume_ratio': round(vol_ratio, 2),
                                        'alert_type': 'VOL_REVERSAL',
                                        'detail': (f'出来高底打ち反転: '
                                                   f'10日前平均{int(vol_10d_ago):,} → '
                                                   f'5日前{int(vol_5d_ago):,}(減少) → '
                                                   f'本日{int(vol_today):,}(急回復)'),
                                    })
                                    continue

                        # パターン3: 価格収縮 + 出来高増（ブレイクアウト前兆）
                        if len(vol_s) >= 10 and len(high_s) >= 10 and len(low_s) >= 10:
                            # 直近5日の価格レンジ vs 10日前の価格レンジ
                            range_recent = (high_s.iloc[-5:].max() - low_s.iloc[-5:].min()) / price
                            range_old = (high_s.iloc[-10:-5].max() - low_s.iloc[-10:-5].min()) / price

                            vol_recent = vol_s.iloc[-3:].mean()
                            vol_old = vol_s.iloc[-10:-5].mean()

                            if range_old > 0 and vol_old > 0:
                                range_shrink = range_recent / range_old
                                vol_expand = vol_recent / vol_old

                                # レンジ縮小 + 出来高拡大 = ブレイクアウト前兆
                                if range_shrink < 0.5 and vol_expand > 1.5:
                                    vol_ratio = float(vol_s.iloc[-1]) / avg_vol_20
                                    alerts.append({
                                        'ticker': sym,
                                        'price': price,
                                        'volume': int(vol_s.iloc[-1]),
                                        'avg_volume': avg_vol_20,
                                        'volume_ratio': round(vol_ratio, 2),
                                        'alert_type': 'SQUEEZE_SETUP',
                                        'detail': (f'ブレイクアウト前兆: '
                                                   f'レンジ{range_shrink:.0%}縮小 + '
                                                   f'出来高{vol_expand:.1f}倍増加'),
                                        'range_shrink': round(range_shrink, 2),
                                        'vol_expand': round(vol_expand, 2),
                                    })

                    except Exception:
                        continue

            except Exception as e:
                print(f"[Scanner] Accumulation scan error: {e}")
                continue

            time.sleep(5)

        for a in alerts:
            self.calculate_score(a)
        alerts.sort(key=lambda x: x.get('score', 0), reverse=True)
        print(f"[Scanner] Accumulation patterns found: {len(alerts)}")
        return alerts

    # === プレマーケット出来高検出 ===
    def scan_premarket(self, tickers: List[str] = None) -> List[Dict]:
        """プレマーケット（ET 4:00-9:30）で出来高が急増している銘柄を検出"""
        if tickers is None:
            tickers = self.get_universe()

        if not tickers:
            return []

        print(f"[Scanner] Checking premarket volume for {len(tickers)} tickers...")
        alerts = []

        # Polygon.ioの前日比較でプレマーケットデータを取得
        for ticker in tickers[:200]:  # 無料プラン制限を考慮
            try:
                # 前日の出来高を取得
                resp = requests.get(
                    f"https://api.polygon.io/v2/aggs/ticker/{ticker}/prev",
                    params={'apiKey': self.api_key},
                    timeout=10)
                if resp.status_code != 200:
                    continue

                data = resp.json()
                results = data.get('results', [])
                if not results:
                    continue

                prev = results[0]
                prev_vol = prev.get('v', 0)
                prev_close = prev.get('c', 0)

                if prev_vol <= 0 or prev_close <= 0 or prev_close > Config.MAX_PRICE:
                    continue

                # 当日のスナップショット（プレマーケット含む）
                snap_resp = requests.get(
                    f"https://api.polygon.io/v2/snapshot/locale/us/markets/stocks/tickers/{ticker}",
                    params={'apiKey': self.api_key},
                    timeout=10)
                if snap_resp.status_code != 200:
                    continue

                snap_data = snap_resp.json()
                ticker_snap = snap_data.get('ticker', {})
                today_vol = ticker_snap.get('day', {}).get('v', 0)
                current_price = ticker_snap.get('lastTrade', {}).get('p', prev_close)
                premarket_change = ticker_snap.get('todaysChangePerc', 0)

                if today_vol <= 0:
                    continue

                vol_ratio = today_vol / (prev_vol / 6.5)  # 前日の1時間あたり出来高と比較

                if vol_ratio >= Config.PREMARKET_VOL_MULT:
                    alerts.append({
                        'ticker': ticker,
                        'price': current_price,
                        'volume': int(today_vol),
                        'avg_volume': int(prev_vol),
                        'volume_ratio': round(vol_ratio, 2),
                        'price_change_pct': round(premarket_change, 1),
                        'alert_type': 'PREMARKET',
                        'detail': f'Premarket vol {vol_ratio:.1f}x, change {premarket_change:+.1f}%',
                    })

                time.sleep(12)  # Polygon無料プラン: 5回/分

            except Exception:
                continue

        alerts.sort(key=lambda x: x['volume_ratio'], reverse=True)
        logger.info(f"Premarket alerts: {len(alerts)}")
        return alerts

    # === 簡易バックテスト ===
    def backtest_score_accuracy(self) -> Dict:
        """過去のアラート結果からスコアリングの精度を検証"""
        if not self.db:
            return {'error': 'DB not available'}

        import sqlite3
        conn = self.db._get_conn()
        try:
            conn.row_factory = sqlite3.Row

            # 全確定結果を取得
            rows = conn.execute("""
                SELECT score, volume_ratio, float_level, alert_type,
                       max_gain_pct, result, change_1d_pct, change_3d_pct
                FROM alert_results WHERE result != 'PENDING'
            """).fetchall()

            if not rows:
                return {'total': 0, 'message': 'No confirmed results yet'}

            results = [dict(r) for r in rows]
            total = len(results)

            # 各スコア閾値での精度
            thresholds = [20, 30, 40, 50, 60]
            threshold_stats = []
            for t in thresholds:
                above = [r for r in results if (r.get('score') or 0) >= t]
                below = [r for r in results if (r.get('score') or 0) < t]
                above_wins = len([r for r in above if r['result'] in ('BIG_WIN', 'WIN', 'SMALL_WIN')])
                below_wins = len([r for r in below if r['result'] in ('BIG_WIN', 'WIN', 'SMALL_WIN')])

                threshold_stats.append({
                    'threshold': t,
                    'above_count': len(above),
                    'above_win_rate': round(above_wins / len(above) * 100, 1) if above else 0,
                    'below_count': len(below),
                    'below_win_rate': round(below_wins / len(below) * 100, 1) if below else 0,
                    'avg_gain_above': round(
                        sum((r.get('max_gain_pct') or 0) for r in above) / len(above), 1) if above else 0,
                })

            # 最適閾値の推奨
            best = max(threshold_stats,
                       key=lambda x: x['above_win_rate'] - x['below_win_rate']
                       if x['above_count'] >= 5 else -999)

            return {
                'total': total,
                'threshold_analysis': threshold_stats,
                'recommended_min_score': best['threshold'],
                'overall_win_rate': round(
                    len([r for r in results if r['result'] in ('BIG_WIN', 'WIN', 'SMALL_WIN')]) / total * 100, 1),
            }
        finally:
            conn.close()
