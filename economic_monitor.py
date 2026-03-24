"""
経済指標リアルタイム監視モジュール
FRED APIで主要経済指標の発表を検出し、即座にDiscord通知する
"""
import os
import time
import threading
import requests
from datetime import datetime, timezone, timedelta
from typing import Dict, List, Optional

# タイムゾーン（サマータイム自動対応）
try:
    from zoneinfo import ZoneInfo
    JST = ZoneInfo('Asia/Tokyo')
    EST = ZoneInfo('America/New_York')  # EST/EDT自動切替
except ImportError:
    JST = timezone(timedelta(hours=9))
    EST = timezone(timedelta(hours=-5))


# 監視する経済指標
INDICATORS = {
    'CPIAUCSL': {
        'name': '消費者物価指数（CPI）',
        'name_en': 'CPI',
        'frequency': 'monthly',
        'impact': 'HIGH',
        'description': 'インフレの主要指標。予想より高い→利上げ懸念→株安、低い→株高',
    },
    'PAYEMS': {
        'name': '非農業部門雇用者数（雇用統計）',
        'name_en': 'Non-Farm Payrolls',
        'frequency': 'monthly',
        'impact': 'HIGH',
        'description': '労働市場の健全性。予想より良い→経済好調だが利上げ懸念、悪い→景気後退懸念',
    },
    'UNRATE': {
        'name': '失業率',
        'name_en': 'Unemployment Rate',
        'frequency': 'monthly',
        'impact': 'HIGH',
        'description': '低い→経済好調、高い→景気後退懸念',
    },
    'FEDFUNDS': {
        'name': 'FF金利（政策金利）',
        'name_en': 'Federal Funds Rate',
        'frequency': 'monthly',
        'impact': 'EXTREME',
        'description': 'FRBの金融政策。利上げ→株安、利下げ→株高',
    },
    'GDP': {
        'name': 'GDP成長率',
        'name_en': 'GDP',
        'frequency': 'quarterly',
        'impact': 'HIGH',
        'description': '経済成長の総合指標',
    },
    'RSAFS': {
        'name': '小売売上高',
        'name_en': 'Retail Sales',
        'frequency': 'monthly',
        'impact': 'MEDIUM',
        'description': '消費者支出の指標。強い→経済好調、弱い→消費減退',
    },
    'PPIFIS': {
        'name': '生産者物価指数（PPI）',
        'name_en': 'PPI',
        'frequency': 'monthly',
        'impact': 'MEDIUM',
        'description': '企業レベルのインフレ。CPIの先行指標',
    },
    'ICSA': {
        'name': '新規失業保険申請件数',
        'name_en': 'Initial Jobless Claims',
        'frequency': 'weekly',
        'impact': 'MEDIUM',
        'description': '毎週発表。増加→雇用悪化、減少→雇用改善',
    },
    'PCEPI': {
        'name': 'PCEデフレーター',
        'name_en': 'PCE Price Index',
        'frequency': 'monthly',
        'impact': 'HIGH',
        'description': 'FRBが最も重視するインフレ指標',
    },
}


class EconomicMonitor:
    def __init__(self):
        self.api_key = os.getenv('FRED_API_KEY', '')
        self.webhook_url = os.getenv('DISCORD_ECON_WEBHOOK_URL', '') or os.getenv('DISCORD_NEWS_WEBHOOK_URL', '') or os.getenv('DISCORD_WEBHOOK_URL', '')
        self._last_values = {}  # 前回の値を記録
        self._running = False
        self._initialized = False

        if self.api_key:
            print("[EconMonitor] FRED APIキー: 設定済み")
        else:
            print("[EconMonitor] FRED APIキー: 未設定")

    def _get_latest(self, series_id: str) -> Optional[Dict]:
        """FRED APIから最新データを取得"""
        try:
            resp = requests.get(
                'https://api.stlouisfed.org/fred/series/observations',
                params={
                    'series_id': series_id,
                    'api_key': self.api_key,
                    'file_type': 'json',
                    'sort_order': 'desc',
                    'limit': 2,
                },
                timeout=10)

            if resp.status_code != 200:
                return None

            data = resp.json()
            obs = data.get('observations', [])
            if not obs:
                return None

            latest = obs[0]
            prev = obs[1] if len(obs) > 1 else None

            return {
                'date': latest['date'],
                'value': latest['value'],
                'prev_date': prev['date'] if prev else None,
                'prev_value': prev['value'] if prev else None,
            }
        except Exception as e:
            print(f"[EconMonitor] {series_id} 取得エラー: {e}")
            return None

    def initialize(self):
        """初回起動時に全指標の現在値を記録"""
        print("[EconMonitor] 初期値を取得中...")
        for series_id in INDICATORS:
            data = self._get_latest(series_id)
            if data:
                self._last_values[series_id] = data['date']
            time.sleep(0.5)
        self._initialized = True
        print(f"[EconMonitor] 初期化完了: {len(self._last_values)}指標")

    def check_updates(self) -> List[Dict]:
        """全指標の更新をチェック"""
        if not self._initialized:
            self.initialize()

        updates = []
        for series_id, info in INDICATORS.items():
            try:
                data = self._get_latest(series_id)
                if not data:
                    continue

                last_date = self._last_values.get(series_id)

                # 新しいデータが出ている
                if last_date and data['date'] != last_date:
                    # 変化を計算
                    try:
                        current = float(data['value'])
                        prev = float(data['prev_value']) if data['prev_value'] else None
                        change = None
                        change_pct = None
                        if prev and prev != 0:
                            change = current - prev
                            change_pct = round((current - prev) / prev * 100, 2)
                    except (ValueError, TypeError):
                        current = data['value']
                        change = None
                        change_pct = None

                    updates.append({
                        'series_id': series_id,
                        'name': info['name'],
                        'name_en': info['name_en'],
                        'impact': info['impact'],
                        'description': info['description'],
                        'date': data['date'],
                        'value': data['value'],
                        'prev_date': data['prev_date'],
                        'prev_value': data['prev_value'],
                        'change': change,
                        'change_pct': change_pct,
                    })

                    # 記録更新
                    self._last_values[series_id] = data['date']
                    print(f"[EconMonitor] 新データ検出: {info['name']} = {data['value']} ({data['date']})")

                elif not last_date:
                    self._last_values[series_id] = data['date']

            except Exception as e:
                print(f"[EconMonitor] {series_id} チェックエラー: {e}")

            time.sleep(0.3)  # レート制限対策

        return updates

    # 各指標の市場影響ルール
    # direction: 'higher_bearish' = 上昇→株安, 'higher_bullish' = 上昇→株高
    IMPACT_RULES = {
        'CPIAUCSL': {
            'direction': 'higher_bearish',
            'up_text': 'インフレ加速 → 利上げ懸念 → 株安要因',
            'down_text': 'インフレ鈍化 → 利下げ期待 → 株高要因',
            'flat_text': '横ばい → 市場への影響は限定的',
        },
        'PAYEMS': {
            'direction': 'mixed',
            'up_text': '雇用増加 → 経済好調だがFRB引き締め懸念 → やや株安要因',
            'down_text': '雇用減少 → 景気後退懸念 → 株安要因だが利下げ期待も',
            'flat_text': '横ばい → 市場への影響は限定的',
        },
        'UNRATE': {
            'direction': 'higher_bearish',
            'up_text': '失業率上昇 → 景気悪化 → 株安要因（ただし利下げ期待）',
            'down_text': '失業率低下 → 経済好調 → 株高要因',
            'flat_text': '横ばい → 市場への影響は限定的',
        },
        'FEDFUNDS': {
            'direction': 'higher_bearish',
            'up_text': '利上げ → 借入コスト増 → 株安要因（特にグロース株）',
            'down_text': '利下げ → 金融緩和 → 株高要因（特にペニー株に追い風）',
            'flat_text': '据え置き → 想定通りなら影響小',
        },
        'GDP': {
            'direction': 'higher_bullish',
            'up_text': 'GDP成長 → 経済拡大 → 株高要因',
            'down_text': 'GDP縮小 → 景気後退懸念 → 株安要因',
            'flat_text': '横ばい → 市場への影響は限定的',
        },
        'RSAFS': {
            'direction': 'higher_bullish',
            'up_text': '小売売上増 → 消費好調 → 株高要因',
            'down_text': '小売売上減 → 消費冷え込み → 株安要因',
            'flat_text': '横ばい → 市場への影響は限定的',
        },
        'PPIFIS': {
            'direction': 'higher_bearish',
            'up_text': '生産者物価上昇 → CPIにも波及リスク → 株安要因',
            'down_text': '生産者物価低下 → インフレ鈍化の兆し → 株高要因',
            'flat_text': '横ばい → 市場への影響は限定的',
        },
        'ICSA': {
            'direction': 'higher_bearish',
            'up_text': '申請件数増加 → 雇用悪化の兆し → 株安要因',
            'down_text': '申請件数減少 → 雇用安定 → 株高要因',
            'flat_text': '横ばい → 市場への影響は限定的',
        },
        'PCEPI': {
            'direction': 'higher_bearish',
            'up_text': 'PCE上昇 → FRBが最重視するインフレ指標悪化 → 株安要因',
            'down_text': 'PCE低下 → インフレ鈍化 → 利下げ期待 → 株高要因',
            'flat_text': '横ばい → 市場への影響は限定的',
        },
    }

    def _assess_market_impact(self, update: Dict) -> Dict:
        """経済指標の変化から市場への影響を判定"""
        series_id = update.get('series_id', '')
        change_pct = update.get('change_pct')
        rule = self.IMPACT_RULES.get(series_id, {})

        if change_pct is None or not rule:
            return {'assessment': '判定不可', 'emoji': '', 'color_override': None}

        # 変化の大きさで判定
        threshold = 0.05  # 0.05%未満は横ばい扱い
        if abs(change_pct) < threshold:
            return {
                'assessment': rule.get('flat_text', '横ばい'),
                'market_direction': '中立',
                'emoji': '➡',
                'color_override': 0x3D9EFF,
            }

        if change_pct > 0:
            text = rule.get('up_text', '')
            if rule.get('direction') == 'higher_bearish':
                direction = '株安方向'
                emoji = '↘'
                color = 0xFF0000
            elif rule.get('direction') == 'higher_bullish':
                direction = '株高方向'
                emoji = '↗'
                color = 0x00CC00
            else:  # mixed
                direction = '判断分かれる'
                emoji = '↕'
                color = 0xFFAA00
        else:
            text = rule.get('down_text', '')
            if rule.get('direction') == 'higher_bearish':
                direction = '株高方向'
                emoji = '↗'
                color = 0x00CC00
            elif rule.get('direction') == 'higher_bullish':
                direction = '株安方向'
                emoji = '↘'
                color = 0xFF0000
            else:
                direction = '判断分かれる'
                emoji = '↕'
                color = 0xFFAA00

        return {
            'assessment': text,
            'market_direction': direction,
            'emoji': emoji,
            'color_override': color,
        }

    def notify_update(self, update: Dict):
        """経済指標の更新をDiscordに通知（市場影響分析付き）"""
        if not self.webhook_url:
            return

        impact = update['impact']
        impact_labels = {
            'EXTREME': '最重要',
            'HIGH': '重要',
            'MEDIUM': '注目',
        }
        impact_label = impact_labels.get(impact, '')

        # 市場影響を判定
        market = self._assess_market_impact(update)
        color = market.get('color_override') or {
            'EXTREME': 0xFF0000, 'HIGH': 0xFF6600, 'MEDIUM': 0xFFAA00,
        }.get(impact, 0x3D9EFF)

        now = datetime.now(JST).strftime('%Y/%m/%d %H:%M')

        # 説明文に市場影響を追加
        desc = update['description']
        if market.get('assessment'):
            desc += f"\n\n{market['emoji']} **市場への影響: {market.get('market_direction', '')}**\n{market['assessment']}"

        fields = [
            {'name': '最新値', 'value': str(update['value']), 'inline': True},
            {'name': '発表日', 'value': update['date'], 'inline': True},
        ]

        if update.get('prev_value'):
            fields.append({'name': '前回値', 'value': str(update['prev_value']), 'inline': True})

        if update.get('change_pct') is not None:
            change_pct = update['change_pct']
            change_str = f"{change_pct:+.2f}%"
            if change_pct > 0:
                change_str += '（上昇）'
            elif change_pct < 0:
                change_str += '（低下）'
            fields.append({'name': '前回比', 'value': change_str, 'inline': True})

        if market.get('market_direction'):
            fields.append({
                'name': '株価への影響',
                'value': f"{market['emoji']} {market['market_direction']}",
                'inline': True,
            })

        embed = {
            'title': f'経済指標速報: {update["name"]} [{impact_label}]',
            'description': desc,
            'color': color,
            'fields': fields,
            'footer': {'text': f'FRED API | ミーム株スキャナー | {now}'},
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }

        # DB保存（結果追跡用）
        self._save_prediction(update, market)

        try:
            resp = requests.post(self.webhook_url, json={'embeds': [embed]}, timeout=10)
            if resp.status_code in (200, 204):
                print(f"[EconMonitor] Discord通知送信: {update['name']}")
            else:
                print(f"[EconMonitor] Discord送信エラー: {resp.status_code}")
        except Exception as e:
            print(f"[EconMonitor] Discord送信例外: {e}")

    def _save_prediction(self, update: Dict, market: Dict):
        """予測をDBに保存（後で結果追跡）"""
        try:
            import sqlite3
            conn = sqlite3.connect(os.path.join(os.path.dirname(__file__), 'meme_stocks.db'))
            conn.execute("""
                INSERT OR IGNORE INTO economic_results
                (series_id, indicator_name, release_date, value, prev_value,
                 change_pct, predicted_direction)
                VALUES (?, ?, ?, ?, ?, ?, ?)
            """, (
                update.get('series_id', ''),
                update.get('name', ''),
                update.get('date', ''),
                float(update['value']) if update.get('value') else None,
                float(update['prev_value']) if update.get('prev_value') else None,
                update.get('change_pct'),
                market.get('market_direction', ''),
            ))
            conn.commit()
            conn.close()
        except Exception as e:
            print(f"[EconMonitor] DB保存エラー: {e}")

    def track_market_reaction(self):
        """過去の経済指標発表後の市場反応を追跡（SPY=S&P500で計測）

        改善版: 分足データで発表直後の反応（1時間後、4時間後）も追跡
        """
        try:
            import sqlite3
            import yfinance as yf

            db_path = os.path.join(os.path.dirname(__file__), 'meme_stocks.db')
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row

            # 追跡待ちレコード（作成から1時間以上経過）
            pending = conn.execute("""
                SELECT * FROM economic_results
                WHERE spy_price_1d IS NULL
                  AND created_at < datetime('now', '-1 hour')
                ORDER BY created_at ASC
            """).fetchall()

            if not pending:
                conn.close()
                return

            print(f"[EconTracker] {len(pending)}件の市場反応を追跡中...")

            # SPYの分足データ（直近5日、発表直後の動きを捉える）
            spy_intraday = None
            try:
                spy_intraday = yf.download('SPY', period='5d', interval='5m',
                                            progress=False, ignore_tz=True)
                if hasattr(spy_intraday.get('Close', None), 'columns'):
                    spy_intraday_close = spy_intraday['Close'].iloc[:, 0]
                elif spy_intraday is not None and not spy_intraday.empty:
                    spy_intraday_close = spy_intraday['Close']
                else:
                    spy_intraday_close = None
            except Exception:
                spy_intraday_close = None

            # SPYの日足データ（翌日終値用）
            spy_daily = yf.download('SPY', period='30d', progress=False, ignore_tz=True)
            spy_daily_close = None
            if spy_daily is not None and not spy_daily.empty:
                spy_daily_close = spy_daily['Close']
                if hasattr(spy_daily_close, 'columns'):
                    spy_daily_close = spy_daily_close.iloc[:, 0]

            for record in pending:
                release_date = record['release_date']
                predicted = record['predicted_direction']

                try:
                    from datetime import datetime as dt
                    rel_date = dt.strptime(release_date, '%Y-%m-%d')

                    price_at = None
                    price_1h = None
                    price_4h = None
                    price_1d = None

                    # 分足データから発表直後の反応を取得
                    if spy_intraday_close is not None and len(spy_intraday_close) > 0:
                        for i, d in enumerate(spy_intraday_close.index):
                            if d.date() == rel_date.date():
                                # 発表時刻付近（8:30 ET前後）の価格
                                if d.hour == 8 and d.minute >= 25 and price_at is None:
                                    price_at = float(spy_intraday_close.iloc[i])
                                # 1時間後（9:30前後）
                                if d.hour == 9 and d.minute >= 25 and price_1h is None and price_at:
                                    price_1h = float(spy_intraday_close.iloc[i])
                                # 4時間後（12:30前後）
                                if d.hour == 12 and d.minute >= 25 and price_4h is None and price_at:
                                    price_4h = float(spy_intraday_close.iloc[i])

                    # 日足から発表日と翌日の終値を取得
                    if spy_daily_close is not None:
                        for i, d in enumerate(spy_daily_close.index):
                            if d.date() == rel_date.date():
                                if price_at is None:
                                    price_at = float(spy_daily_close.iloc[i])
                                if i + 1 < len(spy_daily_close):
                                    price_1d = float(spy_daily_close.iloc[i + 1])
                                break

                    if price_at is None or price_at <= 0:
                        continue

                    # 変化率を計算
                    change_1h = round((price_1h - price_at) / price_at * 100, 3) if price_1h else None
                    change_4h = round((price_4h - price_at) / price_at * 100, 3) if price_4h else None
                    change_1d = round((price_1d - price_at) / price_at * 100, 3) if price_1d else None

                    # 実際の方向判定（1時間後 > 4時間後 > 翌日の優先順位）
                    best_change = change_1h or change_4h or change_1d
                    actual_direction = None
                    prediction_correct = None

                    if best_change is not None:
                        if best_change > 0.1:
                            actual_direction = '株高方向'
                        elif best_change < -0.1:
                            actual_direction = '株安方向'
                        else:
                            actual_direction = '中立'

                        if predicted and actual_direction:
                            if predicted == actual_direction:
                                prediction_correct = 1
                            elif actual_direction == '中立':
                                prediction_correct = None
                            else:
                                prediction_correct = 0

                    conn.execute("""
                        UPDATE economic_results SET
                            spy_price_at_release = ?,
                            spy_price_1h = ?,
                            spy_price_4h = ?,
                            spy_price_1d = ?,
                            spy_change_1h_pct = ?,
                            spy_change_4h_pct = ?,
                            spy_change_1d_pct = ?,
                            actual_direction = ?,
                            prediction_correct = ?,
                            tracked_at = datetime('now')
                        WHERE id = ?
                    """, (price_at, price_1h, price_4h, price_1d,
                          change_1h, change_4h, change_1d,
                          actual_direction, prediction_correct, record['id']))

                except Exception:
                    continue

            conn.commit()
            conn.close()
            print(f"[EconTracker] 追跡完了")

        except Exception as e:
            print(f"[EconTracker] エラー: {e}")

    def get_prediction_accuracy(self) -> Dict:
        """予測精度の統計を返す"""
        try:
            import sqlite3
            db_path = os.path.join(os.path.dirname(__file__), 'meme_stocks.db')
            conn = sqlite3.connect(db_path)
            conn.row_factory = sqlite3.Row

            # 全体精度
            total = conn.execute("""
                SELECT COUNT(*) as n,
                    SUM(CASE WHEN prediction_correct = 1 THEN 1 ELSE 0 END) as correct,
                    SUM(CASE WHEN prediction_correct = 0 THEN 1 ELSE 0 END) as wrong,
                    ROUND(AVG(spy_change_1d_pct), 3) as avg_spy_change
                FROM economic_results
                WHERE prediction_correct IS NOT NULL
            """).fetchone()

            # 指標別精度
            by_indicator = conn.execute("""
                SELECT indicator_name, series_id,
                    COUNT(*) as n,
                    SUM(CASE WHEN prediction_correct = 1 THEN 1 ELSE 0 END) as correct,
                    ROUND(AVG(spy_change_1d_pct), 3) as avg_spy_change
                FROM economic_results
                WHERE prediction_correct IS NOT NULL
                GROUP BY series_id
            """).fetchall()

            conn.close()

            total_n = total['n'] or 0
            accuracy = round(total['correct'] / total_n * 100, 1) if total_n > 0 else None

            return {
                'total': total_n,
                'correct': total['correct'] or 0,
                'wrong': total['wrong'] or 0,
                'accuracy': accuracy,
                'avg_spy_change': total['avg_spy_change'],
                'by_indicator': [dict(r) for r in by_indicator],
            }
        except Exception as e:
            return {'error': str(e)}

    def get_all_current(self) -> List[Dict]:
        """全指標の現在値を取得（API/ダッシュボード用）"""
        result = []
        for series_id, info in INDICATORS.items():
            data = self._get_latest(series_id)
            if data:
                try:
                    prev = float(data['prev_value']) if data['prev_value'] else None
                    current = float(data['value'])
                    change_pct = round((current - prev) / prev * 100, 2) if prev and prev != 0 else None
                except (ValueError, TypeError):
                    change_pct = None

                result.append({
                    'series_id': series_id,
                    'name': info['name'],
                    'name_en': info['name_en'],
                    'impact': info['impact'],
                    'value': data['value'],
                    'date': data['date'],
                    'prev_value': data['prev_value'],
                    'change_pct': change_pct,
                })
            time.sleep(0.3)
        return result

    # 主要指標の発表時間（ET）
    # ほとんどの指標は 8:30 AM ET、FOMCは 2:00 PM ET
    RELEASE_HOURS_ET = [8, 9, 10, 13, 14]  # この時間帯は高頻度チェック

    def _get_check_interval(self) -> int:
        """現在時刻に応じたチェック間隔を返す"""
        now_et = datetime.now(EST)
        hour = now_et.hour
        minute = now_et.minute
        weekday = now_et.weekday()

        # 土日はチェック不要
        if weekday >= 5:
            return 600  # 10分

        # 発表時間帯（ET 8:25-9:05, 13:55-14:10）→ 1分間隔
        if hour in self.RELEASE_HOURS_ET:
            if (hour == 8 and minute >= 25) or (hour == 9 and minute <= 5):
                return 60  # 1分（8:30発表を即座に検出）
            elif (hour == 13 and minute >= 55) or (hour == 14 and minute <= 10):
                return 60  # 1分（14:00 FOMC発表を即座に検出）
            elif (hour == 10 and minute <= 10):
                return 60  # 1分（10:00発表）
            else:
                return 180  # 3分（発表時間帯だがピーク外）

        return 600  # 10分（通常時間帯）

    def start_monitor(self):
        """バックグラウンドで監視を開始（発表時間帯は1分、通常は10分間隔）"""
        if self._running:
            return
        if not self.api_key:
            print("[EconMonitor] APIキーなし、監視をスキップ")
            return

        self._running = True

        def _loop():
            self.initialize()
            while self._running:
                try:
                    interval = self._get_check_interval()
                    updates = self.check_updates()
                    for u in updates:
                        self.notify_update(u)

                    if interval <= 60:
                        now_et = datetime.now(EST)
                        print(f"[EconMonitor] 高頻度監視中 (ET {now_et.strftime('%H:%M')}, {interval}秒間隔)")

                    # 過去の予測の結果追跡（1時間ごと）
                    if not hasattr(self, '_last_track_time'):
                        self._last_track_time = 0
                    if time.time() - self._last_track_time > 3600:
                        self.track_market_reaction()
                        self._last_track_time = time.time()

                except Exception as e:
                    print(f"[EconMonitor] 監視エラー: {e}")
                    interval = 600

                time.sleep(interval)

        t = threading.Thread(target=_loop, daemon=True)
        t.start()
        print("[EconMonitor] 経済指標監視を開始（発表時間帯=1分、通常=10分間隔）")
