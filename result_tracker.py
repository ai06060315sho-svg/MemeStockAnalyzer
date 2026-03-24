"""
結果追跡モジュール
アラート後の値動きを自動追跡し、勝敗を判定する
（時間外取引データ含む、日付ベース価格追跡）
"""
import logging
import yfinance as yf
import pandas as pd
import time
from datetime import datetime, timedelta
from typing import List, Dict

logger = logging.getLogger('MemeStock.Tracker')


class ResultTracker:
    """アラート発生後の価格変動を追跡"""

    # 結果判定基準（検知価格から1度でも到達したか）
    WIN_PCT = 25.0        # +25%以上到達 → 成功（利益機会あり）
    # 7日間で+25%に1度も届かない → ハズレ

    def __init__(self, db):
        self.db = db

    def track_pending(self):
        """PENDING状態の追跡レコードを一括更新"""
        pending = self.db.get_pending_tracking()
        if not pending:
            return

        logger.info(f"Checking {len(pending)} pending alerts...")

        # ティッカーごとにまとめる
        tickers = list(set(r['ticker'] for r in pending))

        # バッチで価格履歴を取得（時間外データ含む）
        price_cache = {}
        batch_size = 50
        for i in range(0, len(tickers), batch_size):
            batch = tickers[i:i + batch_size]
            try:
                data = yf.download(batch, period='10d', progress=False,
                                   threads=True, ignore_tz=True,
                                   prepost=True)
                if not data.empty:
                    close = data.get('Close')
                    high = data.get('High')
                    if close is not None:
                        if isinstance(close, pd.Series):
                            close = close.to_frame()
                            high = high.to_frame() if high is not None else None
                        for sym in batch:
                            if sym in close.columns:
                                price_cache[sym] = {
                                    'close': close[sym].dropna(),
                                    'high': high[sym].dropna() if high is not None and sym in high.columns else None,
                                }
            except Exception as e:
                logger.error(f"Batch download error: {e}")
            time.sleep(1)

        # 各追跡レコードを更新
        updated = 0
        for record in pending:
            ticker = record['ticker']
            entry_price = record['entry_price']
            created = record['created_at']

            if ticker not in price_cache:
                continue

            # entry_priceが0以下の場合はスキップ（ゼロ除算防止）
            if not entry_price or entry_price <= 0:
                logger.warning(f"{ticker}: entry_price={entry_price} (invalid, skipping)")
                continue

            cache = price_cache[ticker]
            close_series = cache['close']
            high_series = cache['high']

            if close_series is None or len(close_series) < 2:
                continue

            # アラート発生日からの経過日数を計算
            try:
                alert_date = datetime.strptime(created[:10], '%Y-%m-%d').date()
                today = datetime.now().date()
                days_elapsed = (today - alert_date).days
            except Exception:
                days_elapsed = 0

            # まだ1日も経っていない → スキップ
            if days_elapsed < 1:
                continue

            result_data = {
                'price_1d': None, 'price_3d': None, 'price_7d': None,
                'change_1d_pct': None, 'change_3d_pct': None, 'change_7d_pct': None,
                'max_price_7d': None, 'max_gain_pct': None, 'result': 'PENDING',
            }

            # 日付ベースで価格を取得
            try:
                alert_datetime = pd.Timestamp(alert_date)
                prices_after = close_series[close_series.index >= alert_datetime]
                highs_after = (high_series[high_series.index >= alert_datetime]
                               if high_series is not None else None)

                if len(prices_after) < 1:
                    continue

                # 1日後の終値
                next_day = alert_datetime + pd.Timedelta(days=1)
                prices_1d = prices_after[prices_after.index >= next_day]
                if len(prices_1d) >= 1:
                    p1d = float(prices_1d.iloc[0])
                    result_data['price_1d'] = round(p1d, 4)
                    result_data['change_1d_pct'] = round(
                        (p1d - entry_price) / entry_price * 100, 2)

                # 3日後の終値
                day3 = alert_datetime + pd.Timedelta(days=3)
                prices_3d = prices_after[prices_after.index >= day3]
                if days_elapsed >= 3 and len(prices_3d) >= 1:
                    p3d = float(prices_3d.iloc[0])
                    result_data['price_3d'] = round(p3d, 4)
                    result_data['change_3d_pct'] = round(
                        (p3d - entry_price) / entry_price * 100, 2)

                # 7日後の終値
                day7 = alert_datetime + pd.Timedelta(days=7)
                prices_7d = prices_after[prices_after.index >= day7]
                if days_elapsed >= 7 and len(prices_7d) >= 1:
                    p7d = float(prices_7d.iloc[0])
                    result_data['price_7d'] = round(p7d, 4)
                    result_data['change_7d_pct'] = round(
                        (p7d - entry_price) / entry_price * 100, 2)

                # === 期間内の最高値（検知価格からの最大上昇率） ===
                # これが最も重要: 1度でもこの価格に到達していれば利益機会があった
                if highs_after is not None and len(highs_after) > 0:
                    max_price = float(highs_after.max())
                    result_data['max_price_7d'] = round(max_price, 4)
                    result_data['max_gain_pct'] = round(
                        (max_price - entry_price) / entry_price * 100, 2)

                    # 最高値に到達した日を特定
                    max_date_idx = highs_after.idxmax()
                    if hasattr(max_date_idx, 'strftime'):
                        result_data['max_price_date'] = max_date_idx.strftime('%m/%d')

            except Exception as e:
                logger.error(f"{ticker}: price tracking error: {e}")
                continue

            # === 結果判定 ===
            # 基準: 検知価格から1度でも+25%に到達 → 成功（利益機会あり）
            #       7日間で+25%に1度も届かない → ハズレ
            max_gain = result_data.get('max_gain_pct', 0) or 0

            if max_gain >= self.WIN_PCT:
                # 1度でも+25%に到達 → 即座に成功確定（何日目でも）
                result_data['result'] = 'WIN'
            elif days_elapsed >= 7:
                # 7日間で+25%に届かなかった → ハズレ
                result_data['result'] = 'LOSS'
            # 7日未満で+25%未到達 → まだPENDING（チャンスが残っている）

            self.db.update_tracking(record['id'], result_data)
            updated += 1

        if updated > 0:
            logger.info(f"Updated {updated} tracking records")

    def get_pattern_analysis(self) -> Dict:
        """勝ちパターンの分析"""
        stats = self.db.get_tracking_stats()
        if stats.get('total', 0) == 0:
            return stats

        # スコア帯別の勝率を計算
        for band in stats.get('by_score', []):
            if band['n'] > 0:
                band['win_rate'] = round(band['wins'] / band['n'] * 100, 1)

        # タイプ別の勝率
        for t in stats.get('by_type', []):
            if t['n'] > 0:
                t['win_rate'] = round(t['wins'] / t['n'] * 100, 1)

        # フロート別の勝率
        for f in stats.get('by_float', []):
            if f['n'] > 0:
                f['win_rate'] = round(f['wins'] / f['n'] * 100, 1)

        return stats
