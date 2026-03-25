"""SQLiteデータベース管理"""
import logging
import os
import shutil
import sqlite3
import threading
from datetime import datetime
from typing import Dict, List, Optional
from config import Config

logger = logging.getLogger('MemeStock.DB')


class StockDB:
    def __init__(self, db_path: str = Config.DB_PATH):
        self.db_path = db_path
        self._lock = threading.Lock()
        self._backup_on_startup()
        self._init_db()

    def _backup_on_startup(self):
        """起動時にDBを自動バックアップ（最新3世代保持）"""
        if not os.path.exists(self.db_path):
            return
        try:
            backup_dir = os.path.join(os.path.dirname(self.db_path) or '.', 'backups')
            os.makedirs(backup_dir, exist_ok=True)
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            backup_path = os.path.join(backup_dir, f'meme_stocks_{timestamp}.db')
            shutil.copy2(self.db_path, backup_path)
            logger.info(f"DB backup created: {backup_path}")

            # 古いバックアップを削除（3世代保持）
            backups = sorted([
                f for f in os.listdir(backup_dir) if f.endswith('.db')
            ])
            while len(backups) > 3:
                old = backups.pop(0)
                os.remove(os.path.join(backup_dir, old))
                logger.info(f"Old backup removed: {old}")
        except Exception as e:
            logger.warning(f"Backup failed: {e}")

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.execute("PRAGMA journal_mode=WAL")
        return conn

    def _init_db(self):
        conn = self._get_conn()
        try:
            # アラートテーブル
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_alerts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    timestamp TEXT NOT NULL,
                    ticker TEXT NOT NULL,
                    company_name TEXT,
                    price REAL NOT NULL,
                    volume INTEGER,
                    avg_volume INTEGER,
                    volume_ratio REAL,
                    alert_type TEXT NOT NULL,
                    insider_name TEXT,
                    insider_title TEXT,
                    insider_shares INTEGER,
                    insider_buy_amount REAL,
                    market_cap REAL,
                    float_shares REAL,
                    float_level TEXT,
                    short_ratio REAL,
                    short_pct REAL,
                    price_change_pct REAL,
                    sector TEXT,
                    industry TEXT,
                    score INTEGER DEFAULT 0,
                    score_detail TEXT,
                    detail TEXT,
                    has_reverse_split INTEGER DEFAULT 0,
                    reverse_split_ratio TEXT,
                    sentiment TEXT,
                    buzz_score INTEGER,
                    mention_count INTEGER,
                    notified INTEGER DEFAULT 0,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ticker ON stock_alerts (ticker)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_type ON stock_alerts (alert_type)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_ts ON stock_alerts (timestamp)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_alerts_score ON stock_alerts (score)")

            # 既存テーブルに不足カラムを追加（ALTER TABLE）
            for col, ctype in [
                ('detail', 'TEXT'), ('has_reverse_split', 'INTEGER DEFAULT 0'),
                ('reverse_split_ratio', 'TEXT'), ('sentiment', 'TEXT'),
                ('buzz_score', 'INTEGER'), ('mention_count', 'INTEGER'),
            ]:
                try:
                    conn.execute(f"ALTER TABLE stock_alerts ADD COLUMN {col} {ctype}")
                except Exception:
                    pass  # 既に存在する場合

            # 結果追跡テーブル（アラート後の値動きを記録）
            conn.execute("""
                CREATE TABLE IF NOT EXISTS alert_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    alert_id INTEGER NOT NULL,
                    ticker TEXT NOT NULL,
                    entry_price REAL NOT NULL,
                    alert_type TEXT,
                    score INTEGER,
                    float_level TEXT,
                    volume_ratio REAL,
                    has_reverse_split INTEGER DEFAULT 0,
                    -- 追跡結果
                    price_1d REAL,
                    price_3d REAL,
                    price_7d REAL,
                    change_1d_pct REAL,
                    change_3d_pct REAL,
                    change_7d_pct REAL,
                    max_price_7d REAL,
                    max_gain_pct REAL,
                    result TEXT DEFAULT 'PENDING',
                    tracked_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP,
                    FOREIGN KEY (alert_id) REFERENCES stock_alerts(id)
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_results_alert ON alert_results (alert_id)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_results_result ON alert_results (result)")
            conn.execute("CREATE INDEX IF NOT EXISTS idx_results_float ON alert_results (float_level)")

            # alert_resultsに不足カラム追加
            try:
                conn.execute("ALTER TABLE alert_results ADD COLUMN has_reverse_split INTEGER DEFAULT 0")
            except Exception:
                pass

            # スコア重み調整テーブル
            conn.execute("""
                CREATE TABLE IF NOT EXISTS score_weights (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    milestone INTEGER,
                    weights TEXT,
                    analysis TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 経済指標の結果追跡テーブル
            conn.execute("""
                CREATE TABLE IF NOT EXISTS economic_results (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    series_id TEXT NOT NULL,
                    indicator_name TEXT NOT NULL,
                    release_date TEXT NOT NULL,
                    value REAL,
                    prev_value REAL,
                    change_pct REAL,
                    predicted_direction TEXT,
                    -- 発表後の市場反応（S&P 500 = SPY）
                    spy_price_at_release REAL,
                    spy_price_1h REAL,
                    spy_price_4h REAL,
                    spy_price_1d REAL,
                    spy_change_1h_pct REAL,
                    spy_change_4h_pct REAL,
                    spy_change_1d_pct REAL,
                    -- 判定
                    actual_direction TEXT,
                    prediction_correct INTEGER,
                    tracked_at TEXT,
                    created_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)
            conn.execute("CREATE INDEX IF NOT EXISTS idx_econ_series ON economic_results (series_id)")

            # ウォッチリスト
            conn.execute("""
                CREATE TABLE IF NOT EXISTS watched_tickers (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    ticker TEXT UNIQUE NOT NULL,
                    notes TEXT,
                    added_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            # 銘柄ユニバース（$5以下銘柄のキャッシュ）
            conn.execute("""
                CREATE TABLE IF NOT EXISTS stock_universe (
                    ticker TEXT PRIMARY KEY,
                    name TEXT,
                    last_price REAL,
                    market_cap REAL,
                    sector TEXT,
                    industry TEXT,
                    country TEXT,
                    updated_at TEXT DEFAULT CURRENT_TIMESTAMP
                )
            """)

            conn.commit()
        finally:
            conn.close()

    # 同一銘柄の1日あたり最大アラート数（タイプ違いを含む）
    MAX_ALERTS_PER_TICKER_PER_DAY = 2

    def save_alert(self, alert: Dict) -> int:
        """アラートを保存"""
        with self._lock:
            conn = self._get_conn()
            try:
                # 同一銘柄 + 同一タイプの同日重複チェック
                existing = conn.execute("""
                    SELECT id FROM stock_alerts
                    WHERE ticker = ? AND alert_type = ?
                      AND date(timestamp) = date(?)
                """, (alert['ticker'], alert['alert_type'],
                      alert['timestamp'])).fetchone()
                if existing:
                    return -1  # 重複

                # 同一銘柄の同日アラート数上限チェック（タイプ違いでも制限）
                day_count = conn.execute("""
                    SELECT COUNT(*) FROM stock_alerts
                    WHERE ticker = ? AND date(timestamp) = date(?)
                """, (alert['ticker'], alert['timestamp'])).fetchone()[0]
                if day_count >= self.MAX_ALERTS_PER_TICKER_PER_DAY:
                    return -1  # 同日の銘柄あたり上限超過

                cursor = conn.execute("""
                    INSERT INTO stock_alerts
                    (timestamp, ticker, company_name, price, volume, avg_volume,
                     volume_ratio, alert_type, insider_name, insider_title,
                     insider_shares, insider_buy_amount, market_cap, float_shares,
                     float_level, short_ratio, short_pct, price_change_pct,
                     sector, industry, score, score_detail, detail,
                     has_reverse_split, reverse_split_ratio,
                     sentiment, buzz_score, mention_count)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
                """, (
                    alert['timestamp'], alert['ticker'],
                    alert.get('company_name'), alert['price'],
                    alert.get('volume'), alert.get('avg_volume'),
                    alert.get('volume_ratio'), alert['alert_type'],
                    alert.get('insider_name'), alert.get('insider_title'),
                    alert.get('insider_shares'), alert.get('insider_buy_amount'),
                    alert.get('market_cap'), alert.get('float_shares'),
                    alert.get('float_level'), alert.get('short_ratio'),
                    alert.get('short_pct'), alert.get('price_change_pct'),
                    alert.get('sector'), alert.get('industry'),
                    alert.get('score', 0), alert.get('score_detail'),
                    alert.get('detail', ''),
                    1 if alert.get('has_reverse_split') else 0,
                    alert.get('reverse_split_ratio'),
                    alert.get('sentiment', ''),
                    alert.get('buzz_score', 0) or 0,
                    alert.get('mention_count', 0) or 0,
                ))
                conn.commit()
                return cursor.lastrowid
            finally:
                conn.close()

    def mark_notified(self, alert_id: int):
        """通知済みフラグを更新"""
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("UPDATE stock_alerts SET notified = 1 WHERE id = ?", (alert_id,))
                conn.commit()
            finally:
                conn.close()

    def get_recent_alerts(self, limit: int = 50) -> List[Dict]:
        """最近のアラートを取得"""
        conn = self._get_conn()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT * FROM stock_alerts
                ORDER BY timestamp DESC LIMIT ?
            """, (limit,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_alerts_by_ticker(self, ticker: str) -> List[Dict]:
        """銘柄別アラート取得"""
        conn = self._get_conn()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT * FROM stock_alerts
                WHERE ticker = ? ORDER BY timestamp DESC
            """, (ticker,)).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def get_stats(self) -> Dict:
        """統計情報"""
        conn = self._get_conn()
        try:
            conn.row_factory = sqlite3.Row
            total = conn.execute("SELECT COUNT(*) as c FROM stock_alerts").fetchone()['c']
            today = conn.execute("""
                SELECT COUNT(*) as c FROM stock_alerts
                WHERE date(timestamp) = date('now')
            """).fetchone()['c']
            by_type = conn.execute("""
                SELECT alert_type, COUNT(*) as c FROM stock_alerts
                GROUP BY alert_type
            """).fetchall()
            return {
                'total': total,
                'today': today,
                'by_type': {r['alert_type']: r['c'] for r in by_type},
            }
        finally:
            conn.close()

    # === 結果追跡 ===
    def create_tracking(self, alert_id: int, alert: Dict):
        """アラート発生時に追跡レコードを作成"""
        with self._lock:
            conn = self._get_conn()
            try:
                # price=0のアラート（SNS等）は追跡しない
                entry_price = alert.get('price', 0) or 0
                if entry_price <= 0:
                    return

                # 株式併合は追跡しない
                if alert.get('has_reverse_split'):
                    return

                # 重複チェック
                existing = conn.execute(
                    "SELECT id FROM alert_results WHERE alert_id = ?",
                    (alert_id,)).fetchone()
                if existing:
                    return

                conn.execute("""
                    INSERT INTO alert_results
                    (alert_id, ticker, entry_price, alert_type, score,
                     float_level, volume_ratio)
                    VALUES (?, ?, ?, ?, ?, ?, ?)
                """, (
                    alert_id, alert['ticker'], entry_price,
                    alert.get('alert_type'), alert.get('score'),
                    alert.get('float_level'), alert.get('volume_ratio'),
                ))
                conn.commit()
            finally:
                conn.close()

    def get_pending_tracking(self) -> List[Dict]:
        """追跡待ちのレコードを取得"""
        conn = self._get_conn()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute("""
                SELECT * FROM alert_results
                WHERE result = 'PENDING'
                ORDER BY created_at ASC
            """).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    def update_tracking(self, tracking_id: int, data: Dict):
        """追跡結果を更新"""
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("""
                    UPDATE alert_results SET
                        price_1d = ?, price_3d = ?, price_7d = ?,
                        change_1d_pct = ?, change_3d_pct = ?, change_7d_pct = ?,
                        max_price_7d = ?, max_gain_pct = ?, max_price_date = ?,
                        result = ?, tracked_at = datetime('now')
                    WHERE id = ?
                """, (
                    data.get('price_1d'), data.get('price_3d'), data.get('price_7d'),
                    data.get('change_1d_pct'), data.get('change_3d_pct'),
                    data.get('change_7d_pct'),
                    data.get('max_price_7d'), data.get('max_gain_pct'),
                    data.get('max_price_date'),
                    data.get('result'), tracking_id,
                ))
                conn.commit()
            finally:
                conn.close()

    def get_tracking_stats(self) -> Dict:
        """追跡結果の統計"""
        conn = self._get_conn()
        try:
            conn.row_factory = sqlite3.Row

            # 全体統計
            total = conn.execute(
                "SELECT COUNT(*) as c FROM alert_results WHERE result NOT IN ('PENDING', 'REVERSE_SPLIT')"
            ).fetchone()['c']

            if total == 0:
                return {'total': 0, 'message': 'まだ結果データがありません'}

            stats = conn.execute("""
                SELECT
                    COUNT(*) as total,
                    SUM(CASE WHEN result = 'BIG_WIN' THEN 1 ELSE 0 END) as big_wins,
                    SUM(CASE WHEN result = 'WIN' THEN 1 ELSE 0 END) as wins,
                    SUM(CASE WHEN result = 'SMALL_WIN' THEN 1 ELSE 0 END) as small_wins,
                    SUM(CASE WHEN result = 'LOSS' THEN 1 ELSE 0 END) as losses,
                    ROUND(AVG(max_gain_pct), 1) as avg_max_gain,
                    ROUND(AVG(change_1d_pct), 1) as avg_1d,
                    ROUND(AVG(change_3d_pct), 1) as avg_3d,
                    ROUND(AVG(change_7d_pct), 1) as avg_7d
                FROM alert_results WHERE result NOT IN ('PENDING', 'REVERSE_SPLIT')
            """).fetchone()

            # スコア帯別の成績
            by_score = conn.execute("""
                SELECT
                    CASE
                        WHEN score >= 60 THEN 'high'
                        WHEN score >= 30 THEN 'mid'
                        ELSE 'low'
                    END as score_band,
                    COUNT(*) as n,
                    ROUND(AVG(max_gain_pct), 1) as avg_max_gain,
                    SUM(CASE WHEN result IN ('BIG_WIN','WIN') THEN 1 ELSE 0 END) as wins
                FROM alert_results WHERE result NOT IN ('PENDING', 'REVERSE_SPLIT')
                GROUP BY score_band
            """).fetchall()

            # アラートタイプ別の成績
            by_type = conn.execute("""
                SELECT alert_type,
                    COUNT(*) as n,
                    ROUND(AVG(max_gain_pct), 1) as avg_max_gain,
                    SUM(CASE WHEN result IN ('BIG_WIN','WIN') THEN 1 ELSE 0 END) as wins
                FROM alert_results WHERE result NOT IN ('PENDING', 'REVERSE_SPLIT')
                GROUP BY alert_type
            """).fetchall()

            # フロート別の成績
            by_float = conn.execute("""
                SELECT float_level,
                    COUNT(*) as n,
                    ROUND(AVG(max_gain_pct), 1) as avg_max_gain,
                    SUM(CASE WHEN result IN ('BIG_WIN','WIN') THEN 1 ELSE 0 END) as wins
                FROM alert_results WHERE result NOT IN ('PENDING', 'REVERSE_SPLIT') AND float_level IS NOT NULL
                GROUP BY float_level
            """).fetchall()

            return {
                'total': dict(stats),
                'by_score': [dict(r) for r in by_score],
                'by_type': [dict(r) for r in by_type],
                'by_float': [dict(r) for r in by_float],
            }
        finally:
            conn.close()

    # === ウォッチリスト ===
    def add_watched(self, ticker: str, notes: str = None):
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute(
                    "INSERT OR IGNORE INTO watched_tickers (ticker, notes) VALUES (?, ?)",
                    (ticker.upper(), notes))
                conn.commit()
            finally:
                conn.close()

    def remove_watched(self, ticker: str):
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("DELETE FROM watched_tickers WHERE ticker = ?",
                             (ticker.upper(),))
                conn.commit()
            finally:
                conn.close()

    def get_watched(self) -> List[Dict]:
        conn = self._get_conn()
        try:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM watched_tickers ORDER BY added_at DESC"
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()

    # === 銘柄ユニバース ===
    def save_universe(self, tickers: List[Dict]):
        """$5以下銘柄リストを保存"""
        with self._lock:
            conn = self._get_conn()
            try:
                conn.execute("DELETE FROM stock_universe")
                for t in tickers:
                    conn.execute("""
                        INSERT OR REPLACE INTO stock_universe
                        (ticker, name, last_price, market_cap, sector, industry, country, updated_at)
                        VALUES (?, ?, ?, ?, ?, ?, ?, datetime('now'))
                    """, (t['ticker'], t.get('name'), t.get('last_price'),
                          t.get('market_cap'), t.get('sector'), t.get('industry'),
                          t.get('country')))
                conn.commit()
                print(f"[DB] Universe saved: {len(tickers)} tickers")
            finally:
                conn.close()

    def get_universe(self) -> List[str]:
        """キャッシュ済みの銘柄リストを返す"""
        conn = self._get_conn()
        try:
            rows = conn.execute(
                "SELECT ticker FROM stock_universe ORDER BY ticker"
            ).fetchall()
            return [r[0] for r in rows]
        finally:
            conn.close()

    def get_universe_age_hours(self) -> float:
        """ユニバースの経過時間（時間）"""
        conn = self._get_conn()
        try:
            row = conn.execute(
                "SELECT MIN(updated_at) as oldest FROM stock_universe"
            ).fetchone()
            if not row or not row[0]:
                return 9999
            from datetime import datetime, timezone
            oldest = datetime.strptime(row[0], '%Y-%m-%d %H:%M:%S')
            return (datetime.now(timezone.utc).replace(tzinfo=None) - oldest).total_seconds() / 3600
        finally:
            conn.close()

    def has_alert_today(self, ticker: str, alert_type: str, date_str: str) -> bool:
        """同一銘柄・同一タイプのアラートが同日に既に存在するか"""
        conn = self._get_conn()
        try:
            row = conn.execute("""
                SELECT COUNT(*) as c FROM stock_alerts
                WHERE ticker = ? AND alert_type = ? AND date(timestamp) = date(?)
            """, (ticker, alert_type, date_str)).fetchone()
            return row[0] > 0 if row else False
        finally:
            conn.close()
