"""設定ファイル"""
import os
import secrets
from dotenv import load_dotenv

load_dotenv()


class Config:
    # API Keys（全て環境変数から取得）
    POLYGON_API_KEY = os.getenv('POLYGON_API_KEY', '')
    DISCORD_WEBHOOK_URL = os.getenv('DISCORD_WEBHOOK_URL', '')
    DISCORD_NEWS_WEBHOOK_URL = os.getenv('DISCORD_NEWS_WEBHOOK_URL', '')
    DISCORD_ECON_WEBHOOK_URL = os.getenv('DISCORD_ECON_WEBHOOK_URL', '')
    GEMINI_API_KEY = os.getenv('GEMINI_API_KEY', '')
    FRED_API_KEY = os.getenv('FRED_API_KEY', '')

    # SEC EDGAR（必須: メールアドレスを含むUser-Agent）
    SEC_EDGAR_USER_AGENT = os.getenv('SEC_EDGAR_USER_AGENT', 'MemeStockAnalyzer contact@example.com')

    # Flask（秘密鍵は環境変数 or 自動生成）
    FLASK_SECRET_KEY = os.getenv('FLASK_SECRET_KEY', secrets.token_hex(32))
    FLASK_PORT = 5001

    # CORS許可オリジン（カンマ区切りで複数指定可）
    CORS_ORIGINS = os.getenv('CORS_ORIGINS', 'http://localhost:5001,http://127.0.0.1:5001')

    # スキャナー設定
    MAX_PRICE = 5.00                # $5以下の銘柄のみ
    MIN_AVG_VOLUME = 50_000         # 最低平均出来高（過疎すぎる銘柄を除外）
    VOLUME_SPIKE_MULT = 3.0         # 出来高が20日平均の3倍以上 → スパイク
    VOLUME_RISING_DAYS = 3          # 出来高が3日連続増加 → じわ上げ検出
    VOLUME_RISING_MULT = 1.5        # 各日が前日の1.5倍以上
    SCAN_INTERVAL_SEC = 1800        # スキャン間隔（30分、yfinanceレート制限対策）
    UNIVERSE_REFRESH_HOURS = 24     # 銘柄リスト更新間隔

    # フロート分析
    LOW_FLOAT_THRESHOLD = 10_000_000  # 1000万株以下 = 低フロート（爆発力高）
    ULTRA_LOW_FLOAT = 5_000_000       # 500万株以下 = 超低フロート

    # プレマーケット検出
    PREMARKET_VOL_MULT = 5.0          # プレマーケット出来高が通常の5倍以上

    # 価格変動検出
    PRICE_SPIKE_PCT = 20.0            # 1日+20%以上の急騰
    PRICE_SPIKE_MIN_VOLUME = 100_000  # 価格急騰時の最低出来高

    # インサイダー設定
    INSIDER_LOOKBACK_DAYS = 30      # Form 4の遡り日数
    MIN_INSIDER_BUY_USD = 5_000     # 最低購入金額

    # Database
    DB_PATH = 'meme_stocks.db'

    @classmethod
    def validate(cls):
        """起動時に必須設定の確認"""
        warnings = []
        if not cls.DISCORD_WEBHOOK_URL:
            warnings.append('DISCORD_WEBHOOK_URL が未設定です（Discord通知が無効）')
        if not cls.GEMINI_API_KEY:
            warnings.append('GEMINI_API_KEY が未設定です（ニュース分析が無効）')
        if not cls.FRED_API_KEY:
            warnings.append('FRED_API_KEY が未設定です（経済指標監視が無効）')
        if not cls.POLYGON_API_KEY:
            warnings.append('POLYGON_API_KEY が未設定です（プレマーケット検出が無効）')
        for w in warnings:
            print(f'[Config] WARNING: {w}')
        if not warnings:
            print('[Config] OK: All API keys configured')
        return len(warnings) == 0
