"""
Meme Stock Analyzer - $5以下の米国株から急騰候補を検出
"""
import logging
import os
import threading
import time
import traceback
import pandas as pd
from datetime import datetime, timezone, timedelta
from flask import Flask, render_template, jsonify, request, redirect
from flask_socketio import SocketIO
from config import Config
from stock_db import StockDB
from stock_scanner import StockScanner
from insider_tracker import InsiderTracker
from discord_notifier import DiscordNotifier
from translator import translate_sector, translate_industry, translate_summary
from result_tracker import ResultTracker
from stock_news import StockNewsAnalyzer
from economic_monitor import EconomicMonitor
from social_scanner import SocialScanner
from ml_predictor import MLPredictor

# ログ設定（UTF-8指定で日本語文字化け防止）
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(name)s] %(levelname)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    encoding='utf-8',
)
logger = logging.getLogger('MemeStock')

# タイムゾーン（サマータイム自動対応）
try:
    from zoneinfo import ZoneInfo
    EST = ZoneInfo('America/New_York')  # EST/EDT自動切替
    JST = ZoneInfo('Asia/Tokyo')
except ImportError:
    # Python 3.8以前のフォールバック
    EST = timezone(timedelta(hours=-5))
    JST = timezone(timedelta(hours=9))

# 通知フィルタ定数
MIN_RESULTS_FOR_FILTER = 50   # この件数未満は全て通知（データ収集優先）
MIN_WIN_RATE_TO_NOTIFY = 30   # 勝率30%未満のパターンは通知しない

# 設定バリデーション
Config.validate()

app = Flask(__name__)
app.config['SECRET_KEY'] = Config.FLASK_SECRET_KEY
cors_origins = [o.strip() for o in Config.CORS_ORIGINS.split(',')]
socketio = SocketIO(app, cors_allowed_origins=cors_origins, async_mode='threading')

# 認証設定（.envで制御）
# ADMIN: 管理者（スキャン実行・全機能アクセス可能）
# USER: 一般ユーザー（閲覧のみ、スキャン実行不可）
_ADMIN_USER = os.environ.get('APP_USER', '')
_ADMIN_PASS = os.environ.get('APP_PASS', '')
_VIEW_USER = os.environ.get('VIEW_USER', '')
_VIEW_PASS = os.environ.get('VIEW_PASS', '')


def _get_user_role():
    """現在のリクエストのユーザーロールを返す"""
    auth = request.authorization
    if not auth:
        return None
    if _ADMIN_USER and auth.username == _ADMIN_USER and auth.password == _ADMIN_PASS:
        return 'admin'
    if _VIEW_USER and auth.username == _VIEW_USER and auth.password == _VIEW_PASS:
        return 'user'
    return None


@app.before_request
def _check_auth():
    # 認証が設定されていない場合はスキップ
    if not _ADMIN_USER and not _VIEW_USER:
        return
    # API内部通信（SocketIO等）はスキップ
    if request.path.startswith('/socket.io'):
        return
    role = _get_user_role()
    if not role:
        return ('認証が必要です', 401,
                {'WWW-Authenticate': 'Basic realm="MemeStockAnalyzer"'})
    # ユーザーロールの場合、許可されたAPIのみアクセス可能
    if role == 'user':
        # ユーザーに許可するパス（閲覧系のみ）
        allowed_paths = [
            '/', '/view', '/criteria',
            '/stock/',              # 個別銘柄ページ
            '/api/alerts',          # 検知アラート一覧
            '/api/stats',           # 基本統計
            '/api/tracking/',       # 検知後の結果
            '/api/iron-patterns',   # 鉄板パターン
            '/api/reddit/trending', # Reddit注目銘柄
            '/api/ml/stats',        # ML統計（閲覧のみ）
            '/api/ml/top-picks',    # ML厳選銘柄
            '/api/stock/',          # 個別銘柄情報
            '/api/search',          # ティッカー検索
            '/api/backtest',        # バックテスト結果
            '/api/economic',        # 経済指標
            '/static/',             # 静的ファイル
        ]
        # 許可リストに一致しなければブロック
        path = request.path
        if not any(path == p or path.startswith(p) for p in allowed_paths):
            return jsonify({'error': 'Access denied'}), 403
        # POST/DELETE/PUTはすべてブロック（閲覧のみ）
        if request.method in ('POST', 'DELETE', 'PUT'):
            return jsonify({'error': 'Read only'}), 403


@app.context_processor
def inject_role():
    """テンプレートにユーザーロールを注入"""
    return {'user_role': _get_user_role() or 'admin'}

# グローバルインスタンス
db = StockDB()
scanner = StockScanner(db=db)
insider = InsiderTracker()
discord = DiscordNotifier()
tracker = ResultTracker(db)
news = StockNewsAnalyzer()
econ_monitor = EconomicMonitor()
social = SocialScanner()
ml = MLPredictor()

# スキャン状態（スレッドセーフ）
_scan_lock = threading.Lock()
scan_status = {
    'running': False,
    'last_scan': None,
    'last_results': {'volume': 0, 'insider': 0, 'combined': 0},
    'universe_size': 0,
    'current_session': 'UNKNOWN',
    'consecutive_failures': 0,
}


# === Routes ===
@app.route('/')
def index():
    role = _get_user_role()
    if role == 'user':
        return render_template('viewer.html')
    return render_template('dashboard.html')


@app.route('/view')
def viewer():
    """ユーザー専用閲覧ページ"""
    return render_template('viewer.html')


@app.route('/portal')
def portal_redirect():
    """ポータルへリダイレクト（管理者のみ）"""
    if _get_user_role() != 'admin':
        return 'Access denied', 403
    host = request.host.split(':')[0]
    return redirect(f'http://{host}:3000')


@app.route('/settings')
def settings_page():
    """設定ページ（テーマ等）"""
    return render_template('settings.html')


@app.route('/criteria')
def criteria_page():
    """基準ページ（シグナル・スコア等）"""
    return render_template('criteria.html')


@app.route('/api/settings')
def api_settings():
    """現在の設定値をJSON返却"""
    from result_tracker import ResultTracker as RT
    return jsonify({
        'scanner': {
            'max_price': Config.MAX_PRICE,
            'min_avg_volume': Config.MIN_AVG_VOLUME,
            'volume_spike_mult': Config.VOLUME_SPIKE_MULT,
            'volume_rising_days': Config.VOLUME_RISING_DAYS,
            'volume_rising_mult': Config.VOLUME_RISING_MULT,
            'scan_interval_sec': Config.SCAN_INTERVAL_SEC,
            'universe_refresh_hours': Config.UNIVERSE_REFRESH_HOURS,
        },
        'float': {
            'low_float_threshold': Config.LOW_FLOAT_THRESHOLD,
            'ultra_low_float': Config.ULTRA_LOW_FLOAT,
        },
        'premarket': {
            'vol_mult': Config.PREMARKET_VOL_MULT,
        },
        'price_spike': {
            'pct': Config.PRICE_SPIKE_PCT,
            'min_volume': Config.PRICE_SPIKE_MIN_VOLUME,
        },
        'insider': {
            'lookback_days': Config.INSIDER_LOOKBACK_DAYS,
            'min_buy_usd': Config.MIN_INSIDER_BUY_USD,
        },
        'scoring': {
            'volume_max': 35,
            'float_max': 25,
            'insider_max': 20,
            'price_max': 15,
            'combined_bonus': 10,
            'short_max': 5,
        },
        'result_tracking': {
            'win_pct': RT.WIN_PCT,
            'judgment_days': 7,
        },
        'notification': {
            'min_results_for_filter': MIN_RESULTS_FOR_FILTER,
            'min_win_rate': MIN_WIN_RATE_TO_NOTIFY,
        },
        'auto_adjust': {
            'milestones': scanner.AUTO_ADJUST_MILESTONES,
        },
    })


@app.route('/stock/<ticker>')
def stock_detail(ticker):
    """銘柄詳細ページ"""
    return render_template('stock_detail.html', ticker=ticker.upper())


@app.route('/api/stock/<ticker>/info')
def api_stock_info(ticker):
    """銘柄の詳細情報API"""
    ticker = ticker.upper()
    try:
        import yfinance as yf
        stock = yf.Ticker(ticker)
        info = stock.info

        # 価格履歴（30日）
        hist = stock.history(period='30d', prepost=True)
        chart_data = []
        if not hist.empty:
            for idx, row in hist.iterrows():
                chart_data.append({
                    'date': idx.strftime('%Y-%m-%d'),
                    'open': round(float(row['Open']), 4),
                    'high': round(float(row['High']), 4),
                    'low': round(float(row['Low']), 4),
                    'close': round(float(row['Close']), 4),
                    'volume': int(row['Volume']),
                })

        # アラート履歴
        alerts = db.get_alerts_by_ticker(ticker)

        # フロート情報
        float_info = scanner.get_float_info(ticker)

        return jsonify({
            'ticker': ticker,
            'name': info.get('shortName', info.get('longName', '')),
            'price': info.get('currentPrice') or info.get('regularMarketPrice'),
            'market_cap': info.get('marketCap'),
            'float_shares': info.get('floatShares'),
            'shares_outstanding': info.get('sharesOutstanding'),
            'short_ratio': info.get('shortRatio'),
            'short_pct': info.get('shortPercentOfFloat'),
            'avg_volume': info.get('averageVolume'),
            'volume': info.get('volume'),
            'sector': info.get('sector', ''),
            'industry': info.get('industry', ''),
            'exchange': info.get('exchange', ''),
            'fifty_two_week_high': info.get('fiftyTwoWeekHigh'),
            'fifty_two_week_low': info.get('fiftyTwoWeekLow'),
            'float_level': float_info.get('float_level') if float_info else None,
            'sector_ja': translate_sector(info.get('sector', '')),
            'industry_ja': translate_industry(info.get('industry', '')),
            'business_summary': translate_summary(
                info.get('longBusinessSummary', ''), ticker=ticker),
            'chart_data': chart_data,
            'alerts': alerts,
        })
    except Exception as e:
        return jsonify({'error': str(e)}), 500


@app.route('/api/search')
def api_search():
    """ティッカー検索API"""
    q = request.args.get('q', '').upper().strip()
    if not q or len(q) < 1:
        return jsonify([])

    import sqlite3 as _sqlite3
    conn = db._get_conn()
    try:
        conn.row_factory = _sqlite3.Row
        # ユニバースから部分一致検索
        rows = conn.execute("""
            SELECT ticker, name, last_price FROM stock_universe
            WHERE ticker LIKE ? OR name LIKE ?
            ORDER BY
                CASE WHEN ticker = ? THEN 0
                     WHEN ticker LIKE ? THEN 1
                     ELSE 2 END,
                ticker
            LIMIT 20
        """, (f'%{q}%', f'%{q}%', q, f'{q}%')).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route('/api/history')
def api_scan_history():
    """スキャン履歴（日別）"""
    conn = db._get_conn()
    try:
        import sqlite3 as _sqlite3
        conn.row_factory = _sqlite3.Row
        rows = conn.execute("""
            SELECT substr(timestamp, 1, 10) as scan_date,
                COUNT(*) as total_alerts,
                SUM(CASE WHEN alert_type = 'VOLUME_SPIKE' THEN 1 ELSE 0 END) as vol_spikes,
                SUM(CASE WHEN alert_type = 'VOLUME_RISING' THEN 1 ELSE 0 END) as vol_rising,
                SUM(CASE WHEN alert_type = 'PRICE_SPIKE' THEN 1 ELSE 0 END) as price_spikes,
                SUM(CASE WHEN alert_type = 'INSIDER_BUY' THEN 1 ELSE 0 END) as insider_buys,
                SUM(CASE WHEN alert_type = 'COMBINED' THEN 1 ELSE 0 END) as combined,
                MAX(score) as max_score,
                ROUND(AVG(score), 0) as avg_score
            FROM stock_alerts
            GROUP BY scan_date
            ORDER BY scan_date DESC
            LIMIT 30
        """).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route('/api/history/<date>')
def api_scan_history_date(date):
    """特定日のアラート一覧"""
    conn = db._get_conn()
    try:
        import sqlite3 as _sqlite3
        conn.row_factory = _sqlite3.Row
        rows = conn.execute("""
            SELECT * FROM stock_alerts
            WHERE substr(timestamp, 1, 10) = ?
            ORDER BY score DESC
        """, (date,)).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route('/api/stock/<ticker>/analyze', methods=['POST'])
def api_stock_analyze(ticker):
    """個別銘柄の分析API"""
    ticker = ticker.upper()
    try:
        import yfinance as yf

        stock = yf.Ticker(ticker)
        hist = stock.history(period='30d', prepost=True)
        info = stock.info

        if hist.empty:
            return jsonify({'error': f'{ticker}のデータが見つかりません'}), 404

        # 現在価格・出来高
        current_price = float(hist['Close'].iloc[-1])
        current_vol = int(hist['Volume'].iloc[-1])
        avg_vol_20 = int(hist['Volume'].iloc[-21:-1].mean()) if len(hist) >= 21 else int(hist['Volume'].mean())
        vol_ratio = round(current_vol / avg_vol_20, 2) if avg_vol_20 > 0 else 0

        # 価格変動
        prev_close = float(hist['Close'].iloc[-2]) if len(hist) >= 2 else current_price
        price_change_pct = round((current_price - prev_close) / prev_close * 100, 2) if prev_close > 0 else 0

        # 出来高トレンド（5日間）
        vol_trend = []
        if len(hist) >= 6:
            for i in range(-5, 0):
                vol_trend.append({
                    'date': hist.index[i].strftime('%m/%d'),
                    'volume': int(hist['Volume'].iloc[i]),
                    'close': round(float(hist['Close'].iloc[i]), 4),
                })

        # 出来高じわ上げ判定
        vol_rising = False
        if len(hist) >= 4:
            recent_vols = hist['Volume'].iloc[-3:]
            vol_rising = all(
                recent_vols.iloc[j] >= recent_vols.iloc[j-1] * 1.2
                for j in range(1, len(recent_vols))
            )

        # フロート分析
        float_info = scanner.get_float_info(ticker)

        # インサイダー買い
        insider_buys = insider.check_insider_buying(ticker)

        # アラートデータ構築 → スコア計算
        alert_data = {
            'ticker': ticker,
            'price': current_price,
            'volume': current_vol,
            'avg_volume': avg_vol_20,
            'volume_ratio': vol_ratio,
            'price_change_pct': price_change_pct,
            'alert_type': 'MANUAL_ANALYSIS',
        }

        if float_info:
            alert_data['float_shares'] = float_info['float_shares']
            alert_data['float_level'] = float_info['float_level']
            alert_data['short_pct'] = float_info.get('short_percent_of_float')
            alert_data['short_ratio'] = float_info.get('short_ratio')

        if insider_buys:
            total_insider = sum(b.get('total_value', 0) for b in insider_buys)
            alert_data['insider_buy_amount'] = total_insider
            if vol_ratio >= 2:
                alert_data['alert_type'] = 'COMBINED'

        scanner.calculate_score(alert_data)
        score = alert_data['score']

        # 判定
        if score >= 70:
            verdict = '上昇の可能性が高い'
            verdict_level = 'HIGH'
            verdict_detail = '複数の強いシグナルが重なっています。注目銘柄です。'
        elif score >= 50:
            verdict = '上昇の兆しあり'
            verdict_level = 'MEDIUM'
            verdict_detail = 'いくつかのシグナルが出ています。ウォッチリストに入れて監視をおすすめします。'
        elif score >= 30:
            verdict = 'やや注目'
            verdict_level = 'LOW'
            verdict_detail = '一部のシグナルが出ていますが、確信度は低めです。'
        else:
            verdict = '現時点では静か'
            verdict_level = 'NONE'
            verdict_detail = '目立ったシグナルはありません。出来高の変化を引き続き監視してください。'

        # シグナル一覧
        signals = []
        if vol_ratio >= 3:
            signals.append({'name': '出来高急増', 'value': f'{vol_ratio}倍', 'strength': 'strong'})
        elif vol_ratio >= 1.5:
            signals.append({'name': '出来高やや増加', 'value': f'{vol_ratio}倍', 'strength': 'weak'})

        if vol_rising:
            signals.append({'name': '出来高じわ上げ', 'value': '3日連続増加中', 'strength': 'medium'})

        if float_info:
            fl = float_info['float_level']
            if fl == 'ULTRA_LOW':
                signals.append({'name': '超低フロート', 'value': f"{float_info['float_shares']/1e6:.1f}M株", 'strength': 'strong'})
            elif fl == 'LOW':
                signals.append({'name': '低フロート', 'value': f"{float_info['float_shares']/1e6:.1f}M株", 'strength': 'medium'})

        if insider_buys:
            for b in insider_buys[:3]:
                signals.append({
                    'name': 'インサイダー買い',
                    'value': f"{b['insider_name']} ${b['total_value']:,.0f}",
                    'strength': 'strong' if b['total_value'] >= 50000 else 'medium',
                })

        short_pct = alert_data.get('short_pct', 0) or 0
        if short_pct >= 0.20:
            signals.append({'name': '高空売り比率', 'value': f'{short_pct*100:.1f}%', 'strength': 'strong'})
        elif short_pct >= 0.10:
            signals.append({'name': '空売り比率', 'value': f'{short_pct*100:.1f}%', 'strength': 'weak'})

        if abs(price_change_pct) >= 10:
            signals.append({'name': '本日の値動き', 'value': f'{price_change_pct:+.1f}%', 'strength': 'medium'})

        return jsonify({
            'ticker': ticker,
            'name': info.get('shortName', ''),
            'price': current_price,
            'price_change_pct': price_change_pct,
            'volume': current_vol,
            'avg_volume': avg_vol_20,
            'volume_ratio': vol_ratio,
            'vol_rising': vol_rising,
            'vol_trend': vol_trend,
            'score': score,
            'score_detail': alert_data.get('score_detail', ''),
            'verdict': verdict,
            'verdict_level': verdict_level,
            'verdict_detail': verdict_detail,
            'signals': signals,
            'float_info': float_info,
            'insider_buys': insider_buys,
        })
    except Exception as e:
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500


@app.route('/api/tracking/stats')
def api_tracking_stats():
    """結果追跡の統計API"""
    stats = tracker.get_pattern_analysis()
    return jsonify(stats)


@app.route('/api/tracking/list')
def api_tracking_list():
    """個別銘柄の追跡結果一覧API"""
    import sqlite3
    conn = db._get_conn()
    try:
        conn.row_factory = sqlite3.Row
        filter_type = request.args.get('filter', 'all')  # all, pending, win, lose, top
        sort = request.args.get('sort', 'date')  # date, gain
        limit = request.args.get('limit', 100, type=int)
        date_from = request.args.get('date_from', '')  # YYYY-MM-DD
        date_to = request.args.get('date_to', '')      # YYYY-MM-DD

        conditions = []
        params = []
        if filter_type == 'pending':
            conditions.append("r.result = 'PENDING'")
        elif filter_type == 'win':
            conditions.append("r.result IN ('BIG_WIN', 'WIN', 'SMALL_WIN')")
        elif filter_type == 'lose':
            conditions.append("r.result = 'LOSS'")
        elif filter_type == 'top':
            conditions.append("r.max_gain_pct IS NOT NULL")

        if date_from:
            conditions.append("substr(r.created_at, 1, 10) >= ?")
            params.append(date_from)
        if date_to:
            conditions.append("substr(r.created_at, 1, 10) <= ?")
            params.append(date_to)

        where = ("WHERE " + " AND ".join(conditions)) if conditions else ""

        order = "r.created_at DESC"
        if sort == 'gain':
            order = "COALESCE(r.max_gain_pct, -999) DESC"

        params.append(limit)
        rows = conn.execute(f"""
            SELECT r.*, a.company_name, a.score_detail as alert_detail,
                   a.price_change_pct as alert_price_change,
                   a.float_shares, a.industry
            FROM alert_results r
            LEFT JOIN stock_alerts a ON r.alert_id = a.id
            {where}
            ORDER BY {order}
            LIMIT ?
        """, params).fetchall()
        return jsonify([dict(r) for r in rows])
    except Exception as e:
        return jsonify({'error': str(e)}), 500
    finally:
        conn.close()


@app.errorhandler(500)
def handle_500(e):
    logger.error(f"Internal error: {e}")
    return jsonify({'error': 'Internal server error', 'detail': str(e)}), 500


@app.errorhandler(404)
def handle_404(e):
    return jsonify({'error': 'Not found'}), 404


def _match_iron_patterns(alert):
    """アラートが鉄板パターンに該当するかチェック（Python側マッチ）

    Returns: list of matched pattern names (best win_rate first),
             best win_rate among matched patterns
    """
    price = alert.get('price', 0) or 0
    vol = alert.get('volume_ratio', 0) or 0
    score = alert.get('score', 0) or 0
    atype = alert.get('alert_type', '') or ''

    # 鉄板パターン条件をPythonで評価（SQL条件と同等）
    # (名前, 条件lambda)
    checks = [
        ('$0.30未満 + 出来高5倍以上', price < 0.3 and price > 0 and vol >= 5),
        ('出来高50倍以上', vol >= 50),
        ('出来高100倍以上', vol >= 100),
        ('$0.50未満 + 出来高10倍以上', price < 0.5 and price > 0 and vol >= 10),
        ('$1未満 + 出来高10倍以上', price < 1.0 and vol >= 10),
        ('$1未満 + スコア30以上', price < 1.0 and price > 0 and score >= 30),
        ('$1未満 + ACCUMULATION', price < 1.0 and atype == 'ACCUMULATION'),
        ('$1未満 + 出来高20倍以上', price < 1.0 and vol >= 20),
        ('$0.50未満 + 出来高5倍以上', price < 0.5 and price > 0 and vol >= 5),
        ('$1未満 + VOLUME_RISING', price < 1.0 and atype == 'VOLUME_RISING'),
        ('出来高10倍以上 + VOL_REVERSAL', vol >= 10 and atype == 'VOL_REVERSAL'),
        ('出来高20倍以上', vol >= 20),
        ('$0.30未満 + 出来高3倍以上', price < 0.3 and price > 0 and vol >= 3),
        ('$0.50未満 + スコア25以上', price < 0.5 and price > 0 and score >= 25),
        ('$1未満 + 出来高5倍以上', price < 1.0 and vol >= 5),
        ('スコア30以上', score >= 30),
        ('出来高10倍以上', vol >= 10),
        ('$0.30未満', price < 0.3 and price > 0),
        ('$0.50未満', price < 0.5 and price > 0),
        ('$1未満', price < 1.0 and price > 0),
        ('出来高5倍以上', vol >= 5),
        ('$1未満 + VOL_REVERSAL', price < 1.0 and atype == 'VOL_REVERSAL'),
        ('$1未満 + PRICE_SPIKE', price < 1.0 and atype == 'PRICE_SPIKE'),
        ('$1未満 + SOCIAL_VOLUME', price < 1.0 and atype in ('SOCIAL_VOLUME',)),
        ('出来高20倍以上 + ACCUMULATION', vol >= 20 and atype == 'ACCUMULATION'),
    ]
    matched = [name for name, cond in checks if cond]
    return matched


# 鉄板パターンの勝率キャッシュ（起動時にDBから計算、定期更新）
_iron_wr_cache = {}
_iron_wr_cache_ts = 0


def _get_iron_win_rates():
    """鉄板パターンごとの勝率を取得（5分キャッシュ）"""
    global _iron_wr_cache, _iron_wr_cache_ts
    import time as _t
    if _iron_wr_cache and (_t.time() - _iron_wr_cache_ts) < 300:
        return _iron_wr_cache

    conn = db._get_conn()
    try:
        wr_map = {}
        for label, where in IRON_PATTERN_QUERIES:
            r = conn.execute(f"""
                SELECT COUNT(*) as n,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins
                FROM alert_results WHERE result IN ('WIN','LOSS') AND {where}
            """).fetchone()
            if r[0] >= 3:
                wr_map[label] = round(r[1] / r[0] * 100, 1)
        _iron_wr_cache = wr_map
        _iron_wr_cache_ts = _t.time()
        return wr_map
    except Exception:
        return _iron_wr_cache
    finally:
        conn.close()


# 要注意セクター（実績で勝率が極端に低い）
CAUTION_SECTORS = {
    'Financial Services': '金融',
    'Finance': '金融',
    'Communication Services': '通信',
}

# リピート検知キャッシュ（ticker -> 過去の検知回数）
_repeat_cache = {}
_repeat_cache_ts = 0


def _get_repeat_counts():
    """銘柄ごとの過去検知回数を取得（5分キャッシュ）"""
    global _repeat_cache, _repeat_cache_ts
    import time as _t
    if _repeat_cache and (_t.time() - _repeat_cache_ts) < 300:
        return _repeat_cache

    conn = db._get_conn()
    try:
        rows = conn.execute("""
            SELECT ticker, COUNT(*) as cnt FROM stock_alerts
            GROUP BY ticker HAVING cnt >= 2
        """).fetchall()
        _repeat_cache = {r[0]: r[1] for r in rows}
        _repeat_cache_ts = _t.time()
        return _repeat_cache
    except Exception:
        return _repeat_cache
    finally:
        conn.close()


def _enrich_alert_with_iron(alert):
    """アラートに鉄板パターン・リピート検知・セクター警告を付与"""
    matched = _match_iron_patterns(alert)
    wr_map = _get_iron_win_rates()

    # 勝率70%以上のパターンのみ残す
    iron_matches = []
    for name in matched:
        wr = wr_map.get(name)
        if wr and wr >= 70:
            iron_matches.append({'name': name, 'win_rate': wr})

    if iron_matches:
        # 勝率の高い順にソート
        iron_matches.sort(key=lambda x: -x['win_rate'])
        alert['iron_patterns'] = iron_matches
        alert['iron_best_wr'] = iron_matches[0]['win_rate']
        alert['iron_best_name'] = iron_matches[0]['name']
        alert['iron_count'] = len(iron_matches)

    # リピート検知
    repeat_counts = _get_repeat_counts()
    ticker = alert.get('ticker', '')
    repeat_n = repeat_counts.get(ticker, 0)
    if repeat_n >= 2:
        alert['repeat_count'] = repeat_n

    # 要注意セクター警告
    sector = alert.get('sector') or ''
    if sector in CAUTION_SECTORS:
        alert['caution_sector'] = CAUTION_SECTORS[sector]

    return alert


@app.route('/api/alerts')
def api_alerts():
    limit = request.args.get('limit', 50, type=int)
    offset = request.args.get('offset', 0, type=int)
    sort = request.args.get('sort', 'time')  # time or recommend
    alerts = db.get_recent_alerts(limit + 1)  # +1 to check hasMore

    # 全アラートに鉄板パターン情報を付与
    for a in alerts:
        _enrich_alert_with_iron(a)

    if sort == 'recommend' and alerts:
        for a in alerts:
            a['invest_score'] = _calc_invest_score(a)
        alerts.sort(key=lambda x: x.get('invest_score', 0), reverse=True)

    return jsonify(alerts)


def _calc_invest_score(alert):
    """投資おすすめ度を計算（311件の実績データから導出）

    実績に基づく成功率:
    - $1未満 + 出来高10倍以上 → 成功率91%
    - $1未満 + 出来高5倍以上 → 成功率86%
    - 出来高20倍以上 → 成功率87%
    - 出来高50倍以上 → 成功率94%
    - $0.50未満 → 成功率75%
    - PRICE_SPIKE(+20%以上) → 成功率44% → 大幅減点
    """
    s = 0
    score = alert.get('score', 0) or 0
    vol = alert.get('volume_ratio', 0) or 0
    fl = alert.get('float_level', '') or ''
    price = alert.get('price', 0) or 0
    pchg = alert.get('price_change_pct', 0) or 0
    atype = alert.get('alert_type', '')

    # === 出来高（最も重要、最大35点）===
    if vol >= 50:
        s += 35  # 成功率94%
    elif vol >= 20:
        s += 30  # 成功率87%
    elif vol >= 10:
        s += 25  # 成功率71%
    elif vol >= 5:
        s += 15
    elif vol >= 3:
        s += 8

    # === 価格帯（2番目に重要、最大25点）===
    if 0 < price < 0.3:
        s += 25  # 超安値は爆発力最大
    elif price < 0.5:
        s += 22  # 成功率75%
    elif price < 1.0:
        s += 18  # 成功率71%
    elif price < 2.0:
        s += 8
    elif price < 3.0:
        s += 3
    # $3以上: 成功率33% → ボーナスなし

    # === フロート（最大15点）===
    if fl == 'ULTRA_LOW':
        s += 15
    elif fl == 'LOW':
        s += 8

    # === タイプボーナス（最大15点）===
    if atype in ('ACCUMULATION', 'SQUEEZE_SETUP'):
        s += 15  # 成功率67-68%
    elif atype == 'VOL_REVERSAL':
        s += 12  # 成功率60%
    elif atype == 'VOLUME_RISING':
        s += 10  # 成功率56%
    elif atype == 'COMBINED':
        s += 15
    elif atype == 'SOCIAL_VOLUME':
        s += 12

    # === スコアボーナス（最大10点）===
    if score >= 40:
        s += 10
    elif score >= 25:
        s += 6

    # === 大幅減点: 天井掴みリスク ===
    # 実績: 検知時0-5%上昇→成功率58%, 20-50%上昇→成功率39%
    if pchg >= 50:
        s -= 40  # 既に+50% → ほぼ天井
    elif pchg >= 30:
        s -= 25  # 既に+30%
    elif pchg >= 20:
        s -= 15  # 既に+20%
    elif pchg >= 10:
        s -= 8   # 既に+10%

    # === ボーナス: 出来高↑なのに価格まだ低い = 初動の可能性 ===
    if vol >= 5 and pchg < 5:
        s += 15  # 出来高急増だが価格はまだ → 仕込み時
    elif vol >= 3 and pchg < 3:
        s += 8   # 出来高増加で価格ほぼ動いていない

    # PRICE_SPIKEタイプは既に高騰後なので減点
    if atype == 'PRICE_SPIKE':
        s -= 10  # 成功率44%と低い

    # 価格0のSNSアラートは情報不足
    if price <= 0:
        s = max(s - 10, s // 2)

    return max(0, min(100, s))


@app.route('/api/reddit/trending')
def api_reddit_trending():
    """Reddit注目銘柄（24時間監視）"""
    data = social.get_trending()
    # dict → リスト形式に変換してbuzz_score順でソート
    tickers = []
    for ticker, info in data.get('tickers', {}).items():
        item = {'ticker': ticker}
        item.update(info)
        tickers.append(item)
    tickers.sort(key=lambda x: -x.get('buzz_score', 0))
    return jsonify({
        'tickers': tickers,
        'updated_at': data.get('updated_at'),
        'count': data.get('count', 0),
    })


@app.route('/api/alerts/<ticker>')
def api_alerts_ticker(ticker):
    alerts = db.get_alerts_by_ticker(ticker.upper())
    return jsonify(alerts)


@app.route('/api/stats')
def api_stats():
    stats = db.get_stats()
    stats['scan_status'] = scan_status
    return jsonify(stats)


@app.route('/api/scan', methods=['POST'])
def api_scan():
    """手動スキャン実行"""
    with _scan_lock:
        if scan_status['running']:
            return jsonify({'error': 'Scan already running'}), 409
        scan_status['running'] = True
    threading.Thread(target=run_scan, daemon=True).start()
    return jsonify({'status': 'started'})


@app.route('/api/watchlist', methods=['GET'])
def api_watchlist_get():
    watched = db.get_watched()
    # 価格・出来高情報を付加
    if watched:
        tickers = [w['ticker'] for w in watched]
        try:
            import yfinance as yf
            data = yf.download(tickers, period='2d', progress=False,
                               threads=True, ignore_tz=True,
                               prepost=True)
            if not data.empty:
                close = data.get('Close')
                volume = data.get('Volume')
                if close is not None:
                    if isinstance(close, pd.Series):
                        close = close.to_frame()
                        volume = volume.to_frame() if volume is not None else None
                    for w in watched:
                        t = w['ticker']
                        try:
                            if t in close.columns:
                                prices = close[t].dropna()
                                if len(prices) >= 1:
                                    w['price'] = round(float(prices.iloc[-1]), 2)
                                if len(prices) >= 2:
                                    prev = float(prices.iloc[-2])
                                    if prev > 0:
                                        w['change_pct'] = round(
                                            (w['price'] - prev) / prev * 100, 2)
                            if volume is not None and t in volume.columns:
                                vols = volume[t].dropna()
                                if len(vols) >= 1:
                                    w['volume'] = int(vols.iloc[-1])
                        except Exception:
                            pass
        except Exception as e:
            print(f"[Watchlist] Price fetch error: {e}")
    return jsonify(watched)


@app.route('/api/watchlist', methods=['POST'])
def api_watchlist_add():
    data = request.get_json()
    ticker = data.get('ticker', '').upper().strip()
    if not ticker:
        return jsonify({'error': 'ticker required'}), 400
    db.add_watched(ticker, data.get('notes'))
    return jsonify({'status': 'added', 'ticker': ticker})


@app.route('/api/watchlist/<ticker>', methods=['DELETE'])
def api_watchlist_remove(ticker):
    db.remove_watched(ticker.upper())
    return jsonify({'status': 'removed'})


@app.route('/api/alerts/filtered')
def api_alerts_filtered():
    """ユーザーフィルタ付きアラート取得API"""
    min_score = request.args.get('min_score', 0, type=int)
    alert_type = request.args.get('type', '')
    limit = request.args.get('limit', 50, type=int)

    import sqlite3 as _sq
    conn = db._get_conn()
    try:
        conn.row_factory = _sq.Row
        where_clauses = ['1=1']
        params = []
        if min_score > 0:
            where_clauses.append('score >= ?')
            params.append(min_score)
        if alert_type:
            where_clauses.append('alert_type = ?')
            params.append(alert_type)

        where = ' AND '.join(where_clauses)
        params.append(limit)
        rows = conn.execute(f"""
            SELECT * FROM stock_alerts
            WHERE {where}
            ORDER BY timestamp DESC LIMIT ?
        """, params).fetchall()

        # 信頼度減衰: 古いアラートのスコアを時間経過で減衰
        results = []
        now = datetime.now()
        for r in rows:
            alert = dict(r)
            try:
                alert_time = datetime.strptime(alert['timestamp'][:19], '%Y-%m-%d %H:%M:%S')
                hours_elapsed = (now - alert_time).total_seconds() / 3600
                # 24時間で10%減衰、48時間で20%減衰
                decay = max(0.5, 1.0 - (hours_elapsed / 240))
                alert['decayed_score'] = round(alert.get('score', 0) * decay, 1)
                alert['hours_elapsed'] = round(hours_elapsed, 1)
            except Exception:
                alert['decayed_score'] = alert.get('score', 0)
                alert['hours_elapsed'] = 0
            results.append(alert)

        return jsonify(results)
    finally:
        conn.close()


@app.route('/api/economic')
def api_economic():
    """経済指標の現在値を取得"""
    data = econ_monitor.get_all_current()
    return jsonify(data)


@app.route('/api/economic/accuracy')
def api_economic_accuracy():
    """経済指標予測の精度統計"""
    return jsonify(econ_monitor.get_prediction_accuracy())


# 鉄板パターン定義（一元管理）
# (表示名, alert_results用WHERE, alert_results+JOIN用WHERE)
# alert_results用はカラム名そのまま、JOIN用は r. プレフィックス付き
IRON_PATTERN_QUERIES = [
    # === 超鉄板（勝率90%+） ===
    ('$0.30未満 + 出来高5倍以上',
     'entry_price < 0.3 AND entry_price > 0 AND volume_ratio >= 5'),
    ('出来高50倍以上',
     'volume_ratio >= 50'),
    ('出来高100倍以上',
     'volume_ratio >= 100'),
    ('$0.50未満 + 出来高10倍以上',
     'entry_price < 0.5 AND entry_price > 0 AND volume_ratio >= 10'),
    ('$1未満 + 出来高10倍以上',
     'entry_price < 1.0 AND volume_ratio >= 10'),
    ('$1未満 + スコア30以上',
     'entry_price < 1.0 AND entry_price > 0 AND score >= 30'),
    ('$1未満 + ACCUMULATION',
     "entry_price < 1.0 AND alert_type = 'ACCUMULATION'"),
    # === 鉄板（勝率80%+） ===
    ('$1未満 + 出来高20倍以上',
     'entry_price < 1.0 AND volume_ratio >= 20'),
    ('$0.50未満 + 出来高5倍以上',
     'entry_price < 0.5 AND entry_price > 0 AND volume_ratio >= 5'),
    ('$1未満 + VOLUME_RISING',
     "entry_price < 1.0 AND alert_type = 'VOLUME_RISING'"),
    ('出来高10倍以上 + VOL_REVERSAL',
     "volume_ratio >= 10 AND alert_type = 'VOL_REVERSAL'"),
    ('出来高20倍以上',
     'volume_ratio >= 20'),
    ('$0.30未満 + 出来高3倍以上',
     'entry_price < 0.3 AND entry_price > 0 AND volume_ratio >= 3'),
    ('$0.50未満 + スコア25以上',
     'entry_price < 0.5 AND entry_price > 0 AND score >= 25'),
    # === 堅実（勝率70%+） ===
    ('$1未満 + 出来高5倍以上',
     'entry_price < 1.0 AND volume_ratio >= 5'),
    ('スコア30以上',
     'score >= 30'),
    ('出来高10倍以上',
     'volume_ratio >= 10'),
    ('$0.30未満',
     'entry_price < 0.3 AND entry_price > 0'),
    ('$0.50未満',
     'entry_price < 0.5 AND entry_price > 0'),
    ('$1未満',
     'entry_price < 1.0 AND entry_price > 0'),
    ('出来高5倍以上',
     'volume_ratio >= 5'),
    ('$1未満 + VOL_REVERSAL',
     "entry_price < 1.0 AND alert_type = 'VOL_REVERSAL'"),
    ('$1未満 + PRICE_SPIKE',
     "entry_price < 1.0 AND alert_type = 'PRICE_SPIKE'"),
    ('$1未満 + SOCIAL_VOLUME',
     "entry_price < 1.0 AND alert_type IN ('SOCIAL_VOLUME')"),
    ('出来高20倍以上 + ACCUMULATION',
     "volume_ratio >= 20 AND alert_type = 'ACCUMULATION'"),
]

def _iron_pattern_map():
    """パターン名 → SQL条件(r.プレフィックス付き)のマッピングを生成"""
    m = {}
    for label, where in IRON_PATTERN_QUERIES:
        # alert_results単体のカラム名を r. プレフィックス付きに変換
        r_where = where
        for col in ['entry_price', 'volume_ratio', 'score', 'alert_type', 'float_level']:
            r_where = r_where.replace(col, f'r.{col}')
        m[label] = r_where
    return m


@app.route('/api/iron-patterns')
def api_iron_patterns():
    """鉄板パターン分析API（勝率70%以上を表示）"""
    import sqlite3 as _sq
    conn = db._get_conn()
    try:
        conn.row_factory = _sq.Row
        patterns = []
        for label, where in IRON_PATTERN_QUERIES:
            r = conn.execute(f"""
                SELECT COUNT(*) as n,
                    SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                    ROUND(AVG(max_gain_pct), 1) as avg_gain,
                    ROUND(AVG(change_1d_pct), 1) as avg_1d
                FROM alert_results WHERE result IN ('WIN','LOSS') AND {where}
            """).fetchone()
            if r['n'] >= 3:
                wr = round(r['wins'] / r['n'] * 100, 1)
                if wr >= 70:
                    patterns.append({
                        'name': label,
                        'total': r['n'],
                        'wins': r['wins'],
                        'win_rate': wr,
                        'avg_gain': r['avg_gain'],
                        'avg_1d': r['avg_1d'],
                    })
        patterns.sort(key=lambda x: (-x['win_rate'], -x['total']))

        # 鉄板パターンに該当する直近の銘柄（最も勝率の高い条件: $1未満+出来高5倍）
        iron_tickers = conn.execute("""
            SELECT r.ticker, r.entry_price, r.volume_ratio, r.float_level,
                   r.score, r.result, r.max_gain_pct, r.change_1d_pct,
                   r.alert_type, r.created_at, r.max_price_date,
                   a.company_name
            FROM alert_results r
            LEFT JOIN stock_alerts a ON r.alert_id = a.id
            WHERE r.entry_price < 1.0 AND r.volume_ratio >= 5
              AND r.result != 'REVERSE_SPLIT'
            ORDER BY r.created_at DESC LIMIT 50
        """).fetchall()

        return jsonify({
            'patterns': patterns,
            'iron_tickers': [dict(t) for t in iron_tickers],
        })
    finally:
        conn.close()


@app.route('/api/iron-patterns/tickers')
def api_iron_pattern_tickers():
    """鉄板パターンに該当する銘柄一覧API（パターン条件指定）"""
    import sqlite3 as _sq
    pattern_name = request.args.get('pattern', '')
    pmap = _iron_pattern_map()
    where = pmap.get(pattern_name)
    if not where:
        return jsonify({'error': 'Unknown pattern'}), 400

    conn = db._get_conn()
    try:
        conn.row_factory = _sq.Row
        rows = conn.execute(f"""
            SELECT r.ticker, r.entry_price, r.volume_ratio, r.float_level,
                   r.score, r.result, r.max_gain_pct, r.change_1d_pct,
                   r.alert_type, r.created_at, r.max_price_date,
                   a.company_name
            FROM alert_results r
            LEFT JOIN stock_alerts a ON r.alert_id = a.id
            WHERE {where} AND r.result != 'REVERSE_SPLIT'
            ORDER BY r.created_at DESC LIMIT 50
        """).fetchall()
        return jsonify([dict(r) for r in rows])
    finally:
        conn.close()


@app.route('/api/backtest')
def api_backtest():
    """スコアリング精度のバックテスト結果"""
    result = scanner.backtest_score_accuracy()
    return jsonify(result)


@app.route('/api/ml/train', methods=['POST'])
def api_ml_train():
    """MLモデルを学習"""
    result = ml.train()
    return jsonify(result)


@app.route('/api/ml/stats')
def api_ml_stats():
    """MLモデルの統計"""
    return jsonify(ml.get_stats())


@app.route('/api/ml/top-picks')
def api_ml_top_picks():
    """ML予測でTop N銘柄を厳選"""
    top_n = request.args.get('n', 5, type=int)
    # 直近のアラートを取得
    alerts = db.get_recent_alerts(100)
    # 株式併合を除外
    alerts = [a for a in alerts if not a.get('has_reverse_split')]
    top = ml.get_top_picks(alerts, top_n=top_n)
    return jsonify({
        'model_trained': ml.is_trained,
        'total_alerts': len(alerts),
        'top_picks': top,
        'stats': ml.training_stats if ml.is_trained else None,
    })


@app.route('/api/ml/predict/<ticker>')
def api_ml_predict(ticker):
    """個別銘柄のML予測"""
    ticker = ticker.upper()
    alerts = db.get_alerts_by_ticker(ticker)
    if not alerts:
        return jsonify({'error': f'{ticker} not found'}), 404
    latest = alerts[0]
    pred = ml.predict(latest)
    pred['ticker'] = ticker
    pred['base_score'] = latest.get('score', 0)
    return jsonify(pred)


@app.route('/api/news/send', methods=['POST'])
def api_news_send():
    """ニュースレポートを手動送信"""
    threading.Thread(target=news.send_now, daemon=True).start()
    return jsonify({'status': 'sending'})


@app.route('/api/universe')
def api_universe():
    tickers = db.get_universe()
    return jsonify({'count': len(tickers), 'tickers': tickers[:100]})


# === SocketIO ===
@socketio.on('connect')
def on_connect():
    alerts = db.get_recent_alerts(20)
    socketio.emit('init', {
        'alerts': alerts,
        'scan_status': scan_status,
    })


# === スキャンロジック ===
# Phase 3: 勝ちパターン通知フィルター
# 結果データが十分にあるとき、勝率の低いパターンの通知を抑制する
# (moved to top of file)

def _should_notify_alert(alert: dict) -> bool:
    """このアラートをDiscordに通知すべきか判定

    311件の実績データに基づく通知基準:
    - $1未満 + 出来高10倍以上 → 成功率91% → 必ず通知
    - $1未満 + 出来高5倍以上 → 成功率86% → 必ず通知
    - 出来高20倍以上 → 成功率87% → 必ず通知
    - ACCUMULATION/SQUEEZE_SETUP → 成功率67-68% → 通知
    - PRICE_SPIKE(既に+20%以上) → 成功率44%+翌日-7% → ブロック
    - 株式併合 → ブロック
    """
    alert_type = alert.get('alert_type', '')
    score = alert.get('score', 0)
    price_change = alert.get('price_change_pct', 0) or 0
    vol_ratio = alert.get('volume_ratio', 0) or 0
    float_level = alert.get('float_level', '')
    price = alert.get('price', 0) or 0
    ticker = alert.get('ticker', '?')

    # === 絶対ブロック ===

    # 株式併合
    if alert.get('has_reverse_split'):
        logger.info(f"BLOCK {ticker}: reverse split")
        return False

    # PRICE_SPIKE（既に急騰済み）→ 翌日平均-7%の天井掴み
    if alert_type == 'PRICE_SPIKE' and price_change >= 20:
        logger.info(f"BLOCK {ticker}: PRICE_SPIKE +{price_change:.0f}% (ceiling risk)")
        return False

    # 既に+50%以上急騰済み
    if price_change >= 50:
        logger.info(f"BLOCK {ticker}: already +{price_change:.0f}%")
        return False

    # === 必ず通知（実績で成功率が高いパターン） ===

    # $1未満 + 出来高10倍以上 → 成功率91%
    if price > 0 and price < 1.0 and vol_ratio >= 10:
        logger.info(f"NOTIFY {ticker}: price<$1 + vol{vol_ratio:.0f}x (91% win rate)")
        return True

    # $1未満 + 出来高5倍以上 → 成功率86%
    if price > 0 and price < 1.0 and vol_ratio >= 5:
        logger.info(f"NOTIFY {ticker}: price<$1 + vol{vol_ratio:.0f}x (86% win rate)")
        return True

    # 出来高20倍以上 → 成功率87%
    if vol_ratio >= 20:
        logger.info(f"NOTIFY {ticker}: vol{vol_ratio:.0f}x (87% win rate)")
        return True

    # 複合シグナル/Reddit+出来高 → 成功率100%
    if alert_type in ('COMBINED', 'SOCIAL_VOLUME'):
        return True

    # 買い集め/ブレイク前兆/底打ち反転 → 成功率60-68%
    if alert_type in ('ACCUMULATION', 'SQUEEZE_SETUP', 'VOL_REVERSAL'):
        return True

    # 出来高じわ上げ → 成功率56%、翌日+9.3%
    if alert_type == 'VOLUME_RISING':
        return True

    # === 条件付き通知 ===

    # 出来高10倍以上 → 成功率71%
    if vol_ratio >= 10:
        return True

    # 超低フロート + 出来高5倍以上 → 成功率68%
    if float_level == 'ULTRA_LOW' and vol_ratio >= 5:
        return True

    # スコア25点以上 → 成功率74%
    if score >= 25:
        return True

    # === それ以外はデータ蓄積のみ（通知しない） ===
    logger.debug(f"BLOCK {ticker}: score={score} vol={vol_ratio:.1f}x price=${price:.2f}")
    return False


def run_scan():
    """メインスキャン処理"""
    # running=True は api_scan 側の _scan_lock 内でセット済み
    print("[Scan] Starting scan...")

    try:
        # 1. 銘柄ユニバース取得 + ウォッチリスト銘柄を統合
        universe = scanner.get_universe()

        if not universe:
            print("[Scan] No tickers in universe, building...")
            universe = scanner.get_universe(force_refresh=True)

        # ウォッチリスト銘柄をユニバースに追加（重複除去）
        watched_tickers = [w['ticker'] for w in db.get_watched()]
        universe_set = set(universe)
        for wt in watched_tickers:
            if wt not in universe_set:
                universe.append(wt)
                universe_set.add(wt)

        scan_status['universe_size'] = len(universe)
        if watched_tickers:
            print(f"[Scan] Universe: {len(universe)} tickers "
                  f"(+{len(watched_tickers)} from watchlist)")

        # メモリ節約: $1以下を優先スキャン、$1-$5は別バッチ
        # DBから価格情報を取得して分類
        import gc
        priority_tickers = []  # $1以下（鉄板パターン対象）
        secondary_tickers = []  # $1-$5
        try:
            conn = db._get_conn()
            price_rows = conn.execute("""
                SELECT ticker, price FROM stock_alerts
                WHERE id IN (SELECT MAX(id) FROM stock_alerts GROUP BY ticker)
            """).fetchall()
            price_map = {r[0]: r[1] for r in price_rows}
            conn.close()
            for t in universe:
                p = price_map.get(t, 0)
                if p > 0 and p >= 1.0:
                    secondary_tickers.append(t)
                else:
                    priority_tickers.append(t)
        except Exception:
            priority_tickers = universe
            secondary_tickers = []

        print(f"[Scan] Priority(<$1): {len(priority_tickers)}, Secondary($1-5): {len(secondary_tickers)}")

        # 2. プレマーケット/アフターマーケット検出
        now_et = datetime.now(EST)
        is_premarket = now_et.hour < 10
        is_afterhours = now_et.hour >= 16

        if is_premarket or is_afterhours:
            session = 'PREMARKET' if is_premarket else 'AFTERHOURS'
            try:
                # Polygon APIでプレマーケットスキャン
                if is_premarket:
                    premarket_alerts = scanner.scan_premarket(universe[:200])
                else:
                    premarket_alerts = []

                if premarket_alerts:
                    logger.info(f"{session} alerts (Polygon): {len(premarket_alerts)}")
                    for pa in premarket_alerts:
                        pa['timestamp'] = datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')
                        scanner.calculate_score(pa)
                        alert_id = db.save_alert(pa)
                        if alert_id > 0:
                            db.create_tracking(alert_id, pa)
                            if _should_notify_alert(pa):
                                discord.notify_volume_spike(pa)
                            socketio.emit('new_alert', pa)
            except Exception as e:
                logger.error(f"{session} scan error: {e}")

        # 3. 出来高スパイク + 価格急騰検出
        # メモリ節約: $1以下を先にスキャン → メモリ解放 → $1以上をスキャン
        volume_alerts = scanner.scan_volume_spikes(priority_tickers)
        gc.collect()
        if secondary_tickers:
            volume_alerts_2 = scanner.scan_volume_spikes(secondary_tickers)
            volume_alerts.extend(volume_alerts_2)
            del volume_alerts_2
            gc.collect()

        # 4. フロート分析（アラートが出た銘柄のみ、効率化）
        if volume_alerts:
            print(f"[Scan] Enriching {len(volume_alerts)} alerts with float data...")
            volume_alerts = scanner.enrich_alerts_with_float(volume_alerts)

        # 4a. 偽シグナルフィルタ + マルチタイムフレーム確認
        if volume_alerts:
            print(f"[Scan] Running catalyst filter & multi-TF check...")
            for alert in volume_alerts:
                ticker = alert['ticker']
                # 偽シグナルチェック（決算・増資・スプリット）
                try:
                    catalyst = scanner.check_catalyst_filter(ticker)
                    if catalyst['should_filter']:
                        alert['catalyst_filtered'] = True
                        alert['catalyst_filter_reason'] = catalyst['filter_reason']
                        # 株式併合フラグを明示的にセット
                        if catalyst.get('has_reverse_split'):
                            alert['has_reverse_split'] = True
                            alert['reverse_split_ratio'] = catalyst.get('reverse_split_ratio', '')
                            logger.warning(
                                f"{ticker}: REVERSE SPLIT detected "
                                f"({catalyst.get('reverse_split_ratio', '?')}) - "
                                f"score will be 0")
                        else:
                            logger.info(f"{ticker}: {catalyst['filter_reason']}")
                except Exception as e:
                    logger.debug(f"{ticker}: catalyst check error: {e}")

                # マルチタイムフレーム出来高確認
                try:
                    mtf = scanner.check_multi_timeframe_volume(ticker)
                    if mtf['confidence_boost'] > 0:
                        alert['mtf_confidence_boost'] = mtf['confidence_boost']
                        alert['mtf_detail'] = mtf['detail']
                        print(f"[Scan] ✓ {ticker}: MTF確認 +{mtf['confidence_boost']}点 ({mtf['detail'].strip()})")
                except Exception:
                    pass

                # スコア再計算（新要素を反映）
                scanner.calculate_score(alert)
                time.sleep(0.3)

        # 5. 出来高アラートのある銘柄でインサイダーチェック
        volume_tickers = list(set(a['ticker'] for a in volume_alerts))
        insider_buys = []
        if volume_tickers:
            insider_buys = insider.scan_tickers(volume_tickers)

        # ウォッチリストのインサイダーもチェック
        watched = [w['ticker'] for w in db.get_watched()]
        if watched:
            watched_insiders = insider.scan_tickers(watched)
            insider_buys.extend(watched_insiders)

        # 4. アラート分類・保存・通知（タイムスタンプはJST）
        now = datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')
        combined_count = 0
        insider_tickers = set(b['ticker'] for b in insider_buys)

        # 出来高アラート（インサイダー/Reddit情報があればスコア再計算）
        for alert in volume_alerts:
            alert['timestamp'] = now
            matching_insider = None
            if alert['ticker'] in insider_tickers:
                alert['alert_type'] = 'COMBINED'
                combined_count += 1
                matching_insider = next(
                    (b for b in insider_buys if b['ticker'] == alert['ticker']), {})
                alert['insider_name'] = matching_insider.get('insider_name')
                alert['insider_buy_amount'] = matching_insider.get('total_value')
                # インサイダー情報を含めてスコア再計算
                scanner.calculate_score(alert, insider_data=matching_insider)

            # Reddit24h監視: 出来高アラートがReddit注目銘柄と一致 → SOCIAL_VOLUME
            reddit_info = social.is_trending(alert['ticker'])
            if reddit_info and alert['alert_type'] not in ('COMBINED',):
                alert['alert_type'] = 'SOCIAL_VOLUME'
                alert['sentiment'] = reddit_info.get('sentiment', 'NEUTRAL')
                alert['buzz_score'] = reddit_info.get('buzz_score', 0)
                alert['mention_count'] = reddit_info.get('mention_count', 0)
                sent_label = {'BULLISH': '強気', 'BEARISH': '弱気', 'NEUTRAL': '中立'}.get(
                    reddit_info.get('sentiment', ''), '')
                alert['detail'] = (
                    f"Reddit注目(buzz:{reddit_info.get('buzz_score', 0)} [{sent_label}]) "
                    f"+ 出来高{alert.get('volume_ratio', 0):.1f}x "
                    f"| {reddit_info.get('mention_count', 0)}回言及"
                )
                scanner.calculate_score(alert)
                print(f"[Social24h] SOCIAL_VOLUME: {alert['ticker']} "
                      f"(Reddit buzz:{reddit_info.get('buzz_score', 0)} + vol:{alert.get('volume_ratio', 0):.1f}x)")

            alert_id = db.save_alert(alert)
            if alert_id > 0:
                db.create_tracking(alert_id, alert)
                socketio.emit('new_alert', alert)

                # 鉄板パターン情報を付与してから通知判定
                _enrich_alert_with_iron(alert)

                # Phase 3: 勝ちパターンのみ通知
                # 結果データが十分にあれば、勝率の高いパターンだけ通知する
                should_notify = _should_notify_alert(alert)
                if should_notify:
                    if alert['alert_type'] == 'COMBINED':
                        discord.notify_combined(alert['ticker'], alert, matching_insider or {})
                    else:
                        discord.notify_volume_spike(alert)
                    db.mark_notified(alert_id)

        # インサイダーのみ（出来高と重複しないもの）
        for buy in insider_buys:
            if buy['ticker'] not in volume_tickers:
                insider_alert = {
                    'timestamp': now,
                    'ticker': buy['ticker'],
                    'price': buy.get('price_per_share', 0),
                    'alert_type': 'INSIDER_BUY',
                    'insider_name': buy.get('insider_name'),
                    'insider_title': buy.get('insider_title'),
                    'insider_shares': buy.get('shares'),
                    'insider_buy_amount': buy.get('total_value'),
                }
                scanner.calculate_score(insider_alert)
                alert_id = db.save_alert(insider_alert)
                if alert_id > 0:
                    db.create_tracking(alert_id, insider_alert)
                    socketio.emit('new_alert', insider_alert)

                    if _should_notify_alert(insider_alert):
                        discord.notify_insider_buy(buy)
                        db.mark_notified(alert_id)

        # 5a. 出来高蓄積パターン検出（急騰の前兆）
        try:
            accum_alerts = scanner.scan_accumulation(universe)
            for alert in accum_alerts:
                alert['timestamp'] = now
                alert_id = db.save_alert(alert)
                if alert_id > 0:
                    db.create_tracking(alert_id, alert)
                    socketio.emit('new_alert', alert)
                    if _should_notify_alert(alert):
                        discord.notify_volume_spike(alert)
                        db.mark_notified(alert_id)
            if accum_alerts:
                print(f"[Scan] Accumulation patterns: {len(accum_alerts)}")
        except Exception as e:
            print(f"[Scan] Accumulation scan error: {e}")

        # 5b. SNS話題検知（Reddit/StockTwits）
        try:
            social_alerts = social.scan_all(penny_tickers=universe)
            social_notified = 0
            for sa in social_alerts:
                if sa.get('buzz_score', 0) < 40:
                    continue
                # SNSで話題 + 出来高アラートがある銘柄は最高優先度
                matching_vol = next((a for a in volume_alerts if a['ticker'] == sa['ticker']), None)
                # センチメント情報
                sentiment = sa.get('sentiment', 'NEUTRAL')
                sent_label = {'BULLISH': '強気', 'BEARISH': '弱気', 'NEUTRAL': '中立'}.get(sentiment, '')
                sent_info = f" [{sent_label}]" if sent_label else ''

                if matching_vol:
                    sa['alert_type'] = 'SOCIAL_VOLUME'
                    sa['price'] = matching_vol.get('price', 0)
                    sa['volume'] = matching_vol.get('volume', 0)
                    sa['volume_ratio'] = matching_vol.get('volume_ratio', 0)
                    sa['float_shares'] = matching_vol.get('float_shares')
                    sa['float_level'] = matching_vol.get('float_level')
                    sa['detail'] = (f"SNS話題(buzz:{sa['buzz_score']}{sent_info}) + 出来高{matching_vol.get('volume_ratio',0):.1f}x "
                                    f"| Reddit {sa.get('mention_count',0)}回言及")
                    scanner.calculate_score(sa)
                else:
                    sa['alert_type'] = 'SOCIAL_BUZZ'
                    sa['price'] = 0
                    sa['volume'] = 0
                    sa['volume_ratio'] = 0
                    sa['detail'] = (f"SNS話題(buzz:{sa['buzz_score']}{sent_info}) "
                                    f"| Reddit {sa.get('mention_count',0)}回言及 "
                                    f"| {', '.join(sa.get('subreddits', []))}"
                                    f"{'| DD投稿あり' if sa.get('has_dd') else ''}")
                    sa['score'] = sa.get('buzz_score', 0)
                    sa['score_detail'] = f"SNS buzz: {sa['score']}"

                # ベアリッシュなSNSアラートは通知抑制
                if sentiment == 'BEARISH':
                    sa['score'] = max(sa['score'] - 15, 0)

                sa['timestamp'] = now
                alert_id = db.save_alert(sa)
                if alert_id > 0:
                    db.create_tracking(alert_id, sa)
                    socketio.emit('new_alert', sa)
                    if sa.get('buzz_score', 0) >= 60 or sa.get('alert_type') == 'SOCIAL_VOLUME':
                        discord.notify_volume_spike(sa)
                        db.mark_notified(alert_id)
                        social_notified += 1
            if social_alerts:
                print(f"[Scan] Social alerts: {len(social_alerts)} detected, {social_notified} notified")
        except Exception as e:
            print(f"[Scan] Social scan error: {e}")

        # 5c. 結果追跡の更新（過去のアラートの値動きチェック）
        try:
            tracker.track_pending()
        except Exception as e:
            print(f"[Tracker] Error: {e}")

        # 6. スコア自動調整チェック（マイルストーン到達時のみ実行）
        try:
            adjust_result = scanner.auto_adjust_score_weights()
            if adjust_result.get('adjusted'):
                print(f"[ScoreAdjust] Milestone {adjust_result['milestone']} reached, "
                      f"weights updated ({adjust_result['total_results']} results)")
        except Exception as e:
            print(f"[ScoreAdjust] Error: {e}")

        # 7. 結果サマリー
        scan_status['last_scan'] = now
        scan_status['last_results'] = {
            'volume': len(volume_alerts),
            'insider': len(insider_buys),
            'combined': combined_count,
        }

        print(f"[Scan] Complete: {len(volume_alerts)} volume, "
              f"{len(insider_buys)} insider, {combined_count} combined")

        # スキャン完了通知（鉄板パターン該当銘柄を含む）
        iron_alerts = []
        for alert in volume_alerts:
            enriched = _enrich_alert_with_iron(alert)
            if enriched.get('iron_patterns'):
                iron_alerts.append(enriched)
        discord.notify_scan_complete(
            len(volume_alerts), len(insider_buys), combined_count,
            len(universe), iron_alerts=iron_alerts)

        # 9. 日次レポート（JST 21:00に1回だけ送信）
        try:
            now_jst = datetime.now(JST)
            if now_jst.hour == 21 and not getattr(background_scanner, '_daily_sent', '') == now_jst.strftime('%Y-%m-%d'):
                background_scanner._daily_sent = now_jst.strftime('%Y-%m-%d')
                _send_daily_report()
        except Exception as e:
            logger.error(f"Daily report error: {e}")

        # 8. ML Top Picks 通知（モデル学習済みの場合）
        try:
            if not ml.is_trained:
                train_result = ml.train()
                if train_result.get('trained'):
                    logger.info("ML model trained automatically")

            if ml.is_trained and volume_alerts:
                top_picks = ml.get_top_picks(volume_alerts, top_n=5)
                if top_picks:
                    discord.notify_top_picks(top_picks)
        except Exception as e:
            logger.error(f"ML top picks error: {e}")

        # スキャン成功 → 連続失敗カウントリセット
        scan_status['consecutive_failures'] = 0

    except Exception as e:
        scan_status['consecutive_failures'] = scan_status.get('consecutive_failures', 0) + 1
        failures = scan_status['consecutive_failures']
        logger.error(f"Scan error (consecutive: {failures}): {e}")
        traceback.print_exc()

        # 3回連続失敗でDiscord通知
        if failures >= 3 and failures % 3 == 0:
            try:
                discord._send({'embeds': [{
                    'title': '⚠️ スキャンエラー',
                    'description': f'スキャンが{failures}回連続で失敗しています\n```{str(e)[:200]}```',
                    'color': 0xFF0000,
                }]})
            except Exception:
                pass
    finally:
        scan_status['running'] = False


def _send_daily_report():
    """日次結果レポートをDiscordに送信"""
    import sqlite3 as _sq
    conn = db._get_conn()
    try:
        conn.row_factory = _sq.Row
        today = datetime.now(JST).strftime('%Y-%m-%d')

        # 累計成績
        overall = conn.execute("""
            SELECT
                SUM(CASE WHEN result='WIN' THEN 1 ELSE 0 END) as wins,
                SUM(CASE WHEN result='LOSS' THEN 1 ELSE 0 END) as losses,
                ROUND(AVG(max_gain_pct), 1) as avg_max_gain
            FROM alert_results WHERE result IN ('WIN','LOSS')
        """).fetchone()

        wins = overall['wins'] or 0
        losses = overall['losses'] or 0
        total = wins + losses

        # 本日の検知数
        today_alerts = conn.execute(
            "SELECT COUNT(*) FROM stock_alerts WHERE date(timestamp) = date(?)",
            (today,)).fetchone()[0]

        # 本日の成功確定数
        today_wins = conn.execute("""
            SELECT COUNT(*) FROM alert_results
            WHERE result='WIN' AND date(tracked_at) = date(?)
        """, (today,)).fetchone()[0]

        # 本日のベスト銘柄
        best = conn.execute("""
            SELECT ticker, max_gain_pct FROM alert_results
            WHERE result='WIN' AND date(tracked_at) = date(?)
            ORDER BY max_gain_pct DESC LIMIT 1
        """, (today,)).fetchone()

        stats = {
            'total_confirmed': total,
            'wins': wins,
            'losses': losses,
            'win_rate': round(wins / total * 100, 1) if total > 0 else 0,
            'avg_max_gain': overall['avg_max_gain'] or 0,
            'today_alerts': today_alerts,
            'today_wins': today_wins,
            'best_ticker': best['ticker'] if best else '',
            'best_gain': best['max_gain_pct'] if best else 0,
        }
        discord.notify_daily_report(stats)
        logger.info(f"Daily report sent: {stats['win_rate']}% win rate")
    finally:
        conn.close()


def background_scanner():
    """バックグラウンドスキャンループ（時間外取引対応版）"""
    time.sleep(10)  # 起動待ち
    while True:
        try:
            now_et = datetime.now(EST)
            now_jst = datetime.now(JST)
            hour_et = now_et.hour
            weekday = now_et.weekday()  # 0=月, 6=日

            # 平日のみ（土日はスキップ）
            is_weekday = weekday < 5

            # プレマーケット（ET 4:00-9:30）〜 アフターマーケット（〜20:00）
            is_extended_hours = 4 <= hour_et <= 20

            if is_weekday and is_extended_hours:
                if hour_et < 9 or (hour_et == 9 and now_et.minute < 30):
                    session = 'プレマーケット'
                elif hour_et < 16:
                    session = '通常取引'
                else:
                    session = 'アフターマーケット'

                scan_status['current_session'] = session
                logger.info(f"スキャン開始 ({session}, "
                            f"JST {now_jst.strftime('%H:%M')} / ET {now_et.strftime('%H:%M')})")

                # スレッドセーフにrunningフラグをセット
                with _scan_lock:
                    if scan_status['running']:
                        logger.warning("前回のスキャンがまだ実行中です。スキップします。")
                    else:
                        scan_status['running'] = True
                        run_scan()
            else:
                logger.info(f"市場時間外 "
                            f"(JST {now_jst.strftime('%H:%M')} / ET {now_et.strftime('%H:%M')})")

        except Exception as e:
            logger.error(f"Background error: {e}")

        # 連続失敗時はバックオフ（通常30分→失敗回数に応じて延長、最大2時間）
        failures = scan_status.get('consecutive_failures', 0)
        if failures >= 5:
            backoff = min(7200, Config.SCAN_INTERVAL_SEC * (1 + failures // 5))
            logger.warning(f"連続{failures}回失敗中。次回スキャンまで{backoff//60}分待機")
            time.sleep(backoff)
        else:
            # プレマーケット時はスキャン頻度を上げる（15分間隔）
            now_et = datetime.now(EST)
            if 4 <= now_et.hour < 9:
                time.sleep(900)
            else:
                time.sleep(Config.SCAN_INTERVAL_SEC)


def reddit_monitor():
    """Reddit 24時間監視ループ（市場時間に関係なく稼働）

    Reddit注目銘柄リストを常に最新に保つ。
    アラートは出さない（出来高など他の条件が揃った時のみ市場スキャン側で判定）。
    """
    time.sleep(15)  # 起動待ち
    logger.info("[Reddit24h] 24時間監視を開始")
    while True:
        try:
            social.update_trending()
        except Exception as e:
            logger.error(f"[Reddit24h] Error: {e}")
        # 30分間隔（Redditのレート制限を考慮）
        time.sleep(1800)


if __name__ == '__main__':
    print("=" * 50)
    print("  Meme Stock Analyzer")
    print(f"  http://localhost:{Config.FLASK_PORT}")
    print("=" * 50)

    # Reddit 24時間監視（市場時間外も稼働）
    threading.Thread(target=reddit_monitor, daemon=True).start()

    # バックグラウンドスキャナー起動
    threading.Thread(target=background_scanner, daemon=True).start()

    # ニューススケジューラー起動（10時・22時 JST）
    news.start_scheduler()

    # 経済指標リアルタイム監視
    econ_monitor.start_monitor()

    socketio.run(
        app, allow_unsafe_werkzeug=True,
        host='0.0.0.0',
        port=Config.FLASK_PORT,
        debug=False,
        use_reloader=False,
    )
