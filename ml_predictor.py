"""
機械学習による銘柄スコア予測
過去の検知結果から「上昇する確率」と「期待上昇率」を予測する
"""
import logging
import sqlite3
import pickle
import os
import numpy as np
from datetime import datetime
from typing import Dict, List, Optional

logger = logging.getLogger('MemeStock.ML')

# MLモデル保存パス
MODEL_PATH = 'ml_model.pkl'
MIN_TRAINING_DATA = 50  # 最低学習データ数


class MLPredictor:
    """過去データから学習し、新しいアラートの期待値を予測"""

    # 特徴量の定義
    FEATURE_NAMES = [
        'volume_ratio', 'score', 'price', 'price_change_pct',
        'short_pct', 'insider_amount',
        'float_ultra_low', 'float_low', 'float_normal',
        'type_volume_spike', 'type_price_spike', 'type_combined',
        'type_accumulation', 'type_social',
        'sector_healthcare', 'sector_tech', 'sector_finance',
        'hour_of_day', 'day_of_week',
    ]

    def __init__(self, db_path='meme_stocks.db'):
        self.db_path = db_path
        self.model = None
        self.scaler = None
        self.is_trained = False
        self.training_stats = {}
        self._load_model()

    def _get_conn(self):
        conn = sqlite3.connect(self.db_path, timeout=10)
        conn.row_factory = sqlite3.Row
        return conn

    def _load_model(self):
        """保存済みモデルを読み込み"""
        if os.path.exists(MODEL_PATH):
            try:
                with open(MODEL_PATH, 'rb') as f:
                    data = pickle.load(f)
                    self.model = data.get('model')
                    self.scaler = data.get('scaler')
                    self.training_stats = data.get('stats', {})
                    self.is_trained = self.model is not None
                    if self.is_trained:
                        logger.info(
                            f"ML model loaded (trained on {self.training_stats.get('n_samples', '?')} samples, "
                            f"accuracy: {self.training_stats.get('accuracy', '?')}%)")
            except Exception as e:
                logger.warning(f"Failed to load model: {e}")

    def _save_model(self):
        """モデルを保存"""
        try:
            with open(MODEL_PATH, 'wb') as f:
                pickle.dump({
                    'model': self.model,
                    'scaler': self.scaler,
                    'stats': self.training_stats,
                }, f)
            logger.info("ML model saved")
        except Exception as e:
            logger.error(f"Failed to save model: {e}")

    def _extract_features(self, alert: Dict) -> np.ndarray:
        """アラートから特徴量ベクトルを抽出"""
        vol_ratio = alert.get('volume_ratio') or 0
        score = alert.get('score') or 0
        price = alert.get('price') or 0
        price_change = alert.get('price_change_pct') or 0
        short_pct = alert.get('short_pct') or 0
        insider_amount = alert.get('insider_buy_amount') or 0
        float_level = alert.get('float_level') or ''
        alert_type = alert.get('alert_type') or ''
        sector = (alert.get('sector') or '').lower()
        timestamp = alert.get('timestamp') or ''

        # 時間特徴量
        hour = 12
        dow = 2
        try:
            dt = datetime.strptime(timestamp[:19], '%Y-%m-%d %H:%M:%S')
            hour = dt.hour
            dow = dt.weekday()
        except Exception:
            pass

        features = [
            min(vol_ratio, 500),  # 外れ値クリップ
            score,
            price,
            min(max(price_change, -100), 200),  # クリップ
            short_pct if short_pct and short_pct < 1 else 0,
            min(insider_amount, 500000),
            # フロート（ワンホット）
            1 if float_level == 'ULTRA_LOW' else 0,
            1 if float_level == 'LOW' else 0,
            1 if float_level == 'NORMAL' or not float_level else 0,
            # アラートタイプ（ワンホット）
            1 if alert_type == 'VOLUME_SPIKE' else 0,
            1 if alert_type == 'PRICE_SPIKE' else 0,
            1 if alert_type == 'COMBINED' else 0,
            1 if alert_type in ('ACCUMULATION', 'VOL_REVERSAL', 'SQUEEZE_SETUP') else 0,
            1 if alert_type in ('SOCIAL_BUZZ', 'SOCIAL_VOLUME') else 0,
            # セクター
            1 if 'health' in sector or 'bio' in sector else 0,
            1 if 'tech' in sector else 0,
            1 if 'financ' in sector else 0,
            # 時間
            hour,
            dow,
        ]
        return np.array(features, dtype=np.float64)

    def train(self) -> Dict:
        """過去データからモデルを学習（回帰: 期待上昇率を予測）"""
        from sklearn.ensemble import GradientBoostingRegressor
        from sklearn.preprocessing import StandardScaler
        from sklearn.model_selection import cross_val_score

        conn = self._get_conn()
        try:
            rows = conn.execute("""
                SELECT r.volume_ratio, r.score, r.float_level, r.alert_type,
                       a.price, a.price_change_pct, a.short_pct,
                       a.insider_buy_amount, a.sector, a.timestamp,
                       r.max_gain_pct, r.result
                FROM alert_results r
                LEFT JOIN stock_alerts a ON r.alert_id = a.id
                WHERE r.result NOT IN ('PENDING', 'REVERSE_SPLIT')
                  AND (a.has_reverse_split IS NULL OR a.has_reverse_split != 1)
                  AND r.max_gain_pct IS NOT NULL
            """).fetchall()
        finally:
            conn.close()

        if len(rows) < MIN_TRAINING_DATA:
            return {
                'trained': False,
                'reason': f'Data insufficient ({len(rows)}/{MIN_TRAINING_DATA})',
            }

        # 特徴量とターゲット（期待上昇率）を構築
        X = []
        y = []  # 実際の最大上昇率(%)
        gains = []

        for row in rows:
            alert = dict(row)
            features = self._extract_features(alert)
            X.append(features)
            max_gain = alert.get('max_gain_pct') or 0
            # 外れ値クリップ（-50%〜+300%）
            max_gain = max(-50, min(300, max_gain))
            y.append(max_gain)
            gains.append(max_gain)

        X = np.array(X)
        y = np.array(y)

        # スケーリング
        self.scaler = StandardScaler()
        X_scaled = self.scaler.fit_transform(X)

        # モデル学習（勾配ブースティング回帰 → 期待上昇率を予測）
        self.model = GradientBoostingRegressor(
            n_estimators=100,
            max_depth=4,
            learning_rate=0.1,
            min_samples_leaf=5,
            random_state=42,
        )
        self.model.fit(X_scaled, y)

        # 交差検証でR2スコア評価
        cv_scores = cross_val_score(self.model, X_scaled, y, cv=min(5, len(y) // 10 + 1))
        r2_score = round(cv_scores.mean() * 100, 1)

        # 特徴量の重要度
        importances = dict(zip(self.FEATURE_NAMES, self.model.feature_importances_))
        top_features = sorted(importances.items(), key=lambda x: x[1], reverse=True)[:5]

        # 上昇率の分布
        wins_20 = sum(1 for g in gains if g >= 20)
        wins_50 = sum(1 for g in gains if g >= 50)

        self.training_stats = {
            'n_samples': len(rows),
            'win_rate_20pct': round(wins_20 / len(rows) * 100, 1),
            'win_rate_50pct': round(wins_50 / len(rows) * 100, 1),
            'avg_gain': round(np.mean(gains), 1),
            'median_gain': round(float(np.median(gains)), 1),
            'r2_score': r2_score,
            'top_features': {k: round(v, 4) for k, v in top_features},
            'trained_at': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        }
        self.is_trained = True
        self._save_model()

        logger.info(
            f"ML model trained: {len(rows)} samples, "
            f"R2={r2_score}%, avg_gain={self.training_stats['avg_gain']}%")

        return {
            'trained': True,
            'stats': self.training_stats,
        }

    def predict(self, alert: Dict) -> Dict:
        """アラートの期待上昇率と推奨度を予測"""
        if not self.is_trained:
            return {
                'ml_score': None,
                'expected_gain': None,
                'win_probability': None,
                'recommendation': 'NO_MODEL',
            }

        try:
            features = self._extract_features(alert)
            X = self.scaler.transform(features.reshape(1, -1))

            # 期待上昇率を予測
            expected_gain = round(float(self.model.predict(X)[0]), 1)

            # 期待値からWin確率を推定
            # 学習データの分布から、期待上昇率が高いほどWin確率が高い
            avg = self.training_stats.get('avg_gain', 30)
            if avg > 0:
                win_prob = min(100, max(0, round(expected_gain / avg * 60, 1)))
            else:
                win_prob = 50.0

            # ML総合スコア（0-100）
            base_score = alert.get('score', 0) or 0
            ml_score = round(
                base_score * 0.3 + min(expected_gain, 100) * 0.4 + win_prob * 0.3, 1)
            ml_score = max(0, min(100, ml_score))

            # 推奨度（期待上昇率ベース）
            if expected_gain >= 50:
                recommendation = 'STRONG_BUY'
                rec_ja = '強く推奨'
            elif expected_gain >= 25:
                recommendation = 'BUY'
                rec_ja = '推奨'
            elif expected_gain >= 10:
                recommendation = 'WATCH'
                rec_ja = '要監視'
            else:
                recommendation = 'SKIP'
                rec_ja = 'スキップ'

            return {
                'ml_score': ml_score,
                'expected_gain': expected_gain,
                'win_probability': win_prob,
                'recommendation': recommendation,
                'recommendation_ja': rec_ja,
            }

        except Exception as e:
            logger.error(f"Prediction error: {e}")
            return {
                'ml_score': None,
                'expected_gain': None,
                'win_probability': None,
                'recommendation': 'ERROR',
            }

    def get_top_picks(self, alerts: List[Dict], top_n: int = 5) -> List[Dict]:
        """アラートリストからML予測でTop N銘柄を厳選"""
        if not self.is_trained:
            # モデル未学習時はスコア順で返す
            sorted_alerts = sorted(alerts, key=lambda x: x.get('score', 0), reverse=True)
            return sorted_alerts[:top_n]

        # 各アラートにML予測を付加
        scored = []
        for alert in alerts:
            # 株式併合は除外
            if alert.get('has_reverse_split'):
                continue
            pred = self.predict(alert)
            alert['ml_score'] = pred['ml_score']
            alert['win_probability'] = pred['win_probability']
            alert['recommendation'] = pred['recommendation']
            alert['recommendation_ja'] = pred.get('recommendation_ja', '')
            scored.append(alert)

        # ML総合スコアで降順ソート
        scored.sort(key=lambda x: x.get('ml_score', 0) or 0, reverse=True)

        # Top N のみ返す（SKIP推奨は除外）
        top = [a for a in scored if a.get('recommendation') != 'SKIP']
        return top[:top_n]

    def get_stats(self) -> Dict:
        """モデルの統計情報"""
        return {
            'is_trained': self.is_trained,
            'stats': self.training_stats,
        }
