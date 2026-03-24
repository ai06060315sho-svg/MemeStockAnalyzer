"""Discord通知（複数Webhook対応）"""
import logging
import os
import threading
import requests
from datetime import datetime, timezone, timedelta

# タイムゾーン（サマータイム自動対応）
try:
    from zoneinfo import ZoneInfo
    JST = ZoneInfo('Asia/Tokyo')
    EST = ZoneInfo('America/New_York')  # EST/EDT自動切替
except ImportError:
    JST = timezone(timedelta(hours=9))
    EST = timezone(timedelta(hours=-5))

logger = logging.getLogger('MemeStock.Discord')


class DiscordNotifier:
    def __init__(self):
        from config import Config
        self.webhook_url = Config.DISCORD_WEBHOOK_URL
        self.news_webhook_url = Config.DISCORD_NEWS_WEBHOOK_URL
        self.econ_webhook_url = Config.DISCORD_ECON_WEBHOOK_URL
        self.enabled = bool(self.webhook_url)
        if self.enabled:
            logger.info("Webhook: enabled (meme stock channel)")
        else:
            logger.warning("Webhook: disabled (DISCORD_WEBHOOK_URL not set)")

    def _send(self, payload, webhook_url=None):
        url = webhook_url or self.webhook_url
        if not url:
            return

        def _post():
            try:
                resp = requests.post(url, json=payload, timeout=10)
                if resp.status_code not in (200, 204):
                    logger.error(f"送信エラー: {resp.status_code}")
            except Exception as e:
                logger.error(f"送信例外: {e}")
        threading.Thread(target=_post, daemon=True).start()

    def notify_volume_spike(self, alert: dict):
        """出来高スパイク・価格急騰・プレマーケット通知（改良版）"""
        now_jst = datetime.now(JST)
        now_est = datetime.now(EST)
        now = now_jst.strftime('%Y/%m/%d %H:%M')
        ticker = alert['ticker']
        vol_ratio = alert.get('volume_ratio', 0)
        alert_type = alert.get('alert_type', 'VOLUME_SPIKE')
        score = alert.get('score', 0)

        # 市場セッション判定
        est_hour = now_est.hour
        est_min = now_est.minute
        if est_hour < 9 or (est_hour == 9 and est_min < 30):
            session_label = '🌅 プレマーケット'
        elif est_hour < 16:
            session_label = '🔔 通常取引'
        elif est_hour < 20:
            session_label = '🌙 アフターマーケット'
        else:
            session_label = '⏸️ 市場閉鎖中'

        type_labels = {
            'VOLUME_SPIKE': '出来高急増',
            'VOLUME_RISING': '出来高じわ上げ',
            'PRICE_SPIKE': '価格急騰',
            'PREMARKET': 'プレマーケット異常',
            'ACCUMULATION': '静かな買い集め',
            'VOL_REVERSAL': '出来高底打ち反転',
            'SQUEEZE_SETUP': 'ブレイクアウト前兆',
            'SOCIAL_BUZZ': 'SNS話題',
            'SOCIAL_VOLUME': 'SNS話題+出来高',
        }
        type_label = type_labels.get(alert_type, alert_type)

        urgency_labels = {
            'PRICE_SPIKE': '価格急騰',
            'PREMARKET': 'プレマーケット',
            'SOCIAL_VOLUME': 'SNS+出来高',
            'SOCIAL_BUZZ': 'SNS話題',
            'ACCUMULATION': '買い集め',
            'VOL_REVERSAL': '底打ち',
            'SQUEEZE_SETUP': 'ブレイク前兆',
        }

        urgency_colors = {
            'PRICE_SPIKE': 0xFF0066,
            'PREMARKET': 0x9933FF,
            'SOCIAL_VOLUME': 0xFF0066,
            'SOCIAL_BUZZ': 0x5865F2,
            'ACCUMULATION': 0x2ECC71,
            'VOL_REVERSAL': 0x3498DB,
            'SQUEEZE_SETUP': 0xE67E22,
        }

        if alert_type in urgency_labels:
            color = urgency_colors.get(alert_type, 0xFFAA00)
            urgency = urgency_labels[alert_type]
        elif vol_ratio >= 10:
            color = 0xFF0000
            urgency = '最高警戒'
        elif vol_ratio >= 5:
            color = 0xFF6600
            urgency = '高警戒'
        else:
            color = 0xFFAA00
            urgency = '注目'

        # スコアバー表示（視覚化）
        score_bar = self._make_score_bar(score)

        # スコアレベル
        if score >= 60:
            confidence = '🔴 高スコア'
        elif score >= 40:
            confidence = '🟡 中スコア'
        elif score >= 20:
            confidence = '🟢 低スコア'
        else:
            confidence = '⚪ データ収集中'

        price = alert.get('price', 0) or 0
        volume = alert.get('volume', 0) or 0

        fields = [
            {'name': '検知時間', 'value': f"JST {now_jst.strftime('%H:%M')} / ET {now_est.strftime('%H:%M')}", 'inline': True},
            {'name': '市場セッション', 'value': session_label, 'inline': True},
            {'name': '\u200b', 'value': '\u200b', 'inline': True},
            {'name': '価格', 'value': f"${price:.2f}" if price > 0 else 'N/A', 'inline': True},
            {'name': '出来高', 'value': f"{volume:,}" if volume > 0 else 'N/A', 'inline': True},
            {'name': '出来高倍率', 'value': f"{vol_ratio:.1f}倍" if vol_ratio > 0 else 'N/A', 'inline': True},
            {'name': f'スコア {score}/100', 'value': score_bar, 'inline': False},
            {'name': '信頼度', 'value': confidence, 'inline': True},
        ]

        if alert.get('price_change_pct'):
            fields.append({'name': '変動率', 'value': f"{alert['price_change_pct']:+.1f}%", 'inline': True})

        # センチメント情報
        sentiment = alert.get('sentiment', '')
        if sentiment:
            sent_emoji = {'BULLISH': '📈 強気', 'BEARISH': '📉 弱気', 'NEUTRAL': '➡️ 中立'}.get(sentiment, '')
            if sent_emoji:
                fields.append({'name': 'SNS感情', 'value': sent_emoji, 'inline': True})

        if alert.get('buzz_score'):
            fields.append({'name': 'SNS注目度', 'value': f"{alert['buzz_score']}点", 'inline': True})
        if alert.get('mention_count'):
            fields.append({'name': 'Reddit言及', 'value': f"{alert['mention_count']}回", 'inline': True})
        if alert.get('accumulation_days'):
            fields.append({'name': '蓄積日数', 'value': f"{alert['accumulation_days']}日", 'inline': True})

        float_shares = alert.get('float_shares')
        if float_shares:
            float_level = alert.get('float_level', '')
            level_ja = {'ULTRA_LOW': '超低', 'LOW': '低', 'NORMAL': '通常'}.get(float_level, '')
            fields.append({
                'name': 'フロート',
                'value': f"{float_shares/1e6:.1f}M株 ({level_ja})",
                'inline': True,
            })

        if alert.get('short_pct'):
            fields.append({'name': '空売り比率', 'value': f"{alert['short_pct']*100:.1f}%", 'inline': True})

        # マルチTF確認情報
        if alert.get('mtf_detail'):
            fields.append({'name': 'マルチTF', 'value': alert['mtf_detail'].strip(), 'inline': True})

        # 株式併合警告（最重要）
        if alert.get('has_reverse_split'):
            ratio = alert.get('reverse_split_ratio', '不明')
            fields.append({
                'name': '!! 株式併合 !!',
                'value': (f'併合比率: {ratio}\n'
                          f'価格上昇は併合による見かけ上の変動です。\n'
                          f'**このアラートは無視してください**'),
                'inline': False,
            })
            color = 0x808080  # グレー
        # 偽シグナル警告
        elif alert.get('catalyst_filtered'):
            fields.append({
                'name': 'WARNING',
                'value': alert.get('catalyst_filter_reason', '非ミーム要因の可能性'),
                'inline': False,
            })

        company = alert.get('company_name', '')
        desc = alert.get('detail', '')
        if company:
            desc = f"**{company}**\n{desc}"

        # 株式併合の場合は警告
        if alert.get('has_reverse_split'):
            desc += '\n\n**⚠ 株式併合による出来高変動の可能性あり**'

        # 鉄板パターン情報
        iron_patterns = alert.get('iron_patterns', [])
        if iron_patterns:
            best = iron_patterns[0]
            desc += f"\n\n**鉄板パターン該当**: {best['name']}（過去勝率{best['win_rate']}%）"
            if len(iron_patterns) > 1:
                desc += f" 他{len(iron_patterns)-1}条件"

        embed = {
            'title': f'{type_label}: ${ticker} [{urgency}]',
            'description': desc,
            'color': color,
            'fields': fields,
            'footer': {'text': f'ミーム株スキャナー | {now} JST | {session_label} | ※投資助言ではありません'},
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        self._send({'embeds': [embed]})

    def _make_score_bar(self, score: int) -> str:
        """スコアをプログレスバーで視覚化"""
        filled = score // 10
        empty = 10 - filled
        bar = '█' * filled + '░' * empty
        return f"`{bar}` {score}点"

    def notify_insider_buy(self, insider: dict):
        """インサイダー買い通知"""
        now = datetime.now(JST).strftime('%Y/%m/%d %H:%M')
        ticker = insider['ticker']
        total = insider.get('total_value', 0)

        if total >= 100_000:
            color = 0x00FF00
            level = '大量購入'
        elif total >= 50_000:
            color = 0x33CC33
            level = '中規模購入'
        else:
            color = 0x66AA66
            level = '少額購入'

        embed = {
            'title': f'インサイダー買い: ${ticker} [{level}]',
            'description': (f"**{insider.get('insider_name', '不明')}** "
                            f"({insider.get('insider_title', '')})"),
            'color': color,
            'fields': [
                {'name': '株数', 'value': f"{insider.get('shares', 0):,}株", 'inline': True},
                {'name': '1株あたり', 'value': f"${insider.get('price_per_share', 0):.2f}", 'inline': True},
                {'name': '購入総額', 'value': f"${total:,.0f}", 'inline': True},
                {'name': '取引日', 'value': insider.get('date', '不明'), 'inline': True},
            ],
            'footer': {'text': f'ミーム株スキャナー | {now} | ※投資助言ではありません'},
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        self._send({'embeds': [embed]})

    def notify_combined(self, ticker: str, volume_alert: dict, insider: dict):
        """複合シグナル（最高優先度）"""
        now = datetime.now(JST).strftime('%Y/%m/%d %H:%M')

        embed = {
            'title': f'複合シグナル: ${ticker}',
            'description': '出来高急増とインサイダー買いが同時に検出されました。',
            'color': 0xFF0066,
            'fields': [
                {'name': '価格', 'value': f"${volume_alert['price']:.2f}", 'inline': True},
                {'name': '出来高倍率', 'value': f"{volume_alert.get('volume_ratio', 0):.1f}倍", 'inline': True},
                {'name': 'インサイダー', 'value': insider.get('insider_name', '不明'), 'inline': True},
                {'name': '購入総額', 'value': f"${insider.get('total_value', 0):,.0f}", 'inline': True},
            ],
            'footer': {'text': f'ミーム株スキャナー | {now} | ※投資助言ではありません'},
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        self._send({'embeds': [embed]})

    def notify_top_picks(self, top_picks: list):
        """ML厳選 Top Picks 通知"""
        if not top_picks:
            return

        now = datetime.now(JST).strftime('%Y/%m/%d %H:%M')

        # Top Picks をフィールドに変換
        fields = []
        for i, pick in enumerate(top_picks[:5], 1):
            ticker = pick.get('ticker', '?')
            price = pick.get('price', 0)
            prob = pick.get('win_probability', 0)
            ml_score = pick.get('ml_score', 0)
            vol = pick.get('volume_ratio', 0)
            rec = pick.get('recommendation_ja', '')
            score_bar = self._make_score_bar(int(ml_score) if ml_score else 0)

            fields.append({
                'name': f'{i}. ${ticker} [{rec}]',
                'value': (f'ML: {score_bar}\n'
                          f'Win確率: **{prob:.0f}%** | '
                          f'${price:.2f} | '
                          f'Vol {vol:.1f}x'),
                'inline': False,
            })

        embed = {
            'title': 'ML厳選 Top Picks',
            'description': (f'**ML分析結果: {len(top_picks)}銘柄を検出**\n'
                            f'過去データに基づくスコアリング結果です。投資判断は自己責任でお願いします。'),
            'color': 0xFFD700,  # ゴールド
            'fields': fields,
            'footer': {'text': f'ML Predictor | {now}'},
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        self._send({'embeds': [embed]})

    def notify_scan_complete(self, volume_count: int, insider_count: int,
                              combined_count: int, universe_size: int,
                              iron_alerts: list = None):
        """スキャン完了通知（鉄板パターン該当銘柄つき）"""
        now_jst = datetime.now(JST)
        now_est = datetime.now(EST)
        now = now_jst.strftime('%Y/%m/%d %H:%M')

        # 市場セッション判定
        est_h = now_est.hour
        est_m = now_est.minute
        if est_h < 9 or (est_h == 9 and est_m < 30):
            session = '🌅 プレマーケット'
        elif est_h < 16:
            session = '🔔 通常取引'
        elif est_h < 20:
            session = '🌙 アフターマーケット'
        else:
            session = '⏸️ 市場閉鎖中'

        embeds = []

        # --- Embed 1: サマリー ---
        summary = (
            f"```\n"
            f"スキャン対象   {universe_size:,}銘柄\n"
            f"出来高異常     {volume_count}件\n"
            f"インサイダー   {insider_count}件\n"
            f"複合シグナル   {combined_count}件\n"
            f"鉄板パターン   {len(iron_alerts) if iron_alerts else 0}件\n"
            f"```"
        )
        embeds.append({
            'title': f'📊 スキャン結果 | {session}',
            'description': (
                f"**{now_jst.strftime('%H:%M')} JST** / {now_est.strftime('%H:%M')} ET\n"
                f"{summary}"
            ),
            'color': 0x3D9EFF,
        })

        # --- Embed 2: 鉄板パターン該当銘柄（メイン情報） ---
        if iron_alerts:
            iron_alerts.sort(key=lambda a: -(a.get('iron_best_wr') or 0))

            for a in iron_alerts[:6]:
                wr = a.get('iron_best_wr', 0)
                ticker = a.get('ticker', '?')
                price = a.get('price', 0) or 0
                vol = a.get('volume_ratio', 0) or 0
                score = a.get('score', 0) or 0
                pattern = a.get('iron_best_name', '')
                atype = a.get('alert_type', '')
                company = a.get('company_name') or ''

                # 色分け
                color = 0xFF0066 if wr >= 90 else 0xFF6600 if wr >= 80 else 0xFFAA00

                # ラベル
                wr_label = '🔴 超鉄板' if wr >= 90 else '🟠 鉄板' if wr >= 80 else '🟡 有望'

                # タイプ
                type_labels = {
                    'VOLUME_SPIKE': '出来高急増', 'VOLUME_RISING': 'じわ上げ',
                    'PRICE_SPIKE': '価格急騰', 'ACCUMULATION': '買い集め',
                    'VOL_REVERSAL': '底打ち反転', 'SQUEEZE_SETUP': 'ブレイク前兆',
                    'SOCIAL_VOLUME': 'Reddit+出来高', 'COMBINED': '複合',
                    'INSIDER_BUY': 'インサイダー', 'PREMARKET': 'プレマーケット',
                }
                type_ja = type_labels.get(atype, atype)

                # 補足情報
                tags = []
                repeat = a.get('repeat_count', 0)
                if repeat >= 2:
                    tags.append(f'🔄 リピート{repeat}回')
                caution = a.get('caution_sector')
                if caution:
                    tags.append(f'⚠️ {caution}セクター')
                fl = a.get('float_level', '')
                if fl == 'ULTRA_LOW':
                    tags.append('💎 超低フロート')
                elif fl == 'LOW':
                    tags.append('💎 低フロート')
                sentiment = a.get('sentiment', '')
                if sentiment == 'BULLISH':
                    tags.append('📈 Reddit強気')
                elif sentiment == 'BEARISH':
                    tags.append('📉 Reddit弱気')
                tags_str = ' | '.join(tags) if tags else ''

                # 鉄板パターン一覧
                iron_list = a.get('iron_patterns', [])
                patterns_str = '\n'.join(
                    f"{'✅' if p['win_rate'] >= 90 else '🔸' if p['win_rate'] >= 80 else '▫️'} {p['name']}（{p['win_rate']}%）"
                    for p in iron_list[:4]
                )

                desc_lines = []
                if company:
                    desc_lines.append(f"*{company}*")
                desc_lines.append(
                    f"```\n"
                    f"価格     ${price:.2f}\n"
                    f"出来高   {vol:.0f}倍（20日平均比）\n"
                    f"スコア   {score}点\n"
                    f"検知     {type_ja}\n"
                    f"```"
                )
                if tags_str:
                    desc_lines.append(tags_str)
                desc_lines.append(f"\n**合致パターン:**\n{patterns_str}")

                embeds.append({
                    'title': f'{wr_label} ${ticker}（勝率{wr}%）',
                    'description': '\n'.join(desc_lines),
                    'color': color,
                })

            # 鉄板パターン未該当の件数
            other_count = volume_count - len(iron_alerts)
            if other_count > 0:
                embeds[-1]['footer'] = {
                    'text': f'他{other_count}件は鉄板パターン未該当 | ※検知情報であり投資助言ではありません'
                }
            else:
                embeds[-1]['footer'] = {
                    'text': '※検知情報であり投資助言ではありません'
                }
        else:
            embeds.append({
                'description': '鉄板パターンに該当する銘柄はありませんでした',
                'color': 0x4A6080,
                'footer': {'text': '※検知情報であり投資助言ではありません'},
            })

        # Discord embed上限は10個
        self._send({'embeds': embeds[:10]})

    def notify_daily_report(self, stats: dict):
        """日次結果レポート（実績公開用）"""
        now = datetime.now(JST).strftime('%Y/%m/%d')

        total = stats.get('total_confirmed', 0)
        wins = stats.get('wins', 0)
        losses = stats.get('losses', 0)
        win_rate = stats.get('win_rate', 0)
        avg_gain = stats.get('avg_max_gain', 0)
        today_alerts = stats.get('today_alerts', 0)
        today_wins = stats.get('today_wins', 0)
        best = stats.get('best_ticker', '')
        best_gain = stats.get('best_gain', 0)

        # 成績バー
        bar = self._make_score_bar(int(win_rate))

        fields = [
            {'name': '本日の検知数', 'value': f'{today_alerts}件', 'inline': True},
            {'name': '本日の成功数', 'value': f'{today_wins}件', 'inline': True},
            {'name': '\u200b', 'value': '\u200b', 'inline': True},
            {'name': '累計成功率', 'value': f'{bar}', 'inline': False},
            {'name': '累計成績', 'value': f'成功: **{wins}件** / ハズレ: {losses}件 / 計{total}件', 'inline': False},
            {'name': '平均最高上昇率', 'value': f'+{avg_gain}%', 'inline': True},
        ]

        if best:
            fields.append({
                'name': '本日のベスト',
                'value': f'**${best}** (+{best_gain:.0f}%)',
                'inline': True,
            })

        embed = {
            'title': f'Daily Report - {now}',
            'description': '**Meme Stock Scanner 日次レポート**',
            'color': 0x3D9EFF,
            'fields': fields,
            'footer': {'text': 'Meme Stock Scanner | This is not investment advice'},
            'timestamp': datetime.now(timezone.utc).isoformat(),
        }
        self._send({'embeds': [embed]})
