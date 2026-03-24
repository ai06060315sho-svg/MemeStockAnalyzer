"""
米国株ニュース・市場分析モジュール
Gemini APIを使用してアメリカ株の最新情報を収集し、Discordに配信する
1日2回（10時・22時 JST）自動配信
"""
import os
import threading
import time
import requests
from datetime import datetime, timezone, timedelta

JST = timezone(timedelta(hours=9))

# 配信時間（JST）
SCHEDULE_HOURS = [10, 22]


class StockNewsAnalyzer:
    def __init__(self, webhook_url: str = None):
        self.api_key = os.getenv('GEMINI_API_KEY', '')
        self.webhook_url = webhook_url or os.getenv('DISCORD_NEWS_WEBHOOK_URL', '') or os.getenv('DISCORD_WEBHOOK_URL', '')
        self.last_sent_hour = -1
        self._running = False

        if self.api_key:
            print("[StockNews] Gemini APIキー: 設定済み")
        else:
            print("[StockNews] Gemini APIキー: 未設定")

    def _call_gemini(self, prompt: str) -> str:
        """Gemini APIを呼び出す（Google Search Grounding付き、リトライ対応）"""
        max_retries = 2
        for attempt in range(max_retries + 1):
            try:
                from google import genai
                from google.genai import types

                client = genai.Client(api_key=self.api_key)

                response = client.models.generate_content(
                    model='gemini-2.5-flash',
                    contents=prompt,
                    config=types.GenerateContentConfig(
                        tools=[types.Tool(
                            google_search=types.GoogleSearch()
                        )],
                        temperature=0.3,
                    )
                )

                return response.text if response.text else "情報を取得できませんでした。"

            except Exception as e:
                err_str = str(e)
                if '429' in err_str and attempt < max_retries:
                    wait = 45 * (attempt + 1)
                    print(f"[StockNews] レート制限、{wait}秒後にリトライ ({attempt+1}/{max_retries})")
                    time.sleep(wait)
                    continue
                print(f"[StockNews] Gemini API エラー: {e}")
                return f"API呼び出しエラー: {e}"

    def generate_report(self) -> dict:
        """米国株市場分析レポートを生成"""
        now = datetime.now(JST)
        time_label = "朝" if now.hour < 15 else "夜"

        prompt = f"""あなたはプロの米国株アナリストです。
現在の日時: {now.strftime('%Y年%m月%d日 %H:%M')} (日本時間)

以下の項目について、最新の情報をGoogle検索で調べて、正確で信頼性の高いレポートを作成してください。
情報源は Bloomberg, Reuters, CNBC, MarketWatch, Yahoo Finance などの信頼できるメディアを優先してください。

【1. 米国市場サマリー】
- S&P 500、NASDAQ、ダウの直近の動き
- 主要な市場トレンド（上昇/下落/横ばい）
- VIX（恐怖指数）の状況

【2. 注目ニュース】
- 米国株に影響する最新ニュース（良いニュースと悪いニュース両方）
- 決算発表の注目銘柄
- IPO・SPAC関連の動き
- 各ニュースの情報源を明記すること

【3. ペニー株・ミーム株の動向】
- $5以下の銘柄で注目されている銘柄
- Reddit (WallStreetBets) やSNSで話題の銘柄
- 急騰・急落した低位株
- ショートスクイーズの可能性がある銘柄

【4. マクロ経済の影響】
- FRB/FOMCの金融政策
- 雇用統計、CPI、GDPなどの経済指標
- 米ドル指数の動き
- 地政学リスク

【5. セクター別の注目点】
- 好調なセクター・不調なセクター
- テクノロジー、バイオ、EV、エネルギーなどの動向
- 新興テーマ（AI、量子コンピューティング等）

【6. 今後の見通し】
- 今日・今週の相場予測
- 注意すべきイベント・経済指標の発表スケジュール
- ペニー株トレーダーへのアドバイス

重要:
- 不確実な情報は「未確認」と明記すること
- 予測は断定せず、可能性として提示すること
- 箇条書きで簡潔にまとめること
- 全て日本語で記述すること
- 情報源を必ず明記すること"""

        report_text = self._call_gemini(prompt)

        return {
            'text': report_text,
            'time_label': time_label,
            'timestamp': now.strftime('%Y/%m/%d %H:%M'),
        }

    def send_to_discord(self, report: dict):
        """レポートをDiscordに送信"""
        if not self.webhook_url:
            print("[StockNews] Webhook URL未設定")
            return

        now_str = report['timestamp']
        time_label = report['time_label']
        text = report['text']

        # Discord embedの文字数制限（4096文字）に対応
        chunks = []
        if len(text) <= 3900:
            chunks = [text]
        else:
            sections = text.split('\n\n')
            current_chunk = ""
            for section in sections:
                if len(current_chunk) + len(section) > 3900:
                    if current_chunk:
                        chunks.append(current_chunk)
                    current_chunk = section
                else:
                    current_chunk += "\n\n" + section if current_chunk else section
            if current_chunk:
                chunks.append(current_chunk)

        for i, chunk in enumerate(chunks):
            if i == 0:
                embed = {
                    'title': f'米国株 {time_label}の市場分析レポート',
                    'description': chunk,
                    'color': 0xFF6600,
                    'footer': {'text': f'ミーム株スキャナー | {now_str}'},
                    'timestamp': datetime.now(timezone.utc).isoformat(),
                }
            else:
                embed = {
                    'title': f'レポート続き ({i+1}/{len(chunks)})',
                    'description': chunk,
                    'color': 0xFF6600,
                }

            try:
                resp = requests.post(
                    self.webhook_url,
                    json={'embeds': [embed]},
                    timeout=15
                )
                if resp.status_code not in (200, 204):
                    print(f"[StockNews] Discord送信エラー: {resp.status_code}")
                else:
                    print(f"[StockNews] Discord送信成功 ({i+1}/{len(chunks)})")
            except Exception as e:
                print(f"[StockNews] Discord送信例外: {e}")

            if i < len(chunks) - 1:
                time.sleep(1)

    def check_and_send(self):
        """スケジュールをチェックして配信する"""
        now = datetime.now(JST)
        current_hour = now.hour

        if current_hour in SCHEDULE_HOURS and current_hour != self.last_sent_hour:
            print(f"[StockNews] {current_hour}時のレポート生成開始...")
            try:
                report = self.generate_report()
                self.send_to_discord(report)
                self.last_sent_hour = current_hour
                print(f"[StockNews] レポート配信完了")
            except Exception as e:
                print(f"[StockNews] レポート生成エラー: {e}")
                import traceback
                traceback.print_exc()

    def send_now(self):
        """今すぐレポートを送信（手動実行用）"""
        print("[StockNews] 手動レポート生成開始...")
        report = self.generate_report()
        self.send_to_discord(report)
        print("[StockNews] 手動レポート送信完了")
        return report

    def start_scheduler(self):
        """バックグラウンドでスケジューラーを開始"""
        if self._running:
            return

        self._running = True

        def _loop():
            while self._running:
                try:
                    self.check_and_send()
                except Exception as e:
                    print(f"[StockNews] スケジューラーエラー: {e}")
                time.sleep(60)

        t = threading.Thread(target=_loop, daemon=True)
        t.start()
        print(f"[StockNews] スケジューラー開始 (配信時間: {SCHEDULE_HOURS} JST)")
