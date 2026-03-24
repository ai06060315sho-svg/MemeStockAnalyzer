"""
SNS話題検知 - Reddit/StockTwits
急騰前にSNSで話題になり始めた銘柄を検出する
センチメント分析・トレンド検出付き
"""
import re
import time
import requests
from collections import OrderedDict
from datetime import datetime, timezone, timedelta
from typing import List, Dict

JST = timezone(timedelta(hours=9))


class SocialScanner:
    """Reddit・StockTwitsから話題の銘柄を検出（センチメント分析付き）"""

    # 監視するRedditサブレディット
    SUBREDDITS = [
        'wallstreetbets',
        'pennystocks',
        'Shortsqueeze',
        'smallstreetbets',
        'stocks',
        'squeezeplays',
        'RobinHoodPennyStocks',
    ]

    # スキャンするソート順（hot + new + rising で網羅率UP）
    SORT_TYPES = ['hot', 'new', 'rising']

    # 除外ティッカー（一般的な単語と被るもの）
    EXCLUDE_TICKERS = {
        'A', 'I', 'AM', 'AN', 'AS', 'AT', 'BE', 'BY', 'DO', 'GO',
        'HE', 'IF', 'IN', 'IS', 'IT', 'ME', 'MY', 'NO', 'OF', 'ON',
        'OR', 'SO', 'TO', 'UP', 'US', 'WE', 'CEO', 'DD', 'EPS', 'ETF',
        'FDA', 'FED', 'GDP', 'IMO', 'IPO', 'ITM', 'OTM', 'SEC', 'WSB',
        'YOLO', 'ALL', 'ARE', 'BIG', 'CAN', 'FOR', 'HAS', 'HAD', 'HOW',
        'LOW', 'NEW', 'OLD', 'ONE', 'OUR', 'OUT', 'RUN', 'SAY', 'THE',
        'TOP', 'TWO', 'WAY', 'WHO', 'WHY', 'WIN', 'NOW', 'SEE', 'ITS',
        'PUT', 'GET', 'GOT', 'MAY', 'DIP', 'RIP', 'ATH', 'LOL', 'OMG',
        'AND', 'BUT', 'NOT', 'YOU', 'HIS', 'HER', 'OUR', 'ANY', 'FEW',
        'OWN', 'BAD', 'DAY', 'END', 'FAR', 'OIL', 'TAX', 'CEO', 'CFO',
        'COO', 'CTO', 'LLC', 'INC', 'USA', 'USD', 'EUR', 'GBP', 'JPY',
        'HOLD', 'SELL', 'CALL', 'LONG', 'NEXT', 'BEST', 'GOOD', 'JUST',
        'LIKE', 'OVER', 'VERY', 'MUCH', 'SOME', 'HELP', 'FREE', 'HIGH',
        'CASH', 'DEAL', 'PUMP', 'DUMP', 'MOON', 'SEND', 'HUGE', 'EASY',
        'PLAY', 'SAFE', 'RISK', 'DOWN', 'GAIN', 'LOSS', 'MOVE', 'BEEN',
        'BACK', 'OPEN', 'LAST', 'ALSO', 'THAN', 'WHAT', 'WHEN', 'THEN',
        'THEM', 'ONLY', 'TAKE', 'MAKE', 'LOOK', 'NEED', 'WILL', 'EACH',
        'FEEL', 'MOST', 'SAVE', 'SURE', 'TELL', 'WANT', 'WEEK', 'YEAR',
    }

    # --- センチメント分析キーワード ---
    BULLISH_KEYWORDS = [
        # 強気表現
        'bullish', 'moon', 'rocket', 'squeeze', 'breakout', 'undervalued',
        'buy the dip', 'btd', 'loading up', 'adding more', 'doubling down',
        'going long', 'calls', 'gamma squeeze', 'short squeeze',
        'to the moon', 'tendies', 'diamond hands', 'hodl', 'lfg',
        'catalyst', 'massive potential', 'hidden gem', 'sleeper',
        'about to pop', 'ready to run', 'gap up', 'breaking out',
        'accumulating', 'insider buying', 'heavily shorted',
        'low float', 'high si', 'ftd', 'threshold list',
        # 🚀 🌙 💎 🙌 関連は正規表現で別途処理
    ]

    BEARISH_KEYWORDS = [
        # 弱気表現
        'bearish', 'puts', 'short', 'dump', 'overvalued', 'scam',
        'avoid', 'sell', 'exit', 'bag holder', 'bagholder', 'bagholding',
        'rug pull', 'rugpull', 'dilution', 'offering', 'shelf offering',
        'reverse split', 'delisting', 'bankruptcy', 'fraud',
        'pump and dump', 'p&d', 'stay away', 'dead cat bounce',
        'falling knife', 'top is in', 'going down', 'crash',
    ]

    BULLISH_EMOJIS = ['🚀', '🌙', '💎', '🙌', '📈', '🔥', '💰', '🤑', '⬆️', '🐂']
    BEARISH_EMOJIS = ['📉', '💀', '🗑️', '⬇️', '🐻', '😱', '☠️', '🔻']

    # 重複追跡の最大保持数
    _SEEN_POST_MAX = 5000

    def __init__(self):
        self._last_scan = {}
        self._mention_history = {}  # ticker -> [count_per_scan]
        self._seen_post_ids = OrderedDict()  # 重複投稿の追跡（LRU方式）
        # Reddit注目銘柄（24時間監視用）
        self._trending_tickers = {}  # ticker -> {buzz_score, sentiment, ...}
        self._trending_updated_at = None

    def scan_reddit(self) -> List[Dict]:
        """Redditの投稿からティッカー言及を検出（センチメント分析付き）"""
        mentions = {}  # ticker -> {count, posts, sentiment_hints, ...}

        for sub in self.SUBREDDITS:
            for sort_type in self.SORT_TYPES:
                try:
                    posts = self._fetch_reddit_posts(sub, sort=sort_type, limit=30)
                    for post in posts:
                        post_id = post.get('id', '')
                        if post_id in self._seen_post_ids:
                            continue
                        self._seen_post_ids[post_id] = True
                        # 古いエントリを段階的に削除（LRU方式）
                        if len(self._seen_post_ids) > self._SEEN_POST_MAX:
                            self._seen_post_ids.popitem(last=False)

                        title = post.get('title', '')
                        selftext = post.get('selftext', '')[:1000]
                        text = f"{title} {selftext}"
                        score = post.get('score', 0)
                        num_comments = post.get('num_comments', 0)
                        created = post.get('created_utc', 0)

                        # 投稿の鮮度チェック（24時間以内のみ）
                        if created > 0:
                            age_hours = (time.time() - created) / 3600
                            if age_hours > 24:
                                continue

                        # センチメント分析
                        sentiment = self._analyze_sentiment(text)

                        # ティッカー抽出
                        tickers = self._extract_tickers(text)
                        for ticker in tickers:
                            if ticker not in mentions:
                                mentions[ticker] = {
                                    'count': 0,
                                    'total_score': 0,
                                    'total_comments': 0,
                                    'posts': [],
                                    'subreddits': set(),
                                    'bullish_count': 0,
                                    'bearish_count': 0,
                                    'sentiment_scores': [],
                                    'has_dd': False,
                                    'max_post_score': 0,
                                }
                            m = mentions[ticker]
                            m['count'] += 1
                            m['total_score'] += score
                            m['total_comments'] += num_comments
                            m['subreddits'].add(sub)
                            m['bullish_count'] += sentiment['bullish_signals']
                            m['bearish_count'] += sentiment['bearish_signals']
                            m['sentiment_scores'].append(sentiment['score'])
                            m['max_post_score'] = max(m['max_post_score'], score)

                            # DD（デューデリジェンス）投稿の検出
                            if any(tag in title.lower() for tag in ['[dd]', 'dd:', 'due diligence']):
                                m['has_dd'] = True

                            if len(m['posts']) < 5:
                                m['posts'].append({
                                    'title': title[:100],
                                    'score': score,
                                    'comments': num_comments,
                                    'sub': sub,
                                    'sort': sort_type,
                                    'sentiment': sentiment['label'],
                                    'age_hours': round(age_hours, 1) if created > 0 else None,
                                })

                    time.sleep(1.5)  # Reddit rate limit
                except Exception as e:
                    print(f"[Social] Reddit r/{sub}/{sort_type} error: {e}")
                    continue

        # 結果をフィルタ・ソート
        alerts = []
        for ticker, data in mentions.items():
            if data['count'] < 2:  # 2回以上言及されたもののみ
                continue

            # 急増検知: 前回スキャンと比較
            prev_count = self._mention_history.get(ticker, 0)
            is_trending = data['count'] >= prev_count * 2 if prev_count > 0 else data['count'] >= 3
            self._mention_history[ticker] = data['count']

            engagement = data['total_score'] + data['total_comments'] * 2
            sub_count = len(data['subreddits'])

            # センチメント集計
            avg_sentiment = (sum(data['sentiment_scores']) / len(data['sentiment_scores'])
                             if data['sentiment_scores'] else 0)
            net_bullish = data['bullish_count'] - data['bearish_count']
            if avg_sentiment >= 0.3:
                overall_sentiment = 'BULLISH'
            elif avg_sentiment <= -0.3:
                overall_sentiment = 'BEARISH'
            else:
                overall_sentiment = 'NEUTRAL'

            alert = {
                'ticker': ticker,
                'mention_count': data['count'],
                'engagement_score': engagement,
                'subreddit_count': sub_count,
                'subreddits': list(data['subreddits']),
                'is_trending': is_trending,
                'top_posts': data['posts'],
                'source': 'reddit',
                'sentiment': overall_sentiment,
                'sentiment_score': round(avg_sentiment, 2),
                'bullish_signals': data['bullish_count'],
                'bearish_signals': data['bearish_count'],
                'has_dd': data['has_dd'],
                'max_post_score': data['max_post_score'],
            }

            # 注目度スコア（改良版）
            buzz_score = 0

            # 言及数（最大25点） - 段階的スコア
            if data['count'] >= 10:
                buzz_score += 25
            elif data['count'] >= 5:
                buzz_score += 18
            else:
                buzz_score += min(data['count'] * 4, 12)

            # エンゲージメント（最大25点） - 高upvote投稿を重視
            if data['max_post_score'] >= 500:
                buzz_score += 25
            elif data['max_post_score'] >= 100:
                buzz_score += 18
            elif data['max_post_score'] >= 50:
                buzz_score += 12
            else:
                buzz_score += min(engagement // 50, 8)

            # 複数サブレディット（最大20点）
            buzz_score += min(sub_count * 7, 20)

            # センチメント（最大15点）
            if overall_sentiment == 'BULLISH':
                buzz_score += min(net_bullish * 3, 15)
            elif overall_sentiment == 'BEARISH':
                buzz_score -= 10  # ネガティブ時は減点

            # 急増ボーナス（15点）
            if is_trending:
                buzz_score += 15

            # DD投稿ボーナス（5点） - 分析付き投稿があると信頼度UP
            if data['has_dd']:
                buzz_score += 5

            # ベアリッシュが圧倒的なら大幅減点
            if data['bearish_count'] > data['bullish_count'] * 2 and data['bearish_count'] >= 3:
                buzz_score = max(buzz_score - 20, 0)

            alert['buzz_score'] = min(max(buzz_score, 0), 100)
            alerts.append(alert)

        alerts.sort(key=lambda x: x['buzz_score'], reverse=True)
        print(f"[Social] Reddit mentions: {len(mentions)} tickers, {len(alerts)} alerts")
        return alerts

    def scan_stocktwits(self) -> List[Dict]:
        """StockTwitsのトレンド銘柄を検出（API制限時は空リスト）"""
        try:
            resp = requests.get(
                'https://api.stocktwits.com/api/2/trending/symbols.json',
                timeout=10,
                headers={'User-Agent': 'MemeStockAnalyzer/1.0'}
            )
            if resp.status_code != 200:
                return []

            data = resp.json()
            alerts = []
            for sym in data.get('symbols', [])[:30]:
                ticker = sym.get('symbol', '')
                if ticker and len(ticker) <= 5:
                    alerts.append({
                        'ticker': ticker,
                        'title': sym.get('title', ''),
                        'source': 'stocktwits',
                        'buzz_score': 50,
                        'sentiment': 'NEUTRAL',
                        'sentiment_score': 0,
                    })
            return alerts
        except Exception:
            return []

    def scan_all(self, penny_tickers: List[str] = None) -> List[Dict]:
        """全SNSソースをスキャンして統合"""
        reddit = self.scan_reddit()
        stocktwits = self.scan_stocktwits()

        # 統合
        combined = {}
        for a in reddit:
            t = a['ticker']
            combined[t] = a

        for a in stocktwits:
            t = a['ticker']
            if t in combined:
                combined[t]['buzz_score'] = min(combined[t]['buzz_score'] + 30, 100)
                combined[t]['on_stocktwits'] = True
            else:
                combined[t] = a

        # penny stocksフィルタ（$5以下のみ関心あり）
        result = list(combined.values())
        if penny_tickers:
            penny_set = set(penny_tickers)
            result = [a for a in result if a['ticker'] in penny_set]

        result.sort(key=lambda x: x.get('buzz_score', 0), reverse=True)
        print(f"[Social] Combined alerts: {len(result)} (penny filtered)")
        return result

    def update_trending(self):
        """Reddit注目銘柄を更新（24時間稼働用、アラートは出さない）

        市場時間外でもRedditをスキャンし、話題の銘柄リストを更新する。
        市場スキャン時にこのリストと出来高を照合してSOCIAL_VOLUMEを生成する。
        """
        reddit = self.scan_reddit()
        stocktwits = self.scan_stocktwits()

        trending = {}
        for a in reddit:
            t = a['ticker']
            if a.get('buzz_score', 0) >= 30:
                trending[t] = {
                    'buzz_score': a['buzz_score'],
                    'sentiment': a.get('sentiment', 'NEUTRAL'),
                    'sentiment_score': a.get('sentiment_score', 0),
                    'mention_count': a.get('mention_count', 0),
                    'subreddits': a.get('subreddits', []),
                    'has_dd': a.get('has_dd', False),
                    'is_trending': a.get('is_trending', False),
                    'top_posts': a.get('top_posts', []),
                    'source': 'reddit',
                }
        for a in stocktwits:
            t = a['ticker']
            if t in trending:
                trending[t]['buzz_score'] = min(trending[t]['buzz_score'] + 30, 100)
                trending[t]['on_stocktwits'] = True
            elif a.get('buzz_score', 0) >= 40:
                trending[t] = {
                    'buzz_score': a['buzz_score'],
                    'sentiment': 'NEUTRAL',
                    'source': 'stocktwits',
                }

        self._trending_tickers = trending
        self._trending_updated_at = datetime.now(JST).strftime('%Y-%m-%d %H:%M:%S')
        print(f"[Social24h] Trending updated: {len(trending)} tickers")
        return trending

    def get_trending(self) -> dict:
        """現在のReddit注目銘柄を取得"""
        return {
            'tickers': self._trending_tickers,
            'updated_at': self._trending_updated_at,
            'count': len(self._trending_tickers),
        }

    def is_trending(self, ticker: str) -> dict:
        """指定銘柄がRedditで話題かどうかを返す"""
        return self._trending_tickers.get(ticker)

    # 否定パターン（"not bullish" → bearish扱い）
    NEGATION_WORDS = ['not', "don't", "doesn't", "won't", "isn't", "aren't",
                      'no', 'never', 'neither', 'nobody', 'nothing',
                      'barely', 'hardly', 'rarely', 'seldom']

    def _analyze_sentiment(self, text: str) -> Dict:
        """テキストのセンチメントを分析（否定文対応）"""
        text_lower = text.lower()

        bullish_count = 0
        bearish_count = 0

        # キーワードマッチ（否定文チェック付き）
        for kw in self.BULLISH_KEYWORDS:
            if kw in text_lower:
                # 否定語が直前にあるか確認
                if self._is_negated(text_lower, kw):
                    bearish_count += 1  # 否定された強気 = 弱気
                else:
                    bullish_count += 1

        for kw in self.BEARISH_KEYWORDS:
            if kw in text_lower:
                if self._is_negated(text_lower, kw):
                    bullish_count += 1  # 否定された弱気 = 強気
                else:
                    bearish_count += 1

        # 絵文字マッチ
        for emoji in self.BULLISH_EMOJIS:
            bullish_count += text.count(emoji)

        for emoji in self.BEARISH_EMOJIS:
            bearish_count += text.count(emoji)

        # スコア計算（-1.0 ~ +1.0）
        total = bullish_count + bearish_count
        if total == 0:
            score = 0.0
            label = 'NEUTRAL'
        else:
            score = (bullish_count - bearish_count) / total
            if score >= 0.3:
                label = 'BULLISH'
            elif score <= -0.3:
                label = 'BEARISH'
            else:
                label = 'NEUTRAL'

        return {
            'score': round(score, 2),
            'label': label,
            'bullish_signals': bullish_count,
            'bearish_signals': bearish_count,
        }

    def _is_negated(self, text: str, keyword: str) -> bool:
        """キーワードの直前に否定語があるか確認"""
        idx = text.find(keyword)
        if idx <= 0:
            return False
        # キーワードの前の30文字をチェック
        prefix = text[max(0, idx - 30):idx].strip()
        words = prefix.split()
        # 直前の3単語以内に否定語があるか
        check_words = words[-3:] if len(words) >= 3 else words
        return any(neg in check_words for neg in self.NEGATION_WORDS)

    def _fetch_reddit_posts(self, subreddit: str, sort: str = 'hot',
                            limit: int = 30) -> list:
        """RedditのJSON APIから投稿を取得（認証不要）"""
        url = f'https://www.reddit.com/r/{subreddit}/{sort}.json'
        resp = requests.get(
            url,
            params={'limit': limit, 't': 'day'},
            headers={'User-Agent': 'MemeStockAnalyzer/1.0 (stock research bot)'},
            timeout=15,
        )
        if resp.status_code != 200:
            return []

        data = resp.json()
        posts = []
        for child in data.get('data', {}).get('children', []):
            post = child.get('data', {})
            posts.append({
                'id': post.get('id', ''),
                'title': post.get('title', ''),
                'selftext': post.get('selftext', ''),
                'score': post.get('score', 0),
                'num_comments': post.get('num_comments', 0),
                'created_utc': post.get('created_utc', 0),
            })
        return posts

    def _extract_tickers(self, text: str) -> List[str]:
        """テキストからティッカーシンボルを抽出（精度改善版）"""
        # $TICKER パターン（最も信頼性が高い）
        dollar_tickers = re.findall(r'\$([A-Z]{2,5})\b', text)

        # 文中のスタンドアロンティッカー（大文字2-5文字）
        standalone = re.findall(r'\b([A-Z]{2,5})\b', text)

        all_tickers = set()

        # $付きのものは2文字でも許可（最優先）
        for t in dollar_tickers:
            if t not in self.EXCLUDE_TICKERS:
                all_tickers.add(t)

        # standalone は3文字以上かつ除外リスト外
        for t in standalone:
            if len(t) >= 3 and t not in self.EXCLUDE_TICKERS:
                # 追加フィルタ: 全て同じ文字（AAA, BBBなど）は除外
                if len(set(t)) > 1:
                    all_tickers.add(t)

        return list(all_tickers)
