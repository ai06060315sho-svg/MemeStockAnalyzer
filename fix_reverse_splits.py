"""併合判定の修正スクリプト
- 1日で+500%以上の異常な上昇のみREVERSE_SPLIT
- それ以外は本物の値動きとしてWIN判定
"""
import sqlite3

conn = sqlite3.connect('meme_stocks.db')

# まず全REVERSE_SPLITを正しい結果に復元
c1 = conn.execute("""UPDATE alert_results SET 
    result = CASE 
        WHEN max_gain_pct >= 25 THEN 'BIG_WIN'
        WHEN max_gain_pct >= 15 THEN 'WIN'
        WHEN max_gain_pct >= 10 THEN 'SMALL_WIN'
        ELSE 'LOSS'
    END
    WHERE result = 'REVERSE_SPLIT' 
    AND (change_1d_pct IS NULL OR change_1d_pct < 500)""").rowcount

# 1日で+500%以上はREVERSE_SPLIT（異常値）
c2 = conn.execute("UPDATE alert_results SET result='REVERSE_SPLIT' WHERE change_1d_pct >= 500 AND result != 'REVERSE_SPLIT'").rowcount

conn.commit()
wins = conn.execute("SELECT COUNT(*) FROM alert_results WHERE result IN ('WIN','BIG_WIN','SMALL_WIN')").fetchone()[0]
rs = conn.execute("SELECT COUNT(*) FROM alert_results WHERE result='REVERSE_SPLIT'").fetchone()[0]
print(f'WIN復元: {c1}件, 新規RS: {c2}件')
print(f'結果: WIN={wins} RS={rs}')
conn.close()
