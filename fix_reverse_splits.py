"""併合銘柄を一括でREVERSE_SPLITに変更するスクリプト"""
import sqlite3

rs_tickers = ['ONCO','RDGT','UCAR','LNAI','PAVS','CMCT','LYRA','MGRX','SER','AZTR','HCWB','WNW','CREG','RBNE','UGRO','BIAF','TWAV','ATXG','HUBC','ORIS','FEED','NXTT','EVTV','QVCGA','LIDR','ORBS','DRMA','GMEX','DWSN','TOVX','TANH','PMAX','OMEX','DVLT','SUNE','CMTL','SOS','SBEV','VEEE','JTAI','GOCO','CYCU','NIXX','ASNS','QNTM','QRHC','MNDR','GNLN','JZ','ISPC','AMZE','BNGO','MLSS','LGVN','EZRA','RZLT','MNTS','IDN','BRTX','NBY','MAXN','EVTL','CHNR','OTLK','INAB','TPST','ADTX','VSA','TLPH','PMCB','FFAI','RPGL','EDBL','SLS','IPW','VBIX','BQ','ASBP','ERNA','PULM','SSP','RAYA','ACHV','BFRI','RETO','SCWO','ENSC','INM','LMFA','AIXI','PNBK','HGBL','IZEA','NUWE','LXRX','AVXL','YYAI','TRIB','CRIS','GROV','IDAI','BIVI','OCGN','COSM','YSG','SURG','AMTX','CISS','TNON','ELDN','YCBD','CLDI','GV','NOTE','ACON','HURA','EP','VUZI','ELAB']

conn = sqlite3.connect('meme_stocks.db')
total = 0
for t in rs_tickers:
    c = conn.execute("UPDATE alert_results SET result='REVERSE_SPLIT' WHERE ticker=? AND result IN ('WIN','BIG_WIN','SMALL_WIN')", (t,)).rowcount
    if c > 0: total += c
    conn.execute('UPDATE stock_alerts SET has_reverse_split=1 WHERE ticker=?', (t,))
conn.commit()
print(f'{total}件をREVERSE_SPLITに変更')
wins = conn.execute("SELECT COUNT(*) FROM alert_results WHERE result IN ('WIN','BIG_WIN','SMALL_WIN')").fetchone()[0]
print(f'修正後WIN: {wins}件')
conn.close()
