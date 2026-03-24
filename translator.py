"""セクター・業種・事業内容の日本語翻訳"""
import os
import hashlib
import json
from pathlib import Path

# セクター翻訳マップ
SECTOR_MAP = {
    'Technology': 'テクノロジー',
    'Healthcare': 'ヘルスケア',
    'Financial Services': '金融サービス',
    'Consumer Cyclical': '一般消費財',
    'Consumer Defensive': '生活必需品',
    'Communication Services': '通信サービス',
    'Industrials': '資本財・産業',
    'Energy': 'エネルギー',
    'Basic Materials': '素材',
    'Real Estate': '不動産',
    'Utilities': '公益事業',
}

# 業種翻訳マップ（主要なもの）
INDUSTRY_MAP = {
    'Internet Retail': 'ネット通販',
    'Software - Application': 'ソフトウェア',
    'Software - Infrastructure': 'インフラソフトウェア',
    'Semiconductors': '半導体',
    'Semiconductor Equipment & Materials': '半導体装置・材料',
    'Drug Manufacturers - General': '医薬品メーカー',
    'Drug Manufacturers - Specialty & Generic': '医薬品（ジェネリック）',
    'Drug Manufacturers': '医薬品メーカー',
    'Biotechnology': 'バイオテクノロジー',
    'Medical Devices': '医療機器',
    'Diagnostics & Research': '診断・研究',
    'Auto Manufacturers': '自動車メーカー',
    'Electric Vehicles': 'EV（電気自動車）',
    'Farm & Heavy Construction Machinery': '重機・建設機械',
    'Entertainment': 'エンターテインメント',
    'Electronic Gaming & Multimedia': 'ゲーム・マルチメディア',
    'Internet Content & Information': 'ネットコンテンツ',
    'Advertising Agencies': '広告代理店',
    'Banks - Regional': '地方銀行',
    'Banks - Diversified': '総合銀行',
    'Capital Markets': '資本市場',
    'Insurance': '保険',
    'Oil & Gas E&P': '石油・ガス（探査・生産）',
    'Oil & Gas Integrated': '石油・ガス（統合）',
    'Solar': '太陽光発電',
    'Uranium': 'ウラン',
    'Gold': '金',
    'Silver': '銀',
    'Copper': '銅',
    'Lithium': 'リチウム',
    'Cannabis': '大麻関連',
    'Marijuana': '大麻関連',
    'Aerospace & Defense': '航空宇宙・防衛',
    'Airlines': '航空会社',
    'Shipping & Ports': '海運・港湾',
    'Trucking': 'トラック運送',
    'Railroads': '鉄道',
    'REIT - Residential': 'REIT（住宅）',
    'REIT - Retail': 'REIT（商業施設）',
    'REIT - Office': 'REIT（オフィス）',
    'Restaurants': '飲食店',
    'Apparel Retail': 'アパレル小売',
    'Home Improvement Retail': 'ホームセンター',
    'Specialty Retail': '専門小売',
    'Gambling': 'ギャンブル',
    'Resorts & Casinos': 'リゾート・カジノ',
    'Security & Protection Services': 'セキュリティサービス',
    'Information Technology Services': 'ITサービス',
    'Electronic Components': '電子部品',
    'Scientific & Technical Instruments': '科学計測機器',
    'Packaged Foods': '加工食品',
    'Beverages - Non-Alcoholic': '飲料（ノンアルコール）',
    'Beverages - Alcoholic': '飲料（アルコール）',
    'Household & Personal Products': '家庭用品',
    'Telecom Services': '通信サービス',
    'Utilities - Regulated Electric': '電力（規制下）',
    'Renewable Energy': '再生可能エネルギー',
    'Waste Management': '廃棄物処理',
    'Staffing & Employment Services': '人材サービス',
    'Consulting Services': 'コンサルティング',
    'Engineering & Construction': 'エンジニアリング・建設',
    'Specialty Chemicals': '特殊化学品',
    'Agricultural Inputs': '農業資材',
    'Steel': '鉄鋼',
    'Aluminum': 'アルミニウム',
    # NASDAQ API 業種名
    'Biotechnology: Pharmaceutical Preparations': 'バイオ医薬品',
    'Biotechnology: Biological Products (No Diagnostic Substances)': 'バイオ製品',
    'Biotechnology: In Vitro & In Vivo Diagnostic Substances': 'バイオ診断',
    'Biotechnology: Electromedical & Electrotherapeutic Apparatus': 'バイオ医療機器',
    'Computer Software: Prepackaged Software': 'ソフトウェア',
    'Computer Software: Programming Data Processing': 'データ処理・プログラミング',
    'EDP Services': 'IT・データサービス',
    'Blank Checks': 'SPAC（特別買収目的会社）',
    'Industrial Machinery/Components': '産業機械・部品',
    'Finance: Consumer Services': '消費者金融サービス',
    'Medical/Dental Instruments': '医療・歯科機器',
    'Other Consumer Services': 'その他消費者サービス',
    'Telecommunications Equipment': '通信機器',
    'Real Estate Investment Trusts': '不動産投資信託（REIT）',
    'Oil & Gas Production': '石油・ガス生産',
    'Major Chemicals': '化学大手',
    'Precious Metals': '貴金属',
    'Industrial Specialties': '産業特殊品',
    'Services-Misc. Amusement & Recreation': 'レジャー・娯楽サービス',
    'Auto Manufacturing': '自動車製造',
    'Auto Parts:O.E.M.': '自動車部品（OEM）',
    'Electric Vehicles': 'EV（電気自動車）',
    'Aerospace & Defense': '航空宇宙・防衛',
    'Space Research & Technology': '宇宙開発・技術',
    'Artificial Intelligence': 'AI（人工知能）',
    'Computer Communications Equipment': 'ネットワーク機器',
    'Electronic Components': '電子部品',
    'Semiconductors': '半導体',
    'Metal Mining': '金属鉱業',
    'Coal Mining': '石炭鉱業',
    'Retail: Building Materials': '建材小売',
    'Retail-Eating Places': '飲食チェーン',
    'Retail-Drug Stores and Proprietary Stores': 'ドラッグストア',
    'Catalog/Specialty Distribution': '専門流通',
    'Radio And Television Broadcasting And Communications Equipment': '放送・通信機器',
    'Movies & Entertainment': '映画・エンタメ',
    'Services-Computer Programming, Data Processing, Etc.': 'IT・プログラミング',
    'Natural Gas Distribution': '天然ガス供給',
    'Trucking Freight/Courier Services': 'トラック・宅配',
    'Air Freight/Delivery Services': '航空貨物',
    'Marine Transportation': '海上輸送',
    'Hotels/Resorts': 'ホテル・リゾート',
    'Hospital/Nursing Management': '病院・介護',
    'Farming/Seeds/Milling': '農業・種子',
    'Forest Products': '林業製品',
    'Containers/Packaging': '容器・包装',
    'Textiles': '繊維',
    'Shoe Manufacturing': '靴製造',
    'Building Products': '建設資材',
    'Homebuilding': '住宅建設',
    'Miscellaneous Manufacturing': '各種製造',
    'Military/Government/Technical': '軍事・政府技術',
    'Water Supply': '水道',
    'Power Generation': '発電',
    'Biotechnology: Commercial Physical & Biological Resarch': 'バイオ研究',
    'Ophthalmic Goods': '眼科用品',
    'Diversified Financial Services': '総合金融',
    'Investment Bankers/Brokers/Service': '投資銀行・証券',
    'Savings Institutions': '貯蓄金融機関',
    'Property-Casualty Insurers': '損害保険',
    'Life Insurance': '生命保険',
    'Major Banks': '大手銀行',
    'Regional Banks': '地方銀行',
    'Consumer Electronics/Appliances': '家電・電化製品',
    'Business Services': 'ビジネスサービス',
    'Mining & Quarrying of Nonmetallic Minerals (No Fuels)': '非金属鉱業',
    'Multi-Sector Companies': '複合企業',
    'Environmental Services': '環境サービス',
    'Specialty Insurers': '専門保険',
    'Metal Fabrications': '金属加工',
    'Electrical Products': '電気製品',
    'Specialty Foods': '特殊食品',
}

# 翻訳キャッシュディレクトリ
CACHE_DIR = Path(__file__).parent / '.translation_cache'


def translate_sector(sector: str) -> str:
    """セクター名を日本語に翻訳"""
    if not sector:
        return ''
    return SECTOR_MAP.get(sector, sector)


def translate_industry(industry: str) -> str:
    """業種名を日本語に翻訳"""
    if not industry:
        return ''
    return INDUSTRY_MAP.get(industry, industry)


def translate_summary(text: str, ticker: str = '') -> str:
    """事業内容をGoogle翻訳で日本語に翻訳（無料・制限なし・キャッシュ付き）"""
    if not text:
        return ''

    # キャッシュチェック
    CACHE_DIR.mkdir(exist_ok=True)
    cache_key = hashlib.md5(text[:200].encode()).hexdigest()
    cache_file = CACHE_DIR / f'{ticker or cache_key}.json'

    if cache_file.exists():
        try:
            cached = json.loads(cache_file.read_text(encoding='utf-8'))
            return cached.get('translation', text)
        except Exception:
            pass

    # Google翻訳（無料・APIキー不要）
    try:
        from deep_translator import GoogleTranslator
        translator = GoogleTranslator(source='en', target='ja')

        # deep_translatorは5000文字制限があるため分割
        if len(text) > 4500:
            text = text[:4500]

        translation = translator.translate(text)

        # キャッシュ保存
        cache_file.write_text(
            json.dumps({'ticker': ticker, 'translation': translation}, ensure_ascii=False),
            encoding='utf-8')

        return translation

    except Exception as e:
        print(f"[Translator] Translation error: {e}")
        return text
