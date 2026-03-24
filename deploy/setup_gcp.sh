#!/bin/bash
# Google Cloud Free VM セットアップスクリプト
# e2-micro (1コア/1GB RAM) 用に最適化

set -e

echo "=== MemeStockAnalyzer GCP Setup ==="

# 1. システム更新
sudo apt update && sudo apt upgrade -y

# 2. Python 3.11 + pip
sudo apt install -y python3.11 python3.11-venv python3-pip git

# 3. アプリ用ディレクトリ
mkdir -p ~/meme-stock
cd ~/meme-stock

echo "=== Pythonセットアップ ==="

# 4. 仮想環境
python3.11 -m venv venv
source venv/bin/activate

# 5. 依存パッケージ（メモリ節約: 1つずつインストール）
pip install --upgrade pip
pip install flask flask-socketio
pip install yfinance requests pandas
pip install python-dotenv
pip install google-genai
pip install deep-translator
pip install scikit-learn numpy

echo "=== セットアップ完了 ==="
echo "次のステップ:"
echo "1. アプリファイルを ~/meme-stock/ にアップロード"
echo "2. .env ファイルを作成"
echo "3. sudo cp memestock.service /etc/systemd/system/"
echo "4. sudo systemctl enable memestock"
echo "5. sudo systemctl start memestock"
