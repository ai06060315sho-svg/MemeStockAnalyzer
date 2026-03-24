#!/bin/bash
# VPS初回セットアップ: GitHubからクローンして自動更新を設定
# ブラウザSSHで1回だけ実行すればOK

set -e

REPO_URL="https://github.com/ai06060315sho-svg/MemeStockAnalyzer.git"
APP_DIR="/home/memestock/meme-stock"
SERVICE_NAME="memestock"

echo "=== Git デプロイ セットアップ ==="

# 1. gitがなければインストール
if ! command -v git &> /dev/null; then
    sudo apt install -y git
fi

# 2. 既存のファイルをバックアップ
if [ -d "$APP_DIR" ] && [ ! -d "$APP_DIR/.git" ]; then
    echo "既存ファイルをバックアップ中..."
    # .envとDBをバックアップ
    cp "$APP_DIR/.env" /tmp/memestock_env_backup 2>/dev/null || true
    cp "$APP_DIR/meme_stocks.db" /tmp/memestock_db_backup 2>/dev/null || true
    cp "$APP_DIR/ml_model.pkl" /tmp/memestock_ml_backup 2>/dev/null || true

    # 古いディレクトリをリネーム
    mv "$APP_DIR" "${APP_DIR}_old_$(date +%Y%m%d)"
fi

# 3. GitHubからクローン
if [ ! -d "$APP_DIR/.git" ]; then
    echo "GitHubからクローン中..."
    git clone "$REPO_URL" "$APP_DIR"

    # バックアップを復元
    cp /tmp/memestock_env_backup "$APP_DIR/.env" 2>/dev/null || true
    cp /tmp/memestock_db_backup "$APP_DIR/meme_stocks.db" 2>/dev/null || true
    cp /tmp/memestock_ml_backup "$APP_DIR/ml_model.pkl" 2>/dev/null || true
fi

# 4. venvがなければ作成
if [ ! -d "$APP_DIR/venv" ]; then
    echo "Python仮想環境を作成中..."
    cd "$APP_DIR"
    python3.11 -m venv venv
    source venv/bin/activate
    pip install --upgrade pip
    pip install flask flask-socketio yfinance requests pandas
    pip install python-dotenv google-genai deep-translator
    pip install scikit-learn numpy
fi

# 5. 自動更新スクリプトを配置
cat > "$APP_DIR/deploy/auto_update.sh" << 'SCRIPT'
#!/bin/bash
# 自動更新スクリプト（cronから実行される）
APP_DIR="/home/memestock/meme-stock"
LOG="/home/memestock/update.log"

cd "$APP_DIR"

# GitHubから最新を取得
git fetch origin main 2>> "$LOG"

# ローカルとリモートの差分を確認
LOCAL=$(git rev-parse HEAD)
REMOTE=$(git rev-parse origin/main)

if [ "$LOCAL" != "$REMOTE" ]; then
    echo "$(date): 更新を検出 - デプロイ中..." >> "$LOG"

    # 最新コードを取得
    git pull origin main >> "$LOG" 2>&1

    # 依存パッケージを更新（requirements.txtがあれば）
    if [ -f requirements.txt ]; then
        source venv/bin/activate
        pip install -r requirements.txt >> "$LOG" 2>&1
    fi

    # サービス再起動
    sudo systemctl restart memestock
    echo "$(date): デプロイ完了!" >> "$LOG"
else
    echo "$(date): 変更なし" >> "$LOG"
fi
SCRIPT

chmod +x "$APP_DIR/deploy/auto_update.sh"

# 6. cronジョブを設定（10分ごとに自動更新チェック）
CRON_JOB="*/10 * * * * $APP_DIR/deploy/auto_update.sh"
(crontab -l 2>/dev/null | grep -v "auto_update.sh"; echo "$CRON_JOB") | crontab -

# 7. sudoパスワードなしでサービス再起動できるようにする
SUDOERS_LINE="memestock ALL=(ALL) NOPASSWD: /usr/bin/systemctl restart memestock, /usr/bin/systemctl stop memestock, /usr/bin/systemctl start memestock"
echo "$SUDOERS_LINE" | sudo tee /etc/sudoers.d/memestock > /dev/null

echo ""
echo "=== セットアップ完了! ==="
echo ""
echo "自動更新: 10分ごとにGitHubをチェック"
echo "更新ログ: /home/memestock/update.log"
echo ""
echo "手動で今すぐ更新するには:"
echo "  bash $APP_DIR/deploy/auto_update.sh"
echo ""
echo "データをリセットするには:"
echo "  bash $APP_DIR/deploy/reset_data.sh"
