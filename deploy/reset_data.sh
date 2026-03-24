#!/bin/bash
# データベースをリセットして新しいデータ収集を開始
# 古いDBはバックアップしてから削除

APP_DIR="/home/memestock/meme-stock"
DB_FILE="$APP_DIR/meme_stocks.db"
BACKUP_DIR="$APP_DIR/backups"

echo "=== データリセット ==="

# 1. サービスを停止
echo "サービスを停止中..."
sudo systemctl stop memestock

# 2. バックアップ
mkdir -p "$BACKUP_DIR"
if [ -f "$DB_FILE" ]; then
    BACKUP_NAME="meme_stocks_reset_$(date +%Y%m%d_%H%M%S).db"
    cp "$DB_FILE" "$BACKUP_DIR/$BACKUP_NAME"
    echo "バックアップ: $BACKUP_DIR/$BACKUP_NAME"

    # 古いDBを削除
    rm "$DB_FILE"
    echo "古いデータベースを削除しました"
fi

# 3. MLモデルもリセット（オプション）
if [ -f "$APP_DIR/ml_model.pkl" ]; then
    cp "$APP_DIR/ml_model.pkl" "$BACKUP_DIR/ml_model_$(date +%Y%m%d).pkl"
    rm "$APP_DIR/ml_model.pkl"
    echo "MLモデルもリセットしました"
fi

# 4. サービスを再起動（新しいDBが自動作成される）
echo "サービスを再起動中..."
sudo systemctl start memestock

echo ""
echo "=== リセット完了! ==="
echo "新しいデータベースが作成され、データ収集が開始されました"
echo "バックアップは $BACKUP_DIR に保存されています"
