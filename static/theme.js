// テーマ切り替え（ライト/ダーク）
// デフォルト: ライト
(function() {
    const STORAGE_KEY = 'meme-stock-theme';

    function getTheme() {
        return localStorage.getItem(STORAGE_KEY) || 'dark';
    }

    function setTheme(theme) {
        localStorage.setItem(STORAGE_KEY, theme);
        document.documentElement.setAttribute('data-theme', theme);
    }

    function toggleTheme() {
        const current = getTheme();
        setTheme(current === 'dark' ? 'light' : 'dark');
    }

    // 初期化
    setTheme(getTheme());

    // グローバルに公開
    window.toggleTheme = toggleTheme;
    window.getTheme = getTheme;
})();
