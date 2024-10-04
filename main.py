from flask import Flask, redirect, request
import sqlite3

app = Flask(__name__)

# SQLiteデータベースに接続
def get_db_connection(db_name):
    conn = sqlite3.connect(db_name)
    conn.row_factory = sqlite3.Row
    print('Connected to SQLite database')
    return conn

# リンクデータベースの初期化（最初に実行する必要があります）
def init_links_db():
    conn = get_db_connection('links.db')
    conn.execute('CREATE TABLE IF NOT EXISTS links (id INTEGER PRIMARY KEY, slug TEXT, url TEXT, clicks INTEGER)')
    
    # リダイレクト先URLとスラグを登録
    urls = [
        ('redirect-01', 'https://example.com/page1'),
        ('redirect-02', 'https://example.com/page2')
    ]
    
    for slug, url in urls:
        conn.execute('INSERT INTO links (slug, url, clicks) VALUES (?, ?, ?)', (slug, url, 0))
    
    conn.commit()
    conn.close()

# クリックストリームデータベースの初期化（最初に実行する必要があります）
def init_click_stream_db():
    conn = get_db_connection('click_stream.db')
    conn.execute('CREATE TABLE IF NOT EXISTS click_stream (id INTEGER PRIMARY KEY, slug TEXT, ip_address TEXT, timestamp TEXT)')
    conn.commit()
    conn.close()

# リダイレクトリンクの作成とクリック数の記録
@app.route('/<slug>')
def redirect_link(slug):
    # リンクデータベースからリダイレクト先URLを取得
    conn = get_db_connection('links.db')
    links_data = conn.execute('SELECT * FROM links WHERE slug = ?', (slug,)).fetchone()
    
    if links_data is None:
        return "Invalid URL slug", 404

    # クリック数を記録
    conn.execute('UPDATE links SET clicks = clicks + 1 WHERE slug = ?', (slug,))
    conn.commit()
    conn.close()

    # クリックストリームデータベースにクリック情報を記録
    conn = get_db_connection('click_stream.db')
    conn.execute('INSERT INTO click_stream (slug, ip_address, timestamp) VALUES (?, ?, datetime("now"))', (slug, request.remote_addr))
    conn.commit()
    conn.close()

    # リダイレクト先に転送
    return redirect(links_data['url'])

# クリック数を表示するページ
@app.route('/stats/<slug>')
def show_stats(slug):
    conn = get_db_connection('links.db')
    links_data = conn.execute('SELECT * FROM links WHERE slug = ?', (slug,)).fetchone()
    
    if links_data is None:
        return "Invalid URL slug", 404

    return f"Slug: {links_data['slug']} - URL: {links_data['url']} - Clicks: {links_data['clicks']}"

if __name__ == '__main__':
    # 初回実行時にデータベースを初期化する場合は、以下を実行
    # init_links_db()
    # init_click_stream_db()

    app.run(debug=True, host='0.0.0.0', port=80)