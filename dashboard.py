# =============================================================================
# dashboard.py
# PURPOSE: A simple Flask web dashboard that reads from crypto_prices.db
# and displays live prices + a chart in the browser.
#
# Usage:
#   python dashboard.py
# Then open: http://127.0.0.1:5000
# =============================================================================

from flask import Flask, render_template_string
import sqlite3
import json
import os

app = Flask(__name__)

DB_PATH = "crypto_prices.db"

# -----------------------------------------------------------------------------
# Coin emoji map — just for fun visual flair in the dashboard
# -----------------------------------------------------------------------------
COIN_EMOJI = {
    "bitcoin":  "₿",
    "ethereum": "Ξ",
    "solana":   "◎",
    "cardano":  "₳",
    "dogecoin": "Ð",
}

# -----------------------------------------------------------------------------
# HTML Template — all-in-one, no separate files needed
# Dark terminal-inspired aesthetic with monospace type and sharp green accents
# -----------------------------------------------------------------------------
HTML_TEMPLATE = """
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="UTF-8" />
  <meta name="viewport" content="width=device-width, initial-scale=1.0"/>
  <title>Crypto ETL Dashboard</title>
  <link href="https://fonts.googleapis.com/css2?family=Share+Tech+Mono&family=Rajdhani:wght@400;600;700&display=swap" rel="stylesheet"/>
  <script src="https://cdnjs.cloudflare.com/ajax/libs/Chart.js/4.4.1/chart.umd.min.js"></script>
  <style>
    :root {
      --bg:        #0a0c0f;
      --surface:   #111417;
      --border:    #1e2329;
      --green:     #00ff88;
      --green-dim: #00cc6a;
      --red:       #ff4466;
      --text:      #c8d0d8;
      --muted:     #4a5568;
      --mono:      'Share Tech Mono', monospace;
      --display:   'Rajdhani', sans-serif;
    }

    *, *::before, *::after { box-sizing: border-box; margin: 0; padding: 0; }

    body {
      background: var(--bg);
      color: var(--text);
      font-family: var(--mono);
      min-height: 100vh;
      padding: 2rem;
      /* Subtle scanline texture */
      background-image: repeating-linear-gradient(
        0deg,
        transparent,
        transparent 2px,
        rgba(0,255,136,0.015) 2px,
        rgba(0,255,136,0.015) 4px
      );
    }

    /* ── Header ── */
    header {
      display: flex;
      align-items: baseline;
      gap: 1.5rem;
      margin-bottom: 2.5rem;
      padding-bottom: 1rem;
      border-bottom: 1px solid var(--border);
      animation: fadeDown 0.5s ease both;
    }

    header h1 {
      font-family: var(--display);
      font-weight: 700;
      font-size: 1.8rem;
      letter-spacing: 0.08em;
      text-transform: uppercase;
      color: #fff;
    }

    header h1 span { color: var(--green); }

    .status-pill {
      font-size: 0.72rem;
      letter-spacing: 0.12em;
      padding: 0.25rem 0.75rem;
      border: 1px solid var(--green);
      color: var(--green);
      border-radius: 2px;
      text-transform: uppercase;
      animation: pulse 2.5s ease infinite;
    }

    .run-time {
      margin-left: auto;
      font-size: 0.72rem;
      color: var(--muted);
    }

    /* ── Stat cards row ── */
    .cards {
      display: grid;
      grid-template-columns: repeat(auto-fit, minmax(180px, 1fr));
      gap: 1rem;
      margin-bottom: 2rem;
    }

    .card {
      background: var(--surface);
      border: 1px solid var(--border);
      border-top: 2px solid var(--green);
      padding: 1.1rem 1.25rem;
      animation: fadeUp 0.5s ease both;
    }

    .card:nth-child(1) { animation-delay: 0.05s; }
    .card:nth-child(2) { animation-delay: 0.10s; }
    .card:nth-child(3) { animation-delay: 0.15s; }
    .card:nth-child(4) { animation-delay: 0.20s; }
    .card:nth-child(5) { animation-delay: 0.25s; }

    .card-label {
      font-size: 0.68rem;
      letter-spacing: 0.12em;
      color: var(--muted);
      text-transform: uppercase;
      margin-bottom: 0.5rem;
    }

    .card-symbol {
      font-family: var(--display);
      font-size: 1rem;
      font-weight: 600;
      color: var(--green);
      margin-bottom: 0.2rem;
    }

    .card-price {
      font-size: 1.35rem;
      color: #fff;
      letter-spacing: 0.02em;
    }

    .card-change {
      font-size: 0.8rem;
      margin-top: 0.4rem;
    }

    .pos { color: var(--green); }
    .neg { color: var(--red); }

    /* ── Two-column layout: table + chart ── */
    .grid {
      display: grid;
      grid-template-columns: 1fr 1fr;
      gap: 1.5rem;
      animation: fadeUp 0.5s 0.3s ease both;
    }

    @media (max-width: 900px) {
      .grid { grid-template-columns: 1fr; }
    }

    .panel {
      background: var(--surface);
      border: 1px solid var(--border);
    }

    .panel-header {
      padding: 0.75rem 1.25rem;
      border-bottom: 1px solid var(--border);
      font-size: 0.72rem;
      letter-spacing: 0.15em;
      text-transform: uppercase;
      color: var(--muted);
    }

    /* ── Table ── */
    table {
      width: 100%;
      border-collapse: collapse;
      font-size: 0.82rem;
    }

    thead th {
      padding: 0.6rem 1rem;
      text-align: left;
      font-size: 0.65rem;
      letter-spacing: 0.12em;
      color: var(--muted);
      text-transform: uppercase;
      border-bottom: 1px solid var(--border);
    }

    tbody tr {
      border-bottom: 1px solid var(--border);
      transition: background 0.15s;
    }

    tbody tr:last-child { border-bottom: none; }
    tbody tr:hover { background: rgba(0,255,136,0.04); }

    tbody td {
      padding: 0.7rem 1rem;
      color: var(--text);
    }

    td.name-cell {
      color: #fff;
      font-family: var(--display);
      font-weight: 600;
      font-size: 0.9rem;
    }

    td.price-cell { color: #fff; }

    /* ── Chart ── */
    .chart-wrap {
      padding: 1.25rem;
      height: 280px;
      position: relative;
    }

    /* ── Footer ── */
    footer {
      margin-top: 2rem;
      font-size: 0.68rem;
      color: var(--muted);
      letter-spacing: 0.08em;
      text-align: center;
      animation: fadeUp 0.5s 0.45s ease both;
    }

    footer a {
      color: var(--green-dim);
      text-decoration: none;
    }

    /* ── No data state ── */
    .empty {
      padding: 3rem;
      text-align: center;
      color: var(--muted);
      font-size: 0.85rem;
      line-height: 1.9;
    }

    .empty strong { color: var(--green); display: block; font-size: 1rem; margin-bottom: 0.5rem; }

    /* ── Animations ── */
    @keyframes fadeDown {
      from { opacity: 0; transform: translateY(-12px); }
      to   { opacity: 1; transform: translateY(0); }
    }

    @keyframes fadeUp {
      from { opacity: 0; transform: translateY(10px); }
      to   { opacity: 1; transform: translateY(0); }
    }

    @keyframes pulse {
      0%, 100% { opacity: 1; }
      50%       { opacity: 0.4; }
    }
  </style>
</head>
<body>

<header>
  <h1>Crypto <span>ETL</span> Dashboard</h1>
  {% if coins %}
    <span class="status-pill">● Live</span>
    <span class="run-time">Last run: {{ last_run }}</span>
  {% endif %}
</header>

{% if not coins %}
  <!-- Empty state: shown before the first pipeline run -->
  <div class="panel">
    <div class="empty">
      <strong>No data yet.</strong>
      Run the pipeline first, then refresh this page.<br>
      <code style="color:var(--green-dim)">python main.py</code>
    </div>
  </div>

{% else %}
  <!-- ── Stat cards ── -->
  <div class="cards">
    {% for c in coins %}
    <div class="card">
      <div class="card-label">{{ c.symbol }}</div>
      <div class="card-symbol">{{ c.glyph }} {{ c.name }}</div>
      <div class="card-price">${{ c.price_fmt }}</div>
      <div class="card-change {{ 'pos' if c.change >= 0 else 'neg' }}">
        {{ '+' if c.change >= 0 else '' }}{{ "%.2f"|format(c.change) }}% (24h)
      </div>
    </div>
    {% endfor %}
  </div>

  <!-- ── Table + Chart ── -->
  <div class="grid">

    <!-- Left: full data table -->
    <div class="panel">
      <div class="panel-header">Market Data</div>
      <table>
        <thead>
          <tr>
            <th>Coin</th>
            <th>Price (USD)</th>
            <th>24h Change</th>
            <th>Market Cap</th>
            <th>Volume</th>
          </tr>
        </thead>
        <tbody>
          {% for c in coins %}
          <tr>
            <td class="name-cell">{{ c.glyph }} {{ c.name }}</td>
            <td class="price-cell">${{ c.price_fmt }}</td>
            <td class="{{ 'pos' if c.change >= 0 else 'neg' }}">
              {{ '+' if c.change >= 0 else '' }}{{ "%.2f"|format(c.change) }}%
            </td>
            <td>${{ c.mcap_fmt }}</td>
            <td>${{ c.vol_fmt }}</td>
          </tr>
          {% endfor %}
        </tbody>
      </table>
    </div>

    <!-- Right: bar chart of current prices -->
    <div class="panel">
      <div class="panel-header">Current Price (USD)</div>
      <div class="chart-wrap">
        <canvas id="priceChart"></canvas>
      </div>
    </div>

  </div>
{% endif %}

<footer>
  Data sourced from <a href="https://www.coingecko.com" target="_blank">CoinGecko</a>
  &nbsp;·&nbsp; Stored in SQLite &nbsp;·&nbsp; Built with Python + Flask
</footer>

{% if coins %}
<script>
  const labels = {{ chart_labels | safe }};
  const prices = {{ chart_prices | safe }};
  const changes = {{ chart_changes | safe }};

  // Color bars green/red based on 24h change
  const colors = changes.map(v => v >= 0 ? 'rgba(0,255,136,0.75)' : 'rgba(255,68,102,0.75)');
  const borders = changes.map(v => v >= 0 ? '#00ff88' : '#ff4466');

  const ctx = document.getElementById('priceChart').getContext('2d');
  new Chart(ctx, {
    type: 'bar',
    data: {
      labels,
      datasets: [{
        label: 'Price (USD)',
        data: prices,
        backgroundColor: colors,
        borderColor: borders,
        borderWidth: 1,
        borderRadius: 2,
      }]
    },
    options: {
      responsive: true,
      maintainAspectRatio: false,
      plugins: {
        legend: { display: false },
        tooltip: {
          callbacks: {
            label: ctx => ' $' + ctx.parsed.y.toLocaleString()
          }
        }
      },
      scales: {
        x: {
          ticks: { color: '#4a5568', font: { family: 'Share Tech Mono', size: 11 } },
          grid: { color: '#1e2329' }
        },
        y: {
          ticks: {
            color: '#4a5568',
            font: { family: 'Share Tech Mono', size: 11 },
            callback: v => '$' + v.toLocaleString()
          },
          grid: { color: '#1e2329' }
        }
      }
    }
  });
</script>
{% endif %}

</body>
</html>
"""


def fmt_large(n):
    """Format large numbers: 1_200_000_000 → '1.20B'"""
    if n is None:
        return "—"
    if n >= 1_000_000_000:
        return f"{n/1_000_000_000:.2f}B"
    if n >= 1_000_000:
        return f"{n/1_000_000:.2f}M"
    return f"{n:,.0f}"


def fmt_price(p):
    """Format price: 65432.1 → '65,432.10', 0.085 → '0.0850'"""
    if p is None:
        return "—"
    if p >= 1:
        return f"{p:,.2f}"
    return f"{p:.4f}"


@app.route("/")
def index():
    # If database doesn't exist yet, show empty state
    if not os.path.exists(DB_PATH):
        return render_template_string(HTML_TEMPLATE, coins=[], last_run="")

    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row

    # Fetch latest snapshot
    rows = conn.execute("""
        SELECT coin_id, symbol, name, current_price,
               market_cap, total_volume, price_change_percentage_24h, extracted_at
        FROM crypto_prices
        WHERE extracted_at = (SELECT MAX(extracted_at) FROM crypto_prices)
        ORDER BY market_cap DESC
    """).fetchall()
    conn.close()

    if not rows:
        return render_template_string(HTML_TEMPLATE, coins=[], last_run="")

    last_run = rows[0]["extracted_at"]

    coins = []
    for r in rows:
        change = r["price_change_percentage_24h"] or 0.0
        coins.append({
            "coin_id":   r["coin_id"],
            "symbol":    r["symbol"],
            "name":      r["name"],
            "glyph":     COIN_EMOJI.get(r["coin_id"], "◈"),
            "price_fmt": fmt_price(r["current_price"]),
            "price_raw": r["current_price"],
            "mcap_fmt":  fmt_large(r["market_cap"]),
            "vol_fmt":   fmt_large(r["total_volume"]),
            "change":    change,
        })

    chart_labels  = json.dumps([c["name"] for c in coins])
    chart_prices  = json.dumps([c["price_raw"] for c in coins])
    chart_changes = json.dumps([c["change"] for c in coins])

    return render_template_string(
        HTML_TEMPLATE,
        coins=coins,
        last_run=last_run,
        chart_labels=chart_labels,
        chart_prices=chart_prices,
        chart_changes=chart_changes,
    )


if __name__ == "__main__":
    print("=" * 50)
    print("  CRYPTO ETL DASHBOARD")
    print("  Open in browser: http://127.0.0.1:5000")
    print("=" * 50)
    # debug=True means the server auto-reloads when you edit the file
    app.run(debug=True)
