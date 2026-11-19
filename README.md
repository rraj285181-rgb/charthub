# ChartView Pro (charthub) 📈

> High-Performance Multi-Chart Trading & Technical Analysis Terminal

An ultra-fast, zero-dependency financial charting terminal inspired by TradingView, built with vanilla HTML5 Canvas, modern JavaScript, and Node.js.

## ✨ Features

- **Multi-Chart Grid Layouts**: 10 flexible layouts (single chart up to 10 split charts simultaneously).
- **Live Crypto Data**: Real-time streaming candlestick charts & orderbook via Binance WebSocket.
- **Global Stocks & Indices**: NSE India, US Stocks, Forex, Commodities, World Indices, Bonds.
- **Custom CSV Mode**: 1-click CSV file upload with automated timestamp and OHLCV parsing.
- **Volume Profile Visible Range (VPVR) & POC**: Dynamic horizontal volume profile with Point of Control marker.
- **Multi-Timeframe (MTF) Confluence Ribbon**: Live trend agreement across 6 timeframes (1m, 5m, 15m, 1h, 4h, 1D).
- **PineScript / Custom Formula Console**: In-browser code editor with presets (EMA Cross, RSI Signals, SuperTrend, Donchian) and direct canvas plotting.
- **Pro HD Snapshot & Analysis Report**: Export watermarked HD PNG charts and printable technical analysis summary.
- **Interactive Technical Indicators**: SMA, EMA (9/21/50), Bollinger Bands, RSI, MACD, Stochastic RSI, ATR, SuperTrend, Volume.
- **OLED Dark Theme**: Professional TradingView-style dark interface.

## 🚀 Quick Start Locally

### Prerequisites
- Node.js (v18 or higher recommended)

### Run
```bash
git clone https://github.com/rraj285181-rgb/charthub.git
cd charthub
npm start
```
Open [http://localhost:3000](http://localhost:3000) in your browser.

---

## 🌐 Hosting Options

### 1. GitHub Pages (Static Hosting)
1. Go to repository **Settings** -> **Pages**.
2. Select **Source**: `Deploy from a branch`.
3. Select Branch: `main`, Folder: `/ (root)`.
4. Click **Save**.
Your live site will be at: `https://rraj285181-rgb.github.io/charthub/`

### 2. Vercel (1-Click Full-Stack Hosting)
1. Import repository on [Vercel](https://vercel.com).
2. Click **Deploy**.

### 3. Render / Railway
1. Create a new Web Service pointing to `https://github.com/rraj285181-rgb/charthub`.
2. Set Build Command: *(leave empty)*
3. Set Start Command: `node server.js`

---

## 📄 License
MIT License.
