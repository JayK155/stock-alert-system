# app.py
import json
import time as tm
from datetime import datetime, timedelta
import pytz
import threading
import pandas as pd
import requests
from flask import Flask, render_template, jsonify
from flask_socketio import SocketIO, emit
from celery import Celery
from config import Config

app = Flask(__name__)
app.config.from_object(Config)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode='eventlet')

# Initialize Celery (optional for background tasks)
celery = Celery(
    app.name,
    broker=Config.REDIS_URL,
    backend=Config.REDIS_URL
)

class PolygonScanner:
    def __init__(self, api_key):
        self.api_key = api_key
        self.base_url = Config.POLYGON_BASE_URL
        self.headers = {"Authorization": f"Bearer {api_key}"}
        self.symbols_cache = []
        self.cache_expiry = None
        
    def get_all_tickers(self, force_refresh=False):
        """Get all stock symbols from Polygon.io"""
        if not force_refresh and self.symbols_cache and self.cache_expiry and datetime.now() < self.cache_expiry:
            return self.symbols_cache
            
        all_symbols = []
        url = f"{self.base_url}/v3/reference/tickers"
        
        for exchange in Config.EXCHANGES:
            next_url = None
            for _ in range(10):  # Limit to 1000 symbols per exchange
                params = {
                    "market": "stocks",
                    "exchange": exchange,
                    "active": "true",
                    "limit": 1000,
                    "sort": "ticker",
                    "order": "asc"
                }
                
                if next_url:
                    response = requests.get(next_url, headers=self.headers)
                else:
                    response = requests.get(url, params=params, headers=self.headers)
                
                if response.status_code == 200:
                    data = response.json()
                    results = data.get('results', [])
                    
                    # Filter for common stocks
                    for ticker in results:
                        if (ticker['type'] == 'CS' and  # Common Stock
                            ticker['active'] and
                            ticker['locale'] == 'us'):
                            all_symbols.append(ticker['ticker'])
                    
                    next_url = data.get('next_url')
                    if not next_url:
                        break
                    
                    # Respect rate limits
                    tm.sleep(0.1)
                else:
                    print(f"Error fetching symbols: {response.status_code}")
                    break
        
        self.symbols_cache = all_symbols
        self.cache_expiry = datetime.now() + timedelta(hours=24)  # Cache for 24 hours
        return all_symbols
    
    def get_prev_day_data(self, symbol):
        """Get previous day's high, low, close"""
        url = f"{self.base_url}/v2/aggs/ticker/{symbol}/prev"
        params = {"adjusted": "true"}
        
        try:
            response = requests.get(url, params=params, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                if data.get('resultsCount', 0) > 0:
                    result = data['results'][0]
                    return {
                        'high': result['h'],
                        'low': result['l'],
                        'close': result['c'],
                        'volume': result['v']
                    }
        except Exception as e:
            print(f"Error getting prev day data for {symbol}: {e}")
        
        return None
    
    def get_current_minute_bar(self, symbol):
        """Get current minute bar data"""
        # Get data for the last 2 minutes to ensure we have current data
        url = f"{self.base_url}/v2/aggs/ticker/{symbol}/range/1/minute/0/2"
        params = {
            "adjusted": "true",
            "sort": "desc"
        }
        
        try:
            response = requests.get(url, params=params, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                if data.get('resultsCount', 0) > 0:
                    # Get the most recent minute bar (index 0)
                    latest = data['results'][0]
                    return {
                        'open': latest['o'],
                        'high': latest['h'],
                        'low': latest['l'],
                        'close': latest['c'],
                        'volume': latest['v'],
                        'timestamp': latest['t']  # Unix timestamp in milliseconds
                    }
        except Exception as e:
            print(f"Error getting minute data for {symbol}: {e}")
        
        return None
    
    def get_ticker_details(self, symbol):
        """Get detailed ticker information including float"""
        url = f"{self.base_url}/v3/reference/tickers/{symbol}"
        
        try:
            response = requests.get(url, headers=self.headers)
            if response.status_code == 200:
                data = response.json()
                info = data.get('results', {})
                
                # Get float shares (weighted shares outstanding)
                float_shares = info.get('weighted_shares_outstanding', 0)
                
                # Get market cap
                market_cap = info.get('market_cap', 0)
                
                # Get company name
                name = info.get('name', '')
                
                return {
                    'float_shares': float_shares,
                    'market_cap': market_cap,
                    'name': name,
                    'sector': info.get('sector', ''),
                    'industry': info.get('industry', '')
                }
        except Exception as e:
            print(f"Error getting details for {symbol}: {e}")
        
        return None
    
    def scan_symbol(self, symbol):
        """Scan a single symbol against all criteria"""
        try:
            # Get all required data
            prev_day_data = self.get_prev_day_data(symbol)
            current_bar = self.get_current_minute_bar(symbol)
            ticker_details = self.get_ticker_details(symbol)
            
            if not all([prev_day_data, current_bar, ticker_details]):
                return None
            
            current_price = current_bar['close']
            current_volume = current_bar['volume']
            prev_high = prev_day_data['high']
            float_shares = ticker_details['float_shares']
            
            # Check all criteria
            if not (Config.MIN_PRICE <= current_price <= Config.MAX_PRICE):
                return None
            
            if float_shares > Config.MAX_FLOAT:
                return None
            
            if current_volume < Config.MIN_VOLUME:
                return None
            
            if current_price <= prev_high:
                return None
            
            # Criteria met - create alert
            return {
                'symbol': symbol,
                'price': current_price,
                'volume': current_volume,
                'prev_high': prev_high,
                'float_millions': float_shares / 1_000_000,
                'prev_close': prev_day_data['close'],
                'change_pct': ((current_price - prev_day_data['close']) / prev_day_data['close']) * 100,
                'timestamp': datetime.fromtimestamp(current_bar['timestamp']/1000, pytz.timezone('US/Eastern')).strftime('%H:%M:%S'),
                'date': datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d'),
                'name': ticker_details['name'][:30] + '...' if len(ticker_details['name']) > 30 else ticker_details['name'],
                'sector': ticker_details['sector']
            }
            
        except Exception as e:
            print(f"Error scanning {symbol}: {e}")
            return None
    
    def batch_scan(self, symbols, batch_size=50):
        """Scan multiple symbols in batches"""
        alerts = []
        
        for i in range(0, len(symbols), batch_size):
            batch = symbols[i:i+batch_size]
            for symbol in batch:
                alert = self.scan_symbol(symbol)
                if alert:
                    alerts.append(alert)
            
            # Respect Polygon.io rate limits (5 requests per minute for free tier)
            if i + batch_size < len(symbols):
                tm.sleep(12)  # Sleep 12 seconds between batches
        
        return alerts

# Initialize scanner
scanner = PolygonScanner(Config.POLYGON_API_KEY)

# Store alerts in memory (use Redis in production)
alerts_history = []
MAX_HISTORY = 1000

@app.route('/')
def index():
    """Render main page"""
    return render_template('index.html')

@app.route('/api/alerts')
def get_alerts():
    """Get recent alerts"""
    return jsonify({
        'alerts': alerts_history[-100:],  # Last 100 alerts
        'total': len(alerts_history),
        'today_count': len([a for a in alerts_history if a.get('date') == datetime.now(pytz.timezone('US/Eastern')).strftime('%Y-%m-%d')])
    })

@app.route('/api/scan-now', methods=['POST'])
def scan_now():
    """Trigger immediate scan"""
    symbols = scanner.get_all_tickers()
    
    # For demo, scan a subset of symbols
    sample_symbols = symbols[:200]  # Scan first 200 symbols for speed
    
    new_alerts = scanner.batch_scan(sample_symbols)
    
    for alert in new_alerts:
        alerts_history.append(alert)
        socketio.emit('new_alert', alert)
    
    return jsonify({
        'status': 'success',
        'alerts_found': len(new_alerts)
    })

@app.route('/api/market-status')
def market_status():
    """Get current market status"""
    eastern = pytz.timezone('US/Eastern')
    now_et = datetime.now(eastern)
    current_time = now_et.time()
    
    is_market_open = (
        Config.MARKET_HOURS['start'] <= current_time <= Config.MARKET_HOURS['end']
    )
    
    # Get next scan time
    next_scan = now_et.replace(second=0, microsecond=0) + timedelta(minutes=1)
    
    return jsonify({
        'is_market_open': is_market_open,
        'current_time_et': now_et.strftime('%H:%M:%S'),
        'next_scan': next_scan.strftime('%H:%M:%S'),
        'symbols_count': len(scanner.symbols_cache)
    })

@socketio.on('connect')
def handle_connect():
    """Handle WebSocket connection"""
    emit('connected', {
        'message': 'Connected to Stock Alert System',
        'criteria': {
            'price_range': f"${Config.MIN_PRICE}-${Config.MAX_PRICE}",
            'max_float': f"{Config.MAX_FLOAT/1_000_000}M",
            'min_volume': Config.MIN_VOLUME
        }
    })

def background_scanner():
    """Background scanner running in separate thread"""
    eastern = pytz.timezone('US/Eastern')
    
    while True:
        try:
            now_et = datetime.now(eastern)
            current_time = now_et.time()
            
            # Check if within market hours
            if Config.MARKET_HOURS['start'] <= current_time <= Config.MARKET_HOURS['end']:
                print(f"[{now_et.strftime('%H:%M:%S')}] Starting scan...")
                
                # Get symbols (cached)
                symbols = scanner.get_all_tickers()
                
                # For production, use all symbols but batch carefully
                # For demo, use a smaller subset
                symbols_to_scan = symbols[:300]  # Adjust based on your API limits
                
                # Perform scan
                new_alerts = scanner.batch_scan(symbols_to_scan, batch_size=30)
                
                if new_alerts:
                    print(f"[{now_et.strftime('%H:%M:%S')}] Found {len(new_alerts)} alerts")
                    for alert in new_alerts:
                        # Add to history
                        alerts_history.append(alert)
                        if len(alerts_history) > MAX_HISTORY:
                            alerts_history.pop(0)
                        
                        # Send via WebSocket
                        socketio.emit('new_alert', alert)
                else:
                    print(f"[{now_et.strftime('%H:%M:%S')}] No alerts found")
            
            # Wait for next scan
            tm.sleep(Config.SCAN_INTERVAL_MINUTES * 60)
            
        except Exception as e:
            print(f"Scanner error: {e}")
            tm.sleep(60)

if __name__ == '__main__':
    # Start background scanner
    scanner_thread = threading.Thread(target=background_scanner, daemon=True)
    scanner_thread.start()
    
    # Run Flask app
    print("Starting Stock Alert System...")
    print(f"Market hours: {Config.MARKET_HOURS['start']} to {Config.MARKET_HOURS['end']} ET")
    socketio.run(app, debug=True, port=5000, host='0.0.0.0')