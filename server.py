import http.server
import socketserver
import os
import json
import requests
import uuid
import time
import threading
from datetime import datetime
from pathlib import Path
from urllib.parse import urlparse, parse_qs
# Lazy import for heavy dependencies
import gc
import sys
import platform
import subprocess

def get_data_processor():
    """Lazy load the data processor to save memory when not processing data"""
    try:
        from process_raw_data import RawDataProcessor
        return RawDataProcessor()
    except ImportError as e:
        print(f"Warning: Could not import RawDataProcessor: {e}")
        return None

def cleanup_memory():
    """Force garbage collection to minimize memory usage"""
    gc.collect()
    # Clear Python's internal caches
    sys.intern('')  # Clear string intern cache

# Load environment variables from .env file for local development
def load_env_file():
    """Load environment variables from .env file if it exists"""
    try:
        env_path = Path('.env')
        if env_path.exists():
            with open(env_path, 'r', encoding='utf-8') as f:
                for line in f:
                    line = line.strip()
                    if line and not line.startswith('#') and '=' in line:
                        key, value = line.split('=', 1)
                        os.environ.setdefault(key.strip(), value.strip())
    except Exception as e:
        print(f"Warning: Could not load .env file: {e}")

# Load environment variables for local development
load_env_file()

# Production configuration
PORT = int(os.environ.get('PORT', 8000))
HOST = os.environ.get('HOST', '0.0.0.0')  # Use 0.0.0.0 for cloud deployment

# Global admin sessions and data
admin_sessions = {}
last_refresh_time = 0
dashboard_data = {}

# Advanced auto-refresh settings with time-based scheduling
auto_refresh_settings = {
    "enabled": True,
    "mode": "advanced",  # "simple" or "advanced"
    "simple_interval_minutes": 120,  # For simple mode
    "advanced_schedule": {
        "work_days": {  # Sunday to Thursday (0=Sunday, 6=Saturday)
            "days": [0, 1, 2, 3, 4],  # Sunday to Thursday
            "work_hours": {
                "start": "08:00",  # 8 AM
                "end": "17:00",    # 5 PM
                "interval_minutes": 10
            },
            "after_hours": {
                "interval_minutes": 180  # 3 hours
            }
        },
        "weekend_days": {  # Friday and Saturday
            "days": [5, 6],  # Friday and Saturday
            "interval_minutes": 240  # 4 hours
        },
        "timezone": "Asia/Riyadh"  # Default timezone
    },
    "thread": None,
    "stop_event": None,
    "last_refresh_time": None
}

def load_auto_refresh_settings():
    """Load auto-refresh settings from file"""
    global auto_refresh_settings
    try:
        if os.path.exists('auto_refresh_config.json'):
            with open('auto_refresh_config.json', 'r', encoding='utf-8') as f:
                saved_settings = json.load(f)
                # Load settings with backward compatibility
                auto_refresh_settings["enabled"] = saved_settings.get("enabled", True)
                
                # Handle both old and new format
                if "simple_interval_minutes" in saved_settings:
                    auto_refresh_settings["simple_interval_minutes"] = saved_settings["simple_interval_minutes"]
                    auto_refresh_settings["mode"] = saved_settings.get("mode", "simple")
                else:
                    # Backward compatibility
                    auto_refresh_settings["simple_interval_minutes"] = saved_settings.get("interval_minutes", 120)
                    auto_refresh_settings["mode"] = "simple"
                
                # Load advanced schedule if present
                if "advanced_schedule" in saved_settings:
                    auto_refresh_settings["advanced_schedule"] = saved_settings["advanced_schedule"]
                
                auto_refresh_settings["last_refresh_time"] = saved_settings.get("last_refresh_time", None)
                print(f"Loaded auto-refresh settings: Enabled={auto_refresh_settings['enabled']}, Mode={auto_refresh_settings['mode']}")
    except Exception as e:
        print(f"Error loading auto-refresh settings: {e}")

def save_auto_refresh_settings():
    """Save auto-refresh settings to file"""
    try:
        settings_to_save = {
            "enabled": auto_refresh_settings["enabled"],
            "mode": auto_refresh_settings.get("mode", "simple"),
            "simple_interval_minutes": auto_refresh_settings.get("simple_interval_minutes", 120),
            "last_refresh_time": auto_refresh_settings.get("last_refresh_time", None)
        }
        
        # Save advanced schedule if present
        if "advanced_schedule" in auto_refresh_settings:
            settings_to_save["advanced_schedule"] = auto_refresh_settings["advanced_schedule"]
        
        with open('auto_refresh_config.json', 'w', encoding='utf-8') as f:
            json.dump(settings_to_save, f, indent=2)
        print(f"Saved auto-refresh settings: Mode={settings_to_save['mode']}")
    except Exception as e:
        print(f"Error saving auto-refresh settings: {e}")

def load_dashboard_data():
    """Load dashboard data from JSON file"""
    global dashboard_data
    try:
        if os.path.exists('dashboard_data.json'):
            with open('dashboard_data.json', 'r', encoding='utf-8') as f:
                dashboard_data = json.load(f)
        else:
            dashboard_data = {
                "last_updated": "Not available",
                "total_sectors": 0,
                "total_inspections": 0,
                "avg_score": "N/A",
                "sectors": [],
                "summary": "No data available."
            }
    except Exception as e:
        print(f"Error loading dashboard data: {e}")
        dashboard_data = {
            "last_updated": "Error loading data",
            "total_sectors": 0,
            "total_inspections": 0,
            "avg_score": "N/A",
            "sectors": [],
            "summary": f"Error: {e}"
        }

def load_config():
    """Load configuration with environment variables for security"""
    try:
        with open('config.json', 'r', encoding='utf-8') as f:
            config = json.load(f)
    except FileNotFoundError:
        print("Warning: config.json not found. Using defaults.")
        config = {
            "admin": {"password": "USE_ENVIRONMENT_VARIABLE", "session_timeout_minutes": 30},
            "onedrive": {"download_url": "USE_ENVIRONMENT_VARIABLE"},
            "refresh": {"cooldown_seconds": 10}
        }
    
    # Override with environment variables for security
    admin_password = os.environ.get('ADMIN_PASSWORD')
    onedrive_url = os.environ.get('ONEDRIVE_DOWNLOAD_URL')
    
    if admin_password and admin_password != 'USE_ENVIRONMENT_VARIABLE':
        config['admin']['password'] = admin_password
    else:
        print("WARNING: ADMIN_PASSWORD environment variable not set!")
        
    if onedrive_url and onedrive_url != 'USE_ENVIRONMENT_VARIABLE':
        config['onedrive']['download_url'] = onedrive_url
    else:
        print("WARNING: ONEDRIVE_DOWNLOAD_URL environment variable not set!")
    
    return config

def download_from_onedrive(url):
    """Download Excel file from OneDrive"""
    try:
        # Convert OneDrive sharing URL to direct download URL
        if "1drv.ms" in url:
            # For 1drv.ms links, add &download=1 parameter
            if "download=1" not in url:
                if "?" in url:
                    url += "&download=1"
                else:
                    url += "?download=1"
        elif "onedrive.live.com" in url and "download?" not in url:
            # Already a download URL format, use as-is
            pass
        
        print(f"Downloading from: {url}")
        
        # Set headers to mimic a browser request
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36'
        }
        
        response = requests.get(url, headers=headers, timeout=30, allow_redirects=True)
        response.raise_for_status()
        
        # Check if we got an Excel file or HTML
        content_type = response.headers.get('content-type', '')
        if 'text/html' in content_type:
            raise Exception("Received HTML instead of Excel file - check OneDrive URL permissions")
        
        # Verify it's an Excel file by checking the first few bytes
        if not response.content.startswith(b'PK'):  # Excel files start with 'PK' (ZIP signature)
            raise Exception("Downloaded file is not a valid Excel format")
        
        # Save to local file
        with open('raw_query_data.xlsx', 'wb') as f:
            f.write(response.content)
        
        print(f"Successfully downloaded {len(response.content)} bytes")
        return True, len(response.content)
        
    except requests.exceptions.RequestException as e:
        return False, f"Network error: {str(e)}"
    except Exception as e:
        return False, str(e)

def is_admin_authenticated(auth_header):
    """Check if admin is authenticated"""
    if not auth_header or not auth_header.startswith('Bearer '):
        return False
    
    token = auth_header[7:]  # Remove 'Bearer ' prefix
    
    if token not in admin_sessions:
        return False
    
    # Check if session is expired
    session = admin_sessions[token]
    if time.time() > session['expires']:
        del admin_sessions[token]
        return False
    
    return True

class DashboardHandler(http.server.SimpleHTTPRequestHandler):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, directory=os.getcwd(), **kwargs)
    
    def end_headers(self):
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        super().end_headers()
    
    def do_GET(self):
        if self.path == '/api/data':
            self.serve_dashboard_data()
        elif self.path == '/api/status':
            self.serve_status()
        elif self.path == '/admin':
            self.serve_admin_panel()
        elif self.path.startswith('/admin/status'):
            self.admin_status()
        elif self.path == '/admin/auto-refresh-settings':
            self.get_auto_refresh_settings()
        elif self.path == '/admin/test-settings':
            self.test_auto_refresh_settings()
        elif self.path == '/' or self.path == '/index.html':
            self.serve_main_dashboard()
        else:
            super().do_GET()
    
    def do_POST(self):
        if self.path == '/api/refresh':
            self.refresh_data()
        elif self.path == '/admin/login':
            self.admin_login()
        elif self.path == '/admin/refresh':
            self.admin_refresh()
        elif self.path == '/admin/force-cache-clear':
            self.force_cache_clear()
        elif self.path == '/admin/auto-refresh-settings':
            self.set_auto_refresh_settings()
        else:
            self.send_error(404)
    
    def do_OPTIONS(self):
        """Handle CORS preflight requests"""
        self.send_response(200)
        self.send_header('Access-Control-Allow-Origin', '*')
        self.send_header('Access-Control-Allow-Methods', 'GET, POST, OPTIONS')
        self.send_header('Access-Control-Allow-Headers', 'Content-Type, Authorization')
        self.end_headers()
    
    def serve_admin_panel(self):
        """Serve the admin panel HTML with cache busting"""
        try:
            with open('admin.html', 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Add cache busting timestamp
            cache_buster = str(int(time.time()))
            content = content.replace('</head>', f'<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">\n<meta http-equiv="Pragma" content="no-cache">\n<meta http-equiv="Expires" content="0">\n<meta name="cache-buster" content="{cache_buster}">\n</head>')
            
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
            self.send_header('Pragma', 'no-cache')
            self.send_header('Expires', '0')
            self.end_headers()
            self.wfile.write(content.encode())
        except FileNotFoundError:
            self.send_error(404, "Admin panel not found")
        except Exception as e:
            self.send_error(500, f"Error loading admin panel: {str(e)}")
    
    def serve_main_dashboard(self):
        """Serve the main dashboard HTML with cache busting"""
        try:
            with open('index.html', 'r', encoding='utf-8') as f:
                content = f.read()
            
            # Add cache busting timestamp
            cache_buster = str(int(time.time()))
            content = content.replace('</head>', f'<meta http-equiv="Cache-Control" content="no-cache, no-store, must-revalidate">\n<meta http-equiv="Pragma" content="no-cache">\n<meta http-equiv="Expires" content="0">\n<meta name="cache-buster" content="{cache_buster}">\n</head>')
            
            self.send_response(200)
            self.send_header('Content-type', 'text/html; charset=utf-8')
            self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
            self.send_header('Pragma', 'no-cache')
            self.send_header('Expires', '0')
            self.end_headers()
            self.wfile.write(content.encode())
        except FileNotFoundError:
            self.send_error(404, "Dashboard not found")
        except Exception as e:
            self.send_error(500, f"Error loading dashboard: {str(e)}")
    
    def admin_login(self):
        """Handle admin login"""
        try:
            content_length = int(self.headers['Content-Length'])
            post_data = self.rfile.read(content_length)
            data = json.loads(post_data.decode('utf-8'))
            
            config = load_config()
            admin_password = config['admin']['password']
            
            if data.get('password') == admin_password:
                # Generate session token
                token = str(uuid.uuid4())
                session_timeout = config['admin']['session_timeout_minutes']
                admin_sessions[token] = {
                    'expires': time.time() + (session_timeout * 60),
                    'created': time.time()
                }
                
                response = {
                    "success": True,
                    "token": token,
                    "message": "Login successful"
                }
                
                self.send_response(200)
            else:
                response = {
                    "success": False,
                    "message": "Invalid password"
                }
                
                self.send_response(401)
            
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
            
        except Exception as e:
            self.send_error(500, f"Login error: {str(e)}")
    
    def admin_status(self):
        """Get admin dashboard status"""
        auth_header = self.headers.get('Authorization')
        if not is_admin_authenticated(auth_header):
            self.send_error(401, "Unauthorized")
            return
        
        try:
            data_file = Path('dashboard_data.json')
            excel_file = Path('raw_query_data.xlsx')
            
            # Get data info
            last_update = None
            total_records = 0
            
            if data_file.exists():
                stat = data_file.stat()
                last_update = time.ctime(stat.st_mtime)
                
                # Count records
                with open(data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if 'overview' in data:
                        total_records = data['overview'].get('total_population', 0)
            
            # Test OneDrive connection
            config = load_config()
            onedrive_url = config['onedrive'].get('download_url', '')
            onedrive_accessible = False
            
            if onedrive_url:
                try:
                    response = requests.head(onedrive_url, timeout=5)
                    onedrive_accessible = response.status_code == 200
                except:
                    onedrive_accessible = False
            
            status_data = {
                "success": True,
                "data": {
                    "last_update": last_update,
                    "total_records": total_records,
                    "onedrive_accessible": onedrive_accessible,
                    "excel_file_exists": excel_file.exists(),
                    "excel_file_size": excel_file.stat().st_size if excel_file.exists() else 0
                }
            }
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(status_data).encode())
            
        except Exception as e:
            self.send_error(500, f"Status error: {str(e)}")
    
    def admin_refresh(self):
        """Handle admin data refresh"""
        global last_refresh_time
        
        auth_header = self.headers.get('Authorization')
        if not is_admin_authenticated(auth_header):
            self.send_error(401, "Unauthorized")
            return
        
        try:
            config = load_config()
            cooldown = config['refresh']['cooldown_seconds']
            
            # Check cooldown
            current_time = time.time()
            if current_time - last_refresh_time < cooldown:
                remaining = int(cooldown - (current_time - last_refresh_time))
                response = {
                    "success": False,
                    "message": f"Please wait {remaining} seconds before refreshing again"
                }
                
                self.send_response(429)  # Too Many Requests
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response).encode())
                return
            
            # Download from OneDrive
            download_url = config['onedrive'].get('download_url', '')
            if not download_url:
                response = {
                    "success": False,
                    "message": "OneDrive URL not configured"
                }
                
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response).encode())
                return
            
            # Try the configured URL first, then fallback to auto-conversion
            download_success = False
            download_result = None
            
            # Attempt 1: Use configured download URL
            download_success, download_result = download_from_onedrive(download_url)
            
            # Attempt 2: If failed, try the excel_url with &download=1
            if not download_success:
                excel_url = config['onedrive'].get('excel_url', '')
                if excel_url and excel_url != download_url:
                    print(f"First attempt failed: {download_result}")
                    print("Trying alternative URL format...")
                    download_success, download_result = download_from_onedrive(excel_url)
            
            if not download_success:
                response = {
                    "success": False,
                    "message": f"Failed to download from OneDrive: {download_result}"
                }
                
                self.send_response(500)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(response).encode())
                return
            
            # Process data with memory optimization
            processor = get_data_processor()
            if processor:
                processor.process_raw_data()
            
            # PRIVACY: Force close and delete the downloaded Excel file
            cleanup_memory()  # Enhanced memory cleanup
            
            # Wait for file handles to release
            time.sleep(2)  # Reduced wait time
            
            try:
                if os.path.exists('raw_query_data.xlsx'):
                    # Multiple deletion attempts with increasing delays
                    for attempt in range(5):
                        try:
                            # On Windows, try to force unlock the file
                            if platform.system() == "Windows":
                                try:
                                    # Try to terminate any process holding the file
                                    subprocess.run(['taskkill', '/f', '/im', 'excel.exe'], 
                                                 capture_output=True, check=False)
                                except:
                                    pass
                            
                            # Attempt deletion
                            os.remove('raw_query_data.xlsx')
                            print("✅ Downloaded Excel file deleted for privacy")
                            break
                            
                        except (PermissionError, OSError) as e:
                            if attempt < 4:
                                time.sleep(2)  # Wait 2 seconds between attempts
                                gc.collect()   # Force garbage collection again
                                continue
                            else:
                                # Final attempt: rename to temporary file and schedule for deletion
                                import uuid
                                temp_name = f"temp_delete_{uuid.uuid4().hex[:8]}.tmp"
                                try:
                                    os.rename('raw_query_data.xlsx', temp_name)
                                    print(f"⚠️ File renamed to {temp_name} - will be cleaned up later")
                                    
                                    # Try to delete the temp file in background
                                    def delayed_delete():
                                        time.sleep(10)
                                        try:
                                            os.remove(temp_name)
                                            print(f"✅ Delayed cleanup successful: {temp_name}")
                                        except:
                                            print(f"⚠️ Manual cleanup required: {temp_name}")
                                    
                                    threading.Thread(target=delayed_delete, daemon=True).start()
                                    
                                except Exception as rename_error:
                                    print(f"❌ PRIVACY WARNING: Could not secure file: {rename_error}")
                                    
            except Exception as e:
                print(f"⚠️ File cleanup error: {e}")
            
            # Update refresh time
            last_refresh_time = current_time
            
            # Count processed records
            data_file = Path('dashboard_data.json')
            records_processed = 0
            if data_file.exists():
                with open(data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                    if 'overview' in data:
                        records_processed = data['overview'].get('total_population', 0)
            
            response = {
                "success": True,
                "message": "Data refreshed successfully",
                "file_size": download_result,
                "records_processed": records_processed
            }
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
            
        except Exception as e:
            response = {
                "success": False,
                "message": f"Refresh error: {str(e)}"
            }
            
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
    
    def force_cache_clear(self):
        """Force clear all cache and regenerate data"""
        global dashboard_data, last_refresh_time
        
        auth_header = self.headers.get('Authorization')
        if not is_admin_authenticated(auth_header):
            self.send_error(401, "Unauthorized")
            return
        
        try:
            print("🧹 FORCE CACHE CLEAR INITIATED")
            
            # Step 1: Clear in-memory cache
            dashboard_data = {}
            print("✅ In-memory cache cleared")
            
            # Step 2: Delete dashboard_data.json to force regeneration
            if os.path.exists('dashboard_data.json'):
                os.remove('dashboard_data.json')
                print("✅ dashboard_data.json deleted")
            
            # Step 3: Delete raw Excel file to force fresh download
            if os.path.exists('raw_query_data.xlsx'):
                os.remove('raw_query_data.xlsx')
                print("✅ raw_query_data.xlsx deleted - will force fresh download")
            
            # Step 4: Delete auto-refresh config to reset settings
            if os.path.exists('auto_refresh_config.json'):
                os.remove('auto_refresh_config.json')
                print("✅ auto_refresh_config.json cleared")
            
            # Step 5: Force garbage collection
            import gc
            gc.collect()
            print("✅ Garbage collection completed")
            
            # Step 6: Reset refresh timer
            last_refresh_time = 0
            print("✅ Refresh timer reset")
            
            # Step 7: Force fresh download and regenerate data
            print("🔄 Forcing fresh download and regeneration with updated counting logic...")
            
            # Download fresh data
            config = load_config()
            download_url = config['onedrive'].get('download_url', '')
            
            if download_url and download_url != 'USE_ENVIRONMENT_VARIABLE':
                download_success, download_result = download_from_onedrive(download_url)
                
                if download_success:
                    print("✅ Fresh data downloaded successfully")
                    
                    # Process with updated counting logic
                    processor = get_data_processor()
                    if processor:
                        processor.process_raw_data()
                        print("✅ Data regenerated with UPDATED COUNTING LOGIC")
                        
                        # Clean up Excel file for privacy
                        try:
                            if os.path.exists('raw_query_data.xlsx'):
                                os.remove('raw_query_data.xlsx')
                                print("✅ Excel file cleaned up for privacy")
                        except Exception as e:
                            print(f"⚠️ Excel cleanup warning: {e}")
                    else:
                        print("⚠️ Could not load data processor")
                else:
                    print(f"❌ Fresh download failed: {download_result}")
            else:
                print("⚠️ OneDrive URL not configured - cannot download fresh data")
            
            # Step 8: Reload dashboard data
            load_dashboard_data()
            print("✅ Dashboard data reloaded with updated counting")
            
            # Get the new count to verify
            new_count = "Unknown"
            if dashboard_data and 'overview' in dashboard_data:
                new_count = dashboard_data['overview'].get('total_population', 'Unknown')
            
            response = {
                "success": True,
                "message": f"Cache forcefully cleared and data regenerated! New count: {new_count}",
                "timestamp": datetime.now().isoformat(),
                "new_population_count": new_count,
                "actions_performed": [
                    "In-memory cache cleared",
                    "dashboard_data.json deleted",
                    "raw_query_data.xlsx deleted for fresh download",
                    "auto_refresh_config.json cleared", 
                    "Garbage collection performed",
                    "Refresh timer reset",
                    "Fresh data downloaded from OneDrive",
                    "Data regenerated with UPDATED COUNTING LOGIC",
                    "Excel file cleaned up for privacy",
                    "Dashboard data reloaded"
                ]
            }
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.send_header('Cache-Control', 'no-cache, no-store, must-revalidate')
            self.send_header('Pragma', 'no-cache')
            self.send_header('Expires', '0')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
            
            print("🎉 FORCE CACHE CLEAR COMPLETED SUCCESSFULLY")
            
        except Exception as e:
            print(f"❌ Force cache clear error: {e}")
            response = {
                "success": False,
                "message": f"Force cache clear error: {str(e)}"
            }
            
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
    
    def get_auto_refresh_settings(self):
        """Get current auto-refresh settings"""
        auth_header = self.headers.get('Authorization')
        if not is_admin_authenticated(auth_header):
            self.send_error(401, "Unauthorized")
            return
        
        try:
            global auto_refresh_settings
            
            # Calculate next refresh time more accurately
            next_refresh = None
            time_until_next = None
            current_interval = get_next_refresh_interval()  # Get dynamic interval
            
            if auto_refresh_settings["enabled"] and auto_refresh_settings["thread"] and auto_refresh_settings["thread"].is_alive():
                last_refresh = auto_refresh_settings.get("last_refresh_time")
                if last_refresh:
                    # Calculate based on last actual refresh time
                    next_refresh_time = last_refresh + (current_interval * 60)
                    time_until_next = max(0, int(next_refresh_time - time.time()))
                    next_refresh = time.strftime('%H:%M:%S', time.localtime(next_refresh_time))
                else:
                    # Fallback to current time + interval
                    next_refresh_time = time.time() + (current_interval * 60)
                    time_until_next = current_interval * 60
                    next_refresh = time.strftime('%H:%M:%S', time.localtime(next_refresh_time))
            
            response = {
                "enabled": auto_refresh_settings["enabled"],
                "interval_minutes": current_interval,
                "next_refresh": next_refresh,
                "time_until_next_seconds": time_until_next,
                "last_refresh_time": auto_refresh_settings.get("last_refresh_time"),
                "thread_active": auto_refresh_settings["thread"] and auto_refresh_settings["thread"].is_alive()
            }
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
            
        except Exception as e:
            self.send_error(500, f"Error getting settings: {str(e)}")
    
    def test_auto_refresh_settings(self):
        """Test endpoint to debug auto-refresh settings"""
        auth_header = self.headers.get('Authorization')
        if not is_admin_authenticated(auth_header):
            self.send_error(401, "Unauthorized")
            return
        
        try:
            global auto_refresh_settings
            
            response = {
                "current_settings": auto_refresh_settings,
                "thread_info": {
                    "exists": auto_refresh_settings.get("thread") is not None,
                    "alive": auto_refresh_settings.get("thread") and auto_refresh_settings["thread"].is_alive() if auto_refresh_settings.get("thread") else False
                },
                "stop_event_info": {
                    "exists": auto_refresh_settings.get("stop_event") is not None,
                    "is_set": auto_refresh_settings.get("stop_event") and auto_refresh_settings["stop_event"].is_set() if auto_refresh_settings.get("stop_event") else False
                }
            }
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response, indent=2).encode())
            
        except Exception as e:
            self.send_error(500, f"Error testing settings: {str(e)}")
    
    def set_auto_refresh_settings(self):
        """Set advanced auto-refresh settings with time-based scheduling"""
        auth_header = self.headers.get('Authorization')
        if not is_admin_authenticated(auth_header):
            self.send_error(401, "Unauthorized")
            return
        
        try:
            content_length = int(self.headers.get('Content-Length', 0))
            if content_length == 0:
                raise ValueError("No data received")
                
            post_data = self.rfile.read(content_length)
            new_settings = json.loads(post_data.decode('utf-8'))
            
            enabled = new_settings.get('enabled', False)
            mode = new_settings.get('mode', 'simple')
            
            print(f"🔧 Setting advanced auto-refresh: enabled={enabled}, mode={mode}")
            
            global auto_refresh_settings
            
            # Stop current thread if running
            try:
                if auto_refresh_settings.get("stop_event"):
                    auto_refresh_settings["stop_event"].set()
                    print("🛑 Stopped existing auto-refresh thread")
            except Exception as e:
                print(f"⚠️ Warning stopping thread: {e}")
            
            # Update settings with new structure
            auto_refresh_settings["enabled"] = enabled
            auto_refresh_settings["mode"] = mode
            auto_refresh_settings["simple_interval_minutes"] = new_settings.get('simple_interval_minutes', 120)
            
            # Update advanced schedule if provided
            if mode == 'advanced' and 'advanced_schedule' in new_settings:
                auto_refresh_settings["advanced_schedule"] = new_settings['advanced_schedule']
                print(f"📅 Updated advanced schedule: {new_settings['advanced_schedule']}")
            
            auto_refresh_settings["last_refresh_time"] = time.time()
            
            # Save settings to file for persistence
            save_auto_refresh_settings()
            
            print(f"📝 Updated and saved advanced settings")
            
            # Start new thread if enabled
            if enabled:
                try:
                    auto_refresh_settings["stop_event"] = threading.Event()
                    
                    # Use current interval for initial start (will dynamically adjust)
                    initial_interval = auto_refresh_settings.get("simple_interval_minutes", 120)
                    if mode == 'advanced':
                        initial_interval = get_next_refresh_interval()  # Get current interval
                    
                    auto_refresh_settings["thread"] = threading.Thread(
                        target=auto_refresh_data_with_settings, 
                        args=(auto_refresh_settings["stop_event"], initial_interval),
                        daemon=True
                    )
                    auto_refresh_settings["thread"].start()
                    
                    if mode == 'advanced':
                        print(f"🔄 Advanced auto-refresh enabled with dynamic scheduling")
                    else:
                        print(f"🔄 Simple auto-refresh enabled: Every {initial_interval} minutes")
                        
                except Exception as e:
                    print(f"❌ Error starting auto-refresh thread: {e}")
                    raise e
            else:
                print("⏸️ Auto-refresh disabled")
            
            # Calculate next refresh info
            next_refresh = None
            current_interval = None
            
            if enabled:
                if mode == 'advanced':
                    current_interval = get_next_refresh_interval()
                    next_refresh_time = time.time() + (current_interval * 60)
                    next_refresh = time.strftime('%H:%M:%S', time.localtime(next_refresh_time))
                else:
                    interval_minutes = auto_refresh_settings["simple_interval_minutes"]
                    next_refresh_time = time.time() + (interval_minutes * 60)
                    next_refresh = time.strftime('%H:%M:%S', time.localtime(next_refresh_time))
                    current_interval = interval_minutes
            
            response = {
                "success": True,
                "message": f"Advanced auto-refresh {'enabled' if enabled else 'disabled'} in {mode} mode",
                "settings": {
                    "enabled": enabled,
                    "mode": mode,
                    "current_interval_minutes": current_interval,
                    "next_refresh": next_refresh,
                    "thread_active": enabled and auto_refresh_settings["thread"] and auto_refresh_settings["thread"].is_alive(),
                    "advanced_schedule_active": mode == 'advanced'
                }
            }
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
            
        except Exception as e:
            print(f"❌ Error saving advanced auto-refresh settings: {str(e)}")
            response = {
                "success": False,
                "message": f"Error saving advanced settings: {str(e)}"
            }
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
    
    def serve_dashboard_data(self):
        """Serve the processed dashboard data as JSON"""
        try:
            data_file = Path('dashboard_data.json')
            if data_file.exists():
                with open(data_file, 'r', encoding='utf-8') as f:
                    data = json.load(f)
                
                self.send_response(200)
                self.send_header('Content-type', 'application/json')
                self.end_headers()
                self.wfile.write(json.dumps(data).encode())
            else:
                self.send_error(404, "Dashboard data not found")
        except Exception as e:
            self.send_error(500, f"Error loading dashboard data: {str(e)}")
    
    def serve_status(self):
        """Serve the current status of the dashboard"""
        try:
            data_file = Path('dashboard_data.json')
            status = {
                "status": "ready" if data_file.exists() else "no_data",
                "last_updated": None,
                "file_exists": os.path.exists("raw_query_data.xlsx")
            }
            
            if data_file.exists():
                stat = data_file.stat()
                status["last_updated"] = time.ctime(stat.st_mtime)
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(status).encode())
        except Exception as e:
            self.send_error(500, f"Error getting status: {str(e)}")
    
    def refresh_data(self):
        """Manually refresh the dashboard data (legacy endpoint)"""
        try:
            processor = get_data_processor()
            if processor:
                processor.process_raw_data()
                cleanup_memory()  # Free memory after processing
            
            response = {
                "success": True,
                "message": "Data refreshed successfully"
            }
            
            self.send_response(200)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())
        except Exception as e:
            response = {
                "success": False,
                "message": f"Error refreshing data: {str(e)}"
            }
            self.send_response(500)
            self.send_header('Content-type', 'application/json')
            self.end_headers()
            self.wfile.write(json.dumps(response).encode())

def cleanup_temp_files():
    """Clean up any temporary files from previous runs"""
    try:
        # Clean up any Excel files that weren't deleted
        for file in os.listdir('.'):
            if (file.startswith('raw_query_data') and file.endswith('.xlsx')) or \
               (file.startswith('temp_delete_') and file.endswith('.tmp')):
                try:
                    os.remove(file)
                    print(f"✅ Cleaned up leftover file: {file}")
                except:
                    print(f"⚠️ Could not clean up: {file}")
    except Exception as e:
        print(f"Cleanup warning: {e}")

def auto_refresh_data():
    """Auto-refresh data from OneDrive every hour"""
    global last_refresh_time
    
    while True:
        try:
            # Wait for 1 hour (3600 seconds)
            time.sleep(3600)
            
            config = load_config()
            download_url = config['onedrive'].get('download_url', '')
            
            if not download_url or download_url == 'USE_ENVIRONMENT_VARIABLE':
                print("⚠️ Auto-refresh skipped: OneDrive URL not configured")
                continue
                
            print("🔄 Starting automatic hourly data refresh...")
            
            # Download from OneDrive
            download_success, download_result = download_from_onedrive(download_url)
            
            if download_success:
                # Process data with memory optimization
                processor = get_data_processor()
                if processor:
                    processor.process_raw_data()
                    cleanup_memory()  # Free memory after processing
                
                # Update refresh time
                last_refresh_time = time.time()
                
                print(f"✅ Automatic refresh completed successfully at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Clean up Excel file
                try:
                    if os.path.exists('raw_query_data.xlsx'):
                        os.remove('raw_query_data.xlsx')
                        print("✅ Auto-refresh: Excel file cleaned up")
                except Exception as e:
                    print(f"⚠️ Auto-refresh cleanup warning: {e}")
                    
            else:
                print(f"❌ Automatic refresh failed: {download_result}")
                
        except Exception as e:
            print(f"❌ Auto-refresh error: {e}")
            # Continue the loop even if there's an error

def get_next_refresh_interval():
    """Calculate next refresh interval based on advanced scheduling"""
    global auto_refresh_settings
    
    if auto_refresh_settings["mode"] == "simple":
        return auto_refresh_settings["simple_interval_minutes"]
    
    # Advanced mode - time-based scheduling
    try:
        from datetime import datetime, timedelta
        import pytz
        
        # Get current time in configured timezone
        timezone_str = auto_refresh_settings["advanced_schedule"].get("timezone", "Asia/Riyadh")
        try:
            tz = pytz.timezone(timezone_str)
            now = datetime.now(tz)
        except:
            # Fallback to system time if timezone is invalid
            now = datetime.now()
        
        current_weekday = now.weekday()  # 0=Monday, 6=Sunday
        # Convert to our format: 0=Sunday, 6=Saturday
        current_weekday = (current_weekday + 1) % 7
        
        schedule = auto_refresh_settings["advanced_schedule"]
        
        # Check if it's a weekend day
        if current_weekday in schedule["weekend_days"]["days"]:
            interval = schedule["weekend_days"]["interval_minutes"]
            print(f"⏰ Weekend schedule: {interval} minutes")
            return interval
        
        # Check if it's a work day
        if current_weekday in schedule["work_days"]["days"]:
            work_hours = schedule["work_days"]["work_hours"]
            
            # Parse work hours
            start_time = datetime.strptime(work_hours["start"], "%H:%M").time()
            end_time = datetime.strptime(work_hours["end"], "%H:%M").time()
            
            current_time = now.time()
            
            # Check if within work hours
            if start_time <= current_time <= end_time:
                interval = work_hours["interval_minutes"]
                print(f"⏰ Work hours schedule: {interval} minutes")
                return interval
            else:
                interval = schedule["work_days"]["after_hours"]["interval_minutes"]
                print(f"⏰ After hours schedule: {interval} minutes")
                return interval
        
        # Fallback to simple mode if day not configured
        print("⚠️ Day not configured, using simple mode")
        return auto_refresh_settings["simple_interval_minutes"]
        
    except Exception as e:
        print(f"⚠️ Error calculating interval, using simple mode: {e}")
        return auto_refresh_settings["simple_interval_minutes"]

def auto_refresh_data_with_settings(stop_event, initial_interval_minutes):
    """Advanced auto-refresh with dynamic time-based scheduling"""
    global last_refresh_time, auto_refresh_settings
    
    print("🕒 Advanced auto-refresh started with dynamic scheduling")
    
    while not stop_event.is_set():
        try:
            # Get current interval based on schedule
            current_interval = get_next_refresh_interval()
            wait_time = current_interval * 60  # Convert to seconds
            
            print(f"⏱️ Next refresh in {current_interval} minutes (mode: {auto_refresh_settings['mode']})")
            
            # Wait for the calculated interval
            elapsed = 0
            while elapsed < wait_time and not stop_event.is_set():
                sleep_duration = min(60, wait_time - elapsed)  # Check every 60 seconds
                if stop_event.wait(sleep_duration):
                    print("🛑 Auto-refresh stopped by user")
                    return  # Stop event was set
                elapsed += sleep_duration
            
            if stop_event.is_set():
                return
            
            config = load_config()
            download_url = config['onedrive'].get('download_url', '')
            
            if not download_url or download_url == 'USE_ENVIRONMENT_VARIABLE':
                print("⚠️ Auto-refresh skipped: OneDrive URL not configured")
                continue
                
            print(f"🔄 Starting automatic data refresh (interval: {current_interval} minutes)...")
            
            # Download from OneDrive
            download_success, download_result = download_from_onedrive(download_url)
            
            if download_success:
                # Process data with memory optimization
                processor = get_data_processor()
                if processor:
                    processor.process_raw_data()
                    cleanup_memory()  # Free memory after processing
                
                # Update refresh time
                last_refresh_time = time.time()
                auto_refresh_settings["last_refresh_time"] = last_refresh_time
                
                # Save updated settings with refresh time
                save_auto_refresh_settings()
                
                print(f"✅ Automatic refresh completed successfully at {time.strftime('%Y-%m-%d %H:%M:%S')}")
                
                # Clean up Excel file
                try:
                    if os.path.exists('raw_query_data.xlsx'):
                        os.remove('raw_query_data.xlsx')
                        print("✅ Auto-refresh: Excel file cleaned up")
                except Exception as e:
                    print(f"⚠️ Auto-refresh cleanup warning: {e}")
                    
            else:
                print(f"❌ Automatic refresh failed: {download_result}")
                
        except Exception as e:
            print(f"❌ Auto-refresh error: {e}")
            # Continue the loop even if there's an error

def start_dashboard_server(port=8000):
    """Start the dashboard web server"""
    try:
        # Clean up any leftover files from previous runs
        cleanup_temp_files()
        
        # Load auto-refresh settings from file
        load_auto_refresh_settings()
        
        # Check if we have existing dashboard data
        if os.path.exists("dashboard_data.json"):
            print("Loading existing dashboard data...")
            load_dashboard_data()
        else:
            # Create minimal dashboard data structure
            print("No existing dashboard data found, creating minimal structure...")
            minimal_data = {
                "last_updated": "Not available",
                "total_sectors": 0,
                "total_inspections": 0,
                "avg_score": "N/A",
                "sectors": [],
                "summary": "No data available. Please refresh from admin panel."
            }
            with open('dashboard_data.json', 'w') as f:
                json.dump(minimal_data, f, indent=2)
            dashboard_data = minimal_data
        
        # Try to process initial data from raw data if it exists and is valid
        if os.path.exists("raw_query_data.xlsx"):
            try:
                print("Processing raw query data...")
                processor = get_data_processor()
                if processor:
                    processor.process_raw_data()
                    cleanup_memory()  # Free memory after processing
                if processor:
                    processor.process_raw_data()
                    cleanup_memory()  # Free memory after processing
                load_dashboard_data()  # Reload after processing
                print("✅ Raw data processed successfully")
                
                # PRIVACY: Delete the Excel file after processing
                import gc
                import time
                
                # Force garbage collection and wait
                gc.collect()
                time.sleep(1)
                
                try:
                    for attempt in range(3):
                        try:
                            os.remove("raw_query_data.xlsx")
                            print("✅ Excel file deleted for privacy")
                            break
                        except PermissionError:
                            if attempt < 2:
                                time.sleep(1)
                                continue
                            else:
                                import datetime
                                timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
                                backup_name = f"raw_query_data_backup_{timestamp}.xlsx"
                                os.rename("raw_query_data.xlsx", backup_name)
                                print(f"⚠️ File renamed to {backup_name} for manual cleanup")
                except Exception as e:
                    print(f"⚠️ Could not delete Excel file: {e}")
                    
            except Exception as e:
                print(f"⚠️ Could not process raw data file: {e}")
                print("Will rely on OneDrive for data updates.")
        else:
            print("No local data file found. Will rely on OneDrive for data.")
        
        # Perform initial OneDrive sync on server startup
        print("🔄 Performing initial OneDrive data sync...")
        try:
            config = load_config()
            download_url = config['onedrive'].get('download_url', '')
            
            if download_url and download_url != 'USE_ENVIRONMENT_VARIABLE':
                download_success, download_result = download_from_onedrive(download_url)
                
                if download_success:
                    # Process data immediately with memory optimization
                    processor = get_data_processor()
                    if processor:
                        processor.process_raw_data()
                        cleanup_memory()  # Free memory after processing
                    print("✅ Initial OneDrive sync completed successfully")
                    
                    # Clean up Excel file
                    try:
                        if os.path.exists('raw_query_data.xlsx'):
                            os.remove('raw_query_data.xlsx')
                            print("✅ Initial sync: Excel file cleaned up")
                    except Exception as e:
                        print(f"⚠️ Initial sync cleanup warning: {e}")
                else:
                    print(f"⚠️ Initial OneDrive sync failed: {download_result}")
            else:
                print("⚠️ Initial sync skipped: OneDrive URL not configured")
        except Exception as e:
            print(f"⚠️ Initial sync error: {e}")
        
        # Start configurable auto-refresh system
        global auto_refresh_settings
        if auto_refresh_settings["enabled"]:
            auto_refresh_settings["stop_event"] = threading.Event()
            auto_refresh_settings["thread"] = threading.Thread(
                target=auto_refresh_data_with_settings, 
                args=(auto_refresh_settings["stop_event"], auto_refresh_settings["interval_minutes"]),
                daemon=True
            )
            auto_refresh_settings["thread"].start()
            print(f"🔄 Auto-refresh started - will sync OneDrive data every {auto_refresh_settings['interval_minutes']} minutes")
        else:
            print("⏸️ Auto-refresh disabled - can be enabled from admin panel")
        
        # Start web server
        with socketserver.TCPServer(("0.0.0.0", port), DashboardHandler) as httpd:
            print(f"Dashboard server started at http://0.0.0.0:{port}")
            print("Available endpoints:")
            print(f"  - Main dashboard: http://0.0.0.0:{port}")
            print(f"  - Admin panel: http://0.0.0.0:{port}/admin")
            print(f"  - API data: http://0.0.0.0:{port}/api/data")
            print(f"  - API status: http://0.0.0.0:{port}/api/status")
            print("\nPress Ctrl+C to stop the server")
            
            try:
                httpd.serve_forever()
            except KeyboardInterrupt:
                print("\nServer stopped.")
    except Exception as e:
        print(f"Error starting server: {str(e)}")

if __name__ == "__main__":
    import sys
    
    # Use environment PORT for production deployments
    port = PORT
    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
            print(f"Using command line port: {port}")
        except ValueError:
            print(f"Invalid port number. Using environment PORT: {PORT}")
    
    start_dashboard_server(port)