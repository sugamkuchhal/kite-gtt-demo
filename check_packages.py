#!/usr/bin/env python3
"""
Package Installation Checker for NSE Data Fetcher
Run this script to check which packages are installed and their versions
"""

import sys
import subprocess

# List of required packages
required_packages = [
    'pandas',
    'yfinance', 
    'requests',
    'beautifulsoup4',
    'gspread',
    'google-auth',
    'google-auth-oauthlib',
    'google-auth-httplib2'
]

def check_package(package_name):
    """Check if a package is installed and return its version"""
    try:
        if package_name == 'beautifulsoup4':
            # beautifulsoup4 is imported as bs4
            import bs4
            version = bs4.__version__
            import_name = 'bs4'
        elif package_name == 'google-auth':
            import google.auth
            version = google.auth.__version__
            import_name = 'google.auth'
        elif package_name == 'google-auth-oauthlib':
            import google_auth_oauthlib
            version = getattr(google_auth_oauthlib, '__version__', 'Unknown')
            import_name = 'google_auth_oauthlib'
        elif package_name == 'google-auth-httplib2':
            import google_auth_httplib2
            version = getattr(google_auth_httplib2, '__version__', 'Unknown')
            import_name = 'google_auth_httplib2'
        else:
            # Standard import
            module = __import__(package_name)
            version = getattr(module, '__version__', 'Unknown')
            import_name = package_name
            
        return True, version, import_name
        
    except ImportError:
        return False, None, package_name

def install_package(package_name):
    """Install a package using pip"""
    try:
        print(f"Installing {package_name}...")
        subprocess.check_call([sys.executable, "-m", "pip", "install", package_name])
        return True
    except subprocess.CalledProcessError:
        return False

def main():
    print("🔍 Checking Required Packages for NSE Data Fetcher")
    print("=" * 60)
    
    missing_packages = []
    installed_packages = []
    
    for package in required_packages:
        is_installed, version, import_name = check_package(package)
        
        if is_installed:
            print(f"✅ {package:<25} | Version: {version:<15} | Import: {import_name}")
            installed_packages.append(package)
        else:
            print(f"❌ {package:<25} | NOT INSTALLED")
            missing_packages.append(package)
    
    print("\n" + "=" * 60)
    print(f"📊 SUMMARY:")
    print(f"   Installed: {len(installed_packages)}/{len(required_packages)}")
    print(f"   Missing: {len(missing_packages)}")
    
    if missing_packages:
        print(f"\n📦 Missing Packages:")
        for package in missing_packages:
            print(f"   - {package}")
        
        print(f"\n💡 Installation Commands:")
        print(f"   Install all missing:")
        print(f"   pip install {' '.join(missing_packages)}")
        
        print(f"\n   Install one by one:")
        for package in missing_packages:
            print(f"   pip install {package}")
        
        # Ask if user wants to install missing packages
        print(f"\n🤖 Auto-install missing packages? (y/n): ", end="")
        try:
            choice = input().lower().strip()
            if choice in ['y', 'yes']:
                print(f"\n🚀 Installing missing packages...")
                failed_installs = []
                
                for package in missing_packages:
                    if install_package(package):
                        print(f"✅ Successfully installed {package}")
                    else:
                        print(f"❌ Failed to install {package}")
                        failed_installs.append(package)
                
                if failed_installs:
                    print(f"\n⚠️  Failed to install: {', '.join(failed_installs)}")
                    print(f"   Try installing manually with:")
                    for package in failed_installs:
                        print(f"   pip install {package}")
                else:
                    print(f"\n🎉 All packages installed successfully!")
                    
        except KeyboardInterrupt:
            print(f"\n\n👋 Installation cancelled by user")
    
    else:
        print(f"\n🎉 All required packages are installed!")
        print(f"   You're ready to run the NSE Data Fetcher!")
    
    # Test critical imports
    print(f"\n🧪 Testing Critical Imports:")
    test_imports = [
        ('pandas', 'pd'),
        ('yfinance', 'yf'), 
        ('requests', 'requests'),
        ('bs4', 'BeautifulSoup'),
        ('gspread', 'gspread'),
        ('google.auth', 'google.auth')
    ]
    
    all_imports_work = True
    for module, alias in test_imports:
        try:
            if alias == 'pd':
                import pandas as pd
            elif alias == 'yf':
                import yfinance as yf
            elif alias == 'requests':
                import requests
            elif alias == 'BeautifulSoup':
                from bs4 import BeautifulSoup
            elif alias == 'gspread':
                import gspread
            elif alias == 'google.auth':
                import google.auth
            
            print(f"✅ {module} import successful")
        except ImportError as e:
            print(f"❌ {module} import failed: {e}")
            all_imports_work = False
    
    if all_imports_work:
        print(f"\n🚀 All imports working! Ready to fetch NSE data!")
    else:
        print(f"\n⚠️  Some imports failed. Please install missing packages.")

if __name__ == "__main__":
    main()