import pandas as pd
import json

def verify_population_count():
    """
    Verify the population count and show detailed breakdown to identify discrepancy
    """
    print("🔍 POPULATION COUNT VERIFICATION")
    print("=" * 50)
    
    try:
        # Open Excel file
        xl = pd.ExcelFile('raw_query_data.xlsx', engine='openpyxl')
        print(f"📊 Available sheets: {xl.sheet_names}")
        
        total_raw_count = 0
        total_processed_count = 0
        sector_details = {}
        
        sectors = ['western_sector', 'eastern_sector', 'northern_sector', 'southern_sector']
        
        for sector in sectors:
            print(f"\n📍 SECTOR: {sector}")
            print("-" * 30)
            
            try:
                # Load raw data
                df = pd.read_excel(xl, sheet_name=sector, engine='openpyxl')
                raw_count = len(df)
                total_raw_count += raw_count
                
                print(f"Raw rows in {sector}: {raw_count:,}")
                
                # Clean PHC names (same logic as process_raw_data.py)
                df['PHC_Clean'] = df['Source.Name'].apply(clean_phc_name)
                
                # Count by PHC groups (this is how the system counts)
                phc_counts = {}
                processed_count = 0
                
                for phc_name, group in df.groupby('PHC_Clean'):
                    if pd.isna(phc_name) or phc_name == '':
                        print(f"⚠️  Skipping {len(group)} rows with empty/null PHC name")
                        continue
                    
                    group_size = len(group)
                    phc_counts[phc_name] = group_size
                    processed_count += group_size
                
                total_processed_count += processed_count
                sector_details[sector] = {
                    'raw_count': raw_count,
                    'processed_count': processed_count,
                    'phc_counts': phc_counts,
                    'skipped': raw_count - processed_count
                }
                
                print(f"Processed rows in {sector}: {processed_count:,}")
                print(f"Rows skipped (empty PHC): {raw_count - processed_count}")
                print(f"PHC Centers found: {len(phc_counts)}")
                
                # Show largest PHCs for verification
                sorted_phcs = sorted(phc_counts.items(), key=lambda x: x[1], reverse=True)
                print("Top 5 PHCs by population:")
                for phc, count in sorted_phcs[:5]:
                    print(f"  • {phc}: {count:,} people")
                    
            except Exception as e:
                print(f"❌ Error processing {sector}: {e}")
                
        print(f"\n📊 SUMMARY")
        print("=" * 50)
        print(f"Total RAW rows across all sectors: {total_raw_count:,}")
        print(f"Total PROCESSED rows (after cleaning): {total_processed_count:,}")
        print(f"Difference (rows with empty PHC names): {total_raw_count - total_processed_count}")
        
        # Load current dashboard data to compare
        try:
            with open('dashboard_data.json', 'r', encoding='utf-8') as f:
                dashboard_data = json.load(f)
                dashboard_count = dashboard_data.get('overview', {}).get('total_population', 0)
                print(f"Current dashboard shows: {dashboard_count:,}")
                
                if dashboard_count != total_processed_count:
                    print(f"⚠️  DISCREPANCY: Dashboard ({dashboard_count:,}) vs Calculated ({total_processed_count:,})")
                    print(f"   Difference: {abs(dashboard_count - total_processed_count)}")
                else:
                    print("✅ Dashboard matches calculated count!")
        except:
            print("ℹ️  Could not load dashboard_data.json for comparison")
            
        # Manual verification suggestions
        print(f"\n🔧 MANUAL VERIFICATION STEPS:")
        print("1. Open raw_query_data.xlsx")
        print("2. For each sector sheet, count total rows (excluding header)")
        print("3. Check for rows with empty 'Source.Name' column")
        print("4. The processed count should be: Total rows - Empty Source.Name rows")
        print("5. Sum across all 4 sectors")
        
        xl.close()
        return sector_details
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

def clean_phc_name(source_name):
    """Extract clean PHC name from source name (same logic as process_raw_data.py)"""
    if pd.isna(source_name):
        return ""
    
    # Convert to string and clean
    name = str(source_name).strip()
    
    # Remove common prefixes and suffixes
    prefixes_to_remove = ['PHC', 'Primary Health Center', 'Health Center']
    for prefix in prefixes_to_remove:
        if name.startswith(prefix):
            name = name[len(prefix):].strip()
    
    # Remove special characters and extra spaces
    import re
    name = re.sub(r'[^\w\s-]', '', name)
    name = re.sub(r'\s+', ' ', name).strip()
    
    return name

if __name__ == "__main__":
    verify_population_count()