import pandas as pd
import json

def detailed_count_analysis():
    """
    Detailed analysis to identify potential counting issues
    """
    print("🔍 DETAILED COUNT ANALYSIS")
    print("=" * 60)
    
    try:
        xl = pd.ExcelFile('raw_query_data.xlsx', engine='openpyxl')
        
        # Check each sector for potential issues
        sectors = ['western_sector', 'eastern_sector', 'northern_sector', 'southern_sector']
        all_source_names = []
        total_count = 0
        
        for sector in sectors:
            print(f"\n🔍 Analyzing {sector}...")
            df = pd.read_excel(xl, sheet_name=sector, engine='openpyxl')
            
            # Check for duplicates
            duplicates = df.duplicated()
            duplicate_count = duplicates.sum()
            
            # Check for empty source names
            empty_source = df['Source.Name'].isna() | (df['Source.Name'] == '') | (df['Source.Name'] == ' ')
            empty_count = empty_source.sum()
            
            # Get unique source names
            unique_sources = df['Source.Name'].dropna().unique()
            all_source_names.extend(unique_sources)
            
            print(f"  📊 Total rows: {len(df):,}")
            print(f"  🔄 Duplicate rows: {duplicate_count}")
            print(f"  ❌ Empty source names: {empty_count}")
            print(f"  🏥 Unique PHC sources: {len(unique_sources)}")
            
            total_count += len(df)
            
            # Check for potential data quality issues
            if duplicate_count > 0:
                print(f"  ⚠️  WARNING: Found {duplicate_count} duplicate rows!")
                
            if empty_count > 0:
                print(f"  ⚠️  WARNING: Found {empty_count} rows with empty PHC names!")
        
        print(f"\n📊 OVERALL SUMMARY")
        print("=" * 60)
        print(f"Total rows across all sheets: {total_count:,}")
        print(f"Total unique PHC source names: {len(set(all_source_names))}")
        
        # Manual counting method vs system method
        print(f"\n🔢 COUNTING METHOD COMPARISON")
        print("-" * 40)
        print(f"Method 1 - Simple row count: {total_count:,}")
        
        # Method 2 - System processing method
        processed_total = 0
        phc_centers = 0
        
        for sector in sectors:
            df = pd.read_excel(xl, sheet_name=sector, engine='openpyxl')
            df['PHC_Clean'] = df['Source.Name'].apply(clean_phc_name)
            
            sector_processed = 0
            for phc_name, group in df.groupby('PHC_Clean'):
                if pd.isna(phc_name) or phc_name == '':
                    continue
                sector_processed += len(group)
                phc_centers += 1
            
            processed_total += sector_processed
        
        print(f"Method 2 - System processing: {processed_total:,}")
        print(f"PHC Centers identified: {phc_centers}")
        
        # Check current dashboard
        try:
            with open('dashboard_data.json', 'r', encoding='utf-8') as f:
                dashboard_data = json.load(f)
                dashboard_count = dashboard_data.get('overview', {}).get('total_population', 0)
                print(f"Current dashboard display: {dashboard_count:,}")
        except:
            print("Could not read dashboard data")
        
        # Manual verification guide
        print(f"\n✅ FOR MANUAL VERIFICATION:")
        print("1. Open Excel file 'raw_query_data.xlsx'")
        print("2. Go to each sheet:")
        for i, sector in enumerate(sectors, 1):
            print(f"   {i}. {sector} sheet")
        print("3. For each sheet, select all data and check the row count at bottom")
        print("4. Sum the counts from all 4 sheets")
        print("5. This should match the 'Simple row count' above")
        
        # Potential discrepancy sources
        print(f"\n⚠️  POTENTIAL DISCREPANCY SOURCES:")
        print("1. Data refresh timing - counts change when new data arrives")
        print("2. Caching - old data might be displayed")
        print("3. Row filtering - empty PHC names might be excluded")
        print("4. Duplicate handling - system might deduplicate")
        print("5. Browser display formatting")
        
        xl.close()
        
    except Exception as e:
        print(f"❌ Error: {e}")

def clean_phc_name(source_name):
    """Extract clean PHC name from source name"""
    if pd.isna(source_name):
        return ""
    
    name = str(source_name).strip()
    
    # Remove common prefixes
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
    detailed_count_analysis()