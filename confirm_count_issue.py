import pandas as pd
import json

def confirm_counting_issue():
    """
    Compare row counting vs unique National ID counting to confirm the discrepancy
    """
    print("🔍 CONFIRMING THE COUNTING ISSUE")
    print("=" * 60)
    
    try:
        xl = pd.ExcelFile('raw_query_data.xlsx', engine='openpyxl')
        sectors = ['western_sector', 'eastern_sector', 'northern_sector', 'southern_sector']
        
        total_rows = 0
        total_unique_ids = 0
        all_national_ids = set()
        
        print("Sector Analysis:")
        print("-" * 60)
        
        for sector in sectors:
            print(f"\n📍 {sector.upper()}")
            df = pd.read_excel(xl, sheet_name=sector, engine='openpyxl')
            
            # Method 1: Count all rows (current system method)
            row_count = len(df)
            
            # Method 2: Count unique National IDs
            unique_ids_in_sector = df['National ID'].dropna().nunique()
            
            # Check for duplicates
            all_ids_in_sector = df['National ID'].dropna()
            duplicate_count = len(all_ids_in_sector) - len(all_ids_in_sector.unique())
            
            # Check for empty National IDs
            empty_ids = df['National ID'].isna().sum()
            
            print(f"  📊 Total rows: {row_count:,}")
            print(f"  🆔 Unique National IDs: {unique_ids_in_sector:,}")
            print(f"  🔄 Duplicate National IDs: {duplicate_count}")
            print(f"  ❌ Empty National IDs: {empty_ids}")
            
            if duplicate_count > 0:
                print(f"  ⚠️  DUPLICATES FOUND: {duplicate_count} duplicate National IDs!")
                
                # Show some duplicate examples
                duplicated_ids = all_ids_in_sector[all_ids_in_sector.duplicated(keep=False)]
                unique_duplicates = duplicated_ids.unique()[:3]  # Show first 3
                print(f"     Examples of duplicated IDs: {list(unique_duplicates)}")
            
            total_rows += row_count
            total_unique_ids += unique_ids_in_sector
            
            # Add to global set to check for cross-sector duplicates
            sector_ids = set(df['National ID'].dropna().values)
            cross_sector_duplicates = all_national_ids.intersection(sector_ids)
            if cross_sector_duplicates:
                print(f"  🚨 CROSS-SECTOR DUPLICATES: {len(cross_sector_duplicates)} IDs appear in multiple sectors!")
                print(f"     Examples: {list(cross_sector_duplicates)[:3]}")
            
            all_national_ids.update(sector_ids)
        
        # Global unique count
        global_unique_count = len(all_national_ids)
        
        print(f"\n📊 FINAL COMPARISON")
        print("=" * 60)
        print(f"Method 1 - Total rows (current system): {total_rows:,}")
        print(f"Method 2 - Sum of unique IDs per sector: {total_unique_ids:,}")
        print(f"Method 3 - Global unique National IDs: {global_unique_count:,}")
        print(f"Current dashboard shows: 52,308")
        
        print(f"\n🔍 DISCREPANCY ANALYSIS")
        print("-" * 40)
        discrepancy_1_vs_2 = total_rows - total_unique_ids
        discrepancy_1_vs_3 = total_rows - global_unique_count
        discrepancy_2_vs_3 = total_unique_ids - global_unique_count
        
        print(f"Rows vs Sector Unique IDs: {discrepancy_1_vs_2:+}")
        print(f"Rows vs Global Unique IDs: {discrepancy_1_vs_3:+}")
        print(f"Sector Sum vs Global Unique: {discrepancy_2_vs_3:+}")
        
        if discrepancy_1_vs_2 > 0:
            print(f"✅ Confirmed: {discrepancy_1_vs_2} duplicate National IDs within sectors")
        
        if discrepancy_2_vs_3 > 0:
            print(f"✅ Confirmed: {discrepancy_2_vs_3} National IDs appear in multiple sectors")
        
        print(f"\n💡 RECOMMENDATION")
        print("-" * 40)
        if global_unique_count != total_rows:
            print(f"Use GLOBAL UNIQUE count: {global_unique_count:,}")
            print("This represents the actual number of unique patients")
            print("Cross-sector duplicates should only be counted once")
        else:
            print("No duplicates found - current counting is accurate")
        
        # Show your manual count comparison
        manual_count_1 = 52300
        manual_count_2 = 52299
        
        print(f"\n🧮 MANUAL COUNT COMPARISON")
        print("-" * 40)
        print(f"Your manual count 1: {manual_count_1:,}")
        print(f"Your manual count 2: {manual_count_2:,}")
        print(f"Global unique IDs: {global_unique_count:,}")
        
        if abs(global_unique_count - manual_count_1) <= 1:
            print("✅ Global unique count matches your manual count!")
        elif abs(global_unique_count - manual_count_2) <= 1:
            print("✅ Global unique count matches your manual count!")
        else:
            print(f"❓ Difference from manual count: {global_unique_count - manual_count_1:+}")
        
        xl.close()
        return {
            'total_rows': total_rows,
            'total_unique_ids': total_unique_ids,
            'global_unique_count': global_unique_count
        }
        
    except Exception as e:
        print(f"❌ Error: {e}")
        return None

if __name__ == "__main__":
    confirm_counting_issue()