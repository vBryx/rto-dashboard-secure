import pandas as pd
import json
from datetime import datetime
import re

class RawDataProcessor:
    def __init__(self):
        self.sectors = ['western_sector', 'eastern_sector', 'northern_sector', 'southern_sector']
    
    def process_raw_data(self):
        """Process raw query data and generate dashboard data"""
        
        dashboard_data = {
            "last_updated": datetime.now().isoformat(),
            "sectors": {},
            "overview": {}
        }
        
        # Try to open Excel file with explicit engine
        try:
            xl = pd.ExcelFile('raw_query_data.xlsx', engine='openpyxl')
        except Exception as e:
            print(f"Error opening Excel file: {e}")
            # Try alternative approach
            try:
                xl = pd.ExcelFile('raw_query_data.xlsx')
            except Exception as e2:
                raise Exception(f"Could not open Excel file with any engine: {e2}")
        
        print(f"Excel file opened successfully. Available sheets: {xl.sheet_names}")
        
        for sector in self.sectors:
            print(f"Processing {sector}...")
            try:
                df = pd.read_excel(xl, sheet_name=sector, engine='openpyxl')
                print(f"  - Loaded {len(df)} rows from {sector}")
            except Exception as e:
                print(f"  - Error reading {sector}: {e}")
                print(f"  - Available sheets: {xl.sheet_names}")
                continue
            
            # Process this sector's data
            sector_name = sector.replace('_sector', '')
            dashboard_data["sectors"][sector_name] = self.process_sector_data(df)
        
        # Calculate overview metrics
        dashboard_data["overview"] = self.calculate_overview_metrics(dashboard_data["sectors"])
        
        # Save processed data in root directory (no separate data folder)
        with open('dashboard_data.json', 'w', encoding='utf-8') as f:
            json.dump(dashboard_data, f, indent=2, ensure_ascii=False)
        
        print(f"Dashboard data saved to dashboard_data.json")
        
        # Generate summary
        self.generate_summary_report(dashboard_data)
        
        return dashboard_data
    
    def process_sector_data(self, df):
        """Process sector data and aggregate by PHC"""
        
        # Clean the Source.Name column to extract PHC names
        df['PHC_Clean'] = df['Source.Name'].apply(self.clean_phc_name)
        
        # Group by PHC and calculate metrics
        sector_data = []
        
        for phc_name, group in df.groupby('PHC_Clean'):
            if pd.isna(phc_name) or phc_name == '':
                continue
                
            # Calculate metrics for this PHC
            total_population = len(group)
            
            # Communication metrics
            communicated = len(group[group['Response'].notna()])
            accepted = len(group[group['Response'] == 'Accepted'])
            refused = len(group[group['Response'] == 'Refused'])
            wrong_number = len(group[group['Response'] == 'Wrong number'])
            no_response = len(group[group['Response'] == 'No response'])
            
            # Visit types
            in_person_visits = len(group[group['Scheduled'] == 'In-Person'])
            virtual_visits = len(group[group['Scheduled'] == 'Virtual'])
            
            # Arrival and enrollment
            arrived = len(group[group['Arrived'] == 'Yes'])
            enrolled = len(group[group['Enrollment'] == 'Yes'])
            
            phc_data = {
                "phc_name": phc_name,
                "total_population": total_population,
                "communicated": communicated,
                "accepted": accepted,
                "refused": refused,
                "wrong_number": wrong_number,
                "no_response": no_response,
                "in_person_visits": in_person_visits,
                "virtual_visits": virtual_visits,
                "arrived": arrived,
                "enrolled": enrolled,
                # Calculate percentages
                "acceptance_rate": round((accepted / communicated * 100) if communicated > 0 else 0, 1),
                "enrollment_rate": round((enrolled / accepted * 100) if accepted > 0 else 0, 1),
                "communication_rate": round((communicated / total_population * 100) if total_population > 0 else 0, 1)
            }
            
            sector_data.append(phc_data)
        
        # Sort by total population (descending)
        sector_data.sort(key=lambda x: x['total_population'], reverse=True)
        
        return sector_data
    
    def clean_phc_name(self, source_name):
        """Extract clean PHC name from source name"""
        if pd.isna(source_name):
            return ""
        
        # Remove file extension
        name = str(source_name).replace('.xlsx', '')
        
        # Remove common prefixes
        prefixes_to_remove = [
            'مركز الرعاية الصحية الأولية ب',
            'مركز الرعاية الصحية الأولية',
            'مركز صحي ',
            'Primary Health Care Center',
            'PHC '
        ]
        
        for prefix in prefixes_to_remove:
            if name.startswith(prefix):
                name = name[len(prefix):]
                break
        
        return name.strip()
    
    def calculate_overview_metrics(self, sectors_data):
        """Calculate overall metrics across all sectors"""
        overview = {
            "total_population": 0,
            "total_communicated": 0,
            "total_accepted": 0,
            "total_refused": 0,
            "total_wrong_number": 0,
            "total_no_response": 0,
            "total_enrolled": 0,
            "total_phc_centers": 0,
            "total_arrived": 0,
            "total_in_person": 0,
            "total_virtual": 0
        }
        
        for sector_name, sector_data in sectors_data.items():
            overview["total_phc_centers"] += len(sector_data)
            
            for phc in sector_data:
                overview["total_population"] += phc["total_population"]
                overview["total_communicated"] += phc["communicated"]
                overview["total_accepted"] += phc["accepted"]
                overview["total_refused"] += phc["refused"]
                overview["total_wrong_number"] += phc["wrong_number"]
                overview["total_no_response"] += phc["no_response"]
                overview["total_enrolled"] += phc["enrolled"]
                overview["total_arrived"] += phc["arrived"]
                overview["total_in_person"] += phc["in_person_visits"]
                overview["total_virtual"] += phc["virtual_visits"]
        
        # Calculate percentages
        if overview["total_population"] > 0:
            overview["communication_rate"] = round(
                (overview["total_communicated"] / overview["total_population"]) * 100, 2
            )
        else:
            overview["communication_rate"] = 0
            
        if overview["total_communicated"] > 0:
            overview["acceptance_rate"] = round(
                (overview["total_accepted"] / overview["total_communicated"]) * 100, 2
            )
        else:
            overview["acceptance_rate"] = 0
            
        if overview["total_accepted"] > 0:
            overview["enrollment_rate"] = round(
                (overview["total_enrolled"] / overview["total_accepted"]) * 100, 2
            )
        else:
            overview["enrollment_rate"] = 0
        
        return overview
    
    def generate_summary_report(self, dashboard_data):
        """Generate a summary report"""
        overview = dashboard_data["overview"]
        
        report = f"""
MHC RTO Dashboard Summary Report
Generated: {dashboard_data["last_updated"]}
============================================

OVERALL METRICS:
- Total PHC Centers: {overview["total_phc_centers"]:,}
- Total Population At Risk: {overview["total_population"]:,}
- Total Communicated: {overview["total_communicated"]:,} ({overview["communication_rate"]}%)
- Total Accepted: {overview["total_accepted"]:,} ({overview["acceptance_rate"]}%)
- Total Enrolled: {overview["total_enrolled"]:,} ({overview["enrollment_rate"]}%)

COMMUNICATION BREAKDOWN:
- Accepted: {overview["total_accepted"]:,}
- Refused: {overview["total_refused"]:,}
- Wrong Number: {overview["total_wrong_number"]:,}
- No Response: {overview["total_no_response"]:,}

VISIT TYPES:
- In-Person Visits: {overview["total_in_person"]:,}
- Virtual Visits: {overview["total_virtual"]:,}
- Total Arrived: {overview["total_arrived"]:,}

SECTOR BREAKDOWN:
"""
        
        for sector_name, sector_data in dashboard_data["sectors"].items():
            total_enrolled = sum(phc["enrolled"] for phc in sector_data)
            total_population = sum(phc["total_population"] for phc in sector_data)
            total_communicated = sum(phc["communicated"] for phc in sector_data)
            
            report += f"- {sector_name.title()}: {len(sector_data)} PHCs, {total_population:,} population, {total_communicated:,} communicated, {total_enrolled:,} enrolled\n"
        
        # Save report in root directory
        with open('summary_report.txt', 'w', encoding='utf-8') as f:
            f.write(report)
        
        print(f"Summary report saved to summary_report.txt")
        print(report)

if __name__ == "__main__":
    processor = RawDataProcessor()
    processor.process_raw_data()
    print("✓ Raw data processing completed successfully!")