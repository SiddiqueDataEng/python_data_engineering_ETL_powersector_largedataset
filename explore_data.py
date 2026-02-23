"""
Data Explorer - Interactive tool to query and analyze processed data
"""

import pandas as pd
import dask.dataframe as dd
from pathlib import Path


class DataExplorer:
    """Interactive data exploration tool"""
    
    def __init__(self, data_dir: str = "power_sector_data_silver_layer"):
        self.data_dir = Path(data_dir)
        self.bills_path = self.data_dir / "bills.parquet"
        self.readings_path = self.data_dir / "readings.parquet"
        
        # Load data lazily
        self.bills = None
        self.readings = None
        
        if self.bills_path.exists():
            self.bills = dd.read_parquet(self.bills_path)
        
        if self.readings_path.exists():
            self.readings = dd.read_parquet(self.readings_path)
    
    def show_overview(self):
        """Display data overview"""
        print("\n" + "=" * 70)
        print("DATA OVERVIEW")
        print("=" * 70)
        
        if self.bills is not None:
            print("\n📄 BILLS")
            print(f"  Records: {len(self.bills):,}")
            print(f"  Divisions: {self.bills['division'].nunique().compute()}")
            print(f"  Subdivisions: {self.bills['subdivision'].nunique().compute()}")
            print(f"  Unique Meters: {self.bills['meter_id'].nunique().compute():,}")
        
        if self.readings is not None:
            print("\n📊 READINGS")
            print(f"  Records: {len(self.readings):,}")
            print(f"  Divisions: {self.readings['division'].nunique().compute()}")
            print(f"  Subdivisions: {self.readings['subdivision'].nunique().compute()}")
            print(f"  Unique Meters: {self.readings['meter_id'].nunique().compute():,}")
    
    def top_consumers(self, n: int = 10):
        """Show top N consumers by total consumption"""
        if self.bills is None:
            print("Bills data not available")
            return
        
        print(f"\n🔝 Top {n} Consumers (by total consumption)")
        print("-" * 70)
        
        top = (self.bills
               .groupby('meter_id')['consumption_kwh']
               .sum()
               .nlargest(n)
               .compute())
        
        for i, (meter_id, consumption) in enumerate(top.items(), 1):
            print(f"{i:2d}. Meter {meter_id}: {consumption:,.2f} kWh")
    
    def revenue_by_division(self):
        """Show revenue breakdown by division"""
        if self.bills is None:
            print("Bills data not available")
            return
        
        print("\n💰 Revenue by Division")
        print("-" * 70)
        
        revenue = (self.bills
                   .groupby('division')['total_amount']
                   .sum()
                   .compute()
                   .sort_values(ascending=False))
        
        total = revenue.sum()
        
        for division, amount in revenue.items():
            percentage = (amount / total) * 100
            print(f"{division:20s}: {amount:15,.2f} ({percentage:5.2f}%)")
        
        print("-" * 70)
        print(f"{'TOTAL':20s}: {total:15,.2f}")
    
    def consumption_by_subdivision(self, division: str):
        """Show consumption by subdivision for a division"""
        if self.bills is None:
            print("Bills data not available")
            return
        
        print(f"\n⚡ Consumption by Subdivision - {division}")
        print("-" * 70)
        
        consumption = (self.bills[self.bills['division'] == division]
                       .groupby('subdivision')['consumption_kwh']
                       .sum()
                       .compute()
                       .sort_values(ascending=False))
        
        if len(consumption) == 0:
            print(f"No data found for division: {division}")
            return
        
        total = consumption.sum()
        
        for subdivision, kwh in consumption.items():
            percentage = (kwh / total) * 100
            print(f"{subdivision:30s}: {kwh:12,.2f} kWh ({percentage:5.2f}%)")
        
        print("-" * 70)
        print(f"{'TOTAL':30s}: {total:12,.2f} kWh")
    
    def payment_status_summary(self):
        """Show payment status distribution"""
        if self.bills is None:
            print("Bills data not available")
            return
        
        print("\n💳 Payment Status Summary")
        print("-" * 70)
        
        status = (self.bills
                  .groupby('status')
                  .agg({
                      'bill_id': 'count',
                      'total_amount': 'sum'
                  })
                  .compute())
        
        status.columns = ['Count', 'Amount']
        total_bills = status['Count'].sum()
        total_amount = status['Amount'].sum()
        
        for status_type in status.index:
            count = status.loc[status_type, 'Count']
            amount = status.loc[status_type, 'Amount']
            count_pct = (count / total_bills) * 100
            amount_pct = (amount / total_amount) * 100
            
            print(f"{status_type:15s}: {count:8,} bills ({count_pct:5.2f}%) | "
                  f"{amount:15,.2f} ({amount_pct:5.2f}%)")
        
        print("-" * 70)
        print(f"{'TOTAL':15s}: {total_bills:8,} bills | {total_amount:15,.2f}")
    
    def voltage_quality_report(self):
        """Analyze voltage quality from readings"""
        if self.readings is None:
            print("Readings data not available")
            return
        
        print("\n⚡ Voltage Quality Report")
        print("-" * 70)
        
        # Sample for analysis (full dataset might be too large)
        sample = self.readings.sample(frac=0.1).compute()
        
        print(f"Sample size: {len(sample):,} readings")
        print(f"\nVoltage Statistics:")
        print(f"  Mean: {sample['voltage'].mean():.2f} V")
        print(f"  Std Dev: {sample['voltage'].std():.2f} V")
        print(f"  Min: {sample['voltage'].min():.2f} V")
        print(f"  Max: {sample['voltage'].max():.2f} V")
        
        # Voltage ranges
        print(f"\nVoltage Distribution:")
        low = (sample['voltage'] < 200).sum()
        normal = ((sample['voltage'] >= 200) & (sample['voltage'] <= 240)).sum()
        high = (sample['voltage'] > 240).sum()
        
        total = len(sample)
        print(f"  Low (<200V):     {low:8,} ({low/total*100:5.2f}%)")
        print(f"  Normal (200-240V): {normal:8,} ({normal/total*100:5.2f}%)")
        print(f"  High (>240V):    {high:8,} ({high/total*100:5.2f}%)")
    
    def monthly_trend(self, year: int = 2024):
        """Show monthly consumption trend"""
        if self.bills is None:
            print("Bills data not available")
            return
        
        print(f"\n📈 Monthly Consumption Trend - {year}")
        print("-" * 70)
        
        # Filter by year
        bills_year = self.bills[
            self.bills['billing_month'].dt.year == year
        ].compute()
        
        if len(bills_year) == 0:
            print(f"No data found for year {year}")
            return
        
        monthly = (bills_year
                   .groupby(bills_year['billing_month'].dt.month)
                   .agg({
                       'consumption_kwh': 'sum',
                       'total_amount': 'sum',
                       'bill_id': 'count'
                   }))
        
        monthly.columns = ['Consumption (kWh)', 'Revenue', 'Bills']
        
        months = ['Jan', 'Feb', 'Mar', 'Apr', 'May', 'Jun',
                  'Jul', 'Aug', 'Sep', 'Oct', 'Nov', 'Dec']
        
        for month_num in sorted(monthly.index):
            month_name = months[month_num - 1]
            consumption = monthly.loc[month_num, 'Consumption (kWh)']
            revenue = monthly.loc[month_num, 'Revenue']
            bills = monthly.loc[month_num, 'Bills']
            
            print(f"{month_name} {year}: {consumption:12,.0f} kWh | "
                  f"{revenue:15,.2f} | {bills:6,.0f} bills")


def main():
    """Interactive exploration menu"""
    explorer = DataExplorer()
    
    if explorer.bills is None and explorer.readings is None:
        print("❌ No data found in power_sector_data_silver_layer/")
        print("Please run ETL pipeline first: python etl_pipeline.py --mode full")
        return
    
    while True:
        print("\n" + "=" * 70)
        print("DATA EXPLORER - Power Sector Analytics")
        print("=" * 70)
        print("\n1. Overview")
        print("2. Top Consumers")
        print("3. Revenue by Division")
        print("4. Consumption by Subdivision")
        print("5. Payment Status Summary")
        print("6. Voltage Quality Report")
        print("7. Monthly Trend")
        print("8. Exit")
        
        choice = input("\nSelect option (1-8): ").strip()
        
        if choice == '1':
            explorer.show_overview()
        
        elif choice == '2':
            n = input("How many top consumers? (default 10): ").strip()
            n = int(n) if n else 10
            explorer.top_consumers(n)
        
        elif choice == '3':
            explorer.revenue_by_division()
        
        elif choice == '4':
            division = input("Enter division name: ").strip().upper()
            explorer.consumption_by_subdivision(division)
        
        elif choice == '5':
            explorer.payment_status_summary()
        
        elif choice == '6':
            explorer.voltage_quality_report()
        
        elif choice == '7':
            year = input("Enter year (default 2024): ").strip()
            year = int(year) if year else 2024
            explorer.monthly_trend(year)
        
        elif choice == '8':
            print("\nGoodbye!")
            break
        
        else:
            print("Invalid choice!")
        
        input("\nPress Enter to continue...")


if __name__ == "__main__":
    main()
