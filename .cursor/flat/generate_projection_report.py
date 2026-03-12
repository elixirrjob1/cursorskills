#!/usr/bin/env python3
"""Generate detailed projection report to see projection_method and linear_regression_slope."""
import sys
import json
from pathlib import Path

# Add the skills directory to path
scripts_path = Path(__file__).parent.parent / "skills" / "source-system-analyser" / "scripts"
sys.path.insert(0, str(scripts_path))

from sqlalchemy import create_engine
import importlib.util
spec = importlib.util.spec_from_file_location("predictor", scripts_path / "volume_projection" / "predictor.py")
predictor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(predictor)

def main():
    database_url = "mssql+pyodbc://pioneertest:mango1234!@pioneertest.database.windows.net:1433/free-sql-db-3300567?driver=ODBC+Driver+18+for+SQL+Server&TrustServerCertificate=yes&timeout=30"
    output_path = Path(__file__).parent.parent.parent / "LATEST_SCHEMA" / "capacity_report.json"
    
    engine = create_engine(database_url)
    report = predictor.build_projection_report(engine)
    
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(report, f, indent=2, default=str)
    
    print(f"Generated projection report: {output_path}")
    
    # Print summary of projection methods used
    print("\n=== Projection Methods Summary ===")
    for table in report.get("tables", []):
        growth = table.get("growth", {})
        method = growth.get("projection_method", "none")
        slope = growth.get("linear_regression_slope")
        avg_growth = growth.get("avg_monthly_growth_rows", 0)
        data_points = growth.get("data_points", 0)
        print(f"{table.get('schema')}.{table.get('table')}:")
        print(f"  Method: {method}")
        print(f"  Data points: {data_points}")
        if slope is not None:
            print(f"  Linear regression slope: {slope}")
        print(f"  Avg monthly growth: {avg_growth}")
        print()

if __name__ == "__main__":
    main()
