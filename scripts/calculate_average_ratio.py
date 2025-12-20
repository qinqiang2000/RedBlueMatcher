import os
import sys
import psycopg2
from statistics import mean

# Add root dir to path
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from config import load_config, get_db_config

import argparse

def main():
    parser = argparse.ArgumentParser(description='Calculate average ratio')
    parser.add_argument('--env', default='local', help='Environment name')
    parser.add_argument('--env-file', help='Path to env file')
    args = parser.parse_args()

    env_file = args.env_file
    env = args.env

    if env_file:
        print(f"Loading config from file: {env_file}")
    else:
        print(f"Loading config for env: {env}")
        
    load_config(env=env, env_file=env_file)
        
    db_config = get_db_config()
    
    try:
        conn = psycopg2.connect(**db_config)
        cursor = conn.cursor()
        
        # Query to get ratio per invoice
        query = """
        with invitem as(
            select fid,
            count(1) totalRow,
            sum(famount) famount
            from t_sim_vatinvoice_item_1201
            group by fid
        )
        select 
        count(1)::decimal / nullif(min(invitem.totalrow), 0) ratio
        from t_sim_match_result_1201 re
        join t_sim_vatinvoice_1201 inv on re.finvoiceid = inv.fid
        join invitem on re.finvoiceid = invitem.fid
        group by re.finvoiceid;
        """
        
        print("Executing query...")
        cursor.execute(query)
        rows = cursor.fetchall()
        
        if not rows:
            print("No matches found in database.")
            return

        ratios = [float(row[0]) for row in rows if row[0] is not None]
        
        if not ratios:
            print("No valid ratios found.")
            return
            
        avg_ratio = mean(ratios)
        
        print("-" * 50)
        print(f"整张红冲的行数比例（平均）: {avg_ratio:.2%}")
        print("-" * 50)
        
        cursor.close()
        conn.close()
        
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()
