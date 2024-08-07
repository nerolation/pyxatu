import argparse
from .core import PyXatu

def main():
    parser = argparse.ArgumentParser(description="Run queries against the Xatu API")
    parser.add_argument('query', type=str, help='The SQL query to execute')
    parser.add_argument('--config', type=str, help='Path to the configuration file', default=None)
    parser.add_argument('--columns', type=str, nargs='+', help='List of column names', default=None)
    
    args = parser.parse_args()
    
    xatu = PyXatu(config_path=args.config)
    result = xatu.request_query(args.query, columns=args.columns)
    
    if result is not None:
        print(result)
    else:
        print("Query failed")

if __name__ == "__main__":
    main()