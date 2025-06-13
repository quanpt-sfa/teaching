import pandas as pd

def save_schema_results_csv(results, out_path):
    df = pd.DataFrame(results)
    df.to_csv(out_path, index=False, encoding='utf-8-sig')
