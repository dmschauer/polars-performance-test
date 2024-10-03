import polars as pl
import numpy as np
import time


def custom_transform(x: float) -> float:
    return np.sin(x) * np.exp(-0.1 * x)

def main():
    print("Generating data...")
    num_runs = 20
    size = 10_000_000
    np.random.seed(42)  # for reproducibility
    df = pl.DataFrame({
        'id': range(size),
        'value1': np.random.uniform(0, 100, size),
        'value2': np.random.uniform(0, 100, size),
        'category': np.random.choice(['A', 'B', 'C'], size)
    })

    print("Starting Polars operations...")
    start_time = time.time()

    for i in range(num_runs):  # Run repeatedly to get a more stable measurement
        _ = (df
            .lazy()
            .with_columns((pl.col('value1') + pl.col('value2')).alias('sum'))
            .with_columns(pl.col('sum').mean().over('category').alias('category_mean'))
            .with_columns(((pl.col('sum') - pl.col('category_mean')) ** 2).alias('squared_diff'))
            .group_by('category')
            .agg([
                pl.col('squared_diff').mean().alias('variance'),
                pl.col('sum').count().alias('count')
            ])
            .sort('category')
            .collect()
        )
        duration = time.time() - start_time
        print(f"Time after run {i}: {duration}")

    duration = time.time() - start_time
    print(f"Average Python Polars execution time: {(duration/num_runs):.4f} seconds")

if __name__ == "__main__":
    main()