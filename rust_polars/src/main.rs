use polars::prelude::*;
use rand::Rng;
use std::time::Instant;

fn main() -> Result<(), PolarsError> {
    println!("Generating data...");
    let num_runs = 100;
    let size = 100_000_000;
    let mut rng = rand::thread_rng();
    let id: Vec<i32> = (0..size).collect();
    let value1: Vec<f64> = (0..size).map(|_| rng.gen_range(0.0..100.0)).collect();
    let value2: Vec<f64> = (0..size).map(|_| rng.gen_range(0.0..100.0)).collect();
    let category: Vec<&str> = (0..size)
        .map(|_| match rng.gen_range(0..3) {
            0 => "A",
            1 => "B",
            _ => "C",
        })
        .collect();

    let df = DataFrame::new(vec![
        Series::new("id", id),
        Series::new("value1", value1),
        Series::new("value2", value2),
        Series::new("category", category),
    ])?;

    println!("Starting Polars operations...");
    let start = Instant::now();

    for i in 0..num_runs {
        let _result = df
            .clone()
            .lazy()
            .with_column((col("value1") + col("value2")).alias("sum"))
            .with_column(col("sum").mean().over([col("category")]).alias("category_mean"))
            .with_column((col("sum") - col("category_mean")).pow(2).alias("squared_diff"))
            .group_by([col("category")])
            .agg([
                col("squared_diff").mean().alias("variance"),
                col("sum").count().alias("count"),
            ]) 
            .sort("category", SortOptions::default())
            .collect()?;
        let duration = start.elapsed();
        println!("Time after run {}: {:?}", i+1, duration);
    }

    let duration = start.elapsed();
    println!("Average Rust Polars execution time: {:?}", duration / num_runs);

    Ok(())
}