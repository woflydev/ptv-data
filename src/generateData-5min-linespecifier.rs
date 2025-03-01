use csv::Reader;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{File, create_dir_all};
use std::io::{Write};
use indicatif::ProgressBar;
use chrono::{NaiveTime};
use std::env;
use chrono::Timelike;

#[derive(Debug, Deserialize)]
struct Record {
    Business_Date: String,
    Line_Name: String,
    Departure_Time_Scheduled: String,
    Passenger_Boardings: i32,
    Passenger_Alightings: i32,
}

fn main() -> Result<(), Box<dyn Error>> {
    let file_path = "data.csv";
    let output_dir = "processed";

    let args: Vec<String> = env::args().collect();
    let block_size: u32 = args.get(1)
        .and_then(|s| s.parse().ok())
        .unwrap_or(5); // Default to 5 minutes

    let intervals_per_hour = 60 / block_size;
    let total_intervals = (24 - 3) * intervals_per_hour;

    create_dir_all(output_dir)?;

    let file = File::open(file_path)?;
    let mut rdr = Reader::from_reader(file);
    let total_records = rdr.records().count();
    
    let file = File::open(file_path)?;
    let mut rdr = Reader::from_reader(file);

    let mut time_series: HashMap<String, Vec<f64>> = HashMap::new();
    let mut first_date: Option<String> = None;

    let pb = ProgressBar::new(total_records as u64);
    pb.set_message("Processing CSV...");
    pb.enable_steady_tick(100);

    for result in rdr.deserialize() {
        let record: Record = result?;
        let line = record.Line_Name.to_lowercase();
        let business_date = record.Business_Date.clone();

        // Set first encountered date, but do NOT break the loop
        if first_date.is_none() {
            first_date = Some(business_date.clone());
        }

        // Skip data if it does not belong to the first encountered date
        if let Some(ref date) = first_date {
            if *date != business_date {
                continue;
            }
        }

        if let Ok(departure_time) = NaiveTime::parse_from_str(&record.Departure_Time_Scheduled, "%H:%M:%S") {
            let hour = departure_time.hour();
            let minute = departure_time.minute();
            let decimal_time = if hour < 3 {
                (hour + 24) as f64 + (minute as f64 / 60.0)
            } else {
                hour as f64 + (minute as f64 / 60.0)
            };

            let entry = time_series.entry(line.clone()).or_insert_with(|| vec![0.0; total_intervals as usize]);

            let time_block = ((decimal_time - 3.0) * intervals_per_hour as f64).round() as usize;
            let time_block = time_block.min(total_intervals as usize - 1);

            entry[time_block] += (record.Passenger_Boardings + record.Passenger_Alightings) as f64;
        }

        pb.inc(1);
    }
    pb.finish_with_message("CSV processing complete.");

    for (line, counts) in &time_series {
        let output_file_path = format!("{}/{}_{}min.csv", output_dir, line, block_size);
        let mut file = File::create(&output_file_path)?;

        writeln!(file, "Time,Movements")?;
        for (interval, &count) in counts.iter().enumerate() {
            let decimal_time = 3.0 + (interval as f64 / intervals_per_hour as f64);
            writeln!(file, "{:.2},{:.2}", decimal_time, count)?;
        }
    }

    println!("Processed data saved in '{}'.", output_dir);

    Ok(())
}
