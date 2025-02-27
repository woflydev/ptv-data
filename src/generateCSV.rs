use csv::Reader;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{File, create_dir_all};
use std::io::{BufReader, Write};
use indicatif::{ProgressBar, ProgressIterator};
use chrono::{NaiveTime};
use std::io::BufRead;
use chrono::Timelike;

#[derive(Debug, Deserialize)]
struct Record {
    Business_Date: String,        // e.g. "2022-09-12"
    Day_of_Week: String,          // e.g. "Monday" or "Public Holiday"
    Day_Type: String,             // e.g. "Normal Weekday"
    Mode: String,                 // "Metro" or "V/Line"
    Train_Number: String,         // Using String to avoid parse issues
    Line_Name: String,            // e.g. "Pakenham"
    Group: String,
    Direction: String,            // "U" (Up) or "D" (Down)
    Origin_Station: String,
    Destination_Station: String,
    Station_Name: String,
    Station_Latitude: String,
    Station_Longitude: String,
    Station_Chainage: i32,
    Stop_Sequence_Number: i32,
    Arrival_Time_Scheduled: String,
    Departure_Time_Scheduled: String,
    Passenger_Boardings: i32,
    Passenger_Alightings: i32,
    Passenger_Arrival_Load: i32,
    Passenger_Departure_Load: i32,
}

fn main() -> Result<(), Box<dyn Error>> {
    let file_path = "data.csv";
    let output_dir = "processed";
    
    // Ensure output directory exists
    create_dir_all(output_dir)?;

    let file = File::open(file_path)?;
    let mut rdr = Reader::from_reader(file);

    // Get the total number of records for progress bar calculation.
    let total_records = rdr.records().count();
    let file = File::open(file_path)?;
    let mut rdr = Reader::from_reader(file);
    
    // Initialize aggregation maps and variables.
    let mut boardings_per_line: HashMap<String, i32> = HashMap::new();
    let mut alightings_per_line: HashMap<String, i32> = HashMap::new();
    let mut services_count: HashMap<String, i32> = HashMap::new();
    let mut time_series: HashMap<String, Vec<i32>> = HashMap::new();
    let mut selected_business_date: Option<String> = None;

    let pb = ProgressBar::new(total_records as u64);
    pb.set_message("Processing CSV...");
    pb.set_style(indicatif::ProgressStyle::default_bar()
        .template("{msg} {wide_bar} {pos}/{len} ({eta})")
        .progress_chars("█▒░"));
    pb.enable_steady_tick(100);

    // Process each record with a progress bar.
    for result in rdr.deserialize() {
        let record: Record = result?;
        let line = record.Line_Name.clone();

        // Aggregate totals for boardings and alightings.
        *boardings_per_line.entry(line.clone()).or_insert(0) += record.Passenger_Boardings;
        *alightings_per_line.entry(line.clone()).or_insert(0) += record.Passenger_Alightings;
        *services_count.entry(line.clone()).or_insert(0) += 1;

        // Handle time series only for the first encountered business date.
        if selected_business_date.is_none() {
            selected_business_date = Some(record.Business_Date.clone());
        }

        if let Some(ref business_date) = selected_business_date {
            if &record.Business_Date == business_date {
                if let Ok(departure_time) = NaiveTime::parse_from_str(&record.Departure_Time_Scheduled, "%H:%M:%S") {
                    let hour = departure_time.hour();
                    let business_hour = if hour < 3 { hour + 21 } else { hour - 3 };
                    // Initialize time_series if necessary and accumulate the count.
                    let entry = time_series.entry(line.clone()).or_insert_with(|| vec![0; 24]);
                    entry[business_hour as usize] += record.Passenger_Boardings + record.Passenger_Alightings;
                }
            }
        }
        pb.inc(1);  // Increment the progress bar after each record is processed.
    }
    pb.finish_with_message("CSV processing complete.");

    // Output formatted CSV files for each line (only if time_series data is present)
    for (line, hourly_counts) in &time_series {
        let output_file_path = format!("{}/{}.csv", output_dir, line);
        let mut file = File::create(&output_file_path)?;
        
        writeln!(file, "Hour,Movements")?; // Writing the header
        for (hour, &count) in hourly_counts.iter().enumerate() {
            writeln!(file, "{},{}", hour, count)?; // Writing hour and movement data
        }
    }

    println!("Processed data saved in '{}'.", output_dir);

    Ok(())
}
