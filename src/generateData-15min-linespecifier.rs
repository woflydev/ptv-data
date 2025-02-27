use csv::Reader;
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{File, create_dir_all};
use std::io::{Write};
use indicatif::{ProgressBar, ProgressIterator};
use chrono::{NaiveTime};
use std::env; // To access command-line arguments
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

    // Check if an optional line specifier is provided
    let args: Vec<String> = env::args().collect();
    let specified_line = args.get(1).map(|s| s.to_lowercase());

    // Ensure output directory exists
    create_dir_all(output_dir)?;

    let file = File::open(file_path)?;
    let mut rdr = Reader::from_reader(file);

    // Get the total number of records for progress bar calculation.
    let total_records = rdr.records().count();
    let file = File::open(file_path)?;
    let mut rdr = Reader::from_reader(file);

    // Initialize aggregation maps and variables.
    let mut time_series: HashMap<String, HashMap<String, Vec<f64>>> = HashMap::new(); // Using a HashMap to store data by date

    let pb = ProgressBar::new(total_records as u64);
    pb.set_message("Processing CSV...");
    pb.set_style(indicatif::ProgressStyle::default_bar()
        .template("{msg} {wide_bar} {pos}/{len} ({eta})")
        .progress_chars("█▒░"));
    pb.enable_steady_tick(100);

    // Process each record with a progress bar.
    for result in rdr.deserialize() {
        let record: Record = result?;
        let line = record.Line_Name.to_lowercase();  // Ensure case-insensitivity
        let business_date = record.Business_Date.clone();

        // If a line is specified, skip records that do not match
        if let Some(ref line_specifier) = specified_line {
            if line != *line_specifier {
                continue; // Skip this record if the line doesn't match the specifier
            }
        }

        // Parse the departure time
        if let Ok(departure_time) = NaiveTime::parse_from_str(&record.Departure_Time_Scheduled, "%H:%M:%S") {
            let hour = departure_time.hour();
            let minute = departure_time.minute();
            let decimal_time = if hour < 3 {
                (hour + 24) as f64 + (minute as f64 / 60.0)
            } else {
                hour as f64 + (minute as f64 / 60.0)
            };

            // Initialize time_series if necessary for the specific business_date and line
            let entry = time_series.entry(business_date.clone())
                .or_insert_with(HashMap::new)
                .entry(line.clone())
                .or_insert_with(|| vec![0.0; 96]); // 96 intervals in a day

            let time_block = ((decimal_time - 3.0) * 4.0).round() as usize; // 15-minute intervals
            // Ensure the index is within bounds (0..95)
            let time_block = time_block.min(95);  // Clamps the index to the maximum valid value

            entry[time_block] += (record.Passenger_Boardings + record.Passenger_Alightings) as f64;
        }

        pb.inc(1);  // Increment the progress bar after each record is processed.
    }
    pb.finish_with_message("CSV processing complete.");

    // Output formatted CSV files for each line and each business date
    for (business_date, lines) in &time_series {
        for (line, hourly_counts) in lines {
            let output_file_path = format!("{}/{}_{}.csv", output_dir, business_date, line);
            let mut file = File::create(&output_file_path)?;

            writeln!(file, "Time,Movements")?; // Writing the header
            for (interval, &count) in hourly_counts.iter().enumerate() {
                let hour = 3 + (interval as f64 / 4.0).floor() as i32; // Convert interval back to hour
                let minute = (interval % 4) * 15;
                writeln!(file, "{:02}:{:02},{:.2}", hour, minute, count)?; // Writing time and movement data
            }
        }
    }

    println!("Processed data saved in '{}'.", output_dir);

    Ok(())
}
