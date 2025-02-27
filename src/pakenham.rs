use std::fs::File;
use std::io::{self, BufRead};
use chrono::{NaiveTime, Duration};
use chrono::Timelike;
#[derive(Debug)]
struct TrainService {
    train_number: u32,
    station_name: String,
    arrival_time: NaiveTime,
    departure_time: NaiveTime,
    boardings: u32,
    alightings: u32,
    arrival_load: u32,
    departure_load: u32,
}

fn parse_time(time_str: &str) -> Option<NaiveTime> {
    NaiveTime::parse_from_str(time_str, "%H:%M:%S").ok()
}

fn read_data(file_path: &str) -> io::Result<Vec<TrainService>> {
    let file = File::open(file_path)?;
    let reader = io::BufReader::new(file);
    let mut train_services = Vec::new();

    for line in reader.lines().skip(1) {  // Skipping the header
        let line = line?;
        let parts: Vec<&str> = line.split(',').collect();
        
        let train_number = parts[4].parse().unwrap_or(0);
        let station_name = parts[9].to_string();
        
        let arrival_time = parse_time(parts[14]);
        let departure_time = parse_time(parts[15]);
        
        if arrival_time.is_none() || departure_time.is_none() {
            continue; // Skip invalid time entries
        }
        
        let arrival_time = arrival_time.unwrap();
        let departure_time = departure_time.unwrap();
        
        let boardings = parts[19].parse().unwrap_or(0);
        let alightings = parts[20].parse().unwrap_or(0);
        let arrival_load = parts[21].parse().unwrap_or(0);
        let departure_load = parts[22].parse().unwrap_or(0);

        train_services.push(TrainService {
            train_number,
            station_name,
            arrival_time,
            departure_time,
            boardings,
            alightings,
            arrival_load,
            departure_load,
        });
    }

    Ok(train_services)
}

fn calculate_passenger_flow(train_services: Vec<TrainService>) -> Vec<(f64, f64)> {
    let mut passenger_flow = Vec::new();

    for service in train_services {
        let arrival_minutes = service.arrival_time.num_seconds_from_midnight() as f64 / 60.0;
        let departure_minutes = service.departure_time.num_seconds_from_midnight() as f64 / 60.0;

        // Calculate the flow rate (change in passengers) over the time interval
        let rate_of_change = (service.boardings as f64 - service.alightings as f64) / (departure_minutes - arrival_minutes);

        // Generate points for the graph (time in minutes, passenger count)
        passenger_flow.push((arrival_minutes, service.departure_load as f64));

        // Simulate passenger change over the journey
        let num_points = 100;
        for i in 1..num_points {
            let t = arrival_minutes + (departure_minutes - arrival_minutes) * i as f64 / num_points as f64;
            let passengers_at_t = service.departure_load as f64 + rate_of_change * (t - arrival_minutes);
            passenger_flow.push((t, passengers_at_t));
        }
    }

    return passenger_flow
}

fn main() {
    let file_path = "data.csv"; // Path to your dataset
    match read_data(file_path) {
        Ok(train_services) => {
            let passenger_flow = calculate_passenger_flow(train_services);

            // Output to Desmos-friendly format
            println!("x, y");
            for (time, passengers) in passenger_flow {
                println!("{}, {}", time, passengers);
            }
        }
        Err(e) => println!("Error reading data: {}", e),
    }
}
