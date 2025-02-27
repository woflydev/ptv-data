use csv::{ReaderBuilder};
use serde::Deserialize;
use std::collections::HashMap;
use std::error::Error;
use std::fs::{File, create_dir_all};
use std::io::{BufReader, Write};
use plotters::prelude::*;
use indicatif::{ProgressBar, ProgressIterator};
use chrono::{NaiveDate, NaiveTime};
use chrono::Timelike;
use std::io::BufRead;
use rayon::prelude::*;
use csv::Reader;


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

        // Aggregate totals for boardings and alightings (sequentially, no issues with mutable borrow here).
        *boardings_per_line.entry(line.clone()).or_insert(0) += record.Passenger_Boardings;
        *alightings_per_line.entry(line.clone()).or_insert(0) += record.Passenger_Alightings;
        *services_count.entry(line.clone()).or_insert(0) += 1;

        // Handle time series only for the first encountered business date.
        if selected_business_date.is_none() {
            selected_business_date = Some(record.Business_Date.clone());
        }

        if let Some(ref business_date) = selected_business_date {
            if &record.Business_Date == business_date {
                // Parse departure time.
                if NaiveDate::parse_from_str(&record.Business_Date, "%Y-%m-%d").is_ok() &&
                   NaiveTime::parse_from_str(&record.Departure_Time_Scheduled, "%H:%M:%S").is_ok() {
                    let time = NaiveTime::parse_from_str(&record.Departure_Time_Scheduled, "%H:%M:%S")?;
                    let hour = time.hour();
                    // Adjust to business hour:
                    // 03:00 - 23:59 -> business hour = hour - 3
                    // 00:00 - 02:59 -> business hour = hour + 21
                    let business_hour = if hour < 3 { hour + 21 } else { hour - 3 };
                    // Sum total movements (boardings + alightings) for this hour.
                    let entry = time_series.entry(line.clone()).or_insert([0; 24]);
                    entry[business_hour as usize] += record.Passenger_Boardings + record.Passenger_Alightings;
                }
            }
        }
        pb.inc(1);  // Increment the progress bar after each record is processed.
    }
    pb.finish_with_message("CSV processing complete.");

    // Compute total movements per line
    let total_movements: HashMap<String, i32> = boardings_per_line.iter()
        .map(|(line, &boardings)| {
            let alightings = alightings_per_line.get(line).unwrap_or(&0);
            (line.clone(), boardings + alightings)
        })
        .collect();

    // Generate the charts (total movements and time series).
    if let Some(business_date) = selected_business_date.clone() {
        generate_total_movements_chart("total_movements_chart.png", "Total Movements by Line", &total_movements)?;
        generate_time_series_chart("time_series_chart.png", &business_date, &time_series)?;
        generate_cumulative_time_series_chart("cumulative_time_series_chart.png", &business_date, &time_series)?;
    }

    println!("\nCharts generated successfully.");

    // Output formatted CSV files for each line (only if time_series data is present)
    for (line, hourly_counts) in &time_series {
        let output_file_path = format!("{}/{}.csv", output_dir, line);
        let mut file = File::create(&output_file_path)?;

        writeln!(file, "Hour,Movements")?;
        for (hour, &count) in hourly_counts.iter().enumerate() {
            writeln!(file, "{},{}", hour, count)?;
        }
    }

    println!("Processed data saved in '{}'.", output_dir);

    Ok(())
}

fn get_color_palette() -> Vec<RGBColor> {
    vec![
        RGBColor(255, 0, 0),       // red
        RGBColor(0, 0, 255),       // blue
        RGBColor(0, 128, 0),       // green
        RGBColor(255, 165, 0),     // orange
        RGBColor(128, 0, 128),     // purple
        RGBColor(0, 128, 128),     // teal
        RGBColor(255, 192, 203),   // pink
        RGBColor(128, 128, 0),     // olive
        RGBColor(0, 0, 0),         // black
        RGBColor(165, 42, 42),     // brown
        RGBColor(0, 255, 255),     // cyan
        RGBColor(255, 215, 0),     // gold
        RGBColor(75, 0, 130),      // indigo
        RGBColor(139, 69, 19),     // saddle brown
        RGBColor(60, 179, 113),    // medium sea green
        RGBColor(218, 112, 214),   // orchid
        RGBColor(255, 140, 0),     // dark orange
        RGBColor(47, 79, 79),      // dark slate gray
        RGBColor(123, 104, 238),   // medium slate blue
        RGBColor(255, 99, 71),     // tomato
    ]
}

/// Generates a vertical bar chart for total movements per line.
fn generate_total_movements_chart(
    filename: &str,
    caption: &str,
    data: &HashMap<String, i32>
) -> Result<(), Box<dyn Error>> {
    let mut data_vec: Vec<(&String, &i32)> = data.iter().collect();
    data_vec.sort_by(|a, b| a.0.cmp(b.0));

    let root = BitMapBackend::new(filename, (1600, 1200)).into_drawing_area();
    root.fill(&WHITE)?;
    let max_value = data_vec.iter().map(|(_, &v)| v).max().unwrap_or(0);

    let mut chart = ChartBuilder::on(&root)
        .caption(caption, ("sans-serif", 50))
        .margin(60)
        .x_label_area_size(100)
        .y_label_area_size(80)
        .build_cartesian_2d(0..data_vec.len(), 0..(max_value + max_value / 10 + 1))?;

    chart.configure_mesh()
        .disable_mesh()
        .x_labels(data_vec.len())
        .x_label_formatter(&|idx| {
            data_vec.get(*idx).map_or("".to_string(), |&(line, _)| line.clone())
        })
        .x_desc("Line")
        .y_desc("Total Movements")
        .label_style(("sans-serif", 30))
        .draw()?;

    let palette = get_color_palette();
    for (i, (_, &value)) in data_vec.iter().enumerate() {
        let color = &palette[i % palette.len()];
        chart.draw_series(std::iter::once(Rectangle::new(
            [(i, 0), (i + 1, value)],
            color.filled(),
        )))?;
        chart.draw_series(std::iter::once(Text::new(
            format!("{}", value),
            ((i + 1), value + max_value / 50),
            ("sans-serif", 30).into_font().color(&BLACK),
        ).into_dyn()))?;
    }
    Ok(())
}

/// Generates a time series chart.
fn generate_time_series_chart(
    filename: &str,
    business_date: &str,
    data: &HashMap<String, Vec<i32>>
) -> Result<(), Box<dyn Error>> {
    let root = BitMapBackend::new(filename, (1600, 1200)).into_drawing_area();
    root.fill(&WHITE)?;

    let max_hourly = data.values().flat_map(|arr| arr.iter()).cloned().max().unwrap_or(0);
    let mut chart = ChartBuilder::on(&root)
        .caption(format!("Hourly Total Movements on {} (Business Day)", business_date), ("sans-serif", 50))
        .margin(60)
        .set_label_area_size(LabelAreaPosition::Left, 100)
        .set_label_area_size(LabelAreaPosition::Bottom, 80)
        .build_cartesian_2d(0..23, 0..(max_hourly + max_hourly / 10 + 1))?;

    chart.configure_mesh()
        .x_desc("Business Hour (0 = 03:00, 23 = 02:00)")
        .y_desc("Movements")
        .label_style(("sans-serif", 30))
        .draw()?;

    let palette = get_color_palette();
    let mut color_iter = palette.into_iter().cycle();

    for (line, hourly_counts) in data {
        let color = color_iter.next().unwrap();
        let series: Vec<(i32, i32)> = hourly_counts.iter()
            .enumerate()
            .map(|(hr, &count)| (hr as i32, count))
            .collect();

        chart.draw_series(LineSeries::new(series.clone(), color.stroke_width(3)))?;
        chart.draw_series(series.iter().map(|&point| {
            Circle::new(point, 7, color.filled())
        }))?
        .label(line)
        .legend(move |(x, y)| {
            Circle::new((x + 10, y), 7, color.filled())
        });
    }

    chart.configure_series_labels()
        .position(SeriesLabelPosition::UpperRight)
        .background_style(&WHITE.mix(0.8))
        .border_style(&BLACK)
        .label_font(("sans-serif", 30))
        .draw()?;

    Ok(())
}

/// Generates a cumulative time series line chart (with markers)
/// for hourly cumulative total movements for the selected business day.
fn generate_cumulative_time_series_chart(
    filename: &str,
    business_date: &str,
    data: &HashMap<String, [i32; 24]>
) -> Result<(), Box<dyn Error>> {
    // Create cumulative sums for each line.
    let mut cumulative_data: HashMap<String, Vec<i32>> = HashMap::new();
    for (line, hourly_counts) in data {
        let mut cum_vec = Vec::with_capacity(24);
        let mut sum = 0;
        for &count in hourly_counts.iter() {
            sum += count;
            cum_vec.push(sum);
        }
        cumulative_data.insert(line.clone(), cum_vec);
    }

    let root = BitMapBackend::new(filename, (1600, 1200)).into_drawing_area();
    root.fill(&WHITE)?;

    // Determine maximum cumulative value.
    let max_cumulative = cumulative_data.values()
        .flat_map(|vec| vec.iter())
        .cloned()
        .max()
        .unwrap_or(0);

    let mut chart = ChartBuilder::on(&root)
        .caption(
            format!("Cumulative Movements on {} (Business Day)", business_date),
            ("sans-serif", 50),
        )
        .margin(60)
        .set_label_area_size(LabelAreaPosition::Left, 100)
        .set_label_area_size(LabelAreaPosition::Bottom, 80)
        .build_cartesian_2d(0..23, 0..(max_cumulative + max_cumulative / 10 + 1))?;

    chart.configure_mesh()
        .x_desc("Business Hour (0 = 03:00, 23 = 02:00)")
        .y_desc("Cumulative Movements")
        .label_style(("sans-serif", 30))
        .draw()?;

    let palette = get_color_palette();
    let mut color_iter = palette.into_iter().cycle();

    for (line, cum_series) in &cumulative_data {
        let color = color_iter.next().unwrap();
        let series: Vec<(i32, i32)> = cum_series
            .iter()
            .enumerate()
            .map(|(hr, &value)| (hr as i32, value))
            .collect();

        chart.draw_series(LineSeries::new(series.clone(), color.stroke_width(3)))?;
        chart.draw_series(series.iter().map(|&point| {
            Circle::new(point, 7, color.filled())
        }))?
        .label(line)
        .legend(move |(x, y)| {
            Circle::new((x + 10, y), 7, color.filled())
        });
    }

    chart.configure_series_labels()
        .position(SeriesLabelPosition::UpperRight)
        .background_style(&WHITE.mix(0.8))
        .border_style(&BLACK)
        .label_font(("sans-serif", 30))
        .draw()?;

    Ok(())
}