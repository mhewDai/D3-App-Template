import "./style.css";
import * as d3 from "d3";

import { lineChart } from "./line-chart";
import { Int32, Table, Utf8 } from "apache-arrow";
import { db } from "./duckdb";
import parquet from "./pittsbrughAir.parquet?url";

const app = document.querySelector("#app")!;

// Create the chart. The specific code here makes some assumptions that may not hold for you.
const chart = lineChart();

async function update(City: string) {
  // Query DuckDB for the data we want to visualize.
  let original: Table<{aqiO: Int32; timeO: Utf8;}>;
  if (City == 'All'){
    original = await conn.query(`
    select "US AQI" as aqiO,
    strftime("Timestamp(UTC)", '%Y-%m-%d') as timeO
    from pittsbrughAir.parquet`)
  } else { //When no city is selected
    original = await conn.query(`
    select "US AQI" as aqiO,
    strftime("Timestamp(UTC)", '%Y-%m-%d') as timeO,
    from pittsbrughAir.parquet
    where City = '${City}'`);
  }
  
  let data: Table<{avgAqi: Int32; time: Int32; lo: Int32; up: Int32;}>;
  if (City == 'All'){
  data = await conn.query(`
    select avg("US AQI") as avgAqi,
    quantile_cont("US AQI",0.9) as up,
    quantile_cont("US AQI",0.1) as lo,
    strftime(date_trunc('month', "Timestamp(UTC)")+15, '%Y-%m') as time
    from pittsbrughAir.parquet
    GROUP BY time
    ORDER BY time
    `);
  } else { //When no city is selected
    data = await conn.query(`
    select avg("US AQI") as avgAqi,
    quantile_cont("US AQI",0.9) as up,
    quantile_cont("US AQI",0.1) as lo,
    strftime(date_trunc('month', "Timestamp(UTC)")+15, '%Y-%m') as time
    from pittsbrughAir.parquet
    WHERE City = '${City}'
    GROUP BY time
    ORDER BY time`)
  }
  // Get the X and Y columns for the chart. Instead of using Parquet, DuckDB, and Arrow, we could also load data from CSV or JSON directly.
  const X = data.getChild("time")!.toJSON().map((d) => `${d}`);
  const time_O = original.getChild("timeO")!.toJSON().map((d) => `${d}`);
  const aqi_O = original.getChild("aqiO")!.toArray();
  const Y = data.getChild("avgAqi")!.toArray();
  const Y_lo = data.getChild("lo")!.toArray();
  const Y_up = data.getChild("up")!.toArray();
  chart.update(X, Y, Y_lo, Y_up, time_O.length);
  chart.updatePoints(time_O,aqi_O)
}

// Load a Parquet file and register it with DuckDB. We could request the data from a URL instead.
const res = await fetch(parquet);
await db.registerFileBuffer(
  "pittsbrughAir.parquet",
  new Uint8Array(await res.arrayBuffer())
);

// Query DuckDB for the locations.
const conn = await db.connect();

const locations: Table<{ station: Utf8 }> = await conn.query(`
select distinct City, count(City) as cnt
FROM pittsbrughAir.parquet
Group By City`);

// Create a select element for the locations.
const select = d3.select(app).append("select");
select.append("option").text("All (10385)");
for (const location of locations) {
  select.append("option").text(location.City + " (" + location.cnt + ")");
}

select.on("change", () => {
  const location = select.property("value");
  update(location.split(" ")[0]);
});

// Update the chart with the first location.
update("All");

// Add the chart to the DOM.
app.appendChild(chart.element);
