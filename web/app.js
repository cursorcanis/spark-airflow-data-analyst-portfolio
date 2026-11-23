// Static web map: reads the GeoJSON snapshots in data/ (exported from the dbt marts by atl_transit.webmap).
"use strict";

// Ordinal blue ramp (validated light->dark, single hue). Dark = worse (slower / less reliable / more car-free).
const BLUE = ["#86b6ef", "#3987e5", "#256abf", "#184f95", "#0d366b"];
// Diverging: blue = buses gain time, gray = no change, red = buses lose time.
const DIVERGE = ["#1c5cab", "#6da7ec", "#b5b4ad", "#ec8a89", "#c43a3a"];

const fmt = (v, d = 0) => (v === null || v === undefined ? "–" : Number(v).toLocaleString("en-US", { maximumFractionDigits: d }));
const pct = (v) => (v === null || v === undefined ? "–" : `${fmt(v, 1)}%`);

// step expression: value < breaks[0] -> colors[0], ... >= last break -> last color
const step = (prop, breaks, colors) => {
  const e = ["step", ["get", prop], colors[0]];
  breaks.forEach((b, i) => e.push(b, colors[i + 1]));
  return e;
};

const LAYERS = {
  speed: {
    source: "h3", type: "fill",
    legend: { title: "Median moving bus speed (km/h)", kind: "fill",
      items: [["under 20", BLUE[4]], ["20 – 25", BLUE[3]], ["25 – 32", BLUE[2]], ["32 – 40", BLUE[1]], ["40 or more", BLUE[0]]] },
    paint: { "fill-color": step("speed_kmh", [20, 25, 32, 40], [BLUE[4], BLUE[3], BLUE[2], BLUE[1], BLUE[0]]),
             "fill-opacity": 0.72, "fill-outline-color": "rgba(255,255,255,0.6)" },
    finding: () => "Each hexagon is about 0.7 km² (H3 resolution 8). Within 5 km of Downtown/Midtown buses average about 24 km/h while moving, vs 35 km/h in the rest of the region. Time spent stopped (stops, signals, layovers) is left out of the speed and shown as “% stopped” in the tooltip.",
    tip: (p) => `<b>H3 ${p.h3}</b><br><span class="k">Median moving speed</span> ${fmt(p.speed_kmh, 1)} km/h
      <br><span class="k">Weekday peak</span> ${fmt(p.peak_speed_kmh, 1)} km/h<br><span class="k">Stopped</span> ${pct(p.pct_stopped)} of GPS samples
      <br><span class="k">Routes</span> ${p.routes} · <span class="k">stops</span> ${p.stops}<br><span class="k">On time at stops here</span> ${pct(p.pct_on_time)}`,
  },
  segments: {
    source: "segments", type: "line",
    legend: { title: "Delay added between consecutive stops (avg)", kind: "line",
      items: [["gains 30 s or more", DIVERGE[0]], ["gains 10 – 30 s", DIVERGE[1]], ["within ±10 s", DIVERGE[2]],
              ["loses 10 – 30 s", DIVERGE[3]], ["loses 30 s or more", DIVERGE[4]]] },
    paint: { "line-color": step("avg_added_s", [-30, -10, 10, 30], DIVERGE),
             "line-width": ["interpolate", ["linear"], ["get", "n"], 15, 1.5, 200, 3.5, 1000, 6],
             "line-opacity": 0.9 },
    layout: { "line-cap": "round" },
    finding: () => "Each line is a stop-to-stop hop. Its color is how much later than schedule buses get between the two stops: red is where they lose time, blue is where padded schedules let them catch up. Most hops are slightly blue: delay is mostly built up in a few hot spots, then slowly recovered. Thicker lines have more observations.",
    tip: (p) => `<b>${p.from} → ${p.to}</b><br><span class="k">Routes</span> ${p.routes}
      <br><span class="k">Avg delay added</span> ${fmt(p.avg_added_s)} s (median ${fmt(p.median_added_s)} s)
      <br><span class="k">Observations</span> ${fmt(p.n)} · ${fmt(p.length_m)} m`,
  },
  tracts: {
    source: "tracts", type: "fill",
    metrics: {
      pct_on_time: { title: "Bus arrivals on time at the tract's stops", kind: "fill",
        items: [["under 55%", BLUE[4]], ["55 – 62%", BLUE[3]], ["62 – 70%", BLUE[2]], ["70 – 77%", BLUE[1]], ["77% or more", BLUE[0]], ["too few arrivals", "#e3e2dd"]],
        color: step("pct_on_time", [55, 62, 70, 77], [BLUE[4], BLUE[3], BLUE[2], BLUE[1], BLUE[0]]) },
      pct_no_car: { title: "Households without a car (ACS 5-year)", kind: "fill",
        items: [["under 5%", BLUE[0]], ["5 – 10%", BLUE[1]], ["10 – 20%", BLUE[2]], ["20 – 35%", BLUE[3]], ["35% or more", BLUE[4]], ["no data", "#e3e2dd"]],
        color: step("pct_no_car", [5, 10, 20, 35], BLUE) },
    },
    finding: () => "Compare the two colorings: on-time service next to how many households depend on transit. Census tracts come from TIGERweb, demographics from the ACS 5-year API, and the stop-to-tract join is a PostGIS point-in-polygon.",
    tip: (p) => `<b>Tract ${p.geoid}</b> · ${p.county}<br><span class="k">Population</span> ${fmt(p.population)}
      <br><span class="k">Car-free households</span> ${pct(p.pct_no_car)}<br><span class="k">Median income</span> $${fmt(p.income)}
      <br><span class="k">On time</span> ${pct(p.pct_on_time)} (${fmt(p.arrivals)} arrivals, ${fmt(p.stops)} stops)`,
  },
};

let active = "speed";
let tractMetric = "pct_on_time";

const map = new maplibregl.Map({
  container: "map",
  style: "https://tiles.openfreemap.org/styles/positron",
  center: [-84.39, 33.77],
  zoom: 10.3,
  attributionControl: { compact: true, customAttribution: "Data: MARTA GTFS/GTFS-rt, U.S. Census Bureau" },
});
map.addControl(new maplibregl.NavigationControl({ showCompass: false }), "top-right");
// On wide screens the panel covers the left of the map: pad the viewport so the region stays centered.
if (window.matchMedia("(min-width: 721px)").matches) map.setPadding({ left: 360 });

function tractPaint() {
  const m = LAYERS.tracts.metrics[tractMetric];
  return ["case", ["==", ["get", tractMetric], null], "#e3e2dd", m.color];
}

function renderLegend() {
  const def = active === "tracts" ? LAYERS.tracts.metrics[tractMetric] : LAYERS[active].legend;
  const rows = def.items.map(([label, color]) =>
    `<div class="row"><span class="sw ${def.kind === "line" ? "line" : ""}" style="background:${color}"></span>${label}</div>`).join("");
  document.getElementById("legend").innerHTML = `<div class="title">${def.title}</div>${rows}`;
  document.getElementById("finding").textContent = LAYERS[active].finding();
  document.getElementById("tract-metric").hidden = active !== "tracts";
}

function show(name) {
  active = name;
  for (const key of Object.keys(LAYERS)) {
    if (map.getLayer(key)) map.setLayoutProperty(key, "visibility", key === name ? "visible" : "none");
  }
  renderLegend();
}

async function getJSON(path) {
  const r = await fetch(path);
  if (!r.ok) throw new Error(`${path}: HTTP ${r.status}`);
  return r.json();
}

map.on("load", async () => {
  // Insert data layers below the basemap's labels so street names stay readable.
  const firstSymbol = map.getStyle().layers.find((l) => l.type === "symbol")?.id;
  const files = { h3: "data/h3_bus_speed.geojson", segments: "data/segment_delay.geojson", tracts: "data/tract_equity.geojson" };
  const order = ["tracts", "speed", "segments"];
  try {
    for (const [id, path] of Object.entries(files)) map.addSource(id, { type: "geojson", data: await getJSON(path) });
  } catch (err) {
    document.getElementById("finding").textContent = `Map data not available yet (${err.message}).`;
    return;
  }
  for (const key of order) {
    const def = LAYERS[key];
    const paint = key === "tracts" ? { "fill-color": tractPaint(), "fill-opacity": 0.65, "fill-outline-color": "rgba(255,255,255,0.5)" } : def.paint;
    map.addLayer({ id: key, type: def.type, source: def.source, paint, layout: { ...(def.layout || {}), visibility: "none" } }, firstSymbol);

    const popup = new maplibregl.Popup({ closeButton: false, closeOnClick: false, offset: 8, maxWidth: "280px" });
    map.on("mousemove", key, (e) => {
      map.getCanvas().style.cursor = "pointer";
      popup.setLngLat(e.lngLat).setHTML(`<div class="tip">${def.tip(e.features[0].properties)}</div>`).addTo(map);
    });
    map.on("mouseleave", key, () => { map.getCanvas().style.cursor = ""; popup.remove(); });
  }
  show(active);
});

document.querySelectorAll('input[name="layer"]').forEach((el) => el.addEventListener("change", (e) => show(e.target.value)));
document.querySelectorAll('input[name="tmetric"]').forEach((el) => el.addEventListener("change", (e) => {
  tractMetric = e.target.value;
  if (map.getLayer("tracts")) map.setPaintProperty("tracts", "fill-color", tractPaint());
  renderLegend();
}));
renderLegend();

getJSON("data/summary.json").then((s) => {
  document.getElementById("s-ontime").textContent = pct(s.pct_on_time);
  document.getElementById("s-gps").textContent = pct(s.gps && s.gps.pct_on_time_gps);
  document.getElementById("s-arrivals").textContent = fmt(s.arrivals);
  const g = s.gps || {};
  if (g.pct_early_gps !== undefined) {
    document.getElementById("gps-note").textContent =
      `GPS check: ${pct(g.pct_gps_confirmed)} of arrivals have a GPS fix at the stop. On those, the bus's own GPS pass time ` +
      `cuts “early” from ${pct(g.pct_early_feed)} to ${pct(g.pct_early_gps)} and raises “late” from ` +
      `${pct(g.pct_late_feed)} to ${pct(g.pct_late_gps)}: the feed's times run ahead of the bus.`;
  }
  document.getElementById("daterange").textContent = `Service days ${s.first_service_date} to ${s.last_service_date}.`;
  document.getElementById("exported").textContent = `Snapshot exported ${String(s.exported_at).slice(0, 16).replace("T", " ")} UTC · ${fmt(s.gps_fixes)} GPS fixes`;
  document.querySelector("#worst tbody").innerHTML = (s.worst_routes || []).map((w) =>
    `<tr><td>Route ${w.route}</td><td class="n">${pct(w.pct_on_time)}</td><td class="n">${fmt(w.arrivals)}</td></tr>`).join("");
}).catch(() => {});
