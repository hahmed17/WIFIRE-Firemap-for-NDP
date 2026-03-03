# WIFIRE Firemap

This is a data-driven wildfire simulation workflow for the [WIFIRE Firemap](https://firemap.sdsc.edu/). It uses the FARSITE fire growth model to simulate how a fire spreads over time, driven by real weather data and landscape characteristics retrieved from the WIFIRE platform.

The example fire used throughout this workspace is the **Border 2 fire**. Because satellite observations of fire perimeters are sparse, synthetic perimeters have been interpolated between real observations to increase temporal resolution and give the simulation more frequent reference points to work from.

The workflow is split across two notebooks that must be run in order: one that prepares all the input data, and one that runs the simulation and produces results.

---

---

## Requirements

**Hardware**
- At least 4 CPUs

**Dependencies**

Run the following from the workspace root to install all required packages:
```bash
chmod +x install_packages.sh
./install_packages.sh
```

A full list of Python dependencies is available in `requirements.txt`.

---

## Workspace Structure
```
WIFIRE-Firemap-for-NDP/
│
├── data_preparation.ipynb      # Step 1: Retrieve and prepare all input data
├── Firemap.ipynb                # Step 2: Run FARSITE simulation and visualize results
│
├── data/                        # Input data and configuration
│   ├── workflow_config.json     # Fire parameters shared between notebooks
│   ├── FIRMS_detections.ipynb   # Active fire detection data (VIIRS/MODIS)
│   ├── Firemap_perimeters.ipynb # Observed fire perimeter data
│   ├── Firemap_weather.ipynb    # Weather data retrieval
│   └── LANDFIRE.ipynb           # Landscape and fuel layer data
│
├── src/                         # Source modules used by the notebooks
│   ├── farsite.py               # FARSITE execution wrapper
│   ├── firemap.py               # Firemap catalog data retrieval (weather, perimeters)
│   ├── geometry.py              # Coordinate and geometry utilities
│   ├── config.py                # Shared configuration and default parameters
│   ├── lcpmake                  # Landscape file builder for FARSITE
│   ├── NoBarrier/               # FARSITE dependency
│   └── TestFARSITE              # FARSITE executable
│
├── install_packages.sh          # Dependency installation script
└── requirements.txt             # Python package requirements
```

---

## Notebook Execution Order

### 1. `data_preparation.ipynb`

This notebook retrieves and assembles all the inputs the simulation needs before FARSITE can run. It queries the WIFIRE Firemap platform for each data component and writes them to the `data/` directory, along with a `workflow_config.json` file that records the fire parameters so they can be shared with the simulation notebook without re-entry.

The data components it retrieves are:

- **Active fire detections** — satellite-based observations (VIIRS/MODIS) identifying where fire was detected on the ground, used to establish the initial fire perimeter
- **Observed fire perimeters** — recorded boundary polygons of the fire at points in time, sorted chronologically; includes both real observations and synthetically interpolated perimeters for the Border 2 fire
- **Weather data** — wind speed, wind direction, temperature, and humidity for the fire's location and time window, sourced from real-time sensor networks or NOAA forecast products
- **Landscape data (LANDFIRE)** — static spatial layers describing surface fuel type, canopy characteristics, and topography (elevation, slope, aspect) across the fire area

Run this notebook once before running `Firemap.ipynb`. It does not need to be re-run unless you are changing the fire or time window.

### 2. `Firemap.ipynb`

This notebook runs the FARSITE simulation and produces the final outputs. It loads the configuration and data written by `data_preparation.ipynb` and steps through the observed perimeters sequentially — for each consecutive pair, it fetches weather for that interval, runs FARSITE forward from the earlier perimeter, and compares the predicted result to the later observed perimeter.

The simulation produces both a spatial map and quantitative accuracy metrics for each timestep.

---


## Outputs

After running `Firemap.ipynb`, the following outputs are written to the `data/` directory:

| File | Description |
|------|-------------|
| `<fire_name>_farsite_predictions.geojson` | Predicted fire perimeters for all timesteps (WGS84) |
| `<fire_name>_farsite_results.pkl` | Full results dictionary including all geometries and weather records |
| `<fire_name>_farsite_results.png` | Map panels showing predicted vs. observed perimeters at each timestep |

### Reading the output map

Each panel in the results map corresponds to one simulation timestep and shows three perimeter boundaries overlaid on a basemap:

- **Green** — the initial perimeter used to seed FARSITE at the start of the interval
- **Red** — the perimeter predicted by FARSITE at the end of the interval
- **Blue** — the observed perimeter at the end of the interval (ground truth)

Closer agreement between the red and blue boundaries indicates a more accurate prediction for that timestep. The wind conditions used for that interval are shown in the panel title.

---

## Configuring a Different Fire

To run the workflow on a different fire, open `data_preparation.ipynb` and update the fire parameters at the top of the notebook — fire name, ignition and containment dates, and the coordinate point for weather queries. Then re-run `data_preparation.ipynb` in full before running `Firemap.ipynb`.

The landscape file (LCP) must cover the geographic extent of the new fire. If a pre-built LCP is not available, `src/lcpmake` can be used to generate one from LANDFIRE data for any area of interest.

---

## About

This workspace is part of the NSF-funded [National Data Platfrom](https://nationaldataplatform.org/) project at UC San Diego.