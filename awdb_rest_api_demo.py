# -*- coding: utf-8 -*-
"""
Created by Beau Uriona
Jun 3, 2024
"""

import datetime
from datetime import date

import folium
import pandas as pd
import requests
from altair import Axis, Chart, X

DOMAIN = "https://wcc.sc.egov.usda.gov"
BASE_URL = f"{DOMAIN}/awdbRestApi/services/v1"


def get_ref_data(base_url=BASE_URL):
    endpoint = "reference-data"
    url = f"{base_url}/{endpoint}"
    req = requests.get(url)
    print(f"{'Sucess!' if req.ok else 'Failed!'} - {url}")
    if req.ok:
        return req.json()
    return {}


def get_stations(networks="*", huc_filter="*", active_only=True, base_url=BASE_URL):
    endpoint = "stations"
    network_triplets = [f"*:*:{network}" for network in networks.split(",")]
    args = f"stationTriplets={','.join(network_triplets)}"
    if not active_only:
        args = f"{args}&activeOnly=false"
    url = f"{base_url}/{endpoint}?{args}"
    req = requests.get(url)
    print(f"{'Sucess!' if req.ok else 'Failed!'} - {url}")
    if req.ok:
        results = req.json()
        if huc_filter != "*":
            results[:] = [
                i for i in results if i.get("huc", "NA").startswith(str(huc_filter))
            ]
        return results
    return {}


def get_metadata(triplets, elements="*", durations="DAILY", base_url=BASE_URL):
    endpoint = "stations"
    triplet_arg = f"stationTriplets={triplets}"
    element_arg = (
        f"returnStationElements=true&durations={durations}&elements={elements}"
    )
    args = f"{triplet_arg}&{element_arg}&returnForecastPointMetadata=true&returnReservoirMetadata=true&activeOnly=false"
    url = f"{base_url}/{endpoint}?{args}"
    req = requests.get(url)
    print(f"{'Sucess!' if req.ok else 'Failed!'} - {url}")
    if req.ok:
        return req.json()
    return {}


def get_wy_data(triplet, duration="DAILY", element="WTEQ", base_url=BASE_URL):
    endpoint = "data"
    triplet_arg = f"stationTriplets={triplet}"
    element_arg = f"elements={element}&duration={duration}"
    today = date.today()
    wy_start_date = date(today.year - 1, 10, 1)
    if today.month > 9:
        wy_start_date = date(today.year, 10, 1)
    date_args = f"beginDate={wy_start_date:%Y-%m-%d}&endDate={today:%Y-%m-%d}"
    args = f"{triplet_arg}&{element_arg}&{date_args}&periodRef=START"
    url = f"{base_url}/{endpoint}?{args}"
    req = requests.get(url)
    print(f"{'Sucess!' if req.ok else 'Failed!'} - {url}")
    if req.ok:
        return req.json()[0]
    return {}


def get_stations_wy_data(station):
    triplet = station["stationTriplet"]
    element = station["stationElements"][0]
    element_code = element["elementCode"]
    units = element["originalUnitCode"]
    element_duration = element["durationName"]
    data = get_wy_data(triplet=triplet, element=element_code, duration=element_duration)
    df = pd.DataFrame(data.get("data", [{}])[0].get("values", []))
    df.index.name = f"{element_code} ({units})"
    return df


def get_wy_forecasts(triplet, base_url=BASE_URL):
    endpoint = "forecasts"
    triplet_arg = f"stationTriplets={triplet}"
    element_arg = "elementCodes=SRVO"
    today = date.today()
    wy_start_date = date(today.year - 1, 10, 1)
    if today.month > 9:
        wy_start_date = date(today.year, 10, 1)
    date_args = f"beginPublicationDate={wy_start_date:%Y-%m-%d}&endPublicationDate={today:%Y-%m-%d}"
    args = f"{triplet_arg}&{element_arg}&{date_args}"
    url = f"{base_url}/{endpoint}?{args}"
    req = requests.get(url)
    print(f"{'Sucess!' if req.ok else 'Failed!'} - {url}")
    if req.ok:
        return req.json()[0]
    return {}


def prepare_wy_forecasts(station, period=("04-01", "07-31")):
    triplet = station["stationTriplet"]
    forecasts = get_wy_forecasts(triplet).get("data", [])
    if not forecasts:
        return pd.DataFrame()
    forecasts = [
        {"Date": i["publicationDate"], **i["forecastValues"]}
        for i in forecasts
        if i["forecastPeriod"][0] == period[0] and i["forecastPeriod"][1] == period[1]
    ]

    df = pd.DataFrame(forecasts)
    df = df.melt(
        id_vars="Date",
        var_name="Exceedance",
        value_name="APR-JUL SRVO (kaf)",
        value_vars=(i for i in df.columns if i.isnumeric()),
    )
    df["Exceedance"] = df["Exceedance"].apply(lambda x: f"{x}%")
    return df


ref_data = get_ref_data()
# Roaring Fork HUC = 14010004 - https://nwcc-apps.sc.egov.usda.gov/imap/#version=169&basins=14010004&activeForecastPointsOnly=false&hucLabels=true&hucIdLabels=false&popupBasin=14010004&displayType=basin&basinType=8&dataElement=FCST&parameter=PCTMED&frequency=MONTHLY&duration=primary&month=4&monthPart=B&relativeDate=-2&lat=39.561&lon=-106.562&zoom=8.0
# gage/forecast triplet = 09085000:CO:USGS

stations = get_stations(
    networks="SNTL,USGS,BOR",
    huc_filter="14010004",
)
triplets = [i["stationTriplet"] for i in stations]
met_metadata = get_metadata(
    triplets=",".join(triplets),
    elements="WTEQ",
    durations="DAILY",
)
reservoir_metadata = get_metadata(
    triplets=",".join(triplets),
    elements="RESC",
    durations="MONTHLY",
)
gage_metadata = get_metadata(
    triplets=",".join(triplets),
    elements="SRVO",
    durations="MONTHLY",
)
gage_metadata = [i for i in gage_metadata if "roaring" in i["name"].lower()]
all_metadata = met_metadata + reservoir_metadata + gage_metadata


def get_marker_icon(station):
    icon_markers = {"SNTL": "cloud", "BOR": "droplet", "USGS": "water"}
    icon_colors = {"SNTL": "blue", "BOR": "green", "USGS": "red"}
    network = station["networkCode"]
    icon = icon_markers.get(network, "location-dot")
    color = icon_colors.get(network, "black")
    return folium.Icon(prefix="fa", icon=icon, color=color)


def get_daily_data_chart_popup(station):
    df = get_stations_wy_data(station)
    data_label = df.index.name.replace("_", "-")
    df.rename(
        columns={"date": "Date", "value": data_label},
        inplace=True,
    )
    if not df.empty:
        popup = folium.Popup()
        scatter = (
            Chart(df, title="Snow Water Equivalent")
            .mark_line()
            .encode(
                x="Date:T",
                y=data_label,
            )
            .interactive()
        )
        vega_lite = folium.VegaLite(
            scatter,
            width="100%",
            height="100%",
        )
        vega_lite.add_to(popup)
    else:
        popup = folium.Popup("No data!")
    return popup


def get_monthly_data_chart_popup(station):
    popup = folium.Popup()
    df = get_stations_wy_data(station)
    df["Date"] = df[["year", "month"]].apply(lambda s: datetime.datetime(*s, 1), axis=1)
    data_label = df.index.name.replace("_", "-")
    df.rename(
        columns={"value": data_label},
        inplace=True,
    )
    if not df.empty:
        if "srvo" in data_label.lower():
            df_forecasts = prepare_wy_forecasts(station)
            scatter = (
                Chart(df_forecasts, title="Forecast Data")
                .mark_circle(size=60)
                .encode(
                    x=X("Date:T", axis=Axis(tickCount="month", format="%b %Y")),
                    y="APR-JUL SRVO (kaf):Q",
                    color="Exceedance:N",
                    tooltip=["APR-JUL SRVO (kaf)"],
                )
                .interactive()
            )
            df_obs = df[df["Date"].dt.month.isin([4, 5, 6, 7])]
            df_obs["Observed Volume"] = df_obs[data_label].cumsum() / 1000
            line = (
                Chart(df_obs)
                .mark_line()
                .encode(
                    x=X("Date:T", axis=Axis(tickCount="month", format="%b %Y")),
                    y="Observed Volume",
                )
                .interactive()
            )
            vega_lite_fcst = folium.VegaLite(
                scatter + line,
                width="50%",
                height="100%",
            )
            vega_lite_fcst.add_to(popup)

        else:
            bar = (
                Chart(df, title="Observed Data")
                .mark_bar()
                .encode(
                    x=X("Date:T", axis=Axis(tickCount="month", format="%b %Y")),
                    y=data_label,
                )
                .interactive()
            )
            vega_lite_obs = folium.VegaLite(
                bar,
                width="50%" if "srvo" in data_label.lower() else "100%",
                height="100%",
            )
            vega_lite_obs.add_to(popup)
    else:
        popup = folium.Popup("No data!")
    return popup


map = folium.Map(location=[39.23, -106.90], zoom_start=10)

for station in all_metadata:
    location = [station["latitude"], station["longitude"]]
    if station["networkCode"] == "SNTL":
        popup = get_daily_data_chart_popup(station)
    else:
        popup = get_monthly_data_chart_popup(station)

    folium.Marker(
        location=location,
        tooltip=station["name"],
        popup=popup,
        icon=get_marker_icon(station),
    ).add_to(map)


# https://gist.github.com/beautah/6fd355f70460a361dc3ad51da49df74c
basin_geojson_url = "https://gist.githubusercontent.com/beautah/6fd355f70460a361dc3ad51da49df74c/raw/dd75d0ca57c8c1a8208f4cff3a19f50134e1afb2/roaring_fork_huc8.geojson"
basin_geojson_data = requests.get(basin_geojson_url).json()


# or a local file is included in the repo
#
# import json
#
# with open("./roaring_fork_huc8.geojson", "r") as j:
#    basin_geojson_data = json.load(j)

folium.GeoJson(basin_geojson_data, name="Roraring Fork").add_to(map)

folium.LayerControl().add_to(map)

map
