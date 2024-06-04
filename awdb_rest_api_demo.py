# -*- coding: utf-8 -*-
"""
Created by Beau Uriona - beau.uriona@usda.gov
"""

import datetime
import json
from datetime import date

import folium
import pandas as pd
import requests
from altair import Axis, Chart, Column, X

DOMAIN = "https://wcc.sc.egov.usda.gov"
BASE_URL = f"{DOMAIN}/awdbRestApi/services/v1"


def get_ref_data(table="all", base_url=BASE_URL):
    """returns reference data tables, useful for converting codes to full
    names and descriptions
    """

    endpoint = "reference-data"
    url = f"{base_url}/{endpoint}?referenceLists={table}"
    req = requests.get(url)
    print(f"{'Sucess!' if req.ok else 'Failed!'} - {url}")
    if req.ok:
        return req.json()
    return {}


def get_stations(networks="*", huc_filter="*", active_only=True, base_url=BASE_URL):
    """returns a list of stations and basic metadata based on network and huc,
    could be readily expanded for more filters
    """

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
    return []


def get_metadata(triplets, elements="*", durations="DAILY", base_url=BASE_URL):
    """returns all metadata associated with stations based on a list of
    triplets. Filter station elements based on elementCode and Duration
    """

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
    """returns the current water years data for a single station for a single
    desired element and duration
    """

    endpoint = "data"
    triplet_arg = f"stationTriplets={triplet}"
    element_arg = f"elements={element}&duration={duration}"
    today = date.today()
    wy_start_date = date(today.year - 1, 10, 1)
    if today.month > 9:
        wy_start_date = date(today.year, 10, 1)
    date_args = f"beginDate={wy_start_date:%Y-%m-%d}&endDate={today:%Y-%m-%d}"
    misc_args = "periodRef=START&centralTendencyType=MEDIAN"
    args = f"{triplet_arg}&{element_arg}&{date_args}&{misc_args}"
    url = f"{base_url}/{endpoint}?{args}"
    req = requests.get(url)
    print(f"{'Sucess!' if req.ok else 'Failed!'} - {url}")
    if req.ok:
        return req.json()[0]
    return {}


def get_stations_wy_data(station):
    """formats a stations wy data as a pandas dataframe where the index is
    the name of the element with units
    """

    triplet = station["stationTriplet"]
    element = station["stationElements"][0]
    element_code = element["elementCode"]
    units = element["originalUnitCode"]
    element_duration = element["durationName"]
    data = get_wy_data(triplet=triplet, element=element_code, duration=element_duration)
    df = pd.DataFrame(data.get("data", [{}])[0].get("values", []))
    data_label = f"{element_code} ({units})".replace("_", "-")
    if "date" not in df.columns:
        df["date"] = df[["year", "month"]].apply(
            lambda s: datetime.datetime(*s, 1), axis=1
        )
        # df.drop(columns=("year", "month"), inplace=True)
    df = df.melt(
        id_vars="date",
        var_name="data_type",
        value_name=data_label,
        value_vars=(i for i in df.columns if i in ("value", "median")),
    )
    df.index.name = data_label
    return df


def get_wy_forecasts(triplet, base_url=BASE_URL):
    """returns a forecast points current water year forecasts based on a
    single site triplet
    """

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
    """formats a site's water year forecasts as a pandas dataframe,
    flattened for use in charting/tables
    """

    triplet = station["stationTriplet"]
    forecasts = get_wy_forecasts(triplet).get("data", [])
    if not forecasts:
        return pd.DataFrame()
    forecasts = [
        {"date": i["publicationDate"], **i["forecastValues"]}
        for i in forecasts
        if i["forecastPeriod"][0] == period[0] and i["forecastPeriod"][1] == period[1]
    ]

    df = pd.DataFrame(forecasts)
    df = df.melt(
        id_vars="date",
        var_name="Exceedance",
        value_name="APR-JUL SRVO (kaf)",
        value_vars=(i for i in df.columns if i.isnumeric()),
    )
    df["Exceedance"] = df["Exceedance"].apply(lambda x: f"{x}%")
    return df


# This analysis will focus on
# Roaring Fork HUC = 14010004 - https://nwcc-apps.sc.egov.usda.gov/imap/#version=169&basins=14010004&activeForecastPointsOnly=false&hucLabels=true&hucIdLabels=false&popupBasin=14010004&displayType=basin&basinType=8&dataElement=FCST&parameter=PCTMED&frequency=MONTHLY&duration=primary&month=4&monthPart=B&relativeDate=-2&lat=39.561&lon=-106.562&zoom=8.0
# gage/forecast triplet of pour point = 09085000:CO:USGS - Roaring Fork River at Glenwood Springs, Co. - https://waterdata.usgs.gov/monitoring-location/09085000/#parameterCode=00065&period=P7D&showMedian=false

# get a list of all snotel, streamflow, and reservoir stations in our desired HUC
stations = get_stations(
    networks="SNTL,USGS,BOR",
    huc_filter="14010004",
)

# prepare a list of only the triplets to be fed to metadata method
triplets = [i["stationTriplet"] for i in stations]

# get a list of snotels and the daily snow water equivalent (WTEQ) elements in our desired HUC
met_metadata = get_metadata(
    triplets=",".join(triplets),
    elements="WTEQ",
    durations="DAILY",
)
# get a list of reservoirs and the monthly reservoir storage (RESC) elements in our desired HUC
reservoir_metadata = get_metadata(
    triplets=",".join(triplets),
    elements="RESC",
    durations="MONTHLY",
)
# get a list of streamflow/forecasts and the monthly adjusted streamflow (SRVO) elements in our desired HUC
gage_metadata = get_metadata(
    triplets=",".join(triplets),
    elements="SRVO",
    durations="MONTHLY",
)

# filter out only our desired "pour point" forecast in the Roaring Fork HUC
gage_metadata = [i for i in gage_metadata if "roaring" in i["name"].lower()]

# combine the metadata lists into all the stations we care about in this analysis
all_metadata = met_metadata + reservoir_metadata + gage_metadata

# lets take a look at the first item in the metadata list
print(json.dumps(all_metadata[0], indent=2))


def get_marker_icon(station):
    """returns a network dependent folium icon for use in a folium marker"""

    icon_markers = {"SNTL": "cloud", "BOR": "droplet", "USGS": "water"}
    icon_colors = {"SNTL": "blue", "BOR": "green", "USGS": "red"}
    network = station["networkCode"]
    icon = icon_markers.get(network, "location-dot")
    color = icon_colors.get(network, "black")
    return folium.Icon(prefix="fa", icon=icon, color=color)


def get_daily_snotel_data_chart_popup(station):
    """returns a folium popup embeded with a chart based on current water
    year daily SNOTEL station data
    """

    df = get_stations_wy_data(station)
    if not df.empty:
        data_label = df.index.name.replace("_", "-")
        df.rename(
            columns={"date": "Date"},
            inplace=True,
        )
        popup = folium.Popup()
        scatter = (
            Chart(df, title="Snow Water Equivalent")
            .mark_line()
            .encode(x="Date:T", y=data_label, color="data_type")
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


def get_monthly_fcst_data_chart_popup(station):
    """returns a folium popup embeded with a chart based on current water
    year monthly forecast/observed seasonal streamflow data
    """

    popup = folium.Popup()
    df = get_stations_wy_data(station)
    df_forecasts = prepare_wy_forecasts(station)

    if not df.empty and not df_forecasts.empty:
        df_forecasts.rename(
            columns={"date": "Date"},
            inplace=True,
        )
        scatter = (
            Chart(
                df_forecasts,
                title="Forecast Exceedances and Cumulative Seasonal Streamflow to Date",
            )
            .mark_circle(size=60)
            .encode(
                x=X("Date:T", axis=Axis(tickCount="month", format="%b %Y")),
                y="APR-JUL SRVO (kaf):Q",
                color="Exceedance:N",
                tooltip=["APR-JUL SRVO (kaf)"],
            )
            .interactive()
        )

        df.rename(
            columns={"date": "Date"},
            inplace=True,
        )
        data_label = df.index.name
        df_obs = df[df["Date"].dt.month.isin([4, 5, 6, 7])].copy()
        df_obs.loc[:, "Observed Volume"] = df_obs[data_label].cumsum()
        df_obs.loc[:, "Observed Volume"] = (
            df_obs["Observed Volume"] / 1000
        )  # note forecasts are in kaf and streamflow in ac-ft
        line = (
            Chart(df_obs)
            .mark_line()
            .encode(
                x=X("Date:T", axis=Axis(tickCount="month", format="%b %Y")),
                y="Observed Volume",
                color="data_type",
            )
            .interactive()
        )

        vega_lite_fcst = folium.VegaLite(
            scatter + line,
            width="100%",
            height="100%",
        )

        vega_lite_fcst.add_to(popup)
    else:
        popup = folium.Popup("No data!")
    return popup


def get_monthly_res_data_chart_popup(station):
    """returns a folium popup embeded with a chart based on current water
    year monthly reservoir storage data
    """

    popup = folium.Popup()
    df = get_stations_wy_data(station)

    if not df.empty:
        df.rename(
            columns={"date": "Date"},
            inplace=True,
        )
        data_label = df.index.name
        bar = (
            Chart(df, title="Observed Data")
            .mark_bar()
            .encode(
                x="data_type",
                y=data_label,
                color="data_type",
                column=Column(
                    "Date:T", timeUnit="yearmonth"
                ),  # tickCount="month", format="%b %Y")),
            )
            .configure_view(
                stroke=None,
            )
            .interactive()
        )
        vega_lite_obs = folium.VegaLite(
            bar,
            width="100%",
            height="100%",
        )
        vega_lite_obs.add_to(popup)
    else:
        popup = folium.Popup("No data!")
    return popup


# create a look up dict for network "long names" to be used in map
network_ref_data = get_ref_data(table="networks")
network_name_lookup = {i["code"]: i["name"] for i in network_ref_data["networks"]}

# create a blank folium map
map = folium.Map(location=[39.23, -106.90], zoom_start=10)

# for each station in our list of stations create a folium marker with embedded chart popup
for station in all_metadata:
    location = [station["latitude"], station["longitude"]]
    if station["networkCode"] == "SNTL":
        popup = get_daily_snotel_data_chart_popup(station)
    elif station["networkCode"] == "USGS":
        popup = get_monthly_fcst_data_chart_popup(station)
    elif station["networkCode"] == "BOR":
        popup = get_monthly_res_data_chart_popup(station)

    # get the long name from our network look up dict
    network_long_name = network_name_lookup.get(
        station["networkCode"], "Unknown Network"
    )

    # create a tooltip for each site based on station name and network
    tooltip = f'{station["name"]} ({network_long_name})'

    # add the marker to the map
    folium.Marker(
        location=location,
        tooltip=tooltip,
        popup=popup,
        icon=get_marker_icon(station),
    ).add_to(map)

# add a geojson overlay to show the bounds of our analysis basin

# https://gist.github.com/beautah/6fd355f70460a361dc3ad51da49df74c
basin_geojson_url = "https://gist.githubusercontent.com/beautah/6fd355f70460a361dc3ad51da49df74c/raw/dd75d0ca57c8c1a8208f4cff3a19f50134e1afb2/roaring_fork_huc8.geojson"
basin_geojson_data = requests.get(basin_geojson_url).json()
# or a local file is included in the repo
#
# with open("./roaring_fork_huc8.geojson", "r") as j:
#    basin_geojson_data = json.load(j)

# add the geojson to the map
folium.GeoJson(basin_geojson_data, name="Roraring Fork").add_to(map)

# add layer controls to the map
folium.LayerControl().add_to(map)

# show the map in interactive python terminal or notebook
map
