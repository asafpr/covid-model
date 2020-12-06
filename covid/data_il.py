"""
This module contains all US-specific data loading and data cleaning routines.
"""
import requests
import pandas as pd
import numpy as np

idx = pd.IndexSlice


def get_raw_covidtracking_data_il():
    """ Gets the current daily CSV from COVIDTracking """
    url = 'https://raw.githubusercontent.com/dancarmoz/israel_moh_covid_dashboard_data/master/hospitalized_and_infected.csv'
    data = pd.read_csv(url)
    return data

def get_raw_cities_il_data():
    # Get the latest csv file from this link:
    baseurl = 'https://data.gov.il'
    nextapi = '/api/3/action/datastore_search?resource_id=8a21d39d-91e3-40db-aca1-f73f7ab1df69&limit=100000'
    datafs = []
    while (nextapi):
        url = baseurl + nextapi
        with requests.get(url) as r:
            if (not r.json()['result']['records']):
                break
            datafs.append(pd.DataFrame(r.json()['result']['records']))
            nextapi = r.json()['result']['_links']['next']
    data = pd.concat(datafs)
    return(data)

def process_covidtracking_data_il(data: pd.DataFrame, run_date: pd.Timestamp, norm=True, cities=False):
    """ Processes raw COVIDTracking data to be in a form for the GenerativeModel.
        In many cases, we need to correct data errors or obvious outliers."""
    if not cities:
        data["date"] = pd.to_datetime(data["Date"], format="%Y-%m-%d")
        data = data.rename(columns={"New infected": "positive", "Tests for idenitifaction": "total", "New deaths": "deaths"})
        data['region'] = "Israel"
        data = data.set_index(["region", "date"]).sort_index()
        data = data[["positive", "total", "deaths"]]    
    else:
        # Process the cities data
        data["date"] = pd.to_datetime(data["Date"], format="%Y-%m-%d")
        data = data.rename(columns={"City_Code": "region"})  
        data = data.set_index(['region', 'date']).sort_index()
        data = data.apply(pd.to_numeric, errors='coerce')
        data['positive'] = data.groupby('region')['Cumulative_verified_cases'].diff()
        data['deaths'] = data.groupby('region')['Cumulated_deaths'].diff()
        data['total'] = data.groupby('region')['Cumulated_number_of_diagnostic_tests'].diff()
        # Select relevant columns
        data = data[["positive", "total", "deaths"]].fillna(0)
        # Add the sum of all regions
        da2 = data.groupby("date").sum()
        da2["region"] = "Israel"
        da2 = da2.reset_index().set_index(["region", "date"]).sort_index()
        data = pd.concat([data, da2])
    # At the real time of `run_date`, the data for `run_date` is not yet available!
    # Cutting it away is important for backtesting!
    if not norm:
        data["total"] = 100000
    return data.loc[idx[:, :(run_date - pd.DateOffset(1))], ["positive", "total", "deaths"]]


def get_and_process_covidtracking_data_il(run_date: pd.Timestamp, norm=True, cities=False):
    """ Helper function for getting and processing COVIDTracking data at once """
    if cities:
        data = get_raw_cities_il_data()
    else:
        data = get_raw_covidtracking_data_il()
    data = process_covidtracking_data_il(data, run_date, norm=norm, cities=cities)
    return data
