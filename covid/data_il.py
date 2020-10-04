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


def process_covidtracking_data_il(data: pd.DataFrame, run_date: pd.Timestamp):
    """ Processes raw COVIDTracking data to be in a form for the GenerativeModel.
        In many cases, we need to correct data errors or obvious outliers."""
    data["date"] = pd.to_datetime(data["Date"], format="%Y-%m-%d")
    data = data.rename(columns={"New infected": "positive", "Tests for idenitifaction": "total"})
    data['region'] = "Israel"
    data = data.set_index(["region", "date"]).sort_index()
    data = data[["positive", "total"]]

    # At the real time of `run_date`, the data for `run_date` is not yet available!
    # Cutting it away is important for backtesting!
    return data.loc[idx[:, :(run_date - pd.DateOffset(1))], ["positive", "total"]]


def get_and_process_covidtracking_data_il(run_date: pd.Timestamp):
    """ Helper function for getting and processing COVIDTracking data at once """
    data = get_raw_covidtracking_data_il()
    data = process_covidtracking_data_il(data, run_date)
    return data
