import pandas as pd
import numpy as np
from typing import Dict, List, Tuple, Optional, Union, Any
from sklearn.cluster import KMeans
from sklearn.decomposition import PCA
from sklearn.impute import SimpleImputer
import matplotlib.pyplot as plt
import seaborn as sns
from scipy.stats import ttest_ind
from numpy.typing import ArrayLike

# Event dictionaries
EVENT_MAPPING = {
    1975: "Expulsion of Moroccans from Algeria; Algiers Agreement (Iran-Iraq)",
    1984: "UK-Libya diplomatic crisis; WPC Fletcher killing",
    2004: "Morocco Family Code Reform; Free Trade with US; Earthquake; Migrant crisis",
    2005: "Algeria National Reconciliation Referendum; EU agreement",
    2008: "Global Financial Crisis",
    2010: "Pre-Arab Spring crackdowns in Libya and Morocco; Lebanon border clashes",
    2011: "Arab Spring; Oman Protests",
    2013: "Jordan elections and reform resistance; Saudi repression and migration diplomacy",
    2014: "Oil price crash",
    2015: "Kafala reforms in Qatar; Tunisia resort attack; Algeria low oil prices",
    2016: "UAE gender policy issues; Iraq Mosul offensive ; SDG rollout",
    2019: "Protests in Jordan, Lebanon, Iraq, Tunisia; UAE visas; Oman's Gulf incidents; Saudi reforms",
    2020: "COVID-19 pandemic",
    2022: "Qatar FIFA World Cup; Ukraine War",
}

SHORT_EVENTS = {
    2011: "Arab Spring",
    2020: "COVID-19",
    2014: "Oil Crisis",
    2016: "SDG",
    2022: "Ukraine War"
}

MAJOR_EVENTS = {
    2001: "9/11 Attacks",
    2003: "Iraq War Begins",
    2005: "Syrian Withdrawal from Lebanon",
    2006: "Lebanon-Israel War",
    2008: "Global Financial Crisis",
    2010: "Tunisian Revolution",
    2011: "Arab Spring",
    2012: "Syrian Civil War Escalates",
    2013: "Egypt Military Coup",
    2014: "ISIS Emergence / Oil Crisis",
    2015: "Yemen Civil War Escalates",
    2016: "SDG Implementation Begins",
    2017: "Qatar Diplomatic Crisis",
    2018: "Global #MeToo Movement Peaks",
    2019: "Sudanese Revolution",
    2020: "COVID-19 Pandemic",
    2021: "COVID Vaccination Rollouts / Beirut Recovery",
    2022: "Russia-Ukraine War / Global Inflation Surge",
    2023: "Global Energy Crisis / Turkey-Syria Earthquake",
    2024: "Gaza Conflict Escalation / Global Economic Uncertainty"
}

COUNTRY_YEAR_EVENTS = {
    ('Morocco', 2004): "Family Code Reform; Free Trade with US; Earthquake; Migrant Crisis",
    ('Algeria', 2005): "National Reconciliation Referendum; EU Association Agreement",
    ('Libya', 2010): "Pre-Arab Spring Crackdown",
    ('Morocco', 2010): "Pre-Arab Spring Unrest",
    ('Lebanon', 2010): "Border Clashes with Israel",
    ('Tunisia', 2011): "Arab Spring Begins; Ben Ali Ousted",
    ('Egypt', 2011): "Tahrir Square Revolution; Mubarak Ousted",
    ('Libya', 2011): "Civil War; Gaddafi Killed",
    ('Syria', 2011): "Civil War Begins",
    ('Yemen', 2011): "Protests Escalate; Regime Change",
    ('Bahrain', 2011): "Pearl Roundabout Protests; GCC Intervention",
    ('Oman', 2011): "Protests over Jobs and Corruption",
    ('Jordan', 2013): "Elections; Slow Reform",
    ('Saudi Arabia', 2013): "Repression; Foreign Labor Policy Shift",
    ('Iraq', 2014): "ISIS Captures Mosul",
    ('Saudi Arabia', 2014): "Oil Price Crash Begins",
    ('Tunisia', 2015): "Sousse Resort Attack; Terror Wave",
    ('Qatar', 2015): "Kafala Reform Starts; World Cup Prep",
    ('Algeria', 2015): "Oil Revenue Drop; Economic Tension",
    ('Syria', 2015): "Russian Military Intervention",
    ('UAE', 2016): "Gender Policy Controversy",
    ('Iraq', 2016): "Mosul Liberation Offensive",
    ('Morocco', 2016): "Protests after Fishmonger Death",
    ('Lebanon', 2019): "Mass Protests; Currency Crisis Begins",
    ('Iraq', 2019): "Protests over Corruption and Unemployment",
    ('Jordan', 2019): "Youth-Led Protests over Austerity",
    ('Tunisia', 2019): "Presidential Elections; Economic Dissatisfaction",
    ('UAE', 2019): "New Visa Policies; Soft Power Push",
    ('Oman', 2019): "Tanker Incidents; Gulf Tensions",
    ('Saudi Arabia', 2019): "Liberalization Reforms; Aramco IPO",
    ('Algeria', 2019): "Hirak Movement; Bouteflika Resigns",
    ('All', 2020): "COVID-19 Pandemic",
    ('Lebanon', 2020): "Beirut Port Explosion",
    ('Libya', 2020): "Ceasefire Agreement; UN Peace Talks",
    ('Qatar', 2022): "FIFA World Cup",
    ('All', 2022): "Ukraine War Global Impact",
    ('Iran', 2022): "Mahsa Amini Protests; Regional Ripples",
}
