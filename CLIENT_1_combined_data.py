#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd
import numpy as np
import matplotlib.pyplot as plt


# In[2]:


data= pd.read_csv('CLIENT 1 combined_data(2025-3-1-2026-2-28).csv')


# In[3]:


data


# In[4]:


data.columns


# In[13]:


#conversion to seconds
data['Uploaded Duration (sec)']= pd.to_timedelta(data['Uploaded Duration (hh:mm:ss)']).dt.total_seconds()
data['Created Duration (sec)'] = pd.to_timedelta(data['Created Duration (hh:mm:ss)']).dt.total_seconds()
data['Published Duration (sec)'] = pd.to_timedelta(data['Published Duration (hh:mm:ss)']).dt.total_seconds()


# In[14]:


#total duration
data['Total Duration(sec)']= (data['Uploaded Duration (sec)']+ data['Created Duration (sec)'] +
    data['Published Duration (sec)'])
#average duration per file
data['Avg Uploaded Duration']= data['Uploaded Duration (sec)']/data['Uploaded Count']
data['Avg Created Duration'] = data['Created Duration (sec)'] / data['Created Count']
data['Avg Published Duration'] = data['Published Duration (sec)'] / data['Published Count']


# In[17]:


data


# In[18]:


# Conversion KPI
data['Upload → Publish Rate'] = data['Published Count'] / data['Uploaded Count']


# In[19]:


data


# In[20]:


plt.figure()
plt.bar(data['Channel'], data['Uploaded Duration (sec)'])
plt.xticks(rotation=45)
plt.xlabel('Channel')
plt.ylabel('Uploaded Duration')
plt.title('Channel vs Uploaded Duration')
plt.show()


# In[21]:


plt.figure()
plt.bar(data['Channel'], data['Created Duration (sec)'])
plt.xticks(rotation=45)
plt.xlabel("Channel")
plt.ylabel("Created Duration")
plt.title("Channel vs Created Duration")
plt.show()


# In[28]:


plt.figure()
plt.bar(data['Channel'], data['Published Duration (sec)'])
plt.xticks(rotation=45)
plt.xlabel("Channel")
plt.ylabel("Published Duration")
plt.title("Channel vs Published Duration")
plt.show()


# In[29]:


plt.figure()
plt.bar(data['Channel'], data['Total Duration(sec)'])
plt.xticks(rotation=45)
plt.xlabel("Channel")
plt.ylabel("Total Duration")
plt.title("Channel vs Total Duration")
plt.show()


# In[30]:


data.set_index('Channel')[[
    'Uploaded Duration (sec)',
    'Created Duration (sec)',
    'Published Duration (sec)'
]].plot(kind='bar', stacked=True)

plt.ylabel("Duration (seconds)")
plt.title("Channel Duration Breakdown")
plt.show()


# In[1]:


import pyarrow as pa
from database import get_connection


def get_kpi01_overall_publish_rate() -> tuple[pa.Table, str]:
    """
    KPI-01: Overall Publish Rate
    SUM(Published Count) ÷ SUM(Created Count) × 100
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            ROUND(
                100.0 * SUM("Published Count") / NULLIF(SUM("Created Count"), 0),
                2
            ) AS "Overall_Publish_Rate_%"
        FROM client_1_combined_data
    """).arrow()
    con.close()
    return table, "KPI 01 - Overall Publish Rate (%)"


def get_kpi02_amplification_ratio() -> tuple[pa.Table, str]:
    """
    KPI-02: Amplification Ratio
    SUM(Created Count) ÷ SUM(Uploaded Count)
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            ROUND(
                1.0 * SUM("Created Count") / NULLIF(SUM("Uploaded Count"), 0),
                2
            ) AS "Amplification_Ratio"
        FROM client_1_combined_data
    """).arrow()
    con.close()
    return table, "KPI 02 - Amplification Ratio"


def get_kpi04_dropoff_volume() -> tuple[pa.Table, str]:
    """
    KPI-04: Drop-off Volume
    SUM(Created Count) − SUM(Published Count)
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            SUM("Created Count") - SUM("Published Count") AS "Dropoff_Volume"
        FROM client_1_combined_data
    """).arrow()
    con.close()
    return table, "KPI 04 - Drop-off Volume"


def get_kpi05_total_content_hours() -> tuple[pa.Table, str]:
    """
    KPI-05: Total Content Hours
    SUM(Created Duration)
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            ROUND(
                SUM(
                    EXTRACT(EPOCH FROM CAST("Created Duration (hh:mm:ss)" AS TIME))
                ) / 3600,
                2
            ) AS "Total_Content_Hours"
        FROM client_1_combined_data
    """).arrow()
    con.close()
    return table, "KPI 05 - Total Content Hours"


def get_kpi06_publish_rate_by_channel() -> tuple[pa.Table, str]:
    """
    KPI-06: Publish Rate by Channel
    Channel Published Count ÷ Channel Created Count × 100
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            "Channel",
            "Published Count",
            "Created Count",
            ROUND(
                100.0 * "Published Count" / NULLIF("Created Count", 0),
                2
            ) AS "Publish_Rate_%"
        FROM client_1_combined_data
        ORDER BY "Publish_Rate_%" DESC NULLS LAST
    """).arrow()
    con.close()
    return table, "KPI 06 - Publish Rate by Channel (%)"


def get_kpi08_zero_publish_channels() -> tuple[pa.Table, str]:
    """
    KPI-08: Zero Publish Channel Count
    COUNT(rows WHERE Published Count = 0)
    """
    con = get_connection()
    table = con.execute("""
        SELECT
            COUNT(*) AS "Zero_Publish_Channel_Count"
        FROM client_1_combined_data
        WHERE "Published Count" = 0
    """).arrow()
    con.close()
    return table, "KPI 08 - Zero Publish Channel Count"


# In[ ]:




