import streamlit as st
import boto3
import pandas as pd
import time
import plotly.express as px

# --------------------------------------------------
# Page configuration
# --------------------------------------------------

st.set_page_config(
    page_title="Insight Flow Dashboard",
    layout="wide"
)

st.title("Insight Flow Dashboard")


# --------------------------------------------------
# Athena connection
# --------------------------------------------------

athena = boto3.client(
    "athena",
    region_name="us-east-2"
)

DATABASE = "insight_flow_gold_db"
OUTPUT_LOCATION = "s3://insight-flow-project/athena-results/"

def run_athena_query(query):

    response = athena.start_query_execution(
        QueryString=query,
        QueryExecutionContext={
            "Database": DATABASE
        },
        ResultConfiguration={
            "OutputLocation": OUTPUT_LOCATION
        }
    )

    query_execution_id = response["QueryExecutionId"]

    while True:

        result = athena.get_query_execution(
            QueryExecutionId=query_execution_id
        )

        status = result["QueryExecution"]["Status"]["State"]

        if status == "SUCCEEDED":
            break

        if status in ["FAILED", "CANCELLED"]:
            st.error(f"Athena query {status}")
            st.stop()

        time.sleep(1)

    results = athena.get_query_results(
        QueryExecutionId=query_execution_id
    )

    columns = [
        col["Label"]
        for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
    ]

    rows = []

    for row in results["ResultSet"]["Rows"][1:]:

        values = [
            field.get("VarCharValue")
            for field in row["Data"]
        ]

        rows.append(values)

    return pd.DataFrame(
        rows,
        columns=columns
    )


# --------------------------------------------------
# Query Gold table
# --------------------------------------------------

query = """
SELECT *
FROM insight_flow_gold_db.daily_marketing_performance
ORDER BY activity_date
"""

response = athena.start_query_execution(
    QueryString=query,
    QueryExecutionContext={
        "Database": DATABASE
    },
    ResultConfiguration={
        "OutputLocation": OUTPUT_LOCATION
    }
)

query_execution_id = response["QueryExecutionId"]


# --------------------------------------------------
# Wait for Athena query to finish
# --------------------------------------------------

while True:

    result = athena.get_query_execution(
        QueryExecutionId=query_execution_id
    )

    status = result["QueryExecution"]["Status"]["State"]

    if status == "SUCCEEDED":
        break

    if status in ["FAILED", "CANCELLED"]:
        st.error(f"Athena query {status}")
        st.stop()

    time.sleep(1)


# --------------------------------------------------
# Get Athena results
# --------------------------------------------------

results = athena.get_query_results(
    QueryExecutionId=query_execution_id
)

columns = [
    col["Label"]
    for col in results["ResultSet"]["ResultSetMetadata"]["ColumnInfo"]
]

rows = []

for row in results["ResultSet"]["Rows"][1:]:

    values = [
        field.get("VarCharValue")
        for field in row["Data"]
    ]

    rows.append(values)

df = pd.DataFrame(
    rows,
    columns=columns
)


# --------------------------------------------------
# Convert data types
# --------------------------------------------------

df["activity_date"] = pd.to_datetime(
    df["activity_date"]
)

numeric_columns = [
    "total_marketing_spend",
    "unique_video_visitors",
    "video_events",
    "avg_percent_viewed",
    "total_bookings",
    "new_leads"
]

for column in numeric_columns:

    df[column] = pd.to_numeric(
        df[column],
        errors="coerce"
    )


# --------------------------------------------------
# Consolidated Marketing Performance
# --------------------------------------------------

st.subheader("Marketing Performance Overview")

col1, col2, col3, col4 = st.columns(4)

total_spend = df["total_marketing_spend"].sum()
total_visitors = df["unique_video_visitors"].sum()
total_bookings = df["total_bookings"].sum()
total_leads = df["new_leads"].sum()

with col1:
    st.metric(
        "Marketing Spend",
        f"${total_spend:,.2f}"
    )

with col2:
    st.metric(
        "Wistia Video Visitors",
        f"{total_visitors:,.0f}"
    )

with col3:
    st.metric(
        "Calendly Bookings",
        f"{total_bookings:,.0f}"
    )

with col4:
    st.metric(
        "New CRM Leads",
        f"{total_leads:,.0f}"
    )

# --------------------------------------------------
# Daily Bookings and Leads Trend
# --------------------------------------------------

st.subheader("Daily Bookings & New Leads")

trend_df = df[
    [
        "activity_date",
        "total_bookings",
        "new_leads"
    ]
].set_index("activity_date")

st.line_chart(trend_df)

st.subheader("Marketing Spend vs Calendly Bookings")

spend_booking_df = df.dropna(
    subset=["total_marketing_spend", "total_bookings"]
)

st.scatter_chart(
    spend_booking_df,
    x="total_marketing_spend",
    y="total_bookings"
)

st.subheader("Marketing Spend vs New CRM Leads")

spend_leads_df = df.dropna(
    subset=["total_marketing_spend", "new_leads"]
)

st.scatter_chart(
    spend_leads_df,
    x="total_marketing_spend",
    y="new_leads"
)


########## DAILY CALLS BOOKED BY SOURCE #############

daily_calls_query = """
SELECT
    booking_date,
    facebook,
    youtube,
    tiktok,
    other
FROM insight_flow_gold_db.daily_calls_booked_by_source
ORDER BY booking_date
"""

daily_calls_df = run_athena_query(daily_calls_query)

daily_calls_df["booking_date"] = pd.to_datetime(
    daily_calls_df["booking_date"]
)

for column in ["facebook", "youtube", "tiktok", "other"]:
    daily_calls_df[column] = pd.to_numeric(
        daily_calls_df[column],
        errors="coerce"
    ).fillna(0)

st.subheader("Daily Calls Booked by Source")

calls_chart_df = daily_calls_df.set_index("booking_date")

st.line_chart(
    calls_chart_df,
    x_label="Booking Date",
    y_label="Calls Booked"
)

st.caption(
    "No Facebook, YouTube, or TikTok paid-campaign bookings were captured "
    "during the current data collection period. Captured bookings are categorized as Other."
)


################ COST PER BOOKING PER CHANNEL ##################

st.subheader("Cost Per Booking (CPB) by Channel")

cpb_query = """
SELECT *
FROM insight_flow_gold_db.cost_per_booking_by_channel
"""

cpb_df = run_athena_query(cpb_query)

if cpb_df.empty:
    st.info("No paid campaign bookings available yet to calculate Cost Per Booking.")
else:
    cpb_df["cost_per_booking"] = pd.to_numeric(
        cpb_df["cost_per_booking"]
    )

    cpb_chart = cpb_df.set_index("channel")["cost_per_booking"]

    st.bar_chart(
        cpb_chart,
        x_label="Channel",
        y_label="Cost Per Booking ($)"
    )


################# BOOKING TRENDS OVER TIME ###################

st.subheader("Booking Trends Over Time")

booking_trend_query = """
SELECT *
FROM insight_flow_gold_db.booking_trend_over_time
ORDER BY booking_date
"""

booking_trend_df = run_athena_query(booking_trend_query)

booking_trend_df["calls_booked"] = pd.to_numeric(
    booking_trend_df["calls_booked"]
)

calls_trend_chart = booking_trend_df.pivot(
    index="booking_date",
    columns="channel",
    values="calls_booked"
)

st.line_chart(
    calls_trend_chart,
    x_label="Booking Date",
    y_label="Calls Booked"
)


################### CHANNEL ATTRIBUTION ###############

st.subheader("Channel Attribution (CPB & Volume Leaderboard)")

channel_query = """
SELECT *
FROM insight_flow_gold_db.channel_attribution
"""

channel_df = run_athena_query(channel_query)

if channel_df.empty:
    st.info("No paid campaign bookings available yet.")
else:
    # Leaderboard
    st.dataframe(
        channel_df[[
            "channel",
            "total_bookings",
            "total_spend",
            "cost_per_booking"
        ]],
        hide_index=True
    )

    st.subheader("Top performing sources by booking")

    # Top-performing channels by bookings
    booking_chart = channel_df.set_index("channel")["total_bookings"]

    st.bar_chart(
        booking_chart,
        x_label="Channel",
        y_label="Total Bookings"
    )


################### BOOKING VOLUME BY TIME SLOT / DAY OF WEEK ####################

st.subheader("Booking Volume by Time Slot / Day of Week")

st.subheader("Hours of Day Vs Day of Week")

booking_volume_query = """
SELECT *
FROM insight_flow_gold_db.booking_volume_by_time_slot_dow
"""

booking_volume_df = run_athena_query(booking_volume_query)

booking_volume_df["hour_of_day"] = pd.to_numeric(
    booking_volume_df["hour_of_day"]
)

booking_volume_df["calls_booked"] = pd.to_numeric(
    booking_volume_df["calls_booked"]
)

heatmap_data = booking_volume_df.pivot_table(
    index="day_of_week",
    columns="hour_of_day",
    values="calls_booked",
    aggfunc="sum",
    fill_value=0
)

heatmap_data = heatmap_data.reindex(
    ["Monday", "Tuesday", "Wednesday", "Thursday",
     "Friday", "Saturday", "Sunday"],
    columns=range(24),
    fill_value=0
)

fig = px.imshow(
    heatmap_data,
    labels={
        "x": "Hour of Day",
        "y": "Day of Week",
        "color": "Bookings"
    },
    aspect="auto"
)

st.plotly_chart(fig, use_container_width=True)


st.subheader("Bookings by Hour")

fig = px.histogram(
    booking_volume_df,
    x="hour_of_day",
    y="calls_booked",
    histfunc="sum",
    nbins=24,
    labels={
        "hour_of_day": "Hour of Day",
        "calls_booked": "Bookings"
    }
)

st.plotly_chart(fig, use_container_width=True)


st.subheader("Bookings by Day of Week")

daily_bookings = booking_volume_df.groupby(
    "day_of_week",
    as_index=False
)["calls_booked"].sum()

fig = px.pie(
    daily_bookings,
    names="day_of_week",
    values="calls_booked"
)

st.plotly_chart(fig, use_container_width=True)


#################### MEETING LOAD PER EMPLOYEE #################

st.subheader('Meeting Load Per Employee')

st.subheader('Employee vs Avg Meetings/Week')

booking_volume_query = """
SELECT *
FROM insight_flow_gold_db.meeting_load_per_employee
"""

booking_volume_df = run_athena_query(booking_volume_query)

booking_volume_df["avg_meetings_per_week"] = pd.to_numeric(booking_volume_df["avg_meetings_per_week"])
booking_volume_df["total_meetings"] = pd.to_numeric(booking_volume_df["total_meetings"])

st.bar_chart(
    booking_volume_df,
    x="employee_name",
    y="avg_meetings_per_week",
    x_label="Employee",
    y_label="Avg Meetings/Week",
    use_container_width=True
)

total_meetings = booking_volume_df["total_meetings"].sum()
max_meetings = booking_volume_df["total_meetings"].max()
min_meetings = booking_volume_df["total_meetings"].min()

col1, col2, col3 = st.columns(3)

col1.metric("Total Meetings", total_meetings)
col2.metric("Max Meetings", max_meetings)
col3.metric("Min Meetings", min_meetings)
