"""
UrbanPulse Task C — Live Dashboard
Run with: streamlit run urbanpulse_dashboard.py

Reads real messages from the 3 Task C output topics:
  - urbanpulse.incidents          (Flink: AQI / gridlock / bunching alerts)
  - urbanpulse.ward_energy_summary (Spark: 15-min ward energy windows)
  - urbanpulse.health_advisories   (Spark SQL: 10-min rolling AQI advisories)

Nothing here is mocked — it's a live Kafka consumer rendered as a dashboard.
Designed to be recorded on screen for the demo video.
"""

import json
import time
from collections import deque

import pandas as pd
import streamlit as st
from confluent_kafka import Consumer, KafkaException

# ---------------------------------------------------------------------------
# Config — matches Task_C/flink/kafka_config.py and the Spark job scripts
# ---------------------------------------------------------------------------
BOOTSTRAP_SERVERS = "localhost:9092,localhost:9094,localhost:9096"
TOPIC_INCIDENTS = "urbanpulse.incidents"
TOPIC_WARD_ENERGY = "urbanpulse.ward_energy_summary"
TOPIC_HEALTH_ADVISORIES = "urbanpulse.health_advisories"

MAX_ROWS = 120  # how many recent messages to keep per topic, for display

st.set_page_config(
    page_title="UrbanPulse — Task C Live Dashboard",
    layout="wide",
)


# ---------------------------------------------------------------------------
# Kafka consumer setup (cached so Streamlit doesn't reconnect on every rerun)
# ---------------------------------------------------------------------------
@st.cache_resource
def get_consumer():
    conf = {
        "bootstrap.servers": BOOTSTRAP_SERVERS,
        #"group.id": "urbanpulse-dashboard",
        "group.id": f"urbanpulse-dashboard-{int(time.time())}",
        "auto.offset.reset": "earliest",
        "enable.auto.commit": False,  # dashboard is read-only; never commits offsets
    }
    consumer = Consumer(conf)
    consumer.subscribe([TOPIC_INCIDENTS, TOPIC_WARD_ENERGY, TOPIC_HEALTH_ADVISORIES])
    return consumer


# Session-state buffers so messages accumulate across Streamlit reruns
if "incidents" not in st.session_state:
    st.session_state.incidents = deque(maxlen=MAX_ROWS)
if "ward_energy" not in st.session_state:
    st.session_state.ward_energy = deque(maxlen=MAX_ROWS)
if "health_advisories" not in st.session_state:
    st.session_state.health_advisories = deque(maxlen=MAX_ROWS)


def poll_messages(consumer, poll_seconds=1.5):
    """Poll Kafka for a short window and route messages into the right buffer."""
    end_time = time.time() + poll_seconds
    while time.time() < end_time:
        msg = consumer.poll(timeout=0.2)
        if msg is None:
            continue
        if msg.error():
            # Don't crash the dashboard on transient Kafka errors — just skip.
            continue
        try:
            record = json.loads(msg.value().decode("utf-8"))
        except (json.JSONDecodeError, UnicodeDecodeError):
            continue

        topic = msg.topic()
        if topic == TOPIC_INCIDENTS:
            st.session_state.incidents.appendleft(record)
        elif topic == TOPIC_WARD_ENERGY:
            st.session_state.ward_energy.appendleft(record)
        elif topic == TOPIC_HEALTH_ADVISORIES:
            st.session_state.health_advisories.appendleft(record)


# ---------------------------------------------------------------------------
# Layout
# ---------------------------------------------------------------------------
st.title("UrbanPulse — Task C Live Dashboard")
st.caption("Flink incident detection (speed layer) + Spark structured streaming (serving layer)")

consumer = get_consumer()
poll_messages(consumer)

col1, col2, col3 = st.columns(3)

# --- Column 1: Flink incidents -------------------------------------------------
with col1:
    st.subheader("🚨 Incidents (Flink)")
    st.caption("AQI emergencies · traffic gridlock · bus bunching")
    if st.session_state.incidents:
        df = pd.DataFrame(list(st.session_state.incidents))
        st.dataframe(df, use_container_width=True, height=420)
    else:
        st.info("Waiting for incident alerts...")

# --- Column 2: Spark ward energy -----------------------------------------------
with col2:
    st.subheader("⚡ Ward Energy (Spark)")
    st.caption("15-min tumbling window, per ward_id")
    if st.session_state.ward_energy:
        df = pd.DataFrame(list(st.session_state.ward_energy))
        st.dataframe(df, use_container_width=True, height=250)
        if "ward_id" in df.columns and "total_kwh_consumed" in df.columns:
            chart_df = df.groupby("ward_id")["total_kwh_consumed"].sum().sort_values(ascending=False)
            st.bar_chart(chart_df)
    else:
        st.info("Waiting for ward energy summaries...")

# --- Column 3: Spark SQL health advisories -------------------------------------
with col3:
    st.subheader("🏥 Health Advisories (Spark SQL)")
    st.caption("10-min rolling AQI > 150, joined with zone profile")
    if st.session_state.health_advisories:
        df = pd.DataFrame(list(st.session_state.health_advisories))
        st.dataframe(df, use_container_width=True, height=420)
    else:
        st.info("Waiting for health advisories...")

st.divider()
st.caption(
    f"Consuming: {TOPIC_INCIDENTS}, {TOPIC_WARD_ENERGY}, {TOPIC_HEALTH_ADVISORIES} "
    f"| bootstrap: {BOOTSTRAP_SERVERS} | auto-refreshing"
)

# Auto-rerun the script every couple of seconds to keep polling for new messages
time.sleep(2)
st.rerun()
