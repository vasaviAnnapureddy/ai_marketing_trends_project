
import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import random

st.set_page_config(
    page_title="AI-Based Automated Content Marketing Optimizer",
    layout="wide",
    initial_sidebar_state="expanded"
)

TOPIC_NAME = "Password Recovery Frustration"

VARIANTS = [
    {
        "variant_id": "A",
        "text": "Life’s setbacks like forgotten passwords can feel frustrating, but you’re not alone. Stay calm, recover step by step, and move forward stronger."
    },
    {
        "variant_id": "B",
        "text": "Forgot your password again? It happens to everyone. Don’t stress—reset, regain access, and keep going without letting it slow you down."
    }
]

def simulate_metrics():
    return {
        "likes": random.randint(40, 80),
        "replies": random.randint(0, 10),
        "ctr": round(random.uniform(0.03, 0.07), 4)
    }

METRICS = {v["variant_id"]: simulate_metrics() for v in VARIANTS}

rows = []
for v in VARIANTS:
    m = METRICS[v["variant_id"]]
    rows.append([
        v["variant_id"],
        v["text"],
        m["likes"],
        m["replies"],
        m["ctr"]
    ])

df = pd.DataFrame(
    rows,
    columns=["Variant", "Content", "Likes", "Replies", "CTR"]
)

st.sidebar.title("Navigation")

page = st.sidebar.radio(
    "Go to",
    [
        "Home",
        "Sentiment Analysis",
        "A/B Testing",
        "Performance metrics",
        "Prediction Coach"
    ]
)

if page == "Home":
    st.title("AI-Based Automated Content Marketing Optimizer")

    st.subheader("Project Statement")
    st.write(
        "This system analyzes audience sentiment, evaluates content variants, "
        "and recommends the best-performing version using LLM-style reasoning."
    )

    st.subheader("Focused Topic")
    st.success(TOPIC_NAME)

elif page == "Sentiment Analysis":
    st.title("Sentiment Analysis Dashboard")

    sentiment_scores = {
        "Positive": 61,
        "Neutral": 26,
        "Negative": 13
    }

    col1, col2, col3 = st.columns(3)
    col1.metric("Analyzed Items", "156")
    col2.metric("Positive Content", "60.9%")
    col3.metric("Negative Content", "12.8%")

    df_sent = pd.DataFrame({
        "Sentiment": sentiment_scores.keys(),
        "Percentage": sentiment_scores.values()
    })

    fig = px.bar(
        df_sent,
        x="Sentiment",
        y="Percentage",
        color="Sentiment",
        text="Percentage",
        title="Sentiment Distribution"
    )

    st.plotly_chart(fig, width="stretch")

    st.info("Sentiment is evaluated before content generation to align tone.")

elif page == "A/B Testing":
    st.title("A/B Testing Simulator")

    st.subheader("Experiment Topic")
    st.success(TOPIC_NAME)

    st.dataframe(df, width="stretch", height=280)

    winner = df.sort_values("CTR", ascending=False).iloc[0]

    st.subheader("Winner")
    st.success(f"🏆 Variant {winner['Variant']} wins (CTR = {winner['CTR']})")

elif page == "Performance metrics":
    st.title("Content Performance Metrics")

    col1, col2 = st.columns(2)
    col1.metric("Average CTR", f"{df['CTR'].mean():.2%}")
    col2.metric("Average Likes", int(df["Likes"].mean()))

    fig = px.line(
        df,
        x="Variant",
        y="CTR",
        markers=True,
        title="CTR Comparison Across Variants"
    )

    st.plotly_chart(fig, width="stretch")

elif page == "Prediction Coach":
    st.title("Prediction Coach")

    predictions = []
    for v in VARIANTS:
        predictions.append([
            v["variant_id"],
            round(random.uniform(0.45, 0.65), 3)
        ])

    pred_df = pd.DataFrame(
        predictions,
        columns=["Variant", "Predicted Score"]
    )

    st.dataframe(pred_df, width="stretch")

    best = pred_df.sort_values("Predicted Score", ascending=False).iloc[0]

    st.success(
        f"📌 Recommended Variant: {best['Variant']} "
        f"(Impact Score: {best['Predicted Score']})"
    )

    st.info(
        "Prediction Coach simulates LLM-style evaluation "
        "based on clarity, emotion, and CTA strength."
    )

