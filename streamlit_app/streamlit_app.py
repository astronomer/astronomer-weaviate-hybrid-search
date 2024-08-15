# Import python packages
import streamlit as st
from snowflake.snowpark.context import get_active_session
from snowflake.snowpark.functions import col
import pandas as pd


session = get_active_session()

# Fetch data from Snowflake
search_terms_df = session.table("HYBRID_SEARCH_DEMO.DEV.SEARCH_TERMS").to_pandas()
search_summary_df = session.table("HYBRID_SEARCH_DEMO.DEV.SEARCH_SUMMARY").to_pandas()

st.title("Astriate Store searches")

broadcategory_counts = search_terms_df['BROADCATEGORY'].value_counts().reset_index()
broadcategory_counts.columns = ['BROADCATEGORY', 'COUNT']
broadcategory_counts = broadcategory_counts.sort_values(by='COUNT', ascending=False)

st.write("Top 5 categories:")
st.write(broadcategory_counts.head())

# Data for narrowcategories
narrowcategory_counts = search_terms_df['NARROWCATEGORY'].value_counts().reset_index()
narrowcategory_counts.columns = ['NARROWCATEGORY', 'COUNT']
narrowcategory_counts = narrowcategory_counts.sort_values(by='COUNT', ascending=False)

# Display the broadcategories as a horizontal bar chart
st.header("Broad Categories")
st.bar_chart(broadcategory_counts.set_index('BROADCATEGORY'))

# Display the narrowcategories as a horizontal bar chart
st.header("Narrow Categories")
st.bar_chart(narrowcategory_counts.set_index('NARROWCATEGORY'))

# Display the insight from the first row of the SEARCH_SUMMARY table at the bottom
insight = search_summary_df.loc[0, 'INSIGHT']
st.header("Insight")
st.write(insight)

# Close the Snowflake session
session.close()