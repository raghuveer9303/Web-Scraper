import streamlit as st
import psycopg2
import pandas as pd

# Database connection details
DB_CONFIG = {
    "dbname": "SII",
    "user": "admin",
    "password": "raghu@123",
    "host": "raghuserver",
    "port": "5432"
}

# Connect to PostgreSQL
def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)

# Fetch reviews from amazon_reviews
def fetch_amazon_reviews():
    conn = get_db_connection()
    query = """
        SELECT review_id, data_asin, product_title, product_description, rating, title, review_date, body
        FROM amazon_reviews
        ORDER BY review_date DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Fetch reviews from other_reviews_table
def fetch_other_reviews():
    conn = get_db_connection()
    query = """
        SELECT id, title, author_name, author_location, author_verified, locale, overall_rating,
               rating_delivery, rating_quality, rating_value, rating_ease_of_use, rating_instructors_variety,
               body, frequency, photo_url, main_reason, who_uses, would_recommend, response, review_date, equipment_type
        FROM reviews  -- Replace with actual second table name
        ORDER BY review_date DESC;
    """
    df = pd.read_sql(query, conn)
    conn.close()
    return df

# Streamlit UI
st.title("Amazon & Other Reviews Dashboard 📊")

# Create tabs for Amazon Reviews and Other Reviews
tab1, tab2 = st.tabs(["Amazon Reviews", "Peloton Website Reviews"])

with tab1:
    st.header("Amazon Reviews")
    
    # Fetch data
    df_amazon = fetch_amazon_reviews()
    df_amazon["review_date"] = df_amazon["review_date"].dt.tz_convert(None)
    
    if df_amazon.empty:
        st.warning("No reviews found in the Amazon reviews table.")
    else:
        # Sidebar filters
        st.sidebar.subheader("Amazon Filters")
        asin_filter = st.sidebar.selectbox("Filter by ASIN (Amazon)", ["All"] + list(df_amazon["data_asin"].unique()))
        rating_filter = st.sidebar.selectbox("Filter by Rating (Amazon)", ["All"] + sorted(df_amazon["rating"].unique(), reverse=True))
        date_range = st.sidebar.date_input("Filter by Date (Amazon)", [df_amazon["review_date"].min(), df_amazon["review_date"].max()])
        equipment_filter = st.sidebar.selectbox("Filter by Equipment Type (Amazon)", ["All"] + list(df_amazon["product_description"].dropna().unique()))

        # Apply filters
        if asin_filter != "All":
            df_amazon = df_amazon[df_amazon["data_asin"] == asin_filter]
        if rating_filter != "All":
            df_amazon = df_amazon[df_amazon["rating"] == int(rating_filter)]
        df_amazon = df_amazon[(df_amazon["review_date"] >= pd.Timestamp(date_range[0])) & (df_amazon["review_date"] <= pd.Timestamp(date_range[1]))]
        if equipment_filter != "All":
            df_amazon = df_amazon[df_amazon["product_description"] == equipment_filter]

        # Display DataFrame
        st.dataframe(df_amazon)

        # Summary Statistics
        st.subheader("Amazon Review Summary")
        st.write(f"Total Reviews: {len(df_amazon)}")
        st.write(f"Average Rating: {df_amazon['rating'].mean():.2f}")

        # Rating Distribution Chart
        st.subheader("Amazon Rating Distribution")
        st.bar_chart(df_amazon["rating"].value_counts())

with tab2:
    st.header("Peloton Website Reviews")
    
    # Fetch data
    df_other = fetch_other_reviews()
    df_other['review_date'] = df_other['review_date'].dt.tz_localize(None)

    if df_other.empty:
        st.warning("No reviews found in the Other reviews table.")
    else:
        # Sidebar filters
        st.sidebar.subheader("Peloton Website Reviews Filters")
        rating_filter_other = st.sidebar.selectbox("Filter by Rating (Other)", ["All"] + sorted(df_other["overall_rating"].unique(), reverse=True))
        date_range_other = st.sidebar.date_input("Filter by Date (Other)", [df_other["review_date"].min(), df_other["review_date"].max()])
        equipment_filter_other = st.sidebar.selectbox("Filter by Equipment Type (Other)", ["All"] + list(df_other["equipment_type"].dropna().unique()))

        # Apply filters
        if rating_filter_other != "All":
            df_other = df_other[df_other["overall_rating"] == int(rating_filter_other)]
        df_other = df_other[(df_other["review_date"] >= pd.Timestamp(date_range_other[0])) & (df_other["review_date"] <= pd.Timestamp(date_range_other[1]))]
        if equipment_filter_other != "All":
            df_other = df_other[df_other["equipment_type"] == equipment_filter_other]

        # Display DataFrame
        st.dataframe(df_other)

        # Summary Statistics
        st.subheader("Other Reviews Summary")
        st.write(f"Total Reviews: {len(df_other)}")
        st.write(f"Average Rating: {df_other['overall_rating'].mean():.2f}")

        # Rating Distribution Chart
        st.subheader("Other Reviews Rating Distribution")
        st.bar_chart(df_other["overall_rating"].value_counts())
