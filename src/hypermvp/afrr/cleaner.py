def filter_negative_50hertz(data):
    """
    Filters the DataFrame to retain only the relevant columns for negative regulation from 50Hertz.

    Args:
        data (pd.DataFrame): The original DataFrame.

    Returns:
        pd.DataFrame: A DataFrame with the selected columns.
    """
    try:
        # Ensure column names are strings and strip any extra spaces
        data.columns = data.columns.astype(str).str.strip()

        # Select relevant columns
        relevant_data = data[["Datum", "von", "bis", "50Hertz (Negativ)"]]

        return relevant_data
    except KeyError as e:
        print(f"Required columns missing in data: {e}")
        return None
    except Exception as e:
        print(f"An error occurred during filtering: {e}")
        return None
