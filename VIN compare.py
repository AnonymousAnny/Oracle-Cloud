import pandas as pd
from tkinter import Tk, filedialog

def select_file(prompt):
    root = Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.askopenfilename(title=prompt)
    root.destroy()
    return file_path

def save_file(prompt):
    root = Tk()
    root.withdraw()  # Hide the root window
    file_path = filedialog.asksaveasfilename(title=prompt, defaultextension=".csv", filetypes=[("CSV files", "*.csv")])
    root.destroy()
    return file_path

# Prompt the user to select the Vision.csv file
vision_file = select_file("Select the Vision CSV file")

# Prompt the user to select the Oracle.csv file
oracle_file = select_file("Select the Oracle CSV file")

# Load the Vision file with the extra file_name column
vision_df = pd.read_csv(vision_file, header=None, names=["FILE_NAME", "VIN", "STATUS"])

# Load the Oracle file without headers
oracle_df = pd.read_csv(oracle_file, header=None, names=["VIN", "STATUS"])

# Merge the two dataframes using an outer join on the VIN column
merged_df = pd.merge(vision_df, oracle_df, on='VIN', how='outer', suffixes=('_Vision', '_Oracle'))

# Rename the columns to match the required format
merged_df.rename(columns={'STATUS_Vision': 'Vision Status', 'STATUS_Oracle': 'Oracle Status'}, inplace=True)

# Filter the merged dataframe to include only rows with different statuses
filtered_df = merged_df[merged_df['Vision Status'] != merged_df['Oracle Status']]

# Prompt the user to select the save location
save_path = save_file("Save the merged CSV file")

# Save the filtered dataframe to the specified CSV file
filtered_df.to_csv(save_path, index=False)

print(f"Merge completed. Merged file saved as '{save_path}'.")
