# import numpy as np
# import matplotlib.pyplot as plt

# data = {
#     "pattern1": [0.1214883327, 0.1138093472, 0.1191756725, 0.1137254238, 0.1137359142],
#     "pattern2": [145.26544, 178.901911, 176.3323815, 194.0129008, 164.1226866],
#     "pattern3": [119.4796114, 136.2549927, 116.2129507, 116.1041853, 124.6452403],
#     "pattern4": [17.011585, 18.20104265, 18.7720356, 18.56345868, 18.15814829]
# }

# patterns = list(data.keys())
# means = [np.mean(data[pattern]) for pattern in patterns]
# std_devs = [np.std(data[pattern]) for pattern in patterns]

# plt.figure(figsize=(10, 6))
# plt.errorbar(patterns, means, yerr=std_devs, fmt='o', capsize=5, label='Mean with Standard Deviation')

# plt.xticks(rotation=45, ha='right')
# plt.xlabel('Patterns')
# plt.ylabel('Execution Time (seconds)')
# plt.title('Execution Time Mean and Standard Deviation for Each Pattern')
# plt.legend()

# # Adjust the spacing between the x-axis labels
# plt.subplots_adjust(bottom=0.1)

# plt.tight_layout()
# plt.show()


import numpy as np
import matplotlib.pyplot as plt

# Data
data = {
    "pattern1": [1.1214883327, 1.1138093472, 1.1191756725, 1.1137254238, 1.1137359142],
    "pattern2": [145.26544, 178.901911, 176.3323815, 194.0129008, 164.1226866],
    "pattern3": [119.4796114, 136.2549927, 116.2129507, 116.1041853, 124.6452403],
    "pattern4": [17.011585, 18.20104265, 18.7720356, 18.56345868, 18.15814829]
}

# Calculate means and standard deviations
patterns = list(data.keys())
means = [np.mean(data[pattern]) for pattern in patterns]
std_devs = [np.std(data[pattern]) for pattern in patterns]

# Create bar plot with error bars
plt.figure(figsize=(8, 6))
x = np.arange(len(patterns))  # X axis locations

# Reduce space by setting a small width
bar_width = 0.4

# Plotting bars with error bars for standard deviation
plt.bar(x, means, yerr=std_devs, capsize=5, width=bar_width, label='Mean with Standard Deviation', color='skyblue')

# Adjust x-axis labels to be close together and in the right place
plt.xticks(x, patterns, rotation=45, ha='right')

# Add labels and title
plt.xlabel('Patterns')
plt.ylabel('Execution Time (seconds)')
plt.title('Execution Time Mean and Standard Deviation for Each Pattern')
plt.legend()

plt.tight_layout()  # Ensures everything fits in the plot area
plt.show()
