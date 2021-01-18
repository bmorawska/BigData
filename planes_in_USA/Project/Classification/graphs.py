import matplotlib.pyplot as plt
import seaborn as sns

def check_distribution_classes(df):
    classes = df['FlightDelay'].value_counts()
    print(df['FlightDelay'].value_counts(normalize=True))
    print(classes)

    plt.figure(figsize=(10, 6))
    sns.barplot(x=classes.index, y=classes.values)
    plt.rcParams["figure.facecolor"] = "lightblue"
    plt.title('Class Distribution', fontsize=16)
    plt.xlabel('Delays', fontsize=12)
    plt.ylabel('Number of Flights', fontsize=12)
    plt.xticks(range(len(classes.index)), ['False', 'True'])
    plt.show()

def check_histograms(df):
    df.hist(figsize=[20, 20], bins=9, zorder=2, rwidth=0.9)
    plt.show()

def check_histograms_cat(df):
    fig, axes = plt.subplots(nrows=3, ncols=3, figsize=(16, 14), sharey='all')

    cat = ['Reporting_Airline', 'Origin', 'Dest', 'CRSDepTime', 'CRSArrTime', 'Month', 'DayOfWeek', 'DayofMonth']

    for col, ax in zip(cat, axes.flatten()):
        (df.groupby(col).sum()['FlightDelay'].sort_values().plot.bar(ax=ax))

        ax.set_title(col)

    fig.tight_layout()