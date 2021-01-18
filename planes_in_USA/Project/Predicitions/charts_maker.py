import os
import pandas as pd
import matplotlib.pyplot as plt
import seaborn as sns
from scipy import stats

results_dir = './results'
files = os.listdir(results_dir)
data = []
for file in files:
    if 'crc' not in file:
        data.append(file)

infos = []
for d in data:
    with open(os.path.join(results_dir, d), 'r') as f:
        infos.append(f.readlines())

string = ''
for info in infos:
    for item in info:
        s = item.replace('(', '').replace(')', '').replace("'", ''). \
            replace('[', '').replace(']', '').replace('\n', '')
        string += s + '\n'

with open('delays.txt', 'w') as f:
    f.write(string)

headers = [
    "year",
    "month",
    "day",
    "departure_delay",
    "arrival_delay",
    "distance",
    "carrier_delay",
    "weather_delay",
    "NAS_delay",
    "security_delay",
    "late_aircraft_delay",
    "airtime",
    "number_of_flights"
]

df = pd.read_csv('delays.txt', index_col=False)
df.columns = headers
df['time'] = df.apply(lambda x: pd.datetime.strptime("{0}-{1}-{2}".format(int(x['year']),
                                                                          int(x['month']),
                                                                          int(x['day'])),
                                                     "%Y-%m-%d"), axis=1)

df.sort_values(by='time', inplace=True)
df.to_csv('delays.csv', index=False)

scaler = 60 * 24
df['dd'] = df['departure_delay'] / scaler
df['ad'] = df['arrival_delay'] / scaler
fig, (ax1, ax2) = plt.subplots(2, sharex=True)
ax1.plot(df['time'], df['dd'], 'b')
ax1.axvspan('2001-09-11', '2002-09-11', color=sns.xkcd_rgb['grey'], alpha=0.5)
ax1.axvspan('2020-01-20', '2020-11-30', color=sns.xkcd_rgb['red'], alpha=0.2)
ax1.set_xlim(['1995-01-01', '2020-11-30'])
ax1.axvline(x='2001-09-11', linestyle='dashed', alpha=0.5)
ax1.text(x='1997-10-01', y=600000 / scaler, s='WTC\n11.09.2001', alpha=0.7, color='#334f8d')
ax1.axvline(x='2020-01-20', linestyle='dashed', alpha=0.7, color='red')
ax1.text(x='2014-10-01', y=600000 / scaler, s='USA first case\n of COVID-19\n20.01.2020', alpha=0.5, color='red')
ax1.set_title('departures delay')
ax1.set_ylabel('delay [days]')
ax2.plot(df['time'], df['ad'], 'r')
ax2.axvspan('2001-09-11', '2002-09-11', color=sns.xkcd_rgb['grey'], alpha=0.5)
ax2.axvspan('2020-01-20', '2020-11-30', color=sns.xkcd_rgb['red'], alpha=0.2)
ax2.axvline(x='2001-09-11', linestyle='dashed', alpha=0.5)
ax2.text(x='1997-10-01', y=600000 / scaler, s='WTC\n11.09.2001', alpha=0.7, color='#334f8d')
ax2.axvline(x='2020-01-20', linestyle='dashed', alpha=0.7, color='green')
ax2.text(x='2014-10-01', y=600000 / scaler, s='USA first case\n of COVID-19\n20.01.2020', alpha=0.5, color='red')
ax2.set_xlim(['1995-01-01', '2020-11-30'])
ax2.set_title('arrivals delay')
ax2.set_ylabel('delay [days]')
plt.show()

df['sd'] = df['carrier_delay'] / scaler
df['wd'] = df['weather_delay'] / scaler
df['nasd'] = df['NAS_delay'] / scaler
df['secd'] = df['security_delay'] / scaler
df['lad'] = df['late_aircraft_delay'] / scaler

fig, (ax1, ax2, ax3, ax4, ax5) = plt.subplots(5, sharex=True, sharey=True)
ax1.plot(df['time'], df['sd'], 'b')
ax1.axvspan('2020-01-20', '2020-11-30', color=sns.xkcd_rgb['red'], alpha=0.2)
ax1.set_xlim(['1995-01-01', '2020-11-30'])
ax1.set_title('carrier delay')
ax2.plot(df['time'], df['wd'], 'r')
ax2.axvspan('2020-01-20', '2020-11-30', color=sns.xkcd_rgb['red'], alpha=0.2)
ax2.set_xlim(['1995-01-01', '2020-11-30'])
ax2.set_title('weather delay')
ax3.plot(df['time'], df['nasd'], 'm')
ax3.axvspan('2020-01-20', '2020-11-30', color=sns.xkcd_rgb['red'], alpha=0.2)
ax3.set_xlim(['1995-01-01', '2020-11-30'])
ax3.set_title('NAS delay')
ax4.plot(df['time'], df['secd'], 'k')
ax4.axvspan('2020-01-20', '2020-11-30', color=sns.xkcd_rgb['red'], alpha=0.2)
ax4.set_xlim(['1995-01-01', '2020-11-30'])
ax4.set_title('security delay')
ax5.plot(df['time'], df['lad'], 'g')
ax5.axvspan('2020-01-20', '2020-11-30', color=sns.xkcd_rgb['red'], alpha=0.2)
ax5.set_xlim(['1995-01-01', '2020-11-30'])
ax5.set_title('late aircraft delay')
fig.text(0.00, 0.5, 'delay [days]', va='center', rotation='vertical')
plt.show()

df_before_wtc = df[(df['time'] > '2000-09-11') & (df['time'] < '2001-09-11')]
df_after_wtc = df[(df['time'] > '2001-02-11') & (df['time'] < '2002-02-11')]

df['dd'] = df['departure_delay'] / scaler / df['number_of_flights']
df['ad'] = df['arrival_delay'] / scaler / df['number_of_flights']
fig, (ax1, ax2) = plt.subplots(2, sharex=True, sharey=True)
ax1.plot(df['time'], df['dd'], 'b')
ax1.axvspan('2001-09-11', '2002-09-11', color=sns.xkcd_rgb['grey'], alpha=0.5)
ax1.axvspan('2020-01-20', '2020-11-30', color=sns.xkcd_rgb['red'], alpha=0.2)
ax1.set_xlim(['1995-01-01', '2020-11-30'])
ax1.axvline(x='2001-09-11', linestyle='dashed', alpha=0.5)
ax1.text(x='1997-10-01', y=0.04, s='WTC\n11.09.2001', alpha=0.7, color='#334f8d')
ax1.axvline(x='2020-01-20', linestyle='dashed', alpha=0.7, color='red')
ax1.text(x='2014-10-01', y=0.035, s='USA first case\n of COVID-19\n20.01.2020', alpha=0.5, color='red')
ax2.set_title('arrivals delay divided by flights number')
ax2.plot(df['time'], df['ad'], 'r')
ax2.axvspan('2001-09-11', '2002-09-11', color=sns.xkcd_rgb['grey'], alpha=0.5)
ax2.axvspan('2020-01-20', '2020-11-30', color=sns.xkcd_rgb['red'], alpha=0.2)
ax2.axvline(x='2001-09-11', linestyle='dashed', alpha=0.5)
ax2.text(x='1997-10-01', y=0.04, s='WTC\n11.09.2001', alpha=0.7, color='#334f8d')
ax2.axvline(x='2020-01-20', linestyle='dashed', alpha=0.7, color='green')
ax2.text(x='2014-10-01', y=0.035, s='USA first case\n of COVID-19\n20.01.2020', alpha=0.5, color='red')
ax2.set_xlim(['1995-01-01', '2020-11-30'])
ax2.set_title('arrivals delay divided by flights number')
fig.text(0.00, 0.5, 'delay [day/flights number]', va='center', rotation='vertical')
plt.show()


df['sd'] = df['carrier_delay'] / scaler / df['number_of_flights']
df['wd'] = df['weather_delay'] / scaler / df['number_of_flights']
df['nasd'] = df['NAS_delay'] / scaler / df['number_of_flights']
df['secd'] = df['security_delay'] / scaler / df['number_of_flights']
df['lad'] = df['late_aircraft_delay'] / scaler / df['number_of_flights']

fig, (ax1, ax2, ax3, ax4, ax5) = plt.subplots(5, sharex=True, sharey=True)
ax1.plot(df['time'], df['sd'], 'b')
ax1.axvspan('2020-01-20', '2020-11-30', color=sns.xkcd_rgb['red'], alpha=0.2)
ax1.set_xlim(['1995-01-01', '2020-11-30'])
ax1.set_title('carrier delay')
ax2.plot(df['time'], df['wd'], 'r')
ax2.axvspan('2020-01-20', '2020-11-30', color=sns.xkcd_rgb['red'], alpha=0.2)
ax2.set_xlim(['1995-01-01', '2020-11-30'])
ax2.set_title('weather delay')
ax3.plot(df['time'], df['nasd'], 'm')
ax3.axvspan('2020-01-20', '2020-11-30', color=sns.xkcd_rgb['red'], alpha=0.2)
ax3.set_xlim(['1995-01-01', '2020-11-30'])
ax3.set_title('NAS delay')
ax4.plot(df['time'], df['secd'], 'k')
ax4.axvspan('2020-01-20', '2020-11-30', color=sns.xkcd_rgb['red'], alpha=0.2)
ax4.set_xlim(['1995-01-01', '2020-11-30'])
ax4.set_title('security delay')
ax5.plot(df['time'], df['lad'], 'g')
ax5.axvspan('2020-01-20', '2020-11-30', color=sns.xkcd_rgb['red'], alpha=0.2)
ax5.set_xlim(['1995-01-01', '2020-11-30'])
ax5.set_title('late aircraft delay')
fig.text(0.00, 0.5, 'delay [day/flights number]', va='center', rotation='vertical')
plt.show()

df_before_wtc = df[(df['time'] > '2000-09-11') & (df['time'] < '2001-09-11')]
df_after_wtc = df[(df['time'] > '2001-02-11') & (df['time'] < '2002-02-11')]

fig, (ax1, ax2) = plt.subplots(2, sharex=True)
ax1.hist(df_before_wtc['departure_delay'], bins=30)
ax1.set_title('before WTC (11.09.2000 - 11.09.2001)')
ax2.hist(df_after_wtc['departure_delay'], bins=30, color='red')
ax2.set_title('after WTC (11.09.2001 - 11.09.2002)')
plt.show()

df_after_covid = df[(df['time'] > '2020-02-20') & (df['time'] < '2020-11-30')]
df_before_covid = df[(df['time'] > '2019-02-20') & (df['time'] < '2019-11-30')]

fig, (ax1, ax2) = plt.subplots(2, sharex=True)
ax1.hist(df_before_covid['departure_delay'], bins=30)
ax1.set_title('before COVID-19 (20.01.2019 - 30.11.2019)')
ax2.hist(df_after_covid['departure_delay'], bins=30, color='red')
ax2.set_title('after COVID-19 (20.01.2019 - 30.11.2020)')
plt.show()

kstest_test_wtc_before = stats.kstest(df_before_wtc['departure_delay'], 'norm')
print("Before WTC:", kstest_test_wtc_before.pvalue)
kstest_test_wtc_after = stats.kstest(df_after_wtc['departure_delay'], 'norm')
print("After WTC:", kstest_test_wtc_after.pvalue)
kstest_test_covid_before = stats.kstest(df_before_covid['departure_delay'], 'norm')
print("Before COVID:", kstest_test_covid_before.pvalue)
kstest_test_covid_after = stats.kstest(df_after_covid['departure_delay'], 'norm')
print("After COVID", kstest_test_covid_after.pvalue)