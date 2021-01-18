from keras.models import Sequential
from keras.layers import LSTM, Dense, Dropout, Bidirectional
import pandas as pd
from sklearn.preprocessing import MinMaxScaler
import numpy as np

df = pd.read_csv('delays.csv', index_col=False)
training_set = df['departure_delay'].values
training_set = training_set.reshape(-1, 1)
sc = MinMaxScaler()
training_set_scaled = sc.fit_transform(training_set)

x_train = []
y_train = []
n_future = 30
n_past = 30

for i in range(0, len(df['departure_delay'].values) - n_past - n_future + 1):
    x_train.append(training_set_scaled[i: i + n_past, 0])
    y_train.append(training_set_scaled[i + n_past: i + n_past + n_future, 0])
x_train, y_train = np.array(x_train), np.array(y_train)
x_train = np.reshape(x_train, (x_train.shape[0], x_train.shape[1], 1))

regressor = Sequential()
regressor.add(Bidirectional(LSTM(units=30, return_sequences=True, input_shape=(x_train.shape[1], 1))))
regressor.add(Dropout(0.2))
regressor.add(LSTM(units=30, return_sequences=True))
regressor.add(Dropout(0.2))
regressor.add(LSTM(units=30, return_sequences=True))
regressor.add(Dropout(0.2))
regressor.add(LSTM(units=30))
regressor.add(Dropout(0.2))
regressor.add(Dense(units=n_future, activation='linear'))
regressor.compile(optimizer='adam', loss='mean_squared_error', metrics=['acc'])
regressor.fit(x_train, y_train, epochs=500, batch_size=32)
regressor.summary()

# read test dataset
testdataset = pd.read_csv('test_data.csv')['departure_delay'].values
# get only the temperature column
real_temperature = pd.read_csv('test_data.csv')['departure_delay'].values
testdataset = testdataset.reshape(-1, 1)
testing = sc.transform(testdataset)
testing = np.array(testing)
testing = np.reshape(testing, (testing.shape[1], testing.shape[0], 1))

predicted_temperature = regressor.predict(testing)
predicted_temperature = sc.inverse_transform(predicted_temperature)

np.savetxt('realCOVID2.out', real_temperature, delimiter=',')  # X is an array
np.savetxt('predictedCOVID2.out', predicted_temperature, delimiter=',')  # X is an array


# read test dataset
testdataset = pd.read_csv('test_data2.csv')['departure_delay'].values
# get only the temperature column
real_temperature = pd.read_csv('test_data2.csv')['departure_delay'].values
testdataset = testdataset.reshape(-1, 1)
testing = sc.transform(testdataset)
testing = np.array(testing)
testing = np.reshape(testing, (testing.shape[1], testing.shape[0], 1))

predicted_temperature = regressor.predict(testing)
predicted_temperature = sc.inverse_transform(predicted_temperature)

np.savetxt('realWTC2.out', real_temperature, delimiter=',')  # X is an array
np.savetxt('predictedWTC2.out', predicted_temperature, delimiter=',')  # X is an array

