import pandas as pd
import numpy as np
import pytz 
import schedule
import time
from datetime import datetime, timedelta


class Alarm:
    def __init__(self, path):
        self.df = pd.read_csv(path)
    def preprocess_df(self):
        # преобразуем столбец 'loaded_at' в datetime
        self.df['loaded_at'] = pd.to_datetime(self.df['loaded_at'])

        # сортируем значения по порядку (по времени)
        self.df = self.df.sort_values(by='loaded_at').reset_index(drop=True)

        # отбираем из датасета 2 недели:
        two_weeks_ago = datetime.now(pytz.timezone('Europe/Moscow')) - timedelta(weeks=2)
        self.df = self.df[self.df['loaded_at'] >= two_weeks_ago]

        # проверяем, что день начался с нулевого значения:
        starts = self.df[self.df['loaded_at'].dt.hour == 0]
        left = self.df.set_index(self.df['loaded_at'].dt.date)
        left = left[left['loaded_at'].dt.date != left['loaded_at'].dt.date[0]]
        right = starts.set_index(starts['loaded_at'].dt.date)
        left['daily_duration'] -= right['daily_duration']
        self.df = left

        # добавляем значения по каждому часу не накопом:
        diff = self.df['daily_duration'].diff()
        diff[self.df['loaded_at'].dt.hour == 0] = 0
        self.df['daily_duration_pure'] = diff

    def remove_outliers(self):
        """
        Удаляет выбросы из self.df для каждого часа суток
        Выбросы определяются как значения, выходящие за пределы 1.5 межквартильного размаха
        """
        outliers_indices = []

        # Поиск выбросов для каждого часа суток
        for i in range(24):
            df_hour = self.df[self.df['hour'] == i]
            data = df_hour['daily_duration_pure'].dropna().values
            q1 = np.percentile(data, 25)
            q3 = np.percentile(data, 75)
            iqr = q3 - q1
            upper_bound = q3 + 1.5 * iqr
            lower_bound = q1 - 1.5 * iqr
            outliers_hour = df_hour[
                (df_hour['daily_duration_pure'] > upper_bound) | (df_hour['daily_duration_pure'] < lower_bound)]
            outliers_indices.extend(outliers_hour.index.tolist())

        # Удаление выбросов из self.df
        self.df = self.df.drop(outliers_indices).reset_index(drop=True)

    # сохраним чистые значения, усредненные по каждому часу, в датасет mean_values:
    def calc_mean_values(self):
        self.mean_values = self.df.pivot_table(index='hour', values='daily_duration_pure', aggfunc='mean')

    def calc_trend(self):
        last_row = self.df.iloc[-1]

        current_time = last_row['loaded_at']
        # запишем в переменную кол-во секунд, набежавших в последний час:
        current_value = last_row['daily_duration_pure']
        # запишем в переменную  средндее значение секунд по данному часу (на основе 2-недельного наблюдения)

        mean_value = self.mean_values.loc[current_time.dt.hour, 'daily_duration_pure']
        # посчитаем отклонение в процентах. Считаем его только в том случае, если текущее значение превышает среднее
        deviation = 0
        if current_value > mean_value:
            deviation = (current_value * 100 / mean_value) - 100
        else:
            deviation = 0
        # Используя текущее время, фильтруем строки df до текущего времени включительно и только по текущему дню
        df_filtered = self.df[self.df["loaded_at"].dt.date == current_time.date()]

        # рассчитаем тренд: к текущему значению израсходованных секунд прибавим сумму средних значений последующих часов с учетом тренда:
        last_value = df_filtered['daily_duration'].iloc[-1]
        time_filters = (self.mean_values.index > current_time.hour) & (self.mean_values.index < 21)
        trend = (
                self.mean_values.loc[time_filters, 'daily_duration_pure'].sum()
                * (deviation + 100) / 100
                + last_value
        )
        if trend > 500 * 60:
            print(f'Alarm: произошел скачок dayly_duration. Если тренд сохранится, то к 20.00 составит {trend} секунд')

def job():
    alarm = Alarm('path_to_file.csv') 
    alarm.preprocess_df()
    alarm.remove_outliers()
    alarm.mean_values()
    alarm.calc_trend()

# Запускать функцию job каждый час
schedule.every(1).hours.do(job)

while True:
    # Запуск всех заданий, ожидающих выполнения
    schedule.run_pending()
    time.sleep(1)
