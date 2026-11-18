import logging
import pandas as pd

logger = logging.getLogger(__name__)


def clean(df: pd.DataFrame) -> tuple[pd.DataFrame, pd.DataFrame]:
    logger.info(f"Начало очистки данных. Входных строк: {len(df)}")
    dlq_records = []

    # 1. Валидация бизнес-правил (до очистки — чтобы сохранить причину отбраковки)
    # Отрицательные суммы транзакций — аномалия
    invalid_amount = df[df['TransactionAmount'] <= 0].copy()
    if not invalid_amount.empty:
        invalid_amount['rejection_reason'] = 'negative_amount'
        dlq_records.append(invalid_amount)
        logger.warning(f"Обнаружено {len(invalid_amount)} строк с некорректной суммой")
    df = df[df['TransactionAmount'] > 0]

    # Некорректные даты
    invalid_dates = df[df['TransactionDate'].isna()].copy()
    if not invalid_dates.empty:
        invalid_dates['rejection_reason'] = 'invalid_date'
        dlq_records.append(invalid_dates)
        logger.warning(f"Обнаружено {len(invalid_dates)} строк с некорректной датой")
    df = df[df['TransactionDate'].notna()]

    # 2. Обработка пропусков (аналог SimpleImputer из scikit-learn)
    # Для категориальных признаков — заполнение "Unknown"
    categorical_cols = ['Channel', 'Location', 'DeviceID', 'CustomerOccupation']
    for col in categorical_cols:
        if col in df.columns:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                df[col] = df[col].fillna('Unknown')
                logger.info(f"Заполнено {missing_count} пропусков в колонке {col}")

    # Для числовых признаков — медианное заполнение (устойчиво к выбросам)
    numeric_cols = ['TransactionAmount', 'AccountBalance', 'CustomerAge']
    for col in numeric_cols:
        if col in df.columns:
            missing_count = df[col].isna().sum()
            if missing_count > 0:
                median_value = df[col].median()
                df[col] = df[col].fillna(median_value)
                logger.info(f"Заполнено {missing_count} пропусков в {col} медианой ({median_value:.2f})")

    # 3. Удаление дубликатов по первичному ключу
    duplicates_count = df.duplicated(subset=['TransactionID'], keep='first').sum()
    if duplicates_count > 0:
        df = df.drop_duplicates(subset=['TransactionID'], keep='first')
        logger.warning(f"Удалено дубликатов: {duplicates_count}")

    # 4. Конвертация типов
    df['TransactionDate'] = pd.to_datetime(df['TransactionDate'])

    # Формирование DLQ
    dlq_df = pd.concat(dlq_records, ignore_index=True) if dlq_records else pd.DataFrame()

    logger.info(f"Очистка завершена. Строк на выходе: {len(df)}, отбраковано: {len(dlq_df)}")
    return df, dlq_df

def normalize(df: pd.DataFrame) -> dict[str, pd.DataFrame]:
    logger.info("Начало нормализации данных")

    # 1. Таблица измерений "Канал"
    dim_channel = (
        df[['Channel']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_channel.index = dim_channel.index + 1  # Суррогатный ключ начинается с 1
    dim_channel = dim_channel.reset_index().rename(columns={'index': 'channel_id'})
    logger.info(f"Сформировано измерений Channel: {len(dim_channel)}")

    # 2. Таблица измерений "Устройство"
    dim_device = (
        df[['DeviceID']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_device.index = dim_device.index + 1
    dim_device = dim_device.reset_index().rename(columns={'index': 'device_id'})
    logger.info(f"Сформировано измерений Device: {len(dim_device)}")

    # 3. Таблица измерений "Локация"
    dim_location = (
        df[['Location']]
        .drop_duplicates()
        .reset_index(drop=True)
    )
    dim_location.index = dim_location.index + 1
    dim_location = dim_location.reset_index().rename(columns={'index': 'location_id'})
    logger.info(f"Сформировано измерений Location: {len(dim_location)}")

    # 4. Таблица фактов с внешними ключами
    # Присоединяем суррогатные ключи через merge
    fact = df.copy()

    fact = fact.merge(dim_channel, on='Channel', how='left')
    fact = fact.merge(dim_device, on='DeviceID', how='left')
    fact = fact.merge(dim_location, on='Location', how='left')

    # Формируем итоговую таблицу фактов
    fact_transactions = fact[[
        'TransactionID', 'AccountID', 'channel_id', 'device_id', 'location_id',
        'TransactionAmount', 'TransactionType', 'TransactionDate', 'AccountBalance'
    ]].copy()

    # Обогащение: извлекаем час транзакции для анализа паттернов
    fact_transactions['transaction_hour'] = fact_transactions['TransactionDate'].dt.hour

    logger.info(f"Сформирована таблица фактов: {len(fact_transactions)} строк")

    return {
        'dim_channel': dim_channel,
        'dim_device': dim_device,
        'dim_location': dim_location,
        'fact_transactions': fact_transactions
    }