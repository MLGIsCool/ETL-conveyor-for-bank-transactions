import logging
import pandas as pd
import great_expectations as gx
from great_expectations.core import ExpectationSuite, ExpectationConfiguration

logger = logging.getLogger(__name__)


def create_transaction_suite() -> ExpectationSuite:
    """
    Создаёт набор ожиданий (Expectation Suite) для проверки качества транзакций.
    Правила основаны на бизнес-требованиях из раздела 1.4 курсовой.
    """
    suite = ExpectationSuite(name="bank_transactions_suite")

    # 1. Проверка на отсутствие пропусков в ключевых полях (FR-5.1)
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "TransactionID"}
        )
    )
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_not_be_null",
            kwargs={"column": "TransactionAmount"}
        )
    )

    # 2. Проверка уникальности первичного ключа (FR-5.2)
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_unique",
            kwargs={"column": "TransactionID"}
        )
    )

    # 3. Проверка бизнес-правил: сумма транзакции > 0 (FR-2.3)
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_between",
            kwargs={
                "column": "TransactionAmount",
                "min_value": 0.01,
                "max_value": 1000000.0  # разумный лимит для транзакции
            }
        )
    )

    # 4. Проверка допустимых значений для типа транзакции
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "TransactionType",
                "value_set": ["Debit", "Credit", "debit", "credit"]
            }
        )
    )

    # 5. Проверка допустимых каналов
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_in_set",
            kwargs={
                "column": "Channel",
                "value_set": ["Branch", "ATM", "Online", "Mobile App", "Unknown"]
            }
        )
    )

    # 6. Проверка типа данных даты
    suite.add_expectation(
        ExpectationConfiguration(
            expectation_type="expect_column_values_to_be_of_type",
            kwargs={"column": "TransactionDate", "type_": "datetime64[ns]"}
        )
    )

    return suite


def validate_dataframe(df: pd.DataFrame, suite: ExpectationSuite) -> dict:
    """
    Выполняет валидацию DataFrame по набору ожиданий.

    Args:
        df: DataFrame для проверки
        suite: набор ожиданий Great Expectations

    Returns:
        dict с результатами валидации
    """
    logger.info(f"Начало валидации данных. Строк: {len(df)}")

    # Создаём контекст GX (in-memory, без файловой системы)
    context = gx.get_context(mode="ephemeral")

    # Создаём Data Source из pandas DataFrame
    data_source = context.data_sources.add_pandas(name="pandas_source")
    data_asset = data_source.add_dataframe_asset(name="transactions_asset")
    batch_request = data_asset.build_batch_request(dataframe=df)

    # Добавляем Expectation Suite в контекст
    suite = context.suites.add(suite)

    # Создаём Expectation Validator
    validator = context.get_validator(
        batch_request=batch_request,
        expectation_suite=suite
    )

    # Запускаем валидацию
    results = validator.validate()

    # Формируем отчёт
    report = {
        "success": results.success,
        "statistics": results.statistics,
        "failed_expectations": []
    }

    if not results.success:
        for result in results.results:
            if not result.success:
                report["failed_expectations"].append({
                    "expectation": result.expectation_config.expectation_type,
                    "column": result.expectation_config.kwargs.get("column"),
                    "details": str(result.result)
                })
                logger.warning(
                    f"Проверка не пройдена: {result.expectation_config.expectation_type}"
                    f"для колонки {result.expectation_config.kwargs.get('column')}"
                )
        logger.error(
            f"Валидация НЕ ПРОЙДЕНА. Успешных проверок: "
            f"{results.statistics['successful_expectations']} / "
            f"{results.statistics['evaluated_expectations']}"
        )
    else:
        logger.info(
            f"Валидация пройдена успешно."
            f"Проверок: {results.statistics['evaluated_expectations']}"
        )

    return report


def run_quality_check(df: pd.DataFrame) -> bool:
    """
    Главная функция модуля качества.
    Возвращает True, если данные прошли все проверки, иначе False.
    """
    suite = create_transaction_suite()
    report = validate_dataframe(df, suite)

    if not report["success"]:
        # Логируем все проваленные проверки для аудита
        logger.error("ОТЧЁТ О НАРУШЕНИЯХ КАЧЕСТВА ДАННЫХ")
        for failure in report["failed_expectations"]:
            logger.error(f"  - {failure['expectation']} (колонка: {failure['column']})")
        return False

    return True