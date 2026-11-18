import great_expectations as gx
from great_expectations import expectations as gxe


def build_suite() -> gx.ExpectationSuite:
    """Определяет бизнес-правила валидации данных."""
    return gx.ExpectationSuite(name="bank_transactions_suite", expectations=[
        # 1. Ключевые поля не должны быть пустыми
        gxe.ExpectColumnValuesToNotBeNull(column="transaction_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="account_id"),
        gxe.ExpectColumnValuesToNotBeNull(column="transaction_date"),
        # 2. Идентификатор транзакции должен быть уникальным (защита от дублей)
        gxe.ExpectColumnValuesToBeUnique(column="transaction_id"),
        # 3. Бизнес-правила: сумма строго больше 0
        gxe.ExpectColumnValuesToBeBetween(column="amount", min_value=0, strict_min=True),
        # 4. Категориальные ограничения (защита от опечаток и мусора)
        gxe.ExpectColumnValuesToBeInSet(column="transaction_type", value_set=["Debit", "Credit"]),
        gxe.ExpectColumnValuesToBeInSet(column="channel", value_set=["ATM", "Online", "Branch", "Mobile App"]),
    ])