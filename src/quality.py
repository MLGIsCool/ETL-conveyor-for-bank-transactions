import logging
import great_expectations as gx

from config.great_expectations import build_suite

log = logging.getLogger(__name__)


def validate(df) -> None:
    """Проверяет DataFrame на соответствие бизнес-правилам через Great Expectations."""
    if df.empty:
        log.warning("Quality: данных нет — проверка пропущена")
        return

    # Инициализируем ephemeral контекст (без сохранения на диск)
    context = gx.get_context(mode="ephemeral")
    
    # Настраиваем источник данных и пакет
    batch = (context.data_sources.add_pandas("bank")
             .add_dataframe_asset("transactions")
             .add_batch_definition_whole_dataframe("batch"))
    
    # Добавляем набор ожиданий и определение валидации
    suite = context.suites.add(build_suite())
    vd = context.validation_definitions.add(
        gx.ValidationDefinition(name="bank_vd", data=batch, suite=suite)
    )
    
    # Создаем и запускаем чекпоинт
    cp = context.checkpoints.add(gx.Checkpoint(name="bank_cp", validation_definitions=[vd]))
    result = cp.run(batch_parameters={"dataframe": df})

    # Обработка результата
    if not result.success:
        # Подсчитываем количество упавших проверок для чистого лога
        failed_count = sum(1 for res in result.run_results.values() for r in res.results if not r.success)
        log.error("Quality: ПРОВАЛЕНА (%d проверок не пройдено). Загрузка в DWH остановлена.", failed_count)
        raise ValueError("Проверки качества не пройдены — загрузка в DWH остановлена")
    
    log.info("Quality: все проверки успешно пройдены (%d строк)", len(df))