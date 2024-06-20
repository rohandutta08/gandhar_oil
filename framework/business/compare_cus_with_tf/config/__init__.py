import os
from framework import TemplateType
from framework.business.compare_cus_with_tf.config.dto import DataComparisonConfig
from framework.business.compare_cus_with_tf.config.dto.i_config_loader import ConfigLoaderInterface
from framework.business.compare_cus_with_tf.config.pr_config import PurchaseConfigLoader
from framework.business.compare_cus_with_tf.config.sr_config import SalesConfigLoader
from framework.utils.custlogging import LoggerProvider

logger = LoggerProvider().get_logger(os.path.basename(__file__))


def get_template_config_loader(template_type: TemplateType) -> ConfigLoaderInterface:
    if template_type == TemplateType.SALES:
        return SalesConfigLoader()
    elif template_type == TemplateType.PURCHASE:
        return PurchaseConfigLoader()
    else:
        raise ValueError(f"Unknown template type: {template_type}")


class TemplateRunTime:
    valid: bool = None
    customer_path: str = None
    elt_output_path: str = None
    config_path: str = None
    output_path: str = None
    output_duck_db_path: str = None
    template_config: ConfigLoaderInterface = None
    override_config: DataComparisonConfig = None

    @staticmethod
    def _check_file_existence(file_path, file_description):
        file_exists = os.path.exists(file_path)
        if file_exists:
            logger.info(f'Found {file_description}')
            return file_exists, file_path
        else:
            logger.warning(f'{file_description} is missing')
            return file_exists, None

    def __init__(self, template_type: TemplateType, current_path):
        self.template_config = get_template_config_loader(template_type)
        paths = self.template_config.get_file_path()
        self.valid, self.customer_path = self._check_file_existence(
            os.path.join(current_path, f'input/{paths.customer_file}'), paths.customer_file)

        if self.valid:
            self.valid, self.elt_output_path = self._check_file_existence(
                os.path.join(current_path, f'output/{paths.elt_output_file}'), paths.elt_output_file)
            self.output_path = os.path.join(current_path, f'output/{paths.output_file}')
        else:
            logger.error("customer_file missing")

        if self.valid:
            self.config_path = os.path.join(current_path, f'config/{paths.config_file}')
            self.override_config = DataComparisonConfig(self.config_path)
            logger.info(f" {template_type} exists")
        else:
            logger.error("please run elt transformation")

        self.output_duck_db_path = os.path.join(current_path, f'output/output.duckdb')
