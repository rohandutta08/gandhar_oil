import duckdb
from utils.s3_utils import S3Utils
import sys

# TODO: Log files


db_path = "output/output.duckdb"
connection = duckdb.connect(db_path)
connection.execute(
    "COPY s_sale.clear_sr TO 'output/sales_register.csv' (HEADER, DELIMITER ',')"
)

# connection.execute(
#     "COPY s_purchase.lt_clear_pr TO 'output/purchase_register.csv' (HEADER, DELIMITER ',')"
# )


# connection.execute(
#     "COPY s_glrecon.purchase_gl_recon_invoice_amounts TO 'output/purchase_gl_recon.csv' (HEADER, DELIMITER ',')"
# )

# connection.execute(
#     "COPY s_glrecon.sale_gl_recon_invoice_amounts TO 'output/sale_gl_recon.csv' (HEADER, DELIMITER ',')"
# )

print("Created output/purchase_register.csv")
print("Created output/sales_register.csv")
print("Created purchase_gl_recon.csv")
print("Created sale_gl_recon.csv")

if len(sys.argv) > 1:
    task_id = sys.argv[1]
    task_config_file_s3_path = sys.argv[2]
    s3 = S3Utils()
    ingestion_bucket = s3.get_config_from_json(task_config_file_s3_path)["ingestion_bucket"]
    s3.upload_file_to_s3(
        "output/purchase_register.csv",
        ingestion_bucket,
        f"advance_ingestion/{task_id}/output/PURCHASE/b2c/purchase_{task_id}/purchase_register.csv",
    )
    s3.upload_file_to_s3(
        "output/sales_register.csv",
        ingestion_bucket,
        f"advance_ingestion/{task_id}/output/SALES/b2cs/sales_{task_id}/sales_register.csv",
    )
else:
    print("No task_id specified. Skipping upload to s3")
