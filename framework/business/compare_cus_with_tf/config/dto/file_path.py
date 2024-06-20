class FilePath:
    def __init__(self, customer_file: str, elt_output_file: str, output_file: str,
                 config_file: str):
        self.customer_file = customer_file
        self.elt_output_file = elt_output_file
        self.output_file = output_file
        self.config_file = config_file
