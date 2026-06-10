import pandas as pd
from unidecode import unidecode


class ColumnSanitizer:
    def __init__(self, df):
        self.df = df.copy()

    def sanitize_columns_names(
        self, cols=None, case="lower", space="replace", alfanum="replace"
    ) -> pd.DataFrame:
        cols_to_sanitize = cols if cols else self.df.columns
        new_cols = []

        for col in cols_to_sanitize:
            new_col = unidecode(str(col)).strip()
            if case == "upper":
                new_col = new_col.upper()
            elif case == "lower":
                new_col = new_col.lower()
            if space == "replace":
                new_col = new_col.replace(" ", "_")
            if alfanum == "remove":
                new_col = "".join(
                    c for c in new_col if c.isalnum() or c == "_" or c == " "
                )
            if alfanum == "replace":
                new_col = "".join(
                    c if c.isalnum() or c == "_" or c == " " else "_" for c in new_col
                )
            new_cols.append(new_col)

        col_map = dict(zip(cols_to_sanitize, new_cols))
        self.df.rename(columns=col_map, inplace=True)

        return self

    def sanitize_columns_values(
        self, cols=None, case="upper", space="keep", alfanum="remove"
    ):
        cols_to_sanitize = cols if cols else self.df.columns

        for col in cols_to_sanitize:
            if pd.api.types.is_numeric_dtype(self.df[col]):
                continue

            self.df[col] = self.df[col].astype(str)
            self.df[col] = self.df[col].map(unidecode).str.strip()

            if case == "upper":
                self.df[col] = self.df[col].str.upper()
            elif case == "lower":
                self.df[col] = self.df[col].str.lower()

            if space == "replace":
                self.df[col] = self.df[col].str.replace(" ", "_", regex=False)

            if alfanum == "remove":
                self.df[col] = self.df[col].str.replace(r"[^\w\s]", "", regex=True)

        return self

    def not_sanitize_columns_values(
        self, cols: list, case="upper", space="keep", alfanum="remove"
    ):
        other = [c for c in self.df.columns if c not in cols]
        return self.sanitize_columns_values(
            cols=other, case=case, space=space, alfanum=alfanum
        )


def sanitize_output(df, output_file):
    sanitized_df = ColumnSanitizer(df).sanitize_columns_names().df
    columns = sanitized_df.columns
    with open(output_file, "w") as f:
        for col in columns:
            f.write(f"{col}\n")
        print(f"Arquivo txt com coluna salva em: {output_file}")


if __name__ == "__main__":
    file = "local_setup/data/bronze/senado/senadores/senado_senadores.csv"
    dataframe = pd.read_csv(file, sep=";")
    sanitize_output(dataframe, "local_setup/data/sanitized/senado_senadores.csv")
    pass
