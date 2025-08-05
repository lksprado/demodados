import pandas as pd
import pandera as pa


def infer_the_schema(df, folder, name):
    schema = pa.infer_schema(df)

    with open(f"{folder}{name}.py", "w", encoding="utf-8") as f:
        f.write(schema.to_script())

    print(f"Schema salvo em: {folder}{name}")


if __name__ == "__main__":
    folder = "data/bronze/camara/deputados_detalhes/"
    name = "deputados_detalhes_schema"
    df = pd.read_csv(
        "data/bronze/camara/deputados_detalhes/62881_deputado.csv", sep=";"
    )
    infer_the_schema(df, folder, name)
