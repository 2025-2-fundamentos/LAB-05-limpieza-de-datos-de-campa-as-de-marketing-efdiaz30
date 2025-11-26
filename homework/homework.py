"""
Escriba el codigo que ejecute la accion solicitada.
"""

# pylint: disable=import-outside-toplevel

import pandas as pd
import zipfile
import os
def clean_campaign_data():
    """
    En esta tarea se le pide que limpie los datos de una campaña de
    marketing realizada por un banco, la cual tiene como fin la
    recolección de datos de clientes para ofrecerls un préstamo.

    La información recolectada se encuentra en la carpeta
    files/input/ en varios archivos csv.zip comprimidos para ahorrar
    espacio en disco.

    Usted debe procesar directamente los archivos comprimidos (sin
    descomprimirlos). Se desea partir la data en tres archivos csv
    (sin comprimir): client.csv, campaign.csv y economics.csv.
    Cada archivo debe tener las columnas indicadas.

    Los tres archivos generados se almacenarán en la carpeta files/output/.

    client.csv:
    - client_id
    - age
    - job: se debe cambiar el "." por "" y el "-" por "_"
    - marital
    - education: se debe cambiar "." por "_" y "unknown" por pd.NA
    - credit_default: convertir a "yes" a 1 y cualquier otro valor a 0
    - mortage: convertir a "yes" a 1 y cualquier otro valor a 0

    campaign.csv:
    - client_id
    - number_contacts
    - contact_duration
    - previous_campaing_contacts
    - previous_outcome: cmabiar "success" por 1, y cualquier otro valor a 0
    - campaign_outcome: cambiar "yes" por 1 y cualquier otro valor a 0
    - last_contact_day: crear un valor con el formato "YYYY-MM-DD",
        combinando los campos "day" y "month" con el año 2022.

    economics.csv:
    - client_id
    - const_price_idx
    - eurobor_three_months



    """
    input_folder = "files/input"
    output_folder = "files/output"

    if not os.path.exists(output_folder):
        os.makedirs(output_folder)

    # Leer todos los .csv.zip directamente SIN extraerlos
    frames = []
    for fname in os.listdir(input_folder):
        if fname.endswith(".csv.zip"):
            zip_path = os.path.join(input_folder, fname)
            with zipfile.ZipFile(zip_path) as z:
                csv_name = z.namelist()[0]  # único csv dentro
                with z.open(csv_name) as f:
                    df = pd.read_csv(f)
                    frames.append(df)

    # Unificar
    df = pd.concat(frames, ignore_index=True)

    # Construcción de client.csv
    client = df[
        [
            "client_id",
            "age",
            "job",
            "marital",
            "education",
            "credit_default",
            "mortgage",
        ]
    ].copy()

    # la columna ya se llama 'marital' en los datos originales
    # client.rename(columns={"marital_status": "marital"}, inplace=True)

    client["job"] = (
        client["job"].str.replace(".", "", regex=False)
        .str.replace("-", "_", regex=False)
    )

    client["education"] = (
        client["education"]
        .str.replace(".", "_", regex=False)
        .replace("unknown", pd.NA)
    )

    client["credit_default"] = client["credit_default"].apply(
        lambda x: 1 if str(x).lower() == "yes" else 0
    )

    client["mortgage"] = client["mortgage"].apply(
        lambda x: 1 if str(x).lower() == "yes" else 0
    )

    client.to_csv(os.path.join(output_folder, "client.csv"), index=False)

    # Construcción de campaign.csv
    campaign = df[
        [
            "client_id",
            "number_contacts",
            "contact_duration",
            "previous_campaign_contacts",
            "previous_outcome",
            "campaign_outcome",
            "day",
            "month",
        ]
    ].copy()

    campaign["previous_outcome"] = campaign["previous_outcome"].apply(
        lambda x: 1 if str(x).lower() == "success" else 0
    )

    campaign["campaign_outcome"] = campaign["campaign_outcome"].apply(
        lambda x: 1 if str(x).lower() == "yes" else 0
    )

    # Convert month names (e.g., 'jul') to month numbers '07'
    month_map = {
        "jan": "01",
        "feb": "02",
        "mar": "03",
        "apr": "04",
        "may": "05",
        "jun": "06",
        "jul": "07",
        "aug": "08",
        "sep": "09",
        "oct": "10",
        "nov": "11",
        "dec": "12",
    }

    month_num = (
        campaign["month"].astype(str).str.lower().map(month_map)
    )

    campaign["last_contact_date"] = (
        "2022-" + month_num.astype(str).str.zfill(2) + "-" + campaign["day"].astype(str).str.zfill(2)
    )

    # Remover columnas
    campaign = campaign.drop(columns=["day", "month"])

    campaign.to_csv(os.path.join(output_folder, "campaign.csv"), index=False)

    #Construcción de economics.csv
    economics = df[
        ["client_id", "cons_price_idx", "euribor_three_months"]
    ].copy()

    economics.to_csv(os.path.join(output_folder, "economics.csv"), index=False)

    return


if __name__ == "__main__":
    clean_campaign_data()
