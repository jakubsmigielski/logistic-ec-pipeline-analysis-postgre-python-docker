import os
import sys

DESTINATION_FOLDER = 'datas'
DATASET_NAME = 'olistbr/brazilian-ecommerce'
REQUIRED_FILES = [
    'olist_orders_dataset.csv',
    'olist_customers_dataset.csv',
    'olist_order_items_dataset.csv',
    'olist_sellers_dataset.csv',
    'olist_geolocation_dataset.csv'
]


def download_data():
    if not os.path.exists(DESTINATION_FOLDER):
        print(f" Creating folder: {DESTINATION_FOLDER}...")
        os.makedirs(DESTINATION_FOLDER)

    try:
        from kaggle.api.kaggle_api_extended import KaggleApi
        api = KaggleApi()
        api.authenticate()
    except ImportError:
        print("   Error: 'kaggle' library not installed.")
        print("   Run: pip install kaggle")
        return
    except Exception as e:
        print(f" Kaggle API Error: {e}")
        print(" Make sure you have 'kaggle.json' in your ~/.kaggle/ folder.")
        return

    print(f"Connecting to Kaggle: {DATASET_NAME}...")

    for file_name in REQUIRED_FILES:
        target_path = os.path.join(DESTINATION_FOLDER, file_name)

        if os.path.exists(target_path):
            print(f" Exists: {file_name}")
            continue

        print(f" Downloading: {file_name}...")
        try:
            api.dataset_download_file(DATASET_NAME, file_name, path=DESTINATION_FOLDER)

            zip_file = target_path + ".zip"
            if os.path.exists(zip_file):
                import zipfile
                with zipfile.ZipFile(zip_file, 'r') as zip_ref:
                    zip_ref.extractall(DESTINATION_FOLDER)
                os.remove(zip_file)
                print(f"Unzipped: {file_name}")
            else:
                print(f"Downloaded: {file_name}")

        except Exception as e:
            print(f"Failed to download {file_name}: {e}")

    print("\n Data setup complete! You are ready to run the pipeline.")


if __name__ == "__main__":
    download_data()