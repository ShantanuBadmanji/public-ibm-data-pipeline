import data_ingestion
import day2_update_dim_active

if __name__ == "__main__":
    data_ingestion.main(path_to_source_file="ACTIVE.CSV", table_name="ACTIVE")
    data_ingestion.main(path_to_source_file="CLOSED.CSV", table_name="CLOSED")
    day2_update_dim_active.main()
