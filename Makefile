.PHONY: create_db_schema populate_fake_data

create_db_schema:
	@echo "\nCreating DB Schema ...\n"
	/usr/bin/env python utils/create_schemas.py

populate_fake_data: create_db_schema
	@echo "\nPopulating DB with fake data ...\n"
	/usr/bin/env python utils/seed_tables.py
