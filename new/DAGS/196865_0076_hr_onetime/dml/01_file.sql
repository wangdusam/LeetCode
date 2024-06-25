operator=dmlbigqueryoperator
task_id=create_table_ethnicity
write_disposition=WRITE_TRUNCATE
destination_dataset_table={project_bigdata}.{project_dataset_target}
---sqlscript
sql=
BEGIN
select current_date();
end