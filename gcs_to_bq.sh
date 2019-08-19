#!/bin/sh

TABLE_JSON="_table.json"
SCHEMA_JSON="_schema.json"

AUTO_RETRY_FLG=true


# validate args
if [ $# -ne 2 ]; then
    echo "Less argument. You need input arg1(BigQuery dataset ID), arg2(GCS bucket name)"
    exit 1
fi

# get object list form GCS
object_list=`gsutil ls -r $2/**.csv`

# Load GCS object to bq tables
for gcs_object in $object_list
do
    table_name=`echo $gcs_object | sed -e "s/^.*\/\(.*\).csv/\1/"`
    load_command="bq load --autodetect --source_format=CSV $1.$table_name $gcs_object"
    echo $load_command

    ## fixme: Test code
    if [ $table_name = "d_icd_diagnoses" ]; then  

    # Excecute loadnig and Check result
    # error_count=`echo $load_command | grep -c "Error"`
    error_count=`$load_command | grep -c "Error"`

    # if existing loading error, make table schema manually
    if [ $error_count -gt 0 ]; then
        echo ""
        echo "Loading with schema auto detect ERROR!"
        echo "object:$gcs_object"
        echo "Creating schema setting..."

        # get csv header
        cat_command="gsutil cat -h $gcs_object"
        csv_header=`$cat_command | awk 'NR==1'`

        # make mkdef command
        make_schema_command="bq mkdef --noautodetect --source_format=CSV $gcs_object "

        # add name and type of field to command
        for field in `echo $csv_header | sed 's/,/\'$'\n/g'`
        do
            make_schema_command=`echo $make_schema_command$field:STRING,`
        done
        make_schema_command=`echo $make_schema_command | sed 's/,$//'`

        # create table setting file
        $make_schema_command > ./$table_name$TABLE_JSON

        # extract schema setting from table setting
        tr '\n' '\t' < ./$table_name$TABLE_JSON | sed -e 's/^.*"fields": //' | sed 's/].*/]/' | tr '\t' '\n' > $table_name$SCHEMA_JSON

        # Retrying the execute loading with manually schema setting
        ## fixme: NOT echo, change to execute
        echo $load_command ./$table_name$SCHEMA_JSON | sed 's/--autodetect/--skip_leading_rows=1 /'


    fi

    fi

done




