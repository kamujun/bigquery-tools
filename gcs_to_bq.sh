#!/bin/sh

if [ $# -ne 2 ]; then
    echo "Less argument. You need input arg1(BigQuery dataset ID), arg2(GCS bucket name)"
    exit 1
fi

object_list=`gsutil ls -r $2/**.csv`

for object in $object_list;\
    do file=`echo $object | sed -e "s/^.*\/\(.*\).csv/\1/"`;\
    import_command="bq load --autodetect --source_format=CSV $1.$file $object";\
    echo $import_command;\
    $import_command;\
done;
