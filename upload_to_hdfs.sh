#!/bin/bash

# اسم الكونتينر
CONTAINER=nyc-namenode

# مسارات الملفات على جهازك
LOCAL_BASE=~/ITI_DE_Graduation_Project/data
REMOTE_TMP=/tmp
HDFS_PATH=/data/bronze

# رفع الفولدرات لـ الكونتينر
for YEAR in 2022 2023 2024
do
    echo "Copying $YEAR to Docker container..."
    docker cp $LOCAL_BASE/$YEAR $CONTAINER:$REMOTE_TMP/$YEAR
done

# تنفيذ أوامر HDFS داخل الكونتينر
docker exec -i $CONTAINER bash <<EOF

# إنشاء المجلدات في HDFS
for YEAR in 2022 2023 2024
do
    echo "Creating HDFS folder for \$YEAR..."
    hdfs dfs -mkdir -p $HDFS_PATH/\$YEAR
    echo "Uploading CSV files for \$YEAR..."
    hdfs dfs -put $REMOTE_TMP/\$YEAR/*.csv $HDFS_PATH/\$YEAR/
done

# عرض الملفات للتأكيد
hdfs dfs -ls $HDFS_PATH/2022
hdfs dfs -ls $HDFS_PATH/2023
hdfs dfs -ls $HDFS_PATH/2024

EOF

echo "Upload complete."
