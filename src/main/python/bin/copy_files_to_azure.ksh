############################################################
# Developed By:                                            #
# Developed Date:                                          # 
# Script Name:                                             #
# PURPOSE: Copy input vendor files from local to HDFS.     #
############################################################

# Declare a variable to hold the unix script name.
JOBNAME="copy_files_to_azure.ksh"

#Declare a variable to hold the current date
date=$(date '+%Y-%m-%d_%H:%M:%S')
bucket_subdir_name=$(date '+%Y-%m-%d-%H-%M-%S')

#Define a Log File where logs would be generated
LOGFILE="/home/${USER}/projects/PrescPipeline/src/main/python/logs/${JOBNAME}_${date}.log"

###########################################################################
### COMMENTS: From this point on, all standard output and standard error will
###           be logged in the log file.
###########################################################################
{  # <--- Start of the log file.
echo "${JOBNAME} Started...: $(date)"

### Define Local Directories
LOCAL_OUTPUT_PATH="/home/${USER}/projects/PrescPipeline/src/main/python/output"
LOCAL_CITY_DIR=${LOCAL_OUTPUT_PATH}/dimension_city
LOCAL_FACT_DIR=${LOCAL_OUTPUT_PATH}/presc

### Define SAS URLs
citySasUrl="https://prescpipeline.blob.core.windows.net/dimension-city/${bucket_subdir_name}?st=2022-04-19T15:40:08Z&se=2022-04-19T23:40:08Z&si=writeAccess&spr=https&sv=2020-08-04&sr=c&sig=CH2iN%2BsVg8BA2Yg%2F8B7ivKTRmWUI1oap0n5Q5t6XDZ4%3D"
prescSasUrl="https://prescpipeline.blob.core.windows.net/presc/${bucket_subdir_name}?st=2022-04-19T15:41:58Z&se=2022-04-19T23:41:58Z&si=writeAccess&spr=https&sv=2020-08-04&sr=c&sig=FQvbUbwPMlVgs%2FDQ9PjZdEEyZqO0gzlJ%2BPeOM2mv43E%3D"

### Push City  and Fact files to Azure.
azcopy copy "${LOCAL_CITY_DIR}/*.*" "$citySasUrl"
azcopy copy "${LOCAL_FACT_DIR}/*.*" "$prescSasUrl"

echo "The ${JOBNAME} is Completed...: $(date)"

} > ${LOGFILE} 2>&1  # <--- End of program and end of log.
