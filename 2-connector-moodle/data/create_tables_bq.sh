# Copyright 2022 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     https://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

#!/bin/bash


dir_schm="{{ params.dir_schm }}"
ret_time="{{ params.ret_time }}"
nm_dtst="{{ params.nm_dtst }}"
prj_id="{{ params.prj_id }}"

#Exemplo de comando
#./create_tables_bq.sh gs://edp-dataflow/config_files/mdl_schemas.tar.gz 600 moodle_test_cmd_2 edp-dataflow

#Verifica se as variaveis nao estao nulas/vazias
if [ -z "$dir_schm" ] && [ -z "$ret_time" ] && [ -z "$nm_dtst" ] && [ -n "$prj_id" ] 
then
    echo "All variables must be informed..."
    exit 1
else
    echo "------------------------------------------------"
    echo "Parameters:"
    echo "Dir. Schemas..: "$dir_schm    
    echo "Retention Time: "$ret_time
    echo "Output Dataset: "$nm_dtst
    echo "------------------------------------------------"
    
    cmd_spc=" "
	
	  echo "Accessing the temp dir"
	  cd /tmp
	  exit_code=$?
	  echo $(pwd)
	
    echo "Downloading schema file(s)..."
    gsutil cp $dir_schm .
    echo $(ls mdl_schemas.tar.gz)
	  exit_code=$?

    echo "Unpacking schema file(s)"
    tar -xf mdl_schemas.tar.gz
    echo $(ls -l /tmp)
	  exit_code=$?

    echo "Loading the schemas..."
    listfiles=$(ls ./mdl_schemas) 
    exit_code=$?

    echo "List of schemas:"
    echo $listfiles
	  exit_code=$?

    if  [[ exit_code -eq 0 ]]
    then
        
        #check if dataset exist
        echo "Check if the dataset exists"
        exists=$(bq ls -d --project_id $prj_id | grep -w $nm_dtst)

        if [ -n "$exists" ]; then
            echo "Dataset $nm_dtst already exists"
            exit_code=0
        else
            echo "Creating $nm_dtst"
            bq mk -d $prj_id:$nm_dtst
            exit_code=$?
        fi

        cmd_1="bq mk --table --schema="
            
        if [[ $ret_time -gt 0 ]]
        then
            cmd_2="--time_partitioning_type=HOUR --time_partitioning_expiration "$ret_time
        else
            cmd_2=""
        fi       

        if  [[ exit_code -eq 0 ]]
        then
            count=1
            for filelist in $listfiles
            do
                echo $count"/469"
                nm_arq=$(basename $filelist)
                nm_tbl=${nm_arq:7:-5}
                nm_ds_tbl="$prj_id:$nm_dtst.$nm_tbl"
                echo "Creating the table: $nm_tbl"

                cmd=$cmd_1"./mdl_schemas/"$filelist$cmd_spc$cmd_2$cmd_spc$nm_ds_tbl

                ${cmd}
                exit_code=$?

                let count++

                if  [[ exit_code -ge 1 ]]
                then
                    exit 1
                fi

            done
        fi

        echo "Removing dir mdl_schemas"
        rm -r ./mdl_schemas

        echo "Removing file mdl_schemas.tar.gz"
        rm mdl_schemas.tar.gz

    else
        echo "There is no schemas files to be created!"
    fi

fi