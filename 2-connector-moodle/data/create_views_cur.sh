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

dir_views="{{ params.dir_views }}"
prj_id_ld="{{ params.prj_id_ld }}"
dts_nm_ld="{{ params.dts_nm_ld }}"
prj_id_cr="{{ params.prj_id_cr }}"
dts_nm_cr="{{ params.dts_nm_cr }}"

#Exemplo de comando
#./create_views.sh gs://edp-dataflow/config_files/mdl_views_cur.tar.gz edp-dataflow moodle_db_5 edp-dataflow dwh_cur_views

#Verifica se as variaveis nao estao nulas/vazias
if [ -z "$dir_views" ] && [ -z "$prj_id_ld" ] && [ -z "$dts_nm_ld" ] && [ -n "$prj_id_cr" ] && [ -n "$dts_nm_cr" ] 
then
    echo "All variables must be informed..."
    exit_code=1
else
    
    echo "Accessing the temp dir"
	  cd /tmp
	  exit_code=$?
	
    echo ""
    echo "Downloading views file(s)..."
    gsutil cp $dir_views .
    exit_code=$?
	
    echo ""
    echo "Unpacking view files"
    tar -xf mdl_views_cur.tar.gz
    exit_code=$?
	
    echo ""
    echo "Loading the view files..."
    listfiles=$(ls ./mdl_views_cur) 
    exit_code=$?
        
    echo "List of views:"
    echo $listfiles

    if  [[ exit_code -eq 0 ]] 
    then
        
        echo ""
        echo "Check if the dataset exists"
        exists=$(bq ls -d --project_id $prj_id_cr | grep -w $dts_nm_cr)

        if [ -n "$exists" ]; then
            echo "Dataset $dts_nm_cr already exists"
            exit_code=0
        else
            echo "Creating $dts_nm_cr"
            bq mk -d $prj_id_cr:$dts_nm_cr
            exit_code=$?
        fi

        if  [[ exit_code -eq 0 ]]
        then
            for filelist in $listfiles
            do
                nm_arq=$(basename $filelist)
                nm_view=${nm_arq:0:-4}

                fpath=./mdl_views_cur/$filelist
                sql_view=$(<$fpath)

                sql_rplc1="${sql_view//"[prj_id_lnd]"/"$prj_id_ld"}"
                sql_rplc2="${sql_rplc1//"[dts_nm_lnd]"/"$dts_nm_ld"}"
                exit_code=$?
                echo ""
                echo "Creating View: $nm_view"
                if  [[ exit_code -eq 0 ]]
                then
                    bq mk --use_legacy_sql=false --view "$sql_rplc2" --project_id $prj_id_cr $dts_nm_cr.$nm_view
                    exit_code=$?
                else
                    echo "Error getting select of view"
                    exit_code=1
                fi

            done
        fi
    else
        echo "There is no views file to be created"
    fi

    echo ""
    echo "Removing dir mdl_views_cur"
    rm -r ./mdl_views_cur

    echo ""
    echo "Removing file mdl_views_cur.tar.gz"
    rm mdl_views_cur.tar.gz

    if  [[ exit_code -ge 1 ]]
    then
        exit 1
    fi

fi