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

prj_id="{{ params.prj_id }}"
dtst_nm="{{ params.dtst_nm }}"
dir_orig="{{ params.dir_orig }}"
ret_time="{{ params.ret_time }}"

#Exemplo
#./bq_load-v4.sh edp-dataflow moodle_db_4 gs://edp-dataflow/api_files 600


process_files() {

    dir_nm=$1
    arq_nm=$2
    ext_nm=$3
    ret_tm=$4
    dst_nm=$5
    prj_di=$6
    tbl_nm=$7

    dst_tbl="$prj_di:$dst_nm.tbl_$tbl_nm"
    echo "prj:"$dst_tbl

    cmd_spc=" "

    cmd_1="bq load --autodetect "

    if [ $ext_nm == "json" ];
    then
        cmd_2="--source_format=NEWLINE_DELIMITED_JSON"
    elif [ $ext_nm == "csv" ];
    then
        cmd_2="--source_format=CSV"
    else
        echo "Only CSV or JSON files are supported."
        exit
    fi

    if [[ $ret_tm -gt 0 ]]
    then
        cmd_3="--time_partitioning_type=HOUR --time_partitioning_expiration "$ret_tm
    else
        cmd_3=""
    fi

    cmd=$cmd_1$cmd_spc$cmd_2$cmd_spc$cmd_3$cmd_spc$dst_tbl$cmd_spc$dir_nm"load/"$arq_nm

    #echo "# "$cmd" #"

    #cmd_final="sh "$cmd

    #${cmd_final}
    ${cmd}
    exit_code=$?

    if [[ exit_code -eq 0 ]]
    then
        echo "File "$arq_nm" processed..."
        echo "Moving the file to processed folder"
        cmd_gs="gsutil mv "$dir_nm"load/"$arq_nm" "$dir_nm"processed/"$nm_arq
        #echo $cmd_gs
        ${cmd_gs}
        echo "------------------------------------------------"
    else
        echo "File "$arq_nm" with error..."
        echo "Moving the file to error folder"
        cmd_gs="gsutil mv "$dir_nm"load/"$arq_nm" "$dir_nm"error/"$nm_arq
        #echo $cmd_gs
        ${cmd_gs}
        echo "------------------------------------------------"
    fi
}

#Verifica se as variaveis nao estao nulas/vazias
if [ -z "$dtst_nm" ] && [ -z "$dir_orig" ] && [ -z "$ret_time" ]
then
    echo "All variables must be informed..."
    exit_code=1
else
    echo "------------------------------------------------"
    echo "Parameters: "
    echo "Project ID: "$prj_id
    echo "Dataset: "$dtst_nm
    echo "Path API Files: "$dir_orig
    echo "Retention Time: "$ret_time
    echo "------------------------------------------------"

    dirlist=$(gsutil ls $dir_orig)
    exit_code=$?

    #Se tiver arquivo no diretorio executa bq load, senão nao faz nada
    if  [[ exit_code -eq 0 ]]
    then
        #check if dataset exist
        echo "Check if the dataset exists"
        exists=$(bq ls -d --project_id $prj_id | grep -w $dtst_nm)

        if [ -n "$exists" ]; then
            echo "Dataset $dtst_nm already exists"
            exit_code=0
        else
            echo "Creating $dtst_nm"
            bq mk -d $prj_id:$dtst_nm
            exit_code=$?
        fi

        echo "List of directory to be checked: "$dirlist
        echo "------------------------------------------------"
        echo ""
        echo "Starting file search..."

        for dir in $dirlist
        do

            #echo ""
            echo "Checking the directory "$dir
            #echo ""

            tbl_name=$(basename $dir)

            cmd_fl=$dir"load/*.*"
            #echo "diretorio pesquisa: "$cmd_fl

            listfiles=$(gsutil ls $cmd_fl 2> /dev/null)
            exit_code=$?

            #verifica se a variavel nao esta vazia
            if [[ exit_code -eq 0 ]] && [ -n listfiles ]
            then
                echo "------------------------------------------------"
                echo "Found file(s)..."
                echo ""

                for file in $listfiles
                do
                    nm_arq=$(basename $file)
                    nm_ext="${nm_arq##*.}"

                    if [ $nm_ext == "json" ] || [ $nm_ext == "csv" ]
                    then
                        echo "Processing the file: "$file
                        process_files $dir $nm_arq $nm_ext $ret_time $dtst_nm $prj_id $tbl_name
                    fi

                done
            fi

        done

    else
        echo "There are no files to process/load in BQ"
    fi
fi
